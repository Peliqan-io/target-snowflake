#!/usr/bin/env python3
"""
Capture target-snowflake SQL output for regression comparison.

Feeds Singer JSONL through target-snowflake with a mock Snowflake connection
that captures all SQL. No real Snowflake account needed.

Usage:
    python capture.py --output baseline   # on 3.9 (master)
    python capture.py --output current    # on 3.11 (migration branch)
"""
import argparse
import io
import json
import os
import re
import sys
import tempfile
from contextlib import redirect_stdout
from pathlib import Path


_captured_sql = []


# ── Mock Snowflake ───────────────────────────────────────────────────

class FakeCursor:
    """Fake Snowflake DictCursor that records SQL and returns canned results."""
    sfqid = "fake-query-id-000"
    rowcount = 0

    def __init__(self):
        self.description = []

    def execute(self, sql, params=None):
        _captured_sql.append(sql)
        return self

    def fetchall(self):
        return []

    def fetchone(self):
        return None

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        pass


class FakeConnection:
    """Fake Snowflake connection."""

    def cursor(self, cursor_class=None):
        return FakeCursor()

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        pass


class FakeUploadClient:
    """Fake upload client that does nothing."""

    def upload_file(self, *a, **kw):
        pass

    def delete_object(self, *a, **kw):
        pass

    def copy_object(self, *a, **kw):
        pass


def _make_config():
    """Minimal config that passes DbSync.validate_config()."""
    return {
        "account": "test_account",
        "dbname": "test_db",
        "user": "test_user",
        "password": "test_pass",
        "warehouse": "test_wh",
        "default_target_schema": "test_schema",
        "file_format": "csv",
        "batch_size_rows": 100000,
        "batch_wait_limit_seconds": 999999,
        "disable_collection": True,
        "data_flattening_max_level": 10,
    }


# ── Normalizers ──────────────────────────────────────────────────────

def normalize_sql(statements):
    """Normalize SQL for stable comparison."""
    normalized = []
    for sql in statements:
        # Normalize temp file/stage names with timestamps or UUIDs
        sql = re.sub(r'tmp_[0-9a-f]{8}(?:[_-][0-9a-f]{4}){3}[_-][0-9a-f]{12}',
                      'tmp_NORMALIZED', sql, flags=re.IGNORECASE)
        # Normalize timestamp-based file names
        sql = re.sub(r'\d{8}T\d{6}', 'TSTAMP_NORMALIZED', sql)
        # Normalize timestamps in values
        sql = re.sub(r'\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?',
                      'TIMESTAMP_NORMALIZED', sql)
        # Normalize PRIMARY KEY column order: sort the columns inside primary key(...)
        def _sort_pk(m):
            cols = [c.strip() for c in m.group(1).split(',')]
            return f"primary key({', '.join(sorted(cols))})"
        sql = re.sub(r'primary key\(([^)]+)\)', _sort_pk, sql, flags=re.IGNORECASE)
        # Collapse whitespace
        sql = re.sub(r'\s+', ' ', sql).strip()
        normalized.append(sql)
    # Sort consecutive ALTER COLUMN ... DROP NOT NULL statements together
    # (order doesn't matter semantically)
    return _sort_alter_groups(normalized)


def _sort_alter_groups(stmts):
    """Sort consecutive 'ALTER TABLE ... ALTER COLUMN ... DROP NOT NULL' statements."""
    result = []
    group = []
    for s in stmts:
        if re.match(r'alter table .+ alter column .+ drop not null', s, re.IGNORECASE):
            group.append(s)
        else:
            if group:
                result.extend(sorted(group))
                group = []
            result.append(s)
    if group:
        result.extend(sorted(group))
    return result


# ── Main ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Capture target-snowflake regression output")
    parser.add_argument("--output", required=True, choices=["baseline", "current"],
                        help="Output directory name")
    parser.add_argument("--messages", default=None,
                        help="Path to Singer JSONL messages file")
    args = parser.parse_args()

    regression_dir = Path(__file__).parent
    output_dir = regression_dir / args.output
    output_dir.mkdir(parents=True, exist_ok=True)

    messages_path = args.messages or str(regression_dir / "fixtures" / "messages.jsonl")
    if not Path(messages_path).exists():
        print(f"ERROR: Messages file not found: {messages_path}")
        sys.exit(1)

    print(f"Python version: {sys.version}")
    print(f"Output dir:     {output_dir}")
    print(f"Messages:       {messages_path}")

    config = _make_config()

    with open(messages_path, "r") as f:
        messages_text = f.read()

    # Import target with the real snowflake connector installed,
    # then patch only the connection and upload methods
    from target_snowflake import db_sync
    from target_snowflake.upload_clients.snowflake_upload_client import SnowflakeUploadClient
    import snowflake.connector

    # Patch open_connection to return our fake (no real Snowflake account)
    db_sync.DbSync.open_connection = lambda self: FakeConnection()

    # Patch upload client to do nothing
    original_upload_init = SnowflakeUploadClient.__init__
    SnowflakeUploadClient.__init__ = lambda self, *a, **kw: None
    SnowflakeUploadClient.upload_file = lambda self, *a, **kw: None
    SnowflakeUploadClient.delete_object = lambda self, *a, **kw: None

    # Patch put_to_stage on DbSync (accept any args/kwargs)
    db_sync.DbSync.put_to_stage = lambda self, *a, **kw: None

    # Pre-set file_format_type to skip Snowflake detection query
    from target_snowflake.file_format import FileFormatTypes

    # Feed messages through target
    input_stream = io.TextIOWrapper(io.BytesIO(messages_text.encode('utf-8')), encoding='utf-8')
    stdout_capture = io.StringIO()

    try:
        from target_snowflake import persist_lines
        with redirect_stdout(stdout_capture):
            persist_lines(config, input_stream,
                          table_cache=None,
                          file_format_type=FileFormatTypes.CSV)
    except Exception as e:
        print(f"WARNING: target raised: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)

    # Capture results
    stdout_text = stdout_capture.getvalue()
    states = []
    for line in stdout_text.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            states.append(json.loads(line))
        except json.JSONDecodeError:
            pass

    sql_normalized = normalize_sql(_captured_sql)

    # Write outputs
    meta = {
        "python_version": sys.version.split()[0],
        "message_count": len(messages_text.strip().splitlines()),
        "sql_count": len(sql_normalized),
        "state_count": len(states),
    }

    (output_dir / "meta.json").write_text(json.dumps(meta, indent=2))
    (output_dir / "sql_statements.json").write_text(json.dumps(sql_normalized, indent=2))
    (output_dir / "states.json").write_text(json.dumps(states, indent=2))

    print(f"\nCapture complete:")
    print(f"  SQL statements: {len(sql_normalized)}")
    print(f"  STATE messages: {len(states)}")
    print(f"  Files written to: {output_dir}/")


if __name__ == "__main__":
    main()
