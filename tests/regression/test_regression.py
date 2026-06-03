"""
target-snowflake regression tests — Python 3.9 → 3.11 migration.

Compares SQL output between baseline (3.9) and current (3.11).
No real Snowflake account needed.

Run:
    python -m pytest test_regression.py -v
"""
import json
import sys
from pathlib import Path

import pytest

REGRESSION_DIR = Path(__file__).parent
BASELINE_DIR = REGRESSION_DIR / "baseline"
CURRENT_DIR = REGRESSION_DIR / "current"


def load(directory, filename):
    path = directory / filename
    if not path.exists():
        pytest.skip(f"File not found: {path}. Run capture.py first.")
    return json.loads(path.read_text())


class TestMeta:
    def test_python_version_is_311(self):
        meta = load(CURRENT_DIR, "meta.json")
        parts = meta["python_version"].split(".")
        assert (int(parts[0]), int(parts[1])) >= (3, 11)

    def test_baseline_is_39(self):
        meta = load(BASELINE_DIR, "meta.json")
        assert meta["python_version"].startswith("3.9")

    def test_same_message_count(self):
        b = load(BASELINE_DIR, "meta.json")
        c = load(CURRENT_DIR, "meta.json")
        assert b["message_count"] == c["message_count"]


class TestSQL:
    def test_same_sql_count(self):
        b = load(BASELINE_DIR, "sql_statements.json")
        c = load(CURRENT_DIR, "sql_statements.json")
        assert len(b) == len(c), f"SQL count: baseline={len(b)}, current={len(c)}"

    def test_sql_statements_match(self):
        b = load(BASELINE_DIR, "sql_statements.json")
        c = load(CURRENT_DIR, "sql_statements.json")
        mismatches = []
        for i, (bq, cq) in enumerate(zip(b, c)):
            if bq != cq:
                mismatches.append((i, bq[:200], cq[:200]))
        if mismatches:
            msg = f"{len(mismatches)} SQL mismatch(es):\n"
            for idx, bs, cs in mismatches[:5]:
                msg += f"\n  [{idx}]\n    baseline: {bs}\n    current:  {cs}\n"
            if len(mismatches) > 5:
                msg += f"\n  ... and {len(mismatches) - 5} more"
            pytest.fail(msg)

    def test_sql_kinds_sequence(self):
        """DDL vs DML sequence should match."""
        b = load(BASELINE_DIR, "sql_statements.json")
        c = load(CURRENT_DIR, "sql_statements.json")

        def classify(sql):
            s = sql.upper().strip()
            for kw in ["CREATE ", "ALTER ", "SHOW ", "DROP ", "START ", "MERGE ", "COPY ", "DELETE ", "INSERT "]:
                if s.startswith(kw):
                    return kw.strip()
            return "OTHER"

        b_kinds = [classify(s) for s in b]
        c_kinds = [classify(s) for s in c]
        assert b_kinds == c_kinds, "SQL operation sequence differs"

    def test_create_statements_match(self):
        b = load(BASELINE_DIR, "sql_statements.json")
        c = load(CURRENT_DIR, "sql_statements.json")
        b_creates = [s for s in b if s.upper().strip().startswith("CREATE ")]
        c_creates = [s for s in c if s.upper().strip().startswith("CREATE ")]
        assert b_creates == c_creates, "CREATE statements differ"

    def test_show_statements_match(self):
        b = load(BASELINE_DIR, "sql_statements.json")
        c = load(CURRENT_DIR, "sql_statements.json")
        b_shows = [s for s in b if s.upper().strip().startswith("SHOW ")]
        c_shows = [s for s in c if s.upper().strip().startswith("SHOW ")]
        assert b_shows == c_shows, "SHOW statements differ"


class TestState:
    def test_same_state_count(self):
        b = load(BASELINE_DIR, "states.json")
        c = load(CURRENT_DIR, "states.json")
        assert len(b) == len(c), f"State count: baseline={len(b)}, current={len(c)}"

    def test_state_values_match(self):
        b = load(BASELINE_DIR, "states.json")
        c = load(CURRENT_DIR, "states.json")
        assert b == c, "STATE output differs between 3.9 and 3.11"
