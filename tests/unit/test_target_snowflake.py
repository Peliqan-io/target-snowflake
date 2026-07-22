import io
import json
import unittest
import os
import copy
import itertools
import tempfile

from contextlib import redirect_stdout
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

import target_snowflake


def _mock_record_to_csv_line(record):
    return record


class TestTargetSnowflake(unittest.TestCase):

    def setUp(self):
        self.config = {}
        self.maxDiff = None

    @patch('target_snowflake.flush_streams')
    @patch('target_snowflake.DbSync')
    def test_persist_lines_with_40_records_and_batch_size_of_20_expect_flushing_once(self, dbSync_mock,
                                                                                     flush_streams_mock):
        self.config['batch_size_rows'] = 20
        self.config['flush_all_streams'] = True

        with open(f'{os.path.dirname(__file__)}/resources/logical-streams.json', 'r') as f:
            lines = f.readlines()

        instance = dbSync_mock.return_value
        instance.create_schema_if_not_exists.return_value = None
        instance.sync_table.return_value = None

        flush_streams_mock.return_value = '{"currently_syncing": null}'

        target_snowflake.persist_lines(self.config, lines)

        self.assertEqual(1, flush_streams_mock.call_count)

    @patch('target_snowflake.flush_streams')
    @patch('target_snowflake.DbSync')
    def test_persist_lines_with_same_schema_expect_flushing_once(self, dbSync_mock,
                                                                 flush_streams_mock):
        self.config['batch_size_rows'] = 20

        with open(f'{os.path.dirname(__file__)}/resources/same-schemas-multiple-times.json', 'r') as f:
            lines = f.readlines()

        instance = dbSync_mock.return_value
        instance.create_schema_if_not_exists.return_value = None
        instance.sync_table.return_value = None

        flush_streams_mock.return_value = '{"currently_syncing": null}'

        target_snowflake.persist_lines(self.config, lines)

        self.assertEqual(1, flush_streams_mock.call_count)

    @patch('target_snowflake.datetime')
    @patch('target_snowflake.flush_streams')
    @patch('target_snowflake.DbSync')
    def test_persist_40_records_with_batch_wait_limit(self, dbSync_mock, flush_streams_mock, dateTime_mock):

        start_time = datetime(2021, 4, 6, 0, 0, 0)
        increment = 11
        counter = itertools.count()

        # Move time forward by {{increment}} seconds every time utcnow() is called
        dateTime_mock.utcnow.side_effect = lambda: start_time + timedelta(seconds=increment * next(counter))

        self.config['batch_size_rows'] = 100
        self.config['batch_wait_limit_seconds'] = 10
        self.config['flush_all_streams'] = True

        # Expecting 40 records
        with open(f'{os.path.dirname(__file__)}/resources/logical-streams.json', 'r') as f:
            lines = f.readlines()

        instance = dbSync_mock.return_value
        instance.create_schema_if_not_exists.return_value = None
        instance.sync_table.return_value = None

        flush_streams_mock.return_value = '{"currently_syncing": null}'

        target_snowflake.persist_lines(self.config, lines)

        # Expecting flush after every records + 1 at the end
        self.assertEqual(flush_streams_mock.call_count, 41)

    @patch('target_snowflake.DbSync')
    @patch('target_snowflake.os.remove')
    def test_archive_load_files_incremental_replication(self, os_remove_mock, dbSync_mock):
        self.config['tap_id'] = 'test_tap_id'
        self.config['archive_load_files'] = True
        self.config['s3_bucket'] = 'dummy_bucket'

        with open(f'{os.path.dirname(__file__)}/resources/messages-simple-table.json', 'r') as f:
            lines = f.readlines()

        instance = dbSync_mock.return_value
        instance.create_schema_if_not_exists.return_value = None
        instance.sync_table.return_value = None
        instance.put_to_stage.return_value = 'some-s3-folder/some-name_date_batch_hash.csg.gz'

        target_snowflake.persist_lines(self.config, lines)

        copy_to_archive_args = instance.copy_to_archive.call_args[0]
        self.assertEqual(copy_to_archive_args[0], 'some-s3-folder/some-name_date_batch_hash.csg.gz')
        self.assertEqual(copy_to_archive_args[1], 'test_tap_id/test_simple_table/some-name_date_batch_hash.csg.gz')
        self.assertDictEqual(copy_to_archive_args[2], {
            'tap': 'test_tap_id',
            'schema': 'tap_mysql_test',
            'table': 'test_simple_table',
            'archived-by': 'pipelinewise_target_snowflake',
            'incremental-key': 'id',
            'incremental-key-min': '1',
            'incremental-key-max': '5'
        })

    @patch('target_snowflake.DbSync')
    @patch('target_snowflake.os.remove')
    def test_archive_load_files_log_based_replication(self, os_remove_mock, dbSync_mock):
        self.config['tap_id'] = 'test_tap_id'
        self.config['archive_load_files'] = True

        with open(f'{os.path.dirname(__file__)}/resources/logical-streams.json', 'r') as f:
            lines = f.readlines()

        instance = dbSync_mock.return_value
        instance.create_schema_if_not_exists.return_value = None
        instance.sync_table.return_value = None
        instance.put_to_stage.return_value = 'some-s3-folder/some-name_date_batch_hash.csg.gz'

        target_snowflake.persist_lines(self.config, lines)

        copy_to_archive_args = instance.copy_to_archive.call_args[0]
        self.assertEqual(copy_to_archive_args[0], 'some-s3-folder/some-name_date_batch_hash.csg.gz')
        self.assertEqual(copy_to_archive_args[1], 'test_tap_id/logical1_table2/some-name_date_batch_hash.csg.gz')
        self.assertDictEqual(copy_to_archive_args[2], {
            'tap': 'test_tap_id',
            'schema': 'logical1',
            'table': 'logical1_table2',
            'archived-by': 'pipelinewise_target_snowflake'
        })

    @patch('target_snowflake.flush_streams')
    @patch('target_snowflake.DbSync')
    def test_persist_lines_with_only_state_messages(self, dbSync_mock, flush_streams_mock):
        """
        Given only state messages, target should emit the last one
        """

        self.config['batch_size_rows'] = 5

        with open(f'{os.path.dirname(__file__)}/resources/streams_only_state.json', 'r') as f:
            lines = f.readlines()

        instance = dbSync_mock.return_value
        instance.create_schema_if_not_exists.return_value = None
        instance.sync_table.return_value = None

        # catch stdout
        buf = io.StringIO()
        with redirect_stdout(buf):
            target_snowflake.persist_lines(self.config, lines)

        flush_streams_mock.assert_not_called()

        self.assertEqual(
            buf.getvalue().strip(),
            '{"bookmarks": {"tap_mysql_test-test_simple_table": {"replication_key": "id", '
            '"replication_key_value": 100, "version": 1}}}')

    # --- PQ-3547: a failed Snowflake load must raise (not be swallowed) so that
    #     state is never advanced past data that was not actually written. ---

    def _make_db_sync_with_local_file(self):
        """Build a fake db_sync whose formatter produces a real temp file on disk."""
        fd, tmp_path = tempfile.mkstemp(suffix='.csv.gz')
        with os.fdopen(fd, 'w') as file_handle:
            file_handle.write('some,data\n1,2\n')

        db_sync = MagicMock()
        db_sync.file_format.formatter.records_to_file.return_value = tmp_path
        db_sync.data_flattening_max_level = 0
        return db_sync, tmp_path

    def test_flush_records_raises_when_put_to_stage_fails_and_removes_local_file(self):
        """A failing PUT-to-stage must propagate and the local temp file must be cleaned up."""
        db_sync, tmp_path = self._make_db_sync_with_local_file()
        db_sync.put_to_stage.side_effect = RuntimeError('PUT to stage failed')

        try:
            with self.assertRaises(RuntimeError):
                target_snowflake.flush_records('tap_test-some_table', [{'id': 1}], db_sync)

            # The load never completed, so COPY (load_file) must not have run ...
            db_sync.load_file.assert_not_called()
            # ... and the local temp file must not leak.
            self.assertFalse(
                os.path.exists(tmp_path),
                'local temp file should be removed even when the load fails')
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    def test_flush_records_raises_when_load_file_fails_and_removes_local_file(self):
        """A failing COPY (load_file) must propagate and the local temp file must be cleaned up."""
        db_sync, tmp_path = self._make_db_sync_with_local_file()
        db_sync.put_to_stage.return_value = 'some-s3-folder/some-name.csv.gz'
        db_sync.load_file.side_effect = RuntimeError('Snowflake COPY failed')

        try:
            with self.assertRaises(RuntimeError):
                target_snowflake.flush_records('tap_test-some_table', [{'id': 1}], db_sync)

            self.assertFalse(
                os.path.exists(tmp_path),
                'local temp file should be removed even when the load fails')
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    @patch('target_snowflake.os.remove')
    @patch('target_snowflake.DbSync')
    def test_load_failure_prevents_state_emission(self, dbSync_mock, os_remove_mock):
        """
        End-to-end: if the Snowflake load fails during a flush, persist_lines must raise
        BEFORE any state is emitted, so the bookmark never advances past unwritten data.
        """
        with open(f'{os.path.dirname(__file__)}/resources/messages-simple-table.json', 'r') as f:
            lines = f.readlines()

        instance = dbSync_mock.return_value
        instance.create_schema_if_not_exists.return_value = None
        instance.sync_table.return_value = None
        instance.put_to_stage.side_effect = RuntimeError('Snowflake COPY failed')

        buf = io.StringIO()
        with redirect_stdout(buf):
            with self.assertRaises(RuntimeError):
                target_snowflake.persist_lines(self.config, lines)

        # emit_state writes to stdout; because the load failed, nothing must have been emitted.
        self.assertEqual(buf.getvalue().strip(), '')

    @patch('target_snowflake.emit_state')
    @patch('target_snowflake.flush_streams')
    @patch('target_snowflake.DbSync')
    def test_sync_table_failure_prevents_state_emission(self, dbSync_mock, flush_streams_mock, emit_state_mock):
        """
        Schema-evolution (sync_table) failure must propagate out of persist_lines and
        prevent any state from being emitted, so the bookmark never advances past a
        table whose schema was not actually synced.
        """
        with open(f'{os.path.dirname(__file__)}/resources/messages-simple-table.json', 'r') as f:
            lines = f.readlines()

        instance = dbSync_mock.return_value
        instance.create_schema_if_not_exists.return_value = None
        instance.sync_table.side_effect = RuntimeError('ALTER TABLE add column failed')

        flush_streams_mock.return_value = '{"currently_syncing": null}'

        with self.assertRaises(RuntimeError):
            target_snowflake.persist_lines(self.config, lines)

        # Because sync_table failed, persist_lines must never reach emit_state.
        emit_state_mock.assert_not_called()

    # --- PQ-3547: a completed (small) table must be checkpointed at the next
    #     table's SCHEMA boundary, so a kill during the next table can resume
    #     from the completed one instead of re-syncing it from EOF. ---

    @patch('target_snowflake.emit_state')
    @patch('target_snowflake.flush_streams')
    @patch('target_snowflake.DbSync')
    def test_completed_table_flushed_and_bookmarked_at_next_schema_boundary(
            self, dbSync_mock, flush_streams_mock, emit_state_mock):
        """
        Two small streams (each below batch_size). Feed SCHEMA A, a few RECORDs
        for A, STATE A, then SCHEMA B. Table A must be flushed and its bookmark
        emitted AT the SCHEMA-B boundary - not deferred to EOF.

        Also asserts batching WITHIN a table is preserved: multiple records
        below batch_size do NOT trigger a per-record flush.
        """
        stream_a = 'tap_test-table_a'
        stream_b = 'tap_test-table_b'

        schema = {'properties': {'id': {'type': ['integer']}}, 'type': 'object'}

        def _schema(stream):
            return json.dumps({'type': 'SCHEMA', 'stream': stream,
                               'schema': schema, 'key_properties': ['id']})

        def _record(stream, rid):
            return json.dumps({'type': 'RECORD', 'stream': stream,
                               'record': {'id': rid}, 'version': 1})

        def _state(stream, value):
            return json.dumps({'type': 'STATE', 'value': {'bookmarks': {
                stream: {'replication_key': 'id', 'replication_key_value': value}}}})

        # Table A completes (3 records, well below batch_size) then table B's
        # SCHEMA arrives -> A must be checkpointed at that boundary.
        lines = [
            _schema(stream_a),
            _record(stream_a, 1),
            _record(stream_a, 2),
            _record(stream_a, 3),
            _state(stream_a, 3),
            _schema(stream_b),
            _record(stream_b, 4),
            _record(stream_b, 5),
            _state(stream_b, 5),
        ]

        instance = dbSync_mock.return_value
        instance.create_schema_if_not_exists.return_value = None
        instance.sync_table.return_value = None
        # Distinct primary keys per record so records genuinely accumulate
        # (otherwise a single MagicMock PK would collapse them into one).
        instance.record_primary_key_string.side_effect = lambda record: str(record['id'])

        # Mimic the real (no-filter) flush_streams: reset row counts and return
        # a deepcopy of the current state, so we can observe which bookmark gets
        # emitted at each boundary - without touching Snowflake.
        def _fake_flush(streams, row_count, stream_to_sync, config, state,
                        flushed_state, archive_load_files_data, filter_streams=None):
            for buffered_stream in list(row_count.keys()):
                row_count[buffered_stream] = 0
            return copy.deepcopy(state)

        flush_streams_mock.side_effect = _fake_flush

        target_snowflake.persist_lines(self.config, lines)

        # Exactly two flushes: table A at the SCHEMA-B boundary, table B at EOF.
        # Pre-fix this is 1 (only the EOF flush), so this assertion goes red.
        self.assertEqual(flush_streams_mock.call_count, 2,
                         'expected a flush at the SCHEMA-B boundary (table A) and one at EOF (table B)')
        self.assertEqual(emit_state_mock.call_count, 2,
                         'expected a bookmark emitted at the SCHEMA-B boundary and at EOF')

        # The FIRST emitted bookmark (at the B boundary) must be table A's,
        # and must NOT yet contain table B - proving A is checkpointed before B.
        first_emitted_state = emit_state_mock.call_args_list[0].args[0]
        self.assertIn(stream_a, first_emitted_state['bookmarks'])
        self.assertEqual(
            first_emitted_state['bookmarks'][stream_a]['replication_key_value'], 3)
        self.assertNotIn(stream_b, first_emitted_state['bookmarks'])
