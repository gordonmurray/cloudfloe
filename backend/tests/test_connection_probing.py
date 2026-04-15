"""Tests for ``run_connection_test`` — the diagnostic connection probe.

The probe opens a single DuckDB session, applies the caller's config, and
returns structured Iceberg metadata (format version, snapshot id, row/file
counts, delete flag) so the UI can show users "yes, this is your table"
instead of a bare tick.
"""

import json
from unittest.mock import MagicMock, patch

from main import ConnectionConfig, TableInfo, run_connection_test


def _config(**overrides):
    defaults = dict(
        storageType="s3",
        endpoint="s3.amazonaws.com",
        accessKey="key",
        secretKey="secret",
        tablePath="s3://bucket/table",
    )
    defaults.update(overrides)
    return ConnectionConfig(**defaults)


def _fake_metadata_json():
    return json.dumps(
        {
            "format-version": 2,
            "current-snapshot-id": 6450139374914496971,
            "last-updated-ms": 1776279766193,
        }
    )


def test_probe_returns_none_on_failure():
    with patch("main._duckdb_connection") as mock_ctx:
        mock_conn = MagicMock()
        mock_ctx.return_value.__enter__.return_value = mock_conn
        mock_conn.execute.side_effect = RuntimeError("no such bucket")

        assert run_connection_test(_config(tablePath=None)) is None


def test_probe_reads_latest_metadata_json():
    """The metadata JSON glob — not version-hint.text — is the right source
    of format/snapshot info because pyiceberg never writes version-hint.text."""

    with patch("main._duckdb_connection") as mock_ctx:
        mock_conn = MagicMock()
        mock_ctx.return_value.__enter__.return_value = mock_conn

        def execute_side_effect(sql, params=None):
            cursor = MagicMock()
            if "read_text" in sql:
                cursor.fetchone.return_value = (_fake_metadata_json(),)
            else:  # iceberg_metadata aggregate
                cursor.fetchone.return_value = (37537, 1, False)
            return cursor

        mock_conn.execute.side_effect = execute_side_effect

        result = run_connection_test(_config())

    assert isinstance(result, TableInfo)

    # Must probe the metadata JSON with a glob + ORDER BY filename DESC.
    metadata_call = next(
        call
        for call in mock_conn.execute.call_args_list
        if "read_text" in call.args[0]
    )
    assert "*.metadata.json" in metadata_call.args[1][0]
    assert "ORDER BY filename DESC" in metadata_call.args[0]

    # Must also run iceberg_metadata to get row/file/delete aggregates.
    assert any(
        "iceberg_metadata" in call.args[0]
        for call in mock_conn.execute.call_args_list
    )


def test_probe_populates_table_info_fields():
    with patch("main._duckdb_connection") as mock_ctx:
        mock_conn = MagicMock()
        mock_ctx.return_value.__enter__.return_value = mock_conn

        def execute_side_effect(sql, params=None):
            cursor = MagicMock()
            if "read_text" in sql:
                cursor.fetchone.return_value = (_fake_metadata_json(),)
            else:
                cursor.fetchone.return_value = (37537, 1, False)
            return cursor

        mock_conn.execute.side_effect = execute_side_effect

        result = run_connection_test(_config())

    assert result.format == "iceberg-v2"
    # Stringified to avoid JS 64-bit precision loss on the wire.
    assert result.snapshotId == "6450139374914496971"
    assert result.lastSnapshotAt == "2026-04-15T19:02:46.193000Z"
    assert result.rows == 37537
    assert result.files == 1
    assert result.hasDeletes is False
    assert result.suggestedQuery.startswith("SELECT * FROM iceberg_scan(")


def test_probe_tolerates_missing_metadata_json():
    """If the metadata JSON can't be read (private bucket, weird path), the
    probe must still return a TableInfo with whatever iceberg_metadata gave."""

    with patch("main._duckdb_connection") as mock_ctx:
        mock_conn = MagicMock()
        mock_ctx.return_value.__enter__.return_value = mock_conn

        def execute_side_effect(sql, params=None):
            cursor = MagicMock()
            if "read_text" in sql:
                raise RuntimeError("403 Access Denied")
            cursor.fetchone.return_value = (100, 2, False)
            return cursor

        mock_conn.execute.side_effect = execute_side_effect

        result = run_connection_test(_config())

    assert isinstance(result, TableInfo)
    assert result.format is None
    assert result.snapshotId is None
    assert result.rows == 100
    assert result.files == 2


def test_probe_flags_tables_with_deletes():
    with patch("main._duckdb_connection") as mock_ctx:
        mock_conn = MagicMock()
        mock_ctx.return_value.__enter__.return_value = mock_conn

        def execute_side_effect(sql, params=None):
            cursor = MagicMock()
            if "read_text" in sql:
                cursor.fetchone.return_value = (_fake_metadata_json(),)
            else:
                cursor.fetchone.return_value = (100, 3, True)
            return cursor

        mock_conn.execute.side_effect = execute_side_effect

        result = run_connection_test(_config())

    assert result.hasDeletes is True
