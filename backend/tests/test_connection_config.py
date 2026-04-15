"""Tests for ``ConnectionConfig`` Pydantic validators.

These guard the API boundary: any user-supplied config value that would
break SQL (quotes, semicolons, control chars) must be rejected before it
can reach DuckDB.
"""

import pytest
from pydantic import ValidationError

from main import ConnectionConfig


def _base(**overrides):
    defaults = dict(
        storageType="s3",
        endpoint="s3.amazonaws.com",
        accessKey="AKIAFAKE",
        secretKey="secret",
    )
    defaults.update(overrides)
    return defaults


def test_valid_minimal_config():
    cfg = ConnectionConfig(**_base())
    assert cfg.storageType == "s3"
    assert cfg.catalogType == "none"
    assert cfg.region == "us-east-1"


def test_storage_type_rejects_unknown():
    with pytest.raises(ValidationError):
        ConnectionConfig(**_base(storageType="gcs"))


def test_catalog_type_rejects_unknown():
    with pytest.raises(ValidationError):
        ConnectionConfig(**_base(catalogType="hive"))


@pytest.mark.parametrize(
    "bad_path",
    [
        "s3://bucket/warehouse; DROP TABLE foo",
        "s3://bucket/warehouse' OR '1'='1",
        "s3://bucket/warehouse\nmalicious",
        "http://bucket/warehouse",  # wrong scheme
        "bucket/warehouse",  # missing scheme
    ],
)
def test_table_path_rejects_injection(bad_path):
    with pytest.raises(ValidationError):
        ConnectionConfig(**_base(tablePath=bad_path))


def test_table_path_strips_trailing_slash_and_metadata():
    cfg = ConnectionConfig(**_base(tablePath="s3://bucket/warehouse/db/table/metadata"))
    assert cfg.tablePath == "s3://bucket/warehouse/db/table"

    cfg = ConnectionConfig(**_base(tablePath="s3://bucket/warehouse/db/table/"))
    assert cfg.tablePath == "s3://bucket/warehouse/db/table"


@pytest.mark.parametrize(
    "bad_ns",
    [
        "default; DROP",
        "db with space",
        "db'name",
        "123leading_digit",
    ],
)
def test_namespace_rejects_non_identifier(bad_ns):
    with pytest.raises(ValidationError):
        ConnectionConfig(**_base(catalogType="rest", catalogEndpoint="http://cat:8181", namespace=bad_ns))


def test_namespace_accepts_identifier():
    cfg = ConnectionConfig(**_base(
        catalogType="rest",
        catalogEndpoint="http://cat:8181",
        namespace="my_db_1",
    ))
    assert cfg.namespace == "my_db_1"


@pytest.mark.parametrize(
    "bad_region",
    ["eu west 1", "eu-west-1;", "eu-west-1'"],
)
def test_region_rejects_bad_chars(bad_region):
    with pytest.raises(ValidationError):
        ConnectionConfig(**_base(region=bad_region))


def test_credentials_reject_newline_and_null():
    with pytest.raises(ValidationError):
        ConnectionConfig(**_base(accessKey="AKIA\nBAD"))
    with pytest.raises(ValidationError):
        ConnectionConfig(**_base(secretKey="sec\x00ret"))


def test_catalog_endpoint_requires_http_scheme():
    with pytest.raises(ValidationError):
        ConnectionConfig(**_base(
            catalogType="rest",
            catalogEndpoint="ftp://cat:8181",
            namespace="db",
        ))
