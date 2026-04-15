from main import _convert_to_iceberg_query, ConnectionConfig

def test_parquet_to_iceberg_scan():
    config = ConnectionConfig(
        storageType="s3",
        endpoint="s3.amazonaws.com",
        accessKey="key",
        secretKey="secret"
    )
    sql = "SELECT * FROM read_parquet('s3://bucket/path/to/table/*.parquet')"
    converted = _convert_to_iceberg_query(sql, config)
    assert "iceberg_scan('s3://bucket/path/to/table')" in converted

def test_parquet_to_rest_catalog():
    config = ConnectionConfig(
        storageType="s3",
        endpoint="s3.amazonaws.com",
        accessKey="key",
        secretKey="secret",
        catalogType="rest",
        catalogEndpoint="http://localhost:8181",
        namespace="db"
    )
    sql = "SELECT * FROM read_parquet('s3://bucket/path/to/my_table/*.parquet')"
    converted = _convert_to_iceberg_query(sql, config)
    assert "iceberg_catalog.db.my_table" in converted

def test_non_parquet_query_unchanged():
    config = ConnectionConfig(
        storageType="s3",
        endpoint="s3.amazonaws.com",
        accessKey="key",
        secretKey="secret"
    )
    sql = "SELECT * FROM my_table"
    converted = _convert_to_iceberg_query(sql, config)
    assert converted == sql
