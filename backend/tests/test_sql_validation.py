import pytest
from main import _validate_and_limit_sql
from fastapi import HTTPException

def test_select_allowed():
    sql = "SELECT * FROM my_table"
    validated = _validate_and_limit_sql(sql, 100)
    assert "LIMIT 100" in validated

def test_limit_not_overwritten():
    sql = "SELECT * FROM my_table LIMIT 10"
    validated = _validate_and_limit_sql(sql, 100)
    assert "LIMIT 10" in validated
    assert "LIMIT 100" not in validated

def test_forbidden_statements():
    forbidden = [
        "INSERT INTO table VALUES (1)",
        "DROP TABLE my_table",
        "DELETE FROM my_table WHERE id = 1",
        "UPDATE my_table SET val = 1",
        "CREATE TABLE oops (id INT)",
        "ATTACH 'db.db' AS db",
    ]
    for sql in forbidden:
        with pytest.raises(HTTPException) as exc:
            _validate_and_limit_sql(sql, 100)
        assert exc.value.status_code == 400

def test_multi_statement_rejected():
    sql = "SELECT 1; SELECT 2;"
    with pytest.raises(HTTPException) as exc:
        _validate_and_limit_sql(sql, 100)
    assert exc.value.status_code == 400

def test_with_clause_allowed():
    sql = "WITH cte AS (SELECT 1) SELECT * FROM cte"
    validated = _validate_and_limit_sql(sql, 100)
    assert "LIMIT 100" in validated

def test_union_allowed():
    sql = "SELECT 1 UNION SELECT 2"
    validated = _validate_and_limit_sql(sql, 100)
    assert "LIMIT 100" in validated
