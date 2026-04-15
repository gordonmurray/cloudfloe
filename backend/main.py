"""
Cloudfloe FastAPI Backend
DuckDB-as-a-service for Iceberg data lakes
"""

import duckdb
import json
import logging
import os
import re
import time
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime, timezone
from typing import Any, List, Literal, Optional

import sqlglot
from sqlglot import exp

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, field_validator
import uvicorn

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# --- SQL validation ---------------------------------------------------------
# The submitted SQL is parsed with sqlglot (DuckDB dialect) and rejected
# unless it is a single read-only query. This replaces the previous
# substring-based keyword check, which both over-blocked legitimate queries
# (e.g. `SELECT * FROM update_log`) and could be bypassed with comments or
# unusual whitespace.

_ALLOWED_TOP_LEVEL = (
    exp.Select,
    exp.With,
    exp.Union,
    exp.Intersect,
    exp.Except,
    exp.Values,
)

_FORBIDDEN_NODES = (
    exp.Insert,
    exp.Update,
    exp.Delete,
    exp.Create,
    exp.Drop,
    exp.Alter,
    exp.TruncateTable,
    exp.Commit,
    exp.Rollback,
    exp.Transaction,
    exp.Use,
    exp.Attach,
    exp.Detach,
    exp.Merge,
    exp.Copy,
    exp.Command,
)


def _validate_and_limit_sql(sql: str, row_limit: int) -> str:
    """Parse ``sql`` and return a normalised, read-only query with an outer
    ``LIMIT`` applied if one is not already present.

    Raises ``HTTPException(400)`` on empty input, parse failures,
    multi-statement input, or any non-SELECT top-level or nested
    side-effecting operation.
    """
    if not sql or not sql.strip():
        raise HTTPException(status_code=400, detail="Empty query")

    try:
        parsed = sqlglot.parse(sql, dialect="duckdb")
    except sqlglot.errors.ParseError as e:
        raise HTTPException(status_code=400, detail=f"Invalid SQL: {e}") from None

    statements = [stmt for stmt in parsed if stmt is not None]
    if len(statements) != 1:
        raise HTTPException(
            status_code=400,
            detail="Only a single SQL statement is allowed",
        )
    stmt = statements[0]

    if not isinstance(stmt, _ALLOWED_TOP_LEVEL):
        raise HTTPException(
            status_code=400,
            detail=f"Only SELECT queries are allowed (got {type(stmt).__name__.upper()})",
        )

    for node in stmt.walk():
        if isinstance(node, _FORBIDDEN_NODES):
            raise HTTPException(
                status_code=400,
                detail=f"{type(node).__name__.upper()} statements are not allowed",
            )

    # Inject an outer LIMIT if the user didn't already supply one. Operates on
    # a copy so we don't mutate the parsed AST (avoids side effects if callers
    # keep a reference).
    stmt = stmt.copy()
    target = stmt.this if isinstance(stmt, exp.With) else stmt
    if isinstance(target, (exp.Select, exp.Union, exp.Intersect, exp.Except)):
        if not target.args.get("limit"):
            target.set("limit", exp.Limit(expression=exp.Literal.number(row_limit)))

    return stmt.sql(dialect="duckdb")


# --- Input validation -------------------------------------------------------
# Patterns tight enough to reject SQL-breaking characters (quotes, semicolons,
# whitespace, control chars) without blocking legitimate values. Applied at
# the API boundary by Pydantic validators so they run before any config value
# reaches DuckDB.

_ENDPOINT_RE = re.compile(r"^[A-Za-z0-9\-._:/@+%]+$")
_REGION_RE = re.compile(r"^[A-Za-z0-9\-]+$")
_SESSION_TOKEN_RE = re.compile(r"^[A-Za-z0-9+/=\-_.]+$")
_URL_RE = re.compile(r"^https?://[A-Za-z0-9\-._:/@]+$")
_SQL_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_S3_PATH_RE = re.compile(r"^s3://[A-Za-z0-9\-._/]+$")


def _require_match(value: str, pattern: "re.Pattern[str]", field: str) -> str:
    if not pattern.fullmatch(value):
        raise ValueError(f"{field} contains invalid characters")
    return value


def _sql_string_literal(value: str) -> str:
    """Quote a pre-validated string as a SQL string literal.

    Used only for statements where DuckDB's ``?`` parameter binding does not
    apply (``CREATE SECRET``, ``ATTACH``). The caller must have validated
    ``value`` with one of the regexes above; this helper doubles any embedded
    single-quote and rejects control characters as a last line of defence.
    """
    if "\x00" in value or any(ord(c) < 0x20 and c != "\t" for c in value):
        raise ValueError("Value contains control characters")
    return "'" + value.replace("'", "''") + "'"


# Request/Response Models
class ConnectionConfig(BaseModel):
    storageType: Literal["s3", "r2", "minio"] = Field(
        ..., description="Storage type: s3, r2, minio"
    )
    endpoint: str = Field(..., description="S3 endpoint or bucket path")
    accessKey: str = Field(..., description="Access key ID")
    secretKey: str = Field(..., description="Secret access key")
    sessionToken: Optional[str] = Field(None, description="Session token for STS")
    region: str = Field(default="us-east-1", description="Region")

    # Iceberg-specific fields
    catalogType: Literal["none", "rest", "glue"] = Field(
        default="none", description="Catalog type: none, rest, glue"
    )
    catalogEndpoint: Optional[str] = Field(None, description="REST catalog endpoint URL")
    namespace: Optional[str] = Field(default="default", description="Iceberg namespace/database")
    tablePath: Optional[str] = Field(None, description="Direct path to Iceberg table root")

    @field_validator("endpoint")
    @classmethod
    def _validate_endpoint(cls, v: str) -> str:
        if v == "":
            return v
        return _require_match(v, _ENDPOINT_RE, "endpoint")

    @field_validator("region")
    @classmethod
    def _validate_region(cls, v: str) -> str:
        return _require_match(v, _REGION_RE, "region")

    @field_validator("accessKey", "secretKey")
    @classmethod
    def _validate_key(cls, v: str, info) -> str:
        # AWS/MinIO credentials can contain characters that would need
        # escaping, so we rely on parameter binding or _sql_string_literal for
        # safe interpolation. This guards only against obvious smuggling.
        if "\x00" in v or "\n" in v or "\r" in v:
            raise ValueError(f"{info.field_name} contains invalid characters")
        return v

    @field_validator("sessionToken")
    @classmethod
    def _validate_session_token(cls, v: Optional[str]) -> Optional[str]:
        if not v:
            return v
        return _require_match(v, _SESSION_TOKEN_RE, "sessionToken")

    @field_validator("catalogEndpoint")
    @classmethod
    def _validate_catalog_endpoint(cls, v: Optional[str]) -> Optional[str]:
        if not v:
            return v
        return _require_match(v, _URL_RE, "catalogEndpoint")

    @field_validator("namespace")
    @classmethod
    def _validate_namespace(cls, v: Optional[str]) -> Optional[str]:
        if not v:
            return v
        return _require_match(v, _SQL_IDENT_RE, "namespace")

    @field_validator("tablePath")
    @classmethod
    def _validate_table_path(cls, v: Optional[str]) -> Optional[str]:
        if not v:
            return v
        # Normalise first so downstream code can rely on a canonical value.
        v = v.rstrip("/")
        if v.endswith("/metadata"):
            v = v[: -len("/metadata")]
        return _require_match(v, _S3_PATH_RE, "tablePath")


class TestConnectionRequest(BaseModel):
    connection: ConnectionConfig


class QueryRequest(BaseModel):
    sql: str = Field(..., description="SQL query to execute")
    connection: ConnectionConfig
    rowLimit: int = Field(default=1000, le=10000, description="Maximum rows to return")


class QueryStats(BaseModel):
    executionTimeMs: int
    bytesScanned: int
    rowsReturned: int


class QueryResponse(BaseModel):
    columns: List[str]
    rows: List[List[Any]]
    stats: QueryStats
    truncated: bool = False


class TableInfo(BaseModel):
    """Structured metadata returned by a successful connection probe.

    All Iceberg-specific fields are optional so the same model can describe
    both the rich direct-table-path case and the thinner REST-catalog /
    bundled-demo cases.
    """

    path: str
    suggestedQuery: str
    format: Optional[str] = None
    rows: Optional[int] = None
    files: Optional[int] = None
    hasDeletes: Optional[bool] = None
    snapshotId: Optional[str] = None
    lastSnapshotAt: Optional[str] = None


# --- DuckDB session management ---------------------------------------------
# A fresh in-memory connection is opened per request, configured with the
# caller's S3/Iceberg settings, and closed on exit. The previous design kept
# a single module-level connection that was torn down and rebuilt on every
# query, which was not concurrency-safe: one request's _apply_s3_config call
# could swap the connection out from under another request mid-query.
#
# Extension binaries are cached on disk by DuckDB after the first download,
# so repeated INSTALL/LOAD is a cheap no-op (ms-level) for subsequent
# connections.


def _apply_s3_config(conn: duckdb.DuckDBPyConnection, config: ConnectionConfig) -> None:
    """Apply storage-specific S3 settings to ``conn``.

    All user-supplied values are sent via DuckDB's ``?`` parameter binding
    (the Pydantic validators on :class:`ConnectionConfig` are a second line
    of defence, not the only one).
    """
    logger.info(f"Applying S3 config: {config.storageType}, endpoint: {config.endpoint}")

    if config.storageType == "minio":
        endpoint = config.endpoint
        if "localhost" in endpoint:
            # Replace localhost with container name for internal access
            endpoint = endpoint.replace("localhost", "minio")
        endpoint = endpoint.replace("http://", "").replace("https://", "")
        logger.info(f"Final MinIO endpoint: {endpoint}")
        conn.execute("SET s3_endpoint=?", [endpoint])
        conn.execute("SET s3_url_style='path'")
        conn.execute("SET s3_use_ssl=false")
        # MinIO requires AWS signature v4
        conn.execute("SET s3_region='us-east-1'")
    elif config.storageType == "r2":
        endpoint = config.endpoint.replace("https://", "")
        conn.execute("SET s3_endpoint=?", [endpoint])
        conn.execute("SET s3_url_style='path'")
        conn.execute("SET s3_use_ssl=true")
    else:
        logger.info(f"Setting S3 region: {config.region}")
        conn.execute("SET s3_region=?", [config.region])
        conn.execute("SET s3_use_ssl=true")

    logger.info(
        f"Setting S3 credentials - Access Key starts with: {config.accessKey[:8] if config.accessKey else 'EMPTY'}..."
    )
    conn.execute("SET s3_access_key_id=?", [config.accessKey])
    conn.execute("SET s3_secret_access_key=?", [config.secretKey])

    if config.sessionToken:
        conn.execute("SET s3_session_token=?", [config.sessionToken])

    logger.info(f"Applied {config.storageType} configuration")


def _attach_iceberg_catalog(conn: duckdb.DuckDBPyConnection, config: ConnectionConfig) -> None:
    """Attach an Iceberg REST catalog on ``conn`` if the config requests one."""
    if config.catalogType != "rest":
        return

    if not config.catalogEndpoint:
        raise HTTPException(
            status_code=400,
            detail="catalogEndpoint required for REST catalog",
        )
    if not config.namespace:
        raise HTTPException(
            status_code=400,
            detail="namespace required for REST catalog",
        )

    logger.info(f"Attaching Iceberg REST catalog: {config.catalogEndpoint}")

    # CREATE SECRET and ATTACH do not support prepared-statement placeholders
    # for their option values, so we interpolate after (a) Pydantic-level
    # regex validation on namespace/catalogEndpoint and (b) escaping through
    # _sql_string_literal.
    token_literal = _sql_string_literal(f"{config.accessKey}:{config.secretKey}")
    namespace_literal = _sql_string_literal(config.namespace)
    endpoint_literal = _sql_string_literal(config.catalogEndpoint)

    conn.execute(f"""
        CREATE SECRET iceberg_catalog_secret (
            TYPE iceberg,
            TOKEN {token_literal}
        )
    """)

    conn.execute(f"""
        ATTACH {namespace_literal} AS iceberg_catalog (
            TYPE iceberg,
            SECRET iceberg_catalog_secret,
            ENDPOINT {endpoint_literal}
        )
    """)

    logger.info("Iceberg catalog attached")


@contextmanager
def _duckdb_connection(config: ConnectionConfig):
    """Yield a fresh DuckDB in-memory connection configured for ``config``.

    One connection per caller means concurrent requests can't corrupt each
    other's session state. The connection is always closed on exit.
    """
    conn = duckdb.connect(":memory:")
    try:
        conn.execute("INSTALL httpfs")
        conn.execute("LOAD httpfs")
        conn.execute("INSTALL iceberg")
        conn.execute("LOAD iceberg")
        # DuckDB 1.4+ requires explicit version handling for Iceberg
        conn.execute("SET unsafe_enable_version_guessing=true")
        conn.execute("SET memory_limit='2GB'")
        conn.execute("SET threads=4")
        try:
            _apply_s3_config(conn, config)
            _attach_iceberg_catalog(conn, config)
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Failed to apply S3 config: {e}")
            raise HTTPException(status_code=400, detail=f"Invalid S3 configuration: {e}")
        yield conn
    finally:
        conn.close()


def _validate_iceberg_table(conn: duckdb.DuckDBPyConnection, table_path: str) -> dict:
    """Validate Iceberg table compatibility (v1/v2 only, no deletes)."""
    try:
        metadata = conn.execute(
            "SELECT * FROM iceberg_metadata(?)", [table_path]
        ).fetchdf()

        has_deletes = any(
            'DELETE' in str(v).upper() for v in metadata['manifest_content'].unique()
        )

        if has_deletes:
            raise HTTPException(
                status_code=400,
                detail=(
                    "Table contains row-level deletes which are not supported. "
                    "This application only supports append-only Iceberg v1/v2 tables. "
                    "Reading this table may return incorrect data."
                ),
            )

        logger.info(f"Table validation passed: {table_path}")
        return {"valid": True, "warnings": []}

    except HTTPException:
        raise
    except Exception as e:
        logger.warning(f"Could not validate table (proceeding with caution): {e}")
        return {"valid": True, "warnings": [f"Validation incomplete: {e}"]}


def _convert_to_iceberg_query(sql: str, config: ConnectionConfig) -> str:
    """Convert ``read_parquet('s3://...**/*.parquet')`` calls to
    ``iceberg_scan()`` (or a catalog table reference when a REST catalog is
    configured)."""
    parquet_pattern = r"read_parquet\(['\"]s3://([^/]+)/([^'\"]+?)/?\*?\*?/?\*?\.parquet['\"]\)"

    def replace_with_iceberg(match):
        bucket = match.group(1)
        path = match.group(2).rstrip('/*')

        if config.catalogType == "rest":
            table_name = path.split('/')[-1]
            return f"iceberg_catalog.{config.namespace}.{table_name}"
        iceberg_path = f"s3://{bucket}/{path}"
        return f"iceberg_scan('{iceberg_path}')"

    converted_sql = re.sub(parquet_pattern, replace_with_iceberg, sql, flags=re.IGNORECASE)

    if converted_sql != sql:
        logger.info("Converted query from read_parquet to Iceberg:")
        logger.info(f"  Original: {sql[:100]}...")
        logger.info(f"  Converted: {converted_sql[:100]}...")

    return converted_sql


def _probe_iceberg_table(
    conn: duckdb.DuckDBPyConnection, table_path: str
) -> TableInfo:
    """Gather structured metadata about an Iceberg table.

    Reads the latest ``*.metadata.json`` for format version + current
    snapshot, then queries ``iceberg_metadata()`` for manifest-level row
    and file counts. A single failed sub-probe degrades the returned
    ``TableInfo`` rather than failing the whole connection test.
    """
    info = TableInfo(
        path=table_path,
        suggestedQuery=f"SELECT * FROM iceberg_scan('{table_path}') LIMIT 10",
    )

    # At least one of the two sub-probes must succeed — otherwise we have no
    # evidence the path is a real Iceberg table and should let the caller
    # (run_connection_test) treat it as a failed probe.
    metadata_json_ok = False
    iceberg_metadata_ok = False

    # Latest metadata.json (glob + ORDER BY filename DESC works for both
    # pyiceberg's `NNNNN-<uuid>.metadata.json` and Spark's `vN.metadata.json`
    # naming). Contains format-version, current-snapshot-id, last-updated-ms.
    try:
        meta_row = conn.execute(
            "SELECT content FROM read_text(?) ORDER BY filename DESC LIMIT 1",
            [f"{table_path}/metadata/*.metadata.json"],
        ).fetchone()
        if meta_row and meta_row[0]:
            meta = json.loads(meta_row[0])
            metadata_json_ok = True
            fmt = meta.get("format-version")
            if fmt:
                info.format = f"iceberg-v{fmt}"
            snap_id = meta.get("current-snapshot-id")
            if snap_id is not None:
                # Snapshot IDs are 64-bit — stringify to avoid JS precision loss.
                info.snapshotId = str(snap_id)
            updated_ms = meta.get("last-updated-ms")
            if isinstance(updated_ms, (int, float)):
                info.lastSnapshotAt = (
                    datetime.fromtimestamp(updated_ms / 1000, tz=timezone.utc)
                    .isoformat()
                    .replace("+00:00", "Z")
                )
    except Exception as e:
        logger.warning(f"Could not read table metadata JSON for {table_path}: {e}")

    # Manifest-level aggregate — rows, file count, delete detection.
    try:
        agg = conn.execute(
            """
            SELECT
                COALESCE(SUM(record_count), 0)::BIGINT AS rows,
                COUNT(*)::BIGINT AS files,
                BOOL_OR(manifest_content <> 'DATA') AS has_deletes
            FROM iceberg_metadata(?)
            """,
            [table_path],
        ).fetchone()
        if agg:
            iceberg_metadata_ok = True
            info.rows = int(agg[0]) if agg[0] is not None else None
            info.files = int(agg[1]) if agg[1] is not None else None
            info.hasDeletes = bool(agg[2]) if agg[2] is not None else None
    except Exception as e:
        logger.warning(f"Could not read iceberg_metadata for {table_path}: {e}")

    if not (metadata_json_ok or iceberg_metadata_ok):
        raise RuntimeError(
            f"No Iceberg metadata readable at {table_path} — path may be wrong or credentials may lack access"
        )

    return info


def run_connection_test(config: ConnectionConfig) -> Optional[TableInfo]:
    """Open a fresh connection, apply config, and probe the target.

    Returns a populated :class:`TableInfo` on success, ``None`` on failure.
    Callers should treat ``None`` as a generic "couldn't reach / read the
    table" signal — detailed reasons land in the logs rather than in the
    return value so we don't leak backend internals to unauthenticated
    callers.
    """
    try:
        with _duckdb_connection(config) as conn:
            if config.catalogType == "rest":
                # namespace is validated at ingress against a SQL identifier
                # pattern, so this interpolation is safe.
                conn.execute(
                    f"SHOW TABLES FROM iceberg_catalog.{config.namespace}"
                ).fetchone()
                return TableInfo(
                    path=f"iceberg_catalog.{config.namespace}",
                    suggestedQuery=f"SHOW TABLES FROM iceberg_catalog.{config.namespace}",
                )
            if config.tablePath:
                return _probe_iceberg_table(conn, config.tablePath)

            # Demo MinIO setup (path is hardcoded, not user input)
            demo_path = "s3://movies/warehouse/demo/movies"
            conn.execute(
                f"SELECT COUNT(*) FROM iceberg_scan('{demo_path}') LIMIT 1"
            ).fetchone()
            return _probe_iceberg_table(conn, demo_path)

    except HTTPException:
        raise
    except Exception as e:
        logger.warning(f"Connection test failed: {e}")
        return None


def run_query(
    sql: str, config: ConnectionConfig, row_limit: int = 1000
) -> QueryResponse:
    """Execute ``sql`` against a fresh DuckDB session built from ``config``."""
    start_time = time.time()

    try:
        with _duckdb_connection(config) as conn:
            if config.tablePath:
                _validate_iceberg_table(conn, config.tablePath)

            # Convert any legacy read_parquet() calls to iceberg_scan() first,
            # then validate + LIMIT-inject the resulting SQL with sqlglot.
            converted_sql = _convert_to_iceberg_query(sql, config)
            final_sql = _validate_and_limit_sql(converted_sql, row_limit)

            logger.info(f"Executing full query: {final_sql}")
            logger.info(
                f"Connection config: {config.storageType}, endpoint: {config.endpoint}"
            )

            result = conn.execute(final_sql)
            columns = [desc[0] for desc in result.description]
            rows = result.fetchall()

        execution_time = int((time.time() - start_time) * 1000)

        # Rough estimate; replaced when DuckDB profiling instrumentation lands.
        bytes_scanned = len(str(rows)) * 2

        stats = QueryStats(
            executionTimeMs=execution_time,
            bytesScanned=bytes_scanned,
            rowsReturned=len(rows),
        )
        truncated = len(rows) >= row_limit

        logger.info(f"Query completed: {len(rows)} rows in {execution_time}ms")

        return QueryResponse(
            columns=columns,
            rows=rows,
            stats=stats,
            truncated=truncated,
        )

    except HTTPException:
        # Validation and other deliberate 400s already carry a useful detail
        # string — re-raise unchanged rather than wrapping.
        raise
    except Exception as e:
        execution_time = int((time.time() - start_time) * 1000)
        logger.error(f"Query failed after {execution_time}ms: {e}")
        raise HTTPException(status_code=400, detail=f"Query execution failed: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan management."""
    logger.info("Starting Cloudfloe backend...")

    # Pre-install extensions at startup so the first real request doesn't pay
    # the download cost. DuckDB caches the binaries on disk, so subsequent
    # per-request connections just LOAD them.
    warmup = duckdb.connect(":memory:")
    try:
        warmup.execute("INSTALL httpfs")
        warmup.execute("INSTALL iceberg")
        logger.info("DuckDB extensions warmed up")
    except Exception as e:
        logger.warning(f"Extension warmup failed (will retry on first request): {e}")
    finally:
        warmup.close()

    logger.info("Backend ready!")
    yield
    logger.info("Shutting down Cloudfloe backend...")


# Create FastAPI app
app = FastAPI(
    title="Cloudfloe API",
    description="DuckDB-as-a-service for Iceberg data lakes",
    version="0.1.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify exact origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    """Health check endpoint."""
    return {
        "message": "Cloudfloe API",
        "version": "0.1.0",
        "status": "running"
    }


@app.get("/health")
async def health():
    """Detailed health check."""
    return {
        "status": "healthy",
        "duckdb_version": duckdb.__version__,
        "timestamp": time.time()
    }


@app.post("/api/connect/test")
async def test_connection(request: TestConnectionRequest):
    """Test connection to data source and return a diagnostic summary.

    Successful responses include ``tableInfo`` with Iceberg metadata
    (format version, current snapshot, row/file counts, delete flag)
    so the UI can show users "yes, this is your table" instead of a
    bare tick.
    """
    try:
        table_info = run_connection_test(request.connection)

        if table_info is None:
            raise HTTPException(status_code=400, detail="Connection test failed")

        return {
            "status": "success",
            "message": "Connection successful",
            "tableInfo": table_info.model_dump(exclude_none=True),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Connection test error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/api/query", response_model=QueryResponse)
async def execute_query(request: QueryRequest):
    """Execute SQL query against data source."""
    try:
        return run_query(request.sql, request.connection, request.rowLimit)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Query execution error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/api/demo/connection")
async def get_demo_connection():
    """Get demo connection configuration for MinIO."""
    return {
        "storageType": "minio",
        "endpoint": "http://localhost:9000",
        "accessKey": "cloudfloe",
        "secretKey": "cloudfloe123",
        "region": "us-east-1",
        "tablePath": "s3://movies/warehouse/demo/movies"
    }


@app.get("/api/demo/queries")
async def get_demo_queries():
    """Get sample queries for demo dataset."""
    demo_table = "s3://movies/warehouse/demo/movies"
    return {
        "queries": [
            {
                "name": "Sample Movies",
                "description": "Preview first 10 movies",
                "sql": f"SELECT primaryTitle, startYear, runtimeMinutes, genres FROM iceberg_scan('{demo_table}') WHERE titleType = 'movie' ORDER BY startYear DESC LIMIT 10"
            },
            {
                "name": "Row Count",
                "description": "Count total rows in dataset",
                "sql": f"SELECT COUNT(*) as total_movies FROM iceberg_scan('{demo_table}')"
            },
            {
                "name": "Movies by Decade",
                "description": "Count movies by decade",
                "sql": f"SELECT decade, COUNT(*) as movie_count FROM iceberg_scan('{demo_table}') WHERE titleType = 'movie' GROUP BY decade ORDER BY decade DESC"
            },
            {
                "name": "Long Movies",
                "description": "Find movies over 3 hours",
                "sql": f"SELECT primaryTitle, startYear, runtimeMinutes FROM iceberg_scan('{demo_table}') WHERE titleType = 'movie' AND runtimeMinutes > 180 ORDER BY runtimeMinutes DESC"
            },
            {
                "name": "Popular Genres",
                "description": "Most common genres",
                "sql": f"SELECT TRIM(genre) as genre, COUNT(*) as count FROM (SELECT UNNEST(string_split(genres, ',')) as genre FROM iceberg_scan('{demo_table}') WHERE titleType = 'movie' AND genres IS NOT NULL) GROUP BY genre ORDER BY count DESC"
            }
        ]
    }


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        reload=True,
        log_level="info"
    )
