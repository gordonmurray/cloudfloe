"""
Cloudfloe FastAPI Backend
DuckDB-as-a-service for Iceberg data lakes
"""

import duckdb
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Dict, List, Optional, Any

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import uvicorn

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Request/Response Models
class ConnectionConfig(BaseModel):
    storageType: str = Field(..., description="Storage type: s3, r2, minio")
    endpoint: str = Field(..., description="S3 endpoint or bucket path")
    accessKey: str = Field(..., description="Access key ID")
    secretKey: str = Field(..., description="Secret access key")
    sessionToken: Optional[str] = Field(None, description="Session token for STS")
    region: str = Field(default="us-east-1", description="Region")

    # Iceberg-specific fields
    catalogType: Optional[str] = Field(default="none", description="Catalog type: rest, none")
    catalogEndpoint: Optional[str] = Field(None, description="REST catalog endpoint URL")
    namespace: Optional[str] = Field(default="default", description="Iceberg namespace/database")
    tablePath: Optional[str] = Field(None, description="Direct path to Iceberg table root")


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


class DuckDBManager:
    """Manages DuckDB connections and query execution."""

    def __init__(self):
        self.connection = None
        self._setup_duckdb()

    def _setup_duckdb(self):
        """Initialize DuckDB with required extensions."""
        try:
            logger.info("Setting up DuckDB...")
            self.connection = duckdb.connect(":memory:")

            # Install extensions
            self.connection.execute("INSTALL httpfs")
            self.connection.execute("LOAD httpfs")

            # Iceberg extension is mandatory for correct data reads
            try:
                self.connection.execute("INSTALL iceberg")
                self.connection.execute("LOAD iceberg")
                # DuckDB 1.4+ requires explicit version handling
                self.connection.execute("SET unsafe_enable_version_guessing=true")
                logger.info("✓ Iceberg extension loaded")
            except Exception as e:
                logger.error(f"FATAL: Iceberg extension required but not available: {e}")
                raise RuntimeError("Iceberg extension is required for correct data reads")

            # Set default settings
            self.connection.execute("SET memory_limit='2GB'")
            self.connection.execute("SET threads=4")

            logger.info("✓ DuckDB initialized successfully")

        except Exception as e:
            logger.error(f"Failed to setup DuckDB: {e}")
            raise

    def _apply_s3_config(self, config: ConnectionConfig):
        """Apply S3 configuration to DuckDB session."""
        try:
            logger.info(f"Applying S3 config: {config.storageType}, endpoint: {config.endpoint}")

            # Create a fresh connection to avoid state issues
            if self.connection:
                self.connection.close()
            self.connection = duckdb.connect(":memory:")

            # Reinstall extensions for new connection
            self.connection.execute("INSTALL httpfs")
            self.connection.execute("LOAD httpfs")
            self.connection.execute("INSTALL iceberg")
            self.connection.execute("LOAD iceberg")
            self.connection.execute("SET unsafe_enable_version_guessing=true")

            # Set default settings
            self.connection.execute("SET memory_limit='2GB'")
            self.connection.execute("SET threads=4")

            # Apply new settings based on storage type
            if config.storageType == "minio":
                # MinIO configuration - handle both localhost and container endpoints
                endpoint = config.endpoint
                if "localhost" in endpoint:
                    # Replace localhost with container name for internal access
                    endpoint = endpoint.replace("localhost", "minio")
                endpoint = endpoint.replace("http://", "").replace("https://", "")
                logger.info(f"Final MinIO endpoint: {endpoint}")
                self.connection.execute(f"SET s3_endpoint='{endpoint}'")
                self.connection.execute("SET s3_url_style='path'")
                self.connection.execute("SET s3_use_ssl=false")
                # MinIO requires AWS signature v4
                self.connection.execute("SET s3_region='us-east-1'")  # MinIO default
            elif config.storageType == "r2":
                # Cloudflare R2 configuration
                endpoint = config.endpoint.replace("https://", "")
                self.connection.execute(f"SET s3_endpoint='{endpoint}'")
                self.connection.execute("SET s3_url_style='path'")
                self.connection.execute("SET s3_use_ssl=true")
            else:
                # AWS S3 configuration
                logger.info(f"Setting S3 region: {config.region}")
                self.connection.execute(f"SET s3_region='{config.region}'")
                self.connection.execute("SET s3_use_ssl=true")

            # Set credentials
            logger.info(f"Setting S3 credentials - Access Key starts with: {config.accessKey[:8] if config.accessKey else 'EMPTY'}...")
            # Use parameter binding to handle special characters safely
            self.connection.execute("SET s3_access_key_id=?", [config.accessKey])
            self.connection.execute("SET s3_secret_access_key=?", [config.secretKey])

            if config.sessionToken:
                self.connection.execute(f"SET s3_session_token='{config.sessionToken}'")

            logger.info(f"✓ Applied {config.storageType} configuration")

            # Attach Iceberg catalog if configured
            self._attach_iceberg_catalog(config)

        except Exception as e:
            logger.error(f"Failed to apply S3 config: {e}")
            raise HTTPException(status_code=400, detail=f"Invalid S3 configuration: {e}")

    def _attach_iceberg_catalog(self, config: ConnectionConfig):
        """Attach Iceberg REST catalog if configured."""
        if config.catalogType == "rest":
            if not config.catalogEndpoint:
                raise HTTPException(
                    status_code=400,
                    detail="catalogEndpoint required for REST catalog"
                )

            logger.info(f"Attaching Iceberg REST catalog: {config.catalogEndpoint}")

            # Create secret for catalog authentication
            # Note: For now using S3 credentials; extend for OAuth2 later
            self.connection.execute(f"""
                CREATE SECRET iceberg_catalog_secret (
                    TYPE iceberg,
                    TOKEN '{config.accessKey}:{config.secretKey}'
                )
            """)

            # Attach catalog
            self.connection.execute(f"""
                ATTACH '{config.namespace}' AS iceberg_catalog (
                    TYPE iceberg,
                    SECRET iceberg_catalog_secret,
                    ENDPOINT '{config.catalogEndpoint}'
                )
            """)

            logger.info("✓ Iceberg catalog attached")

    def _validate_iceberg_table(self, table_path: str) -> dict:
        """
        Validate Iceberg table compatibility (v1/v2 only, no deletes).

        Returns dict with validation results or raises HTTPException.
        """
        try:
            # Read metadata to check for delete files
            metadata_query = f"SELECT * FROM iceberg_metadata('{table_path}')"
            metadata = self.connection.execute(metadata_query).fetchdf()

            # Check for delete files in manifests
            has_deletes = any('DELETE' in str(v).upper() for v in metadata['manifest_content'].unique())

            if has_deletes:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Table contains row-level deletes which are not supported. "
                        "This application only supports append-only Iceberg v1/v2 tables. "
                        "Reading this table may return incorrect data."
                    )
                )

            logger.info(f"✓ Table validation passed: {table_path}")
            return {"valid": True, "warnings": []}

        except HTTPException:
            raise
        except Exception as e:
            logger.warning(f"Could not validate table (proceeding with caution): {e}")
            return {"valid": True, "warnings": [f"Validation incomplete: {e}"]}

    def _convert_to_iceberg_query(self, sql: str, config: ConnectionConfig) -> str:
        """
        Convert read_parquet() calls to iceberg_scan() calls.

        Detects:
        - read_parquet('s3://bucket/path/**/*.parquet')

        Converts to:
        - iceberg_scan('s3://bucket/path') OR
        - SELECT * FROM iceberg_catalog.namespace.table
        """
        import re

        # Pattern: read_parquet('s3://...**/*.parquet')
        parquet_pattern = r"read_parquet\(['\"]s3://([^/]+)/([^'\"]+?)/?\*?\*?/?\*?\.parquet['\"]\)"

        def replace_with_iceberg(match):
            bucket = match.group(1)
            path = match.group(2).rstrip('/*')

            if config.catalogType == "rest":
                # Use catalog table reference
                # Assumes table name is last path component
                table_name = path.split('/')[-1]
                return f"iceberg_catalog.{config.namespace}.{table_name}"
            else:
                # Direct iceberg_scan
                iceberg_path = f"s3://{bucket}/{path}"
                return f"iceberg_scan('{iceberg_path}')"

        converted_sql = re.sub(parquet_pattern, replace_with_iceberg, sql, flags=re.IGNORECASE)

        if converted_sql != sql:
            logger.info(f"Converted query from read_parquet to Iceberg:")
            logger.info(f"  Original: {sql[:100]}...")
            logger.info(f"  Converted: {converted_sql[:100]}...")

        return converted_sql

    def test_connection(self, config: ConnectionConfig) -> bool:
        """Test if the connection configuration works."""
        try:
            # Clean up table path if needed
            if config.tablePath:
                # Remove trailing slash if present
                if config.tablePath.endswith('/'):
                    config.tablePath = config.tablePath.rstrip('/')
                    logger.info(f"Cleaned table path (removed trailing slash): {config.tablePath}")

                # Remove /metadata suffix if present
                if config.tablePath.endswith('/metadata'):
                    config.tablePath = config.tablePath[:-9]
                    logger.info(f"Cleaned table path (removed /metadata): {config.tablePath}")

            self._apply_s3_config(config)

            # Test Iceberg-specific connectivity
            if config.catalogType == "rest":
                # Test catalog access
                test_query = f"SHOW TABLES FROM iceberg_catalog.{config.namespace}"
            elif config.tablePath:
                # For direct table access, try to read version hint first to help DuckDB
                try:
                    version_hint = self.connection.execute(
                        f"SELECT * FROM read_text('{config.tablePath}/metadata/version-hint.text')"
                    ).fetchone()
                    if version_hint:
                        logger.info(f"Found version hint: {version_hint[0]}")
                except Exception as e:
                    logger.warning(f"Could not read version-hint.text: {e}")

                # Test direct table access
                test_query = f"SELECT COUNT(*) FROM iceberg_scan('{config.tablePath}') LIMIT 1"
            else:
                # For demo MinIO setup, test with Iceberg scan
                test_query = "SELECT COUNT(*) FROM iceberg_scan('s3://movies/warehouse/demo/movies') LIMIT 1"

            result = self.connection.execute(test_query).fetchone()
            logger.info(f"✓ Connection test successful: {result}")
            return True

        except Exception as e:
            logger.warning(f"Connection test failed: {e}")
            return False

    def execute_query(self, sql: str, config: ConnectionConfig, row_limit: int = 1000) -> QueryResponse:
        """Execute SQL query and return results."""
        start_time = time.time()

        try:
            # Apply S3 configuration
            self._apply_s3_config(config)

            # Validate table if using direct path
            if config.tablePath:
                self._validate_iceberg_table(config.tablePath)

            # Enable profiling to get execution stats
            self.connection.execute("PRAGMA enable_profiling=json")
            self.connection.execute("PRAGMA profiling_output='query_profile.json'")

            # Sanitize and limit the query
            limited_sql = self._limit_query(sql, row_limit)

            # Convert to Iceberg query
            limited_sql = self._convert_to_iceberg_query(limited_sql, config)

            logger.info(f"Executing full query: {limited_sql}")
            logger.info(f"Connection config: {config.storageType}, endpoint: {config.endpoint}")

            # Execute query
            result = self.connection.execute(limited_sql)
            columns = [desc[0] for desc in result.description]
            rows = result.fetchall()

            execution_time = int((time.time() - start_time) * 1000)

            # Calculate stats (simplified for now)
            bytes_scanned = len(str(rows)) * 2  # Rough estimate

            stats = QueryStats(
                executionTimeMs=execution_time,
                bytesScanned=bytes_scanned,
                rowsReturned=len(rows)
            )

            # Check if results were truncated
            truncated = len(rows) >= row_limit

            logger.info(f"✓ Query completed: {len(rows)} rows in {execution_time}ms")

            return QueryResponse(
                columns=columns,
                rows=rows,
                stats=stats,
                truncated=truncated
            )

        except Exception as e:
            execution_time = int((time.time() - start_time) * 1000)
            logger.error(f"Query failed after {execution_time}ms: {e}")
            raise HTTPException(status_code=400, detail=f"Query execution failed: {e}")

    def _limit_query(self, sql: str, limit: int) -> str:
        """Add LIMIT clause to query if not present."""
        sql = sql.strip()

        # Remove trailing semicolon
        if sql.endswith(';'):
            sql = sql[:-1]

        # Check if LIMIT already exists (simple check)
        if 'LIMIT' not in sql.upper():
            sql += f" LIMIT {limit}"

        return sql


# Global DuckDB manager
db_manager = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan management."""
    global db_manager

    # Startup
    logger.info("🌊 Starting Cloudfloe backend...")
    db_manager = DuckDBManager()
    logger.info("✓ Backend ready!")

    yield

    # Shutdown
    logger.info("Shutting down Cloudfloe backend...")
    if db_manager and db_manager.connection:
        db_manager.connection.close()


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
    """Test connection to data source."""
    try:
        success = db_manager.test_connection(request.connection)

        if success:
            # Get table info to show in the UI
            table_info = None
            if request.connection.tablePath:
                table_info = {
                    "path": request.connection.tablePath,
                    "suggestedQuery": f"SELECT * FROM iceberg_scan('{request.connection.tablePath}') LIMIT 10"
                }

            return {
                "status": "success",
                "message": "Connection successful",
                "tableInfo": table_info
            }
        else:
            raise HTTPException(status_code=400, detail="Connection test failed")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Connection test error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/api/query", response_model=QueryResponse)
async def execute_query(request: QueryRequest):
    """Execute SQL query against data source."""
    try:
        # Validate query (basic checks)
        if not request.sql.strip():
            raise HTTPException(status_code=400, detail="Empty query")

        # Prevent destructive operations
        dangerous_keywords = ["DELETE", "DROP", "INSERT", "UPDATE", "CREATE", "ALTER"]
        sql_upper = request.sql.upper()
        for keyword in dangerous_keywords:
            if keyword in sql_upper:
                raise HTTPException(
                    status_code=400,
                    detail=f"Destructive operation '{keyword}' not allowed"
                )

        # Execute query
        result = db_manager.execute_query(
            request.sql,
            request.connection,
            request.rowLimit
        )

        return result

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
        "samplePath": "s3://movies/data/**/*.parquet"
    }


@app.get("/api/debug/test-direct")
async def test_direct_duckdb():
    """Test DuckDB S3 directly without using the manager class."""
    try:
        # Create a fresh connection exactly like the working debug script
        conn = duckdb.connect(":memory:")

        # Install and load extensions
        conn.execute("INSTALL httpfs")
        conn.execute("LOAD httpfs")

        # Configure S3 for MinIO exactly as in working debug script
        conn.execute("SET s3_endpoint='minio:9000'")
        conn.execute("SET s3_url_style='path'")
        conn.execute("SET s3_use_ssl=false")
        conn.execute("SET s3_region='us-east-1'")
        conn.execute("SET s3_access_key_id='cloudfloe'")
        conn.execute("SET s3_secret_access_key='cloudfloe123'")

        # Test the query
        result = conn.execute("SELECT COUNT(*) FROM read_parquet('s3://movies/data/**/*.parquet') LIMIT 1").fetchone()
        conn.close()

        return {
            "status": "success",
            "count": result[0],
            "message": "Direct DuckDB connection works!"
        }

    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }


@app.get("/api/demo/queries")
async def get_demo_queries():
    """Get sample queries for demo dataset."""
    return {
        "queries": [
            {
                "name": "Sample Movies",
                "description": "Preview first 10 movies",
                "sql": "SELECT primaryTitle, startYear, runtimeMinutes, genres FROM read_parquet('s3://movies/data/**/*.parquet') WHERE titleType = 'movie' ORDER BY startYear DESC"
            },
            {
                "name": "Row Count",
                "description": "Count total rows in dataset",
                "sql": "SELECT COUNT(*) as total_movies FROM read_parquet('s3://movies/data/**/*.parquet')"
            },
            {
                "name": "Movies by Decade",
                "description": "Count movies by decade",
                "sql": "SELECT decade, COUNT(*) as movie_count FROM read_parquet('s3://movies/data/**/*.parquet') WHERE titleType = 'movie' GROUP BY decade ORDER BY decade DESC"
            },
            {
                "name": "Long Movies",
                "description": "Find movies over 3 hours",
                "sql": "SELECT primaryTitle, startYear, runtimeMinutes FROM read_parquet('s3://movies/data/**/*.parquet') WHERE titleType = 'movie' AND runtimeMinutes > 180 ORDER BY runtimeMinutes DESC"
            },
            {
                "name": "Popular Genres",
                "description": "Most common genres",
                "sql": "SELECT TRIM(genre) as genre, COUNT(*) as count FROM (SELECT UNNEST(string_split(genres, ',')) as genre FROM read_parquet('s3://movies/data/**/*.parquet') WHERE titleType = 'movie' AND genres IS NOT NULL) GROUP BY genre ORDER BY count DESC"
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