"""
DuckDB Cricket Backend API
Read-only FastAPI server for querying cricket analytics data stored in DuckDB.
"""

from fastapi import FastAPI, HTTPException, Request, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional, List, Any
import duckdb
import os
import logging
import secrets
import json
from datetime import date, datetime, time
from decimal import Decimal

from dotenv import load_dotenv
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from starlette.status import HTTP_429_TOO_MANY_REQUESTS

# Load environment variables
load_dotenv()

# Configuration
API_KEY = os.getenv("API_KEY")
if not API_KEY:
    raise RuntimeError("API_KEY environment variable not set")

DATA_PATH = os.getenv("DATA_PATH", "./data/odi_t20.duckdb")
RATE_LIMIT = os.getenv("RATE_LIMIT", "100/hour")
MAX_ROWS = int(os.getenv("MAX_ROWS", "10000"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
CORS_ORIGINS = os.getenv("CORS_ORIGINS", "*").split(",")

# Version
VERSION = "1.0.0"

# Logging setup
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Custom JSON encoder for dates/decimals
class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        elif isinstance(obj, time):
            return obj.isoformat()
        elif isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)

# Request/Response models
class QueryRequest(BaseModel):
    sql: str
    limit: Optional[int] = None

class QueryResponse(BaseModel):
    columns: List[str]
    rows: List[List[Any]]
    row_count: int
    truncated: bool

class TableSchema(BaseModel):
    column_name: str
    column_type: str
    nullable: bool

# Initialize FastAPI
app = FastAPI(
    title="DuckDB Cricket API",
    description="Read-only API for cricket analytics data",
    version=VERSION
)

# Rate limiting
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter

@app.exception_handler(RateLimitExceeded)
async def rate_limit_handler(request: Request, exc: RateLimitExceeded) -> JSONResponse:
    return JSONResponse(
        status_code=HTTP_429_TOO_MANY_REQUESTS,
        content={"detail": "Rate limit exceeded. Please try again later."}
    )

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# DuckDB connection (opened once at startup)
db_connection: Optional[duckdb.DuckDBPyConnection] = None

@app.on_event("startup")
async def startup():
    global db_connection
    logger.info(f"Loading DuckDB from: {DATA_PATH}")
    if not os.path.exists(DATA_PATH):
        logger.error(f"DuckDB file not found at {DATA_PATH}")
        raise RuntimeError(f"DuckDB file not found at {DATA_PATH}")

    db_connection = duckdb.connect(DATA_PATH, read_only=True)
    logger.info("DuckDB connection established (read-only)")

@app.on_event("shutdown")
async def shutdown():
    global db_connection
    if db_connection:
        db_connection.close()
        logger.info("DuckDB connection closed")

# Auth dependency
async def verify_api_key(request: Request):
    auth = request.headers.get("Authorization")
    if not auth:
        raise HTTPException(
            status_code=401,
            detail="Missing Authorization header",
            headers={"WWW-Authenticate": "Bearer"},
        )

    scheme, _, token = auth.partition(" ")
    if scheme.lower() != "bearer" or not token:
        raise HTTPException(
            status_code=401,
            detail="Invalid auth scheme, use 'Bearer <token>'",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if not secrets.compare_digest(token, API_KEY):
        raise HTTPException(status_code=403, detail="Invalid API key")

# Health check (no auth required)
@app.get("/health")
async def health_check():
    """Health check endpoint for load balancers and monitoring."""
    return {"status": "ok", "version": VERSION}

# List tables
@app.get("/api/v1/tables", dependencies=[Depends(verify_api_key)])
@limiter.limit(RATE_LIMIT)
async def list_tables(request: Request):
    """List all available tables in the database."""
    try:
        result = db_connection.execute("SHOW TABLES").fetchall()
        tables = [row[0] for row in result]
        return {"tables": tables}
    except Exception as e:
        logger.error(f"Error listing tables: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Get table schema
@app.get("/api/v1/schema/{table_name}", dependencies=[Depends(verify_api_key)])
@limiter.limit(RATE_LIMIT)
async def get_schema(table_name: str, request: Request):
    """Get schema for a specific table."""
    try:
        # Validate table exists
        tables = [row[0] for row in db_connection.execute("SHOW TABLES").fetchall()]
        if table_name not in tables:
            raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")

        result = db_connection.execute(f"DESCRIBE {table_name}").fetchall()
        schema = [
            {
                "column_name": row[0],
                "column_type": row[1],
                "nullable": row[2] == "YES"
            }
            for row in result
        ]
        return {"table": table_name, "schema": schema}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting schema for {table_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Execute query
@app.post("/api/v1/query", dependencies=[Depends(verify_api_key)])
@limiter.limit(RATE_LIMIT)
async def execute_query(query: QueryRequest, request: Request):
    """Execute a read-only SQL query."""
    sql = query.sql.strip()

    # Basic SQL injection prevention - only allow SELECT/WITH statements
    sql_lower = sql.lower()
    if not (sql_lower.startswith("select") or sql_lower.startswith("with")):
        raise HTTPException(
            status_code=400,
            detail="Only SELECT and WITH queries are allowed"
        )

    # Disallow dangerous keywords
    dangerous = ["insert", "update", "delete", "drop", "create", "alter", "truncate", "exec", "execute"]
    for keyword in dangerous:
        if keyword in sql_lower:
            raise HTTPException(
                status_code=400,
                detail=f"Query contains forbidden keyword: {keyword}"
            )

    try:
        # Apply row limit
        row_limit = min(query.limit or MAX_ROWS, MAX_ROWS)

        # Execute query
        result = db_connection.execute(sql).fetchmany(row_limit + 1)
        columns = [desc[0] for desc in db_connection.description]

        # Check if truncated
        truncated = len(result) > row_limit
        if truncated:
            result = result[:row_limit]

        # Convert to serializable format
        rows = []
        for row in result:
            rows.append([
                val.isoformat() if isinstance(val, (date, datetime)) else
                float(val) if isinstance(val, Decimal) else
                val
                for val in row
            ])

        return {
            "columns": columns,
            "rows": rows,
            "row_count": len(rows),
            "truncated": truncated
        }

    except duckdb.Error as e:
        logger.error(f"DuckDB error: {e}")
        raise HTTPException(status_code=400, detail=f"Query error: {str(e)}")
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Run with uvicorn
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
