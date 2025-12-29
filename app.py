"""
DuckDB Cricket Backend API
Read-only FastAPI server for querying cricket analytics data stored in DuckDB.
Includes admin endpoints for file management.
"""

from fastapi import FastAPI, HTTPException, Request, Depends, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from pydantic import BaseModel
from typing import Optional, List, Any
import duckdb
import os
import logging
import secrets
import json
import shutil
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path

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
DATA_DIR = os.getenv("DATA_DIR", os.path.dirname(DATA_PATH) or "./data")
MAX_UPLOAD_SIZE = int(os.getenv("MAX_UPLOAD_SIZE", str(500 * 1024 * 1024)))  # 500MB default
RATE_LIMIT = os.getenv("RATE_LIMIT", "100/hour")
MAX_ROWS = int(os.getenv("MAX_ROWS", "10000"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
CORS_ORIGINS = os.getenv("CORS_ORIGINS", "*").split(",")

# Metadata file for caching file info
METADATA_FILE = os.path.join(DATA_DIR, "file_metadata.json")

# Version
VERSION = "1.1.0"

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

class RenameRequest(BaseModel):
    new_name: str

# Initialize FastAPI
app = FastAPI(
    title="DuckDB Cricket API",
    description="Read-only API for cricket analytics data with admin file management",
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

# ============== Metadata helpers ==============

def load_metadata() -> dict:
    """Load metadata from JSON file, or return empty dict if not exists."""
    if os.path.exists(METADATA_FILE):
        try:
            with open(METADATA_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to load metadata: {e}")
    return {}

def save_metadata(metadata: dict):
    """Save metadata to JSON file."""
    try:
        with open(METADATA_FILE, "w") as f:
            json.dump(metadata, f, indent=2)
    except Exception as e:
        logger.error(f"Failed to save metadata: {e}")

def get_duckdb_row_count(filepath: str) -> int:
    """Get total row count from a DuckDB file by summing all tables."""
    try:
        conn = duckdb.connect(filepath, read_only=True)
        tables = [row[0] for row in conn.execute("SHOW TABLES").fetchall()]
        total_rows = 0
        for table in tables:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            total_rows += count
        conn.close()
        return total_rows
    except Exception as e:
        logger.error(f"Failed to count rows in {filepath}: {e}")
        return -1

def get_duckdb_tables(filepath: str) -> List[str]:
    """Get list of tables in a DuckDB file."""
    try:
        conn = duckdb.connect(filepath, read_only=True)
        tables = [row[0] for row in conn.execute("SHOW TABLES").fetchall()]
        conn.close()
        return tables
    except Exception as e:
        logger.error(f"Failed to get tables from {filepath}: {e}")
        return []

def rebuild_metadata_for_file(filename: str, filepath: str) -> dict:
    """Build metadata entry for a single file."""
    size_bytes = os.path.getsize(filepath)
    row_count = get_duckdb_row_count(filepath)
    tables = get_duckdb_tables(filepath)
    return {
        "size_bytes": size_bytes,
        "row_count": row_count,
        "tables": tables,
        "updated_at": datetime.now().isoformat()
    }

def ensure_metadata_exists():
    """Ensure metadata file exists and is up to date with actual files."""
    metadata = load_metadata()
    files_on_disk = set()

    # Scan directory for .duckdb files
    for f in os.listdir(DATA_DIR):
        if f.endswith(".duckdb"):
            files_on_disk.add(f)
            filepath = os.path.join(DATA_DIR, f)
            if f not in metadata:
                logger.info(f"Building metadata for new file: {f}")
                metadata[f] = rebuild_metadata_for_file(f, filepath)

    # Remove metadata for files that no longer exist
    for f in list(metadata.keys()):
        if f not in files_on_disk:
            logger.info(f"Removing metadata for deleted file: {f}")
            del metadata[f]

    save_metadata(metadata)
    return metadata

# ============== Startup/Shutdown ==============

@app.on_event("startup")
async def startup():
    global db_connection
    logger.info(f"Data directory: {DATA_DIR}")
    logger.info(f"Loading DuckDB from: {DATA_PATH}")

    # Ensure data directory exists
    os.makedirs(DATA_DIR, exist_ok=True)

    if not os.path.exists(DATA_PATH):
        logger.warning(f"DuckDB file not found at {DATA_PATH}, some endpoints may fail")
    else:
        db_connection = duckdb.connect(DATA_PATH, read_only=True)
        logger.info("DuckDB connection established (read-only)")

    # Build/update metadata on startup
    ensure_metadata_exists()
    logger.info("Metadata initialized")

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

# ============== Health check ==============

@app.get("/health")
async def health_check():
    """Health check endpoint for load balancers and monitoring."""
    return {"status": "ok", "version": VERSION}

# ============== Existing query endpoints ==============

@app.get("/api/v1/tables", dependencies=[Depends(verify_api_key)])
@limiter.limit(RATE_LIMIT)
async def list_tables(request: Request):
    """List all available tables in the database."""
    if not db_connection:
        raise HTTPException(status_code=503, detail="Database not connected")
    try:
        result = db_connection.execute("SHOW TABLES").fetchall()
        tables = [row[0] for row in result]
        return {"tables": tables}
    except Exception as e:
        logger.error(f"Error listing tables: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/schema/{table_name}", dependencies=[Depends(verify_api_key)])
@limiter.limit(RATE_LIMIT)
async def get_schema(table_name: str, request: Request):
    """Get schema for a specific table."""
    if not db_connection:
        raise HTTPException(status_code=503, detail="Database not connected")
    try:
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

@app.post("/api/v1/query", dependencies=[Depends(verify_api_key)])
@limiter.limit(RATE_LIMIT)
async def execute_query(query: QueryRequest, request: Request):
    """Execute a read-only SQL query."""
    if not db_connection:
        raise HTTPException(status_code=503, detail="Database not connected")

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
        row_limit = min(query.limit or MAX_ROWS, MAX_ROWS)
        result = db_connection.execute(sql).fetchmany(row_limit + 1)
        columns = [desc[0] for desc in db_connection.description]

        truncated = len(result) > row_limit
        if truncated:
            result = result[:row_limit]

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

# ============== Admin file management endpoints ==============

@app.get("/api/v1/admin/files", dependencies=[Depends(verify_api_key)])
@limiter.limit(RATE_LIMIT)
async def list_files(request: Request):
    """List all DuckDB files with their metadata."""
    try:
        metadata = ensure_metadata_exists()
        files = []
        for filename, info in metadata.items():
            files.append({
                "name": filename,
                "size_bytes": info.get("size_bytes", 0),
                "size_mb": round(info.get("size_bytes", 0) / (1024 * 1024), 2),
                "row_count": info.get("row_count", -1),
                "tables": info.get("tables", []),
                "updated_at": info.get("updated_at", "")
            })
        # Sort by name
        files.sort(key=lambda x: x["name"])
        return {"files": files, "count": len(files)}
    except Exception as e:
        logger.error(f"Error listing files: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/admin/files/{filename}/preview", dependencies=[Depends(verify_api_key)])
@limiter.limit(RATE_LIMIT)
async def preview_file(filename: str, request: Request, limit: int = 10):
    """Get sample rows from a DuckDB file."""
    # Security: prevent path traversal
    if "/" in filename or "\\" in filename or ".." in filename:
        raise HTTPException(status_code=400, detail="Invalid filename")

    filepath = os.path.join(DATA_DIR, filename)
    if not os.path.exists(filepath):
        raise HTTPException(status_code=404, detail=f"File '{filename}' not found")

    try:
        conn = duckdb.connect(filepath, read_only=True)
        tables = [row[0] for row in conn.execute("SHOW TABLES").fetchall()]

        preview_data = {}
        for table in tables:
            result = conn.execute(f"SELECT * FROM {table} LIMIT {limit}").fetchall()
            columns = [desc[0] for desc in conn.description]
            rows = []
            for row in result:
                rows.append([
                    val.isoformat() if isinstance(val, (date, datetime)) else
                    float(val) if isinstance(val, Decimal) else
                    val
                    for val in row
                ])
            preview_data[table] = {
                "columns": columns,
                "rows": rows,
                "row_count": len(rows)
            }

        conn.close()
        return {"filename": filename, "tables": preview_data}
    except Exception as e:
        logger.error(f"Error previewing file {filename}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/admin/files/upload", dependencies=[Depends(verify_api_key)])
@limiter.limit("10/hour")
async def upload_file(
    request: Request,
    file: UploadFile = File(...),
    custom_name: Optional[str] = Form(None)
):
    """Upload a new DuckDB file."""
    # Determine filename
    filename = custom_name if custom_name else file.filename
    if not filename:
        raise HTTPException(status_code=400, detail="Filename required")

    # Security: ensure .duckdb extension and no path traversal
    if "/" in filename or "\\" in filename or ".." in filename:
        raise HTTPException(status_code=400, detail="Invalid filename")
    if not filename.endswith(".duckdb"):
        filename = filename + ".duckdb"

    filepath = os.path.join(DATA_DIR, filename)

    try:
        # Save file with size limit check
        total_size = 0
        with open(filepath, "wb") as f:
            while True:
                chunk = await file.read(1024 * 1024)  # 1MB chunks
                if not chunk:
                    break
                total_size += len(chunk)
                if total_size > MAX_UPLOAD_SIZE:
                    f.close()
                    os.remove(filepath)
                    raise HTTPException(
                        status_code=413,
                        detail=f"File too large. Max size: {MAX_UPLOAD_SIZE // (1024*1024)}MB"
                    )
                f.write(chunk)

        # Validate it is a valid DuckDB file
        try:
            test_conn = duckdb.connect(filepath, read_only=True)
            test_conn.execute("SHOW TABLES")
            test_conn.close()
        except Exception as e:
            os.remove(filepath)
            raise HTTPException(status_code=400, detail=f"Invalid DuckDB file: {str(e)}")

        # Update metadata
        metadata = load_metadata()
        metadata[filename] = rebuild_metadata_for_file(filename, filepath)
        save_metadata(metadata)

        logger.info(f"Uploaded file: {filename} ({total_size} bytes)")
        return {
            "message": "File uploaded successfully",
            "filename": filename,
            "size_bytes": total_size,
            "size_mb": round(total_size / (1024 * 1024), 2)
        }
    except HTTPException:
        raise
    except Exception as e:
        # Cleanup on error
        if os.path.exists(filepath):
            os.remove(filepath)
        logger.error(f"Error uploading file: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/v1/admin/files/{filename}", dependencies=[Depends(verify_api_key)])
@limiter.limit(RATE_LIMIT)
async def delete_file(filename: str, request: Request):
    """Delete a DuckDB file."""
    # Security: prevent path traversal
    if "/" in filename or "\\" in filename or ".." in filename:
        raise HTTPException(status_code=400, detail="Invalid filename")

    filepath = os.path.join(DATA_DIR, filename)
    if not os.path.exists(filepath):
        raise HTTPException(status_code=404, detail=f"File '{filename}' not found")

    try:
        os.remove(filepath)

        # Remove from metadata
        metadata = load_metadata()
        if filename in metadata:
            del metadata[filename]
            save_metadata(metadata)

        logger.info(f"Deleted file: {filename}")
        return {"message": f"File '{filename}' deleted successfully"}
    except Exception as e:
        logger.error(f"Error deleting file {filename}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/api/v1/admin/files/{filename}/rename", dependencies=[Depends(verify_api_key)])
@limiter.limit(RATE_LIMIT)
async def rename_file(filename: str, rename_req: RenameRequest, request: Request):
    """Rename a DuckDB file."""
    new_name = rename_req.new_name

    # Security: prevent path traversal
    if "/" in filename or "\\" in filename or ".." in filename:
        raise HTTPException(status_code=400, detail="Invalid filename")
    if "/" in new_name or "\\" in new_name or ".." in new_name:
        raise HTTPException(status_code=400, detail="Invalid new filename")

    # Ensure .duckdb extension
    if not new_name.endswith(".duckdb"):
        new_name = new_name + ".duckdb"

    old_path = os.path.join(DATA_DIR, filename)
    new_path = os.path.join(DATA_DIR, new_name)

    if not os.path.exists(old_path):
        raise HTTPException(status_code=404, detail=f"File '{filename}' not found")
    if os.path.exists(new_path):
        raise HTTPException(status_code=409, detail=f"File '{new_name}' already exists")

    try:
        os.rename(old_path, new_path)

        # Update metadata: move entry from old name to new name
        metadata = load_metadata()
        if filename in metadata:
            metadata[new_name] = metadata.pop(filename)
            metadata[new_name]["updated_at"] = datetime.now().isoformat()
            save_metadata(metadata)

        logger.info(f"Renamed file: {filename} -> {new_name}")
        return {"message": f"File renamed from '{filename}' to '{new_name}'", "new_name": new_name}
    except Exception as e:
        logger.error(f"Error renaming file {filename}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/admin/files/{filename}/download", dependencies=[Depends(verify_api_key)])
@limiter.limit("20/hour")
async def download_file(filename: str, request: Request):
    """Download a DuckDB file."""
    # Security: prevent path traversal
    if "/" in filename or "\\" in filename or ".." in filename:
        raise HTTPException(status_code=400, detail="Invalid filename")

    filepath = os.path.join(DATA_DIR, filename)
    if not os.path.exists(filepath):
        raise HTTPException(status_code=404, detail=f"File '{filename}' not found")

    return FileResponse(
        path=filepath,
        filename=filename,
        media_type="application/octet-stream"
    )

@app.post("/api/v1/admin/files/{filename}/refresh-metadata", dependencies=[Depends(verify_api_key)])
@limiter.limit("10/hour")
async def refresh_file_metadata(filename: str, request: Request):
    """Recalculate metadata for a specific file."""
    # Security: prevent path traversal
    if "/" in filename or "\\" in filename or ".." in filename:
        raise HTTPException(status_code=400, detail="Invalid filename")

    filepath = os.path.join(DATA_DIR, filename)
    if not os.path.exists(filepath):
        raise HTTPException(status_code=404, detail=f"File '{filename}' not found")

    try:
        metadata = load_metadata()
        metadata[filename] = rebuild_metadata_for_file(filename, filepath)
        save_metadata(metadata)

        logger.info(f"Refreshed metadata for: {filename}")
        return {
            "message": f"Metadata refreshed for '{filename}'",
            "metadata": metadata[filename]
        }
    except Exception as e:
        logger.error(f"Error refreshing metadata for {filename}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Run with uvicorn
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
