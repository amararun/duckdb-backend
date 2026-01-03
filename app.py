"""
DuckDB Cricket Backend API
Read-only FastAPI server for querying cricket analytics data stored in DuckDB.
Includes admin endpoints for file management.
"""

from fastapi import FastAPI, HTTPException, Request, Depends, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from tigzig_api_monitor import APIMonitorMiddleware
from fastapi.responses import JSONResponse, FileResponse
from pydantic import BaseModel
from typing import Optional, List, Any
import duckdb
import os
import logging
import secrets
import json
import shutil
import sqlite3
import bcrypt
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path

from dotenv import load_dotenv
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from starlette.status import HTTP_429_TOO_MANY_REQUESTS
import hashlib
import time as time_module

# Load environment variables
load_dotenv()

# Configuration
API_KEY = os.getenv("API_KEY")
if not API_KEY:
    raise RuntimeError("API_KEY environment variable not set")

DATA_PATH = os.getenv("DATA_PATH", "./data/cricket.duckdb")
DATA_DIR = os.getenv("DATA_DIR", os.path.dirname(DATA_PATH) or "./data")
MAX_UPLOAD_SIZE = int(os.getenv("MAX_UPLOAD_SIZE", str(500 * 1024 * 1024)))  # 500MB default
RATE_LIMIT = os.getenv("RATE_LIMIT", "100/hour")
MAX_ROWS = int(os.getenv("MAX_ROWS", "10000"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
CORS_ORIGINS = os.getenv("CORS_ORIGINS", "*").split(",")

# Metadata file for caching file info
METADATA_FILE = os.path.join(DATA_DIR, "file_metadata.json")

# Upload token storage (in-memory, tokens expire after 60 minutes for large file uploads)
UPLOAD_TOKENS: dict[str, dict] = {}
UPLOAD_TOKEN_EXPIRY = 3600  # 60 minutes (for 5GB uploads on slow connections)

# Version
VERSION = "1.2.3"

# Share tokens file for persistent storage
SHARE_TOKENS_FILE = os.path.join(DATA_DIR, "share_tokens.json")

# API tokens SQLite database
TOKENS_DB = os.path.join(DATA_DIR, "tokens.db")

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

class TableRenameRequest(BaseModel):
    new_table_name: str

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
        content={"detail": "Rate limit exceeded. Please try again later."},
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
            "Access-Control-Allow-Headers": "*",
        }
    )

# CORS - explicitly allow all origins for direct upload endpoint
# The CORS_ORIGINS env var can restrict this for other endpoints
cors_origins = CORS_ORIGINS if CORS_ORIGINS != ["*"] else ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["*"],
)

# API Monitor middleware for centralized logging
app.add_middleware(
    APIMonitorMiddleware,
    app_name="DUCKDB_BACKEND",
    include_prefixes=("/api/v1/", "/s/"),  # Log API and share endpoints
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

def get_duckdb_table_info(filepath: str) -> tuple[List[str], dict, int]:
    """Get list of tables, per-table row counts, and total row count from a DuckDB file."""
    try:
        conn = duckdb.connect(filepath, read_only=True)
        tables = [row[0] for row in conn.execute("SHOW TABLES").fetchall()]
        table_row_counts = {}
        total_rows = 0
        for table in tables:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            table_row_counts[table] = count
            total_rows += count
        conn.close()
        return tables, table_row_counts, total_rows
    except Exception as e:
        logger.error(f"Failed to get table info from {filepath}: {e}")
        return [], {}, -1


def get_parquet_info(filepath: str) -> tuple[List[str], int]:
    """Get column names and row count from a Parquet file."""
    try:
        conn = duckdb.connect(":memory:")
        # Get row count
        count_result = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{filepath}')").fetchone()
        row_count = count_result[0] if count_result else 0
        # Get column names
        conn.execute(f"SELECT * FROM read_parquet('{filepath}') LIMIT 0")
        columns = [desc[0] for desc in conn.description]
        conn.close()
        return columns, row_count
    except Exception as e:
        logger.error(f"Failed to get parquet info from {filepath}: {e}")
        return [], -1


def rebuild_metadata_for_file(filename: str, filepath: str) -> dict:
    """Build metadata entry for a single file (DuckDB or Parquet)."""
    size_bytes = os.path.getsize(filepath)

    if filename.endswith(".parquet"):
        columns, row_count = get_parquet_info(filepath)
        return {
            "size_bytes": size_bytes,
            "row_count": row_count,
            "file_type": "parquet",
            "columns": columns,
            "tables": [],  # Parquet files don't have tables
            "table_row_counts": {},
            "updated_at": datetime.now().isoformat()
        }
    else:
        # DuckDB file
        tables, table_row_counts, total_rows = get_duckdb_table_info(filepath)
        return {
            "size_bytes": size_bytes,
            "row_count": total_rows,
            "file_type": "duckdb",
            "tables": tables,
            "table_row_counts": table_row_counts,
            "updated_at": datetime.now().isoformat()
        }


# Supported file extensions
SUPPORTED_EXTENSIONS = (".duckdb", ".parquet")


def ensure_metadata_exists():
    """Ensure metadata file exists and is up to date with actual files."""
    metadata = load_metadata()
    files_on_disk = set()
    needs_save = False

    # Scan directory for .duckdb and .parquet files
    for f in os.listdir(DATA_DIR):
        if f.endswith(SUPPORTED_EXTENSIONS):
            files_on_disk.add(f)
            filepath = os.path.join(DATA_DIR, f)
            # Rebuild if file not in metadata OR if table_row_counts is missing (backward compat)
            # Or if file_type is missing (new field for parquet support)
            if f not in metadata or "table_row_counts" not in metadata[f] or "file_type" not in metadata[f]:
                logger.info(f"Building/updating metadata for file: {f}")
                metadata[f] = rebuild_metadata_for_file(f, filepath)
                needs_save = True

    # Remove metadata for files that no longer exist
    for f in list(metadata.keys()):
        if f not in files_on_disk:
            logger.info(f"Removing metadata for deleted file: {f}")
            del metadata[f]
            needs_save = True

    if needs_save:
        save_metadata(metadata)
    return metadata

# ============== Share Token helpers ==============

def load_share_tokens() -> dict:
    """Load share tokens from JSON file, or return empty dict if not exists."""
    if os.path.exists(SHARE_TOKENS_FILE):
        try:
            with open(SHARE_TOKENS_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to load share tokens: {e}")
    return {}

def save_share_tokens(tokens: dict):
    """Save share tokens to JSON file."""
    try:
        with open(SHARE_TOKENS_FILE, "w") as f:
            json.dump(tokens, f, indent=2)
    except Exception as e:
        logger.error(f"Failed to save share tokens: {e}")

def get_share_token_for_file(filename: str) -> Optional[str]:
    """Get share token for a file if it exists."""
    tokens = load_share_tokens()
    for token, data in tokens.items():
        if data.get("filename") == filename:
            return token
    return None

def get_file_for_share_token(token: str) -> Optional[str]:
    """Get filename for a share token if it exists."""
    tokens = load_share_tokens()
    if token in tokens:
        return tokens[token].get("filename")
    return None

# ============== API Token helpers (SQLite + bcrypt) ==============

def init_tokens_db():
    """Initialize the SQLite database for API tokens."""
    conn = sqlite3.connect(TOKENS_DB)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS api_tokens (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            token_hash TEXT NOT NULL,
            file_name TEXT NOT NULL,
            permissions TEXT DEFAULT 'read',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_used_at TIMESTAMP,
            expires_at TIMESTAMP,
            enabled INTEGER DEFAULT 1
        )
    ''')
    conn.commit()
    conn.close()
    logger.info("API tokens database initialized")

def generate_api_token() -> str:
    """Generate a random API token."""
    return secrets.token_urlsafe(32)

def hash_api_token(token: str) -> str:
    """Hash a token using bcrypt."""
    return bcrypt.hashpw(token.encode(), bcrypt.gensalt()).decode()

def verify_api_token(token: str, token_hash: str) -> bool:
    """Verify a token against its hash."""
    try:
        return bcrypt.checkpw(token.encode(), token_hash.encode())
    except Exception:
        return False

def get_tokens_db_connection():
    """Get a connection to the tokens database."""
    conn = sqlite3.connect(TOKENS_DB)
    conn.row_factory = sqlite3.Row
    return conn

def create_file_token(name: str, file_name: str, permissions: str = 'read', expires_days: Optional[int] = None) -> dict:
    """Create a new API token for a file."""
    raw_token = generate_api_token()
    token_hash = hash_api_token(raw_token)

    expires_at = None
    if expires_days:
        expires_at = (datetime.now() + timedelta(days=expires_days)).isoformat()

    conn = get_tokens_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO api_tokens (name, token_hash, file_name, permissions, expires_at)
        VALUES (?, ?, ?, ?, ?)
    ''', (name, token_hash, file_name, permissions, expires_at))
    token_id = cursor.lastrowid
    conn.commit()
    conn.close()

    return {
        "id": token_id,
        "name": name,
        "token": raw_token,  # Only returned on creation
        "file_name": file_name,
        "permissions": permissions,
        "expires_at": expires_at
    }

def list_file_tokens() -> List[dict]:
    """List all API tokens (without the actual token values)."""
    conn = get_tokens_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT id, name, file_name, permissions, created_at, last_used_at, expires_at, enabled
        FROM api_tokens
        ORDER BY created_at DESC
    ''')
    rows = cursor.fetchall()
    conn.close()

    return [dict(row) for row in rows]

def get_token_by_id(token_id: int) -> Optional[dict]:
    """Get token info by ID (without the hash)."""
    conn = get_tokens_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT id, name, file_name, permissions, created_at, last_used_at, expires_at, enabled
        FROM api_tokens WHERE id = ?
    ''', (token_id,))
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None

def delete_file_token(token_id: int) -> bool:
    """Delete an API token."""
    conn = get_tokens_db_connection()
    cursor = conn.cursor()
    cursor.execute('DELETE FROM api_tokens WHERE id = ?', (token_id,))
    deleted = cursor.rowcount > 0
    conn.commit()
    conn.close()
    return deleted

def rotate_file_token(token_id: int) -> Optional[dict]:
    """Generate a new token value for an existing token entry."""
    conn = get_tokens_db_connection()
    cursor = conn.cursor()

    # Check if token exists
    cursor.execute('SELECT * FROM api_tokens WHERE id = ?', (token_id,))
    row = cursor.fetchone()
    if not row:
        conn.close()
        return None

    # Generate new token
    raw_token = generate_api_token()
    token_hash = hash_api_token(raw_token)

    cursor.execute('''
        UPDATE api_tokens SET token_hash = ? WHERE id = ?
    ''', (token_hash, token_id))
    conn.commit()

    # Get updated record
    cursor.execute('''
        SELECT id, name, file_name, permissions, created_at, last_used_at, expires_at, enabled
        FROM api_tokens WHERE id = ?
    ''', (token_id,))
    updated_row = cursor.fetchone()
    conn.close()

    result = dict(updated_row)
    result["token"] = raw_token  # Only returned on rotation
    return result

def verify_file_token(raw_token: str, file_name: str) -> Optional[dict]:
    """
    Verify a token for a specific file.
    Returns token info if valid, None otherwise.
    """
    conn = get_tokens_db_connection()
    cursor = conn.cursor()

    # Get all tokens for this file
    cursor.execute('''
        SELECT id, name, token_hash, file_name, permissions, expires_at, enabled
        FROM api_tokens WHERE file_name = ? AND enabled = 1
    ''', (file_name,))
    rows = cursor.fetchall()

    for row in rows:
        # Check expiration
        if row['expires_at']:
            expires = datetime.fromisoformat(row['expires_at'])
            if datetime.now() > expires:
                continue

        # Verify token hash
        if verify_api_token(raw_token, row['token_hash']):
            # Update last_used_at
            cursor.execute('''
                UPDATE api_tokens SET last_used_at = ? WHERE id = ?
            ''', (datetime.now().isoformat(), row['id']))
            conn.commit()
            conn.close()

            return {
                "id": row['id'],
                "name": row['name'],
                "file_name": row['file_name'],
                "permissions": row['permissions']
            }

    conn.close()
    return None

# ============== SQL Permission Validation ==============

# Keywords blocked for each permission level
READ_BLOCKED_KEYWORDS = ['INSERT', 'UPDATE', 'DELETE', 'DROP', 'ALTER', 'CREATE', 'TRUNCATE', 'EXEC', 'EXECUTE']
WRITE_BLOCKED_KEYWORDS = ['DELETE', 'DROP', 'ALTER', 'CREATE', 'TRUNCATE']
ADMIN_BLOCKED_KEYWORDS = ['DROP DATABASE']

def validate_sql_for_permission(sql: str, permission: str) -> tuple[bool, Optional[str]]:
    """
    Validate SQL query against permission level.
    Returns (is_valid, error_message).
    """
    sql_upper = sql.upper()

    if permission == 'read':
        for kw in READ_BLOCKED_KEYWORDS:
            if kw in sql_upper:
                return False, f"Read permission does not allow {kw} operations"
    elif permission == 'write':
        for kw in WRITE_BLOCKED_KEYWORDS:
            if kw in sql_upper:
                return False, f"Write permission does not allow {kw} operations"
    elif permission == 'admin':
        for kw in ADMIN_BLOCKED_KEYWORDS:
            if kw in sql_upper:
                return False, f"Operation not allowed: {kw}"
    else:
        return False, f"Unknown permission level: {permission}"

    return True, None

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

    # Initialize API tokens database
    init_tokens_db()

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

# ============== Master Query Endpoint (Admin Key) ==============

@app.post("/api/v1/admin/query", dependencies=[Depends(verify_api_key)])
@limiter.limit(RATE_LIMIT)
async def admin_query(query: QueryRequest, request: Request):
    """
    Execute any SQL query with unrestricted access.

    Uses the master admin API key. DuckDB resolves file paths from the SQL itself.

    Examples:
    - Query a specific file: SELECT * FROM '/data/myfile.duckdb'.table_name
    - Query parquet: SELECT * FROM read_parquet('/data/file.parquet')
    - Cross-file join: SELECT * FROM '/data/a.duckdb'.t1 JOIN read_parquet('/data/b.parquet') ON ...
    - Query main DB (DATA_PATH): SELECT * FROM table_name
    """
    sql = query.sql.strip()

    if not sql:
        raise HTTPException(status_code=400, detail="SQL query is required")

    try:
        # Create a fresh in-memory connection for each query
        # This allows DuckDB to resolve file paths from the SQL
        conn = duckdb.connect(":memory:")

        row_limit = min(query.limit or MAX_ROWS, MAX_ROWS)
        result = conn.execute(sql).fetchmany(row_limit + 1)
        columns = [desc[0] for desc in conn.description]

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

        conn.close()

        logger.info(f"Admin query executed: {sql[:100]}... ({len(rows)} rows)")
        return {
            "columns": columns,
            "rows": rows,
            "row_count": len(rows),
            "truncated": truncated
        }

    except duckdb.Error as e:
        logger.error(f"DuckDB error in admin query: {e}")
        raise HTTPException(status_code=400, detail=f"Query error: {str(e)}")
    except Exception as e:
        logger.error(f"Error in admin query: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ============== Admin file management endpoints ==============

@app.get("/api/v1/admin/files", dependencies=[Depends(verify_api_key)])
@limiter.limit(RATE_LIMIT)
async def list_files(request: Request):
    """List all DuckDB and Parquet files with their metadata."""
    try:
        metadata = ensure_metadata_exists()
        files = []
        for filename, info in metadata.items():
            share_token = get_share_token_for_file(filename)
            file_info = {
                "name": filename,
                "size_bytes": info.get("size_bytes", 0),
                "size_mb": round(info.get("size_bytes", 0) / (1024 * 1024), 2),
                "row_count": info.get("row_count", -1),
                "file_type": info.get("file_type", "duckdb"),
                "tables": info.get("tables", []),
                "table_row_counts": info.get("table_row_counts", {}),
                "updated_at": info.get("updated_at", ""),
                "share_token": share_token
            }
            # Add columns for parquet files
            if info.get("file_type") == "parquet":
                file_info["columns"] = info.get("columns", [])
            files.append(file_info)
        # Sort by name
        files.sort(key=lambda x: x["name"])
        return {"files": files, "count": len(files)}
    except Exception as e:
        logger.error(f"Error listing files: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/admin/files/{filename}/preview", dependencies=[Depends(verify_api_key)])
@limiter.limit(RATE_LIMIT)
async def preview_file(filename: str, request: Request, limit: int = 10):
    """Get sample rows from a DuckDB or Parquet file."""
    # Security: prevent path traversal
    if "/" in filename or "\\" in filename or ".." in filename:
        raise HTTPException(status_code=400, detail="Invalid filename")

    filepath = os.path.join(DATA_DIR, filename)
    if not os.path.exists(filepath):
        raise HTTPException(status_code=404, detail=f"File '{filename}' not found")

    try:
        # Handle parquet files differently
        if filename.endswith(".parquet"):
            conn = duckdb.connect(":memory:")
            result = conn.execute(f"SELECT * FROM read_parquet('{filepath}') LIMIT {limit}").fetchall()
            columns = [desc[0] for desc in conn.description]
            rows = []
            for row in result:
                rows.append([
                    val.isoformat() if isinstance(val, (date, datetime)) else
                    float(val) if isinstance(val, Decimal) else
                    val
                    for val in row
                ])
            conn.close()
            # For parquet, we use "_data" as the pseudo-table name
            return {
                "filename": filename,
                "file_type": "parquet",
                "tables": {
                    "_data": {
                        "columns": columns,
                        "rows": rows,
                        "row_count": len(rows)
                    }
                }
            }
        else:
            # DuckDB file
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
            return {"filename": filename, "file_type": "duckdb", "tables": preview_data}
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
    """Upload a new DuckDB or Parquet file."""
    # Determine filename
    filename = custom_name if custom_name else file.filename
    if not filename:
        raise HTTPException(status_code=400, detail="Filename required")

    # Security: prevent path traversal
    if "/" in filename or "\\" in filename or ".." in filename:
        raise HTTPException(status_code=400, detail="Invalid filename")

    # Determine file type and ensure proper extension
    is_parquet = filename.endswith(".parquet")
    is_duckdb = filename.endswith(".duckdb")

    if not is_parquet and not is_duckdb:
        # Default to duckdb if no extension
        filename = filename + ".duckdb"
        is_duckdb = True

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

        # Validate the file based on type
        try:
            if is_parquet:
                # Validate parquet file
                test_conn = duckdb.connect(":memory:")
                test_conn.execute(f"SELECT * FROM read_parquet('{filepath}') LIMIT 1")
                test_conn.close()
            else:
                # Validate DuckDB file
                test_conn = duckdb.connect(filepath, read_only=True)
                test_conn.execute("SHOW TABLES")
                test_conn.close()
        except Exception as e:
            os.remove(filepath)
            file_type = "Parquet" if is_parquet else "DuckDB"
            raise HTTPException(status_code=400, detail=f"Invalid {file_type} file: {str(e)}")

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

# ============== Table-level operations (within a DuckDB file) ==============

@app.put("/api/v1/admin/files/{filename}/tables/{table_name}/rename", dependencies=[Depends(verify_api_key)])
@limiter.limit(RATE_LIMIT)
async def rename_table(filename: str, table_name: str, rename_req: TableRenameRequest, request: Request):
    """Rename a table within a DuckDB file."""
    new_table_name = rename_req.new_table_name

    # Security: prevent path traversal and SQL injection
    if "/" in filename or "\\" in filename or ".." in filename:
        raise HTTPException(status_code=400, detail="Invalid filename")

    # Validate table names (alphanumeric and underscores only)
    import re
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', table_name):
        raise HTTPException(status_code=400, detail="Invalid table name")
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', new_table_name):
        raise HTTPException(status_code=400, detail="Invalid new table name")

    filepath = os.path.join(DATA_DIR, filename)
    if not os.path.exists(filepath):
        raise HTTPException(status_code=404, detail=f"File '{filename}' not found")

    try:
        # Open connection in write mode
        conn = duckdb.connect(filepath, read_only=False)

        # Check if source table exists
        tables = [row[0] for row in conn.execute("SHOW TABLES").fetchall()]
        if table_name not in tables:
            conn.close()
            raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found in '{filename}'")

        # Check if target name already exists
        if new_table_name in tables:
            conn.close()
            raise HTTPException(status_code=409, detail=f"Table '{new_table_name}' already exists")

        # Rename the table
        conn.execute(f'ALTER TABLE "{table_name}" RENAME TO "{new_table_name}"')
        conn.close()

        # Update metadata
        metadata = load_metadata()
        metadata[filename] = rebuild_metadata_for_file(filename, filepath)
        save_metadata(metadata)

        logger.info(f"Renamed table in {filename}: {table_name} -> {new_table_name}")
        return {
            "message": f"Table renamed from '{table_name}' to '{new_table_name}'",
            "filename": filename,
            "old_table_name": table_name,
            "new_table_name": new_table_name
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error renaming table {table_name} in {filename}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/v1/admin/files/{filename}/tables/{table_name}", dependencies=[Depends(verify_api_key)])
@limiter.limit(RATE_LIMIT)
async def delete_table(filename: str, table_name: str, request: Request):
    """Delete a table from a DuckDB file."""
    # Security: prevent path traversal and SQL injection
    if "/" in filename or "\\" in filename or ".." in filename:
        raise HTTPException(status_code=400, detail="Invalid filename")

    # Validate table name (alphanumeric and underscores only)
    import re
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', table_name):
        raise HTTPException(status_code=400, detail="Invalid table name")

    filepath = os.path.join(DATA_DIR, filename)
    if not os.path.exists(filepath):
        raise HTTPException(status_code=404, detail=f"File '{filename}' not found")

    try:
        # Open connection in write mode
        conn = duckdb.connect(filepath, read_only=False)

        # Check if table exists
        tables = [row[0] for row in conn.execute("SHOW TABLES").fetchall()]
        if table_name not in tables:
            conn.close()
            raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found in '{filename}'")

        # Get row count before deletion for logging
        row_count = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]

        # Drop the table
        conn.execute(f'DROP TABLE "{table_name}"')
        conn.close()

        # Update metadata
        metadata = load_metadata()
        metadata[filename] = rebuild_metadata_for_file(filename, filepath)
        save_metadata(metadata)

        logger.info(f"Deleted table {table_name} ({row_count} rows) from {filename}")
        return {
            "message": f"Table '{table_name}' deleted from '{filename}'",
            "filename": filename,
            "deleted_table": table_name,
            "deleted_rows": row_count
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting table {table_name} from {filename}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============== Direct Upload with Token ==============

def cleanup_expired_tokens():
    """Remove expired tokens from storage."""
    current_time = time_module.time()
    expired = [token for token, data in UPLOAD_TOKENS.items()
               if current_time > data["expires_at"]]
    for token in expired:
        del UPLOAD_TOKENS[token]


class UploadTokenRequest(BaseModel):
    filename: str
    content_length: Optional[int] = None


@app.post("/api/v1/admin/upload-token", dependencies=[Depends(verify_api_key)])
@limiter.limit("20/hour")
async def generate_upload_token(token_req: UploadTokenRequest, request: Request):
    """
    Generate a temporary upload token for direct file upload.
    This allows bypassing the Vercel proxy for large files.
    Supports both DuckDB and Parquet files.
    """
    cleanup_expired_tokens()

    filename = token_req.filename

    # Security: prevent path traversal
    if "/" in filename or "\\" in filename or ".." in filename:
        raise HTTPException(status_code=400, detail="Invalid filename")

    # Determine file type and ensure proper extension
    if not filename.endswith(SUPPORTED_EXTENSIONS):
        # Default to duckdb if no supported extension
        filename = filename + ".duckdb"

    # Check file size limit
    if token_req.content_length and token_req.content_length > MAX_UPLOAD_SIZE:
        raise HTTPException(
            status_code=413,
            detail=f"File too large. Max size: {MAX_UPLOAD_SIZE // (1024*1024)}MB"
        )

    # Generate secure token
    token = secrets.token_urlsafe(32)
    expires_at = time_module.time() + UPLOAD_TOKEN_EXPIRY

    UPLOAD_TOKENS[token] = {
        "filename": filename,
        "content_length": token_req.content_length,
        "expires_at": expires_at,
        "created_at": time_module.time()
    }

    logger.info(f"Generated upload token for: {filename}")

    return {
        "token": token,
        "filename": filename,
        "expires_in": UPLOAD_TOKEN_EXPIRY,
        "upload_url": f"/api/v1/admin/upload-direct/{token}",
        "max_size_mb": MAX_UPLOAD_SIZE // (1024 * 1024)
    }


@app.options("/api/v1/admin/upload-direct/{token}")
async def upload_direct_options(token: str):
    """Handle CORS preflight for direct upload endpoint."""
    return JSONResponse(
        content={},
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "*",
        }
    )


def cors_json_response(content: dict, status_code: int = 200) -> JSONResponse:
    """Return JSONResponse with CORS headers."""
    return JSONResponse(
        content=content,
        status_code=status_code,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "*",
        }
    )


@app.post("/api/v1/admin/upload-direct/{token}")
@limiter.limit("20/hour")  # Generous limit since tokens are already rate-limited
async def upload_direct(
    token: str,
    request: Request,
    file: UploadFile = File(...)
):
    """
    Direct file upload using a pre-generated token.
    No API key required - the token itself is the authorization.
    """
    cleanup_expired_tokens()

    # Verify token
    if token not in UPLOAD_TOKENS:
        return cors_json_response({"detail": "Invalid or expired upload token"}, 401)

    token_data = UPLOAD_TOKENS[token]

    # Check expiry
    if time_module.time() > token_data["expires_at"]:
        del UPLOAD_TOKENS[token]
        return cors_json_response({"detail": "Upload token has expired"}, 401)

    filename = token_data["filename"]
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
                    del UPLOAD_TOKENS[token]
                    return cors_json_response(
                        {"detail": f"File too large. Max size: {MAX_UPLOAD_SIZE // (1024*1024)}MB"},
                        413
                    )
                f.write(chunk)

        # Validate the file based on type
        is_parquet = filename.endswith(".parquet")
        try:
            if is_parquet:
                # Validate parquet file
                test_conn = duckdb.connect(":memory:")
                test_conn.execute(f"SELECT * FROM read_parquet('{filepath}') LIMIT 1")
                test_conn.close()
            else:
                # Validate DuckDB file
                test_conn = duckdb.connect(filepath, read_only=True)
                test_conn.execute("SHOW TABLES")
                test_conn.close()
        except Exception as e:
            os.remove(filepath)
            del UPLOAD_TOKENS[token]
            file_type = "Parquet" if is_parquet else "DuckDB"
            return cors_json_response({"detail": f"Invalid {file_type} file: {str(e)}"}, 400)

        # Update metadata
        metadata = load_metadata()
        metadata[filename] = rebuild_metadata_for_file(filename, filepath)
        save_metadata(metadata)

        # Consume the token (one-time use)
        del UPLOAD_TOKENS[token]

        logger.info(f"Direct upload completed: {filename} ({total_size} bytes)")
        return cors_json_response({
            "message": "File uploaded successfully",
            "filename": filename,
            "size_bytes": total_size,
            "size_mb": round(total_size / (1024 * 1024), 2)
        })
    except Exception as e:
        # Cleanup on error
        if os.path.exists(filepath):
            os.remove(filepath)
        if token in UPLOAD_TOKENS:
            del UPLOAD_TOKENS[token]
        logger.error(f"Error in direct upload: {e}")
        return cors_json_response({"detail": str(e)}, 500)


# ============== File Sharing ==============

@app.post("/api/v1/admin/files/{filename}/share", dependencies=[Depends(verify_api_key)])
@limiter.limit(RATE_LIMIT)
async def share_file(filename: str, request: Request):
    """
    Create a shareable link for a file.
    Returns the share token which can be used to download the file without authentication.
    """
    # Security: prevent path traversal
    if "/" in filename or "\\" in filename or ".." in filename:
        raise HTTPException(status_code=400, detail="Invalid filename")

    filepath = os.path.join(DATA_DIR, filename)
    if not os.path.exists(filepath):
        raise HTTPException(status_code=404, detail=f"File '{filename}' not found")

    # Check if already shared
    existing_token = get_share_token_for_file(filename)
    if existing_token:
        return {
            "message": "File is already shared",
            "token": existing_token,
            "share_url": f"/s/{existing_token}",
            "filename": filename
        }

    # Generate new share token
    token = secrets.token_urlsafe(16)  # Shorter token for share URLs

    tokens = load_share_tokens()
    tokens[token] = {
        "filename": filename,
        "created_at": datetime.now().isoformat()
    }
    save_share_tokens(tokens)

    logger.info(f"Created share link for: {filename}")
    return {
        "message": "Share link created",
        "token": token,
        "share_url": f"/s/{token}",
        "filename": filename
    }


@app.delete("/api/v1/admin/files/{filename}/share", dependencies=[Depends(verify_api_key)])
@limiter.limit(RATE_LIMIT)
async def unshare_file(filename: str, request: Request):
    """
    Remove the shareable link for a file.
    """
    # Security: prevent path traversal
    if "/" in filename or "\\" in filename or ".." in filename:
        raise HTTPException(status_code=400, detail="Invalid filename")

    # Find and remove the share token
    token = get_share_token_for_file(filename)
    if not token:
        raise HTTPException(status_code=404, detail=f"File '{filename}' is not shared")

    tokens = load_share_tokens()
    del tokens[token]
    save_share_tokens(tokens)

    logger.info(f"Removed share link for: {filename}")
    return {
        "message": "Share link removed",
        "filename": filename
    }


@app.get("/s/{token}")
@limiter.limit("100/hour")
async def download_shared_file(token: str, request: Request):
    """
    Download a file using a share token.
    No authentication required - the token is the authorization.
    """
    filename = get_file_for_share_token(token)
    if not filename:
        raise HTTPException(status_code=404, detail="Invalid or expired share link")

    filepath = os.path.join(DATA_DIR, filename)
    if not os.path.exists(filepath):
        # File was deleted but share token still exists - clean it up
        tokens = load_share_tokens()
        if token in tokens:
            del tokens[token]
            save_share_tokens(tokens)
        raise HTTPException(status_code=404, detail="File no longer exists")

    logger.info(f"Shared download: {filename}")
    return FileResponse(
        filepath,
        media_type="application/octet-stream",
        filename=filename
    )


# ============== API Token Management Endpoints ==============

class CreateTokenRequest(BaseModel):
    name: str
    file_name: str
    permissions: str = 'read'  # 'read', 'write', 'admin'
    expires_days: Optional[int] = None


@app.post("/api/v1/tokens", dependencies=[Depends(verify_api_key)])
@limiter.limit(RATE_LIMIT)
async def create_token(token_req: CreateTokenRequest, request: Request):
    """
    Create a new API token for a specific file.
    The raw token is only returned once - store it securely!
    """
    # Validate permissions
    if token_req.permissions not in ['read', 'write', 'admin']:
        raise HTTPException(status_code=400, detail="Invalid permissions. Use 'read', 'write', or 'admin'")

    # Validate file exists
    filepath = os.path.join(DATA_DIR, token_req.file_name)
    if not os.path.exists(filepath):
        raise HTTPException(status_code=404, detail=f"File '{token_req.file_name}' not found")

    try:
        result = create_file_token(
            name=token_req.name,
            file_name=token_req.file_name,
            permissions=token_req.permissions,
            expires_days=token_req.expires_days
        )
        logger.info(f"Created API token '{token_req.name}' for file '{token_req.file_name}'")
        return result
    except Exception as e:
        logger.error(f"Error creating token: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/tokens", dependencies=[Depends(verify_api_key)])
@limiter.limit(RATE_LIMIT)
async def list_tokens(request: Request):
    """
    List all API tokens (without revealing the actual token values).
    """
    try:
        tokens = list_file_tokens()
        return {"tokens": tokens, "count": len(tokens)}
    except Exception as e:
        logger.error(f"Error listing tokens: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/tokens/{token_id}", dependencies=[Depends(verify_api_key)])
@limiter.limit(RATE_LIMIT)
async def get_token(token_id: int, request: Request):
    """
    Get details for a specific token (without the actual token value).
    """
    token = get_token_by_id(token_id)
    if not token:
        raise HTTPException(status_code=404, detail=f"Token with ID {token_id} not found")
    return token


@app.delete("/api/v1/tokens/{token_id}", dependencies=[Depends(verify_api_key)])
@limiter.limit(RATE_LIMIT)
async def delete_token(token_id: int, request: Request):
    """
    Delete an API token.
    """
    deleted = delete_file_token(token_id)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"Token with ID {token_id} not found")
    logger.info(f"Deleted API token with ID {token_id}")
    return {"message": f"Token {token_id} deleted successfully"}


@app.post("/api/v1/tokens/{token_id}/rotate", dependencies=[Depends(verify_api_key)])
@limiter.limit(RATE_LIMIT)
async def rotate_token(token_id: int, request: Request):
    """
    Rotate an API token - generates a new token value while keeping the same metadata.
    The new raw token is only returned once - store it securely!
    """
    result = rotate_file_token(token_id)
    if not result:
        raise HTTPException(status_code=404, detail=f"Token with ID {token_id} not found")
    logger.info(f"Rotated API token with ID {token_id}")
    return result


# ============== Per-File Query Endpoint (Token Auth) ==============

class FileQueryRequest(BaseModel):
    sql: str
    limit: Optional[int] = None


@app.post("/api/v1/files/{filename}/query")
@limiter.limit(RATE_LIMIT)
async def query_file(filename: str, query: FileQueryRequest, request: Request):
    """
    Execute a SQL query against a specific DuckDB file.
    Requires a file-specific API token (not the admin API key).
    Permission level determines what SQL operations are allowed.
    """
    # Security: prevent path traversal
    if "/" in filename or "\\" in filename or ".." in filename:
        raise HTTPException(status_code=400, detail="Invalid filename")

    # Get token from Authorization header
    auth = request.headers.get("Authorization")
    if not auth:
        raise HTTPException(
            status_code=401,
            detail="Missing Authorization header",
            headers={"WWW-Authenticate": "Bearer"},
        )

    scheme, _, raw_token = auth.partition(" ")
    if scheme.lower() != "bearer" or not raw_token:
        raise HTTPException(
            status_code=401,
            detail="Invalid auth scheme, use 'Bearer <token>'",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Verify token for this file
    token_info = verify_file_token(raw_token, filename)
    if not token_info:
        raise HTTPException(status_code=401, detail="Invalid or expired token for this file")

    # Validate SQL against permission level
    is_valid, error_msg = validate_sql_for_permission(query.sql, token_info['permissions'])
    if not is_valid:
        raise HTTPException(status_code=403, detail=error_msg)

    # Check file exists
    filepath = os.path.join(DATA_DIR, filename)
    if not os.path.exists(filepath):
        raise HTTPException(status_code=404, detail=f"File '{filename}' not found")

    try:
        # Open connection to the specific file
        conn = duckdb.connect(filepath, read_only=(token_info['permissions'] == 'read'))

        row_limit = min(query.limit or MAX_ROWS, MAX_ROWS)
        result = conn.execute(query.sql).fetchmany(row_limit + 1)
        columns = [desc[0] for desc in conn.description]

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

        conn.close()

        logger.info(f"Query executed on {filename} by token '{token_info['name']}' ({len(rows)} rows)")
        return {
            "columns": columns,
            "rows": rows,
            "row_count": len(rows),
            "truncated": truncated,
            "file": filename
        }

    except duckdb.Error as e:
        logger.error(f"DuckDB error querying {filename}: {e}")
        raise HTTPException(status_code=400, detail=f"Query error: {str(e)}")
    except Exception as e:
        logger.error(f"Error querying {filename}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Run with uvicorn
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
