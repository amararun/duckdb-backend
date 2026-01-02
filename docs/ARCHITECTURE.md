# DuckDB Backend - Architecture

**Last Updated:** 2026-01-02

## Overview

A **generic FastAPI server** that provides REST API access to DuckDB databases and Parquet files. Supports both master admin access and per-file API tokens with permission levels. Deployed on Hetzner VPS via Coolify.

```
Admin Frontend (React/Vercel)
        ↓
Vercel Proxy (/api/duckdb.ts)  ← Holds Master API Key
        ↓
FastAPI Backend (Hetzner/Coolify)
        ↓
DuckDB/Parquet Files (/data/*.duckdb, *.parquet)

Client Apps → Per-file API Token → FastAPI Backend
```

## Repository

- **GitHub:** `https://github.com/amararun/duckdb-backend`
- **Local:** `C:\AMARDATA\GITHUB\DUCKDB_BACKEND`
- **Backend URL:** `https://duckdb-backend.tigzig.com`
- **Upload URL:** `https://duckdb-upload.tigzig.com` (bypasses Cloudflare for large files)
- **Coolify App UUID:** `b8ogo4k4ckwcckwos8ck4c4w`

## Directory Structure

```
DUCKDB_BACKEND/
├── app.py              # Main FastAPI application (~1450 lines)
├── requirements.txt    # Python dependencies
├── .env.example        # Environment variables template
├── README.md
├── docs/
│   ├── ARCHITECTURE.md # This file
│   └── CLIENTS.md      # Apps accessing this backend
└── data/
    ├── *.duckdb        # DuckDB database files
    ├── *.parquet       # Parquet data files
    ├── file_metadata.json    # Cached file/table metadata
    ├── share_tokens.json     # Public share tokens
    └── tokens.db             # SQLite - per-file API tokens (bcrypt hashed)
```

---

## Authentication Models

### 1. Master Admin Key

- Stored in env var `API_KEY`
- Full access to all admin endpoints
- Used by admin frontend via Vercel proxy
- Never exposed to browser

```bash
Authorization: Bearer YOUR_MASTER_API_KEY
```

### 2. Per-file API Tokens

- Stored in SQLite (`data/tokens.db`) with bcrypt hashes
- Scoped to specific file + permission level
- Created via admin UI or API
- Can be rotated, disabled, or have expiration
- Raw token only shown once on creation

**Permission Levels:**

| Level | Allowed SQL Operations |
|-------|------------------------|
| `read` | SELECT, WITH only |
| `write` | SELECT, WITH, INSERT, UPDATE |
| `admin` | All except DROP DATABASE |

```bash
Authorization: Bearer YOUR_FILE_TOKEN
```

---

## API Endpoints

### Health Check (No Auth)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |

---

### Master Query (Admin Key Only)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/admin/query` | POST | Execute any SQL - DuckDB resolves file paths from SQL |

**SQL Syntax Examples:**

```sql
-- Query DuckDB file
SELECT * FROM '/data/myfile.duckdb'.table_name LIMIT 100

-- Query Parquet file
SELECT * FROM read_parquet('/data/file.parquet') LIMIT 100

-- Cross-file join
SELECT a.*, b.*
FROM '/data/db1.duckdb'.users a
JOIN read_parquet('/data/orders.parquet') b ON a.id = b.user_id

-- Aggregations across files
SELECT COUNT(*) FROM '/data/analytics.duckdb'.events
WHERE event_date > '2024-01-01'
```

**Request:**
```json
{
  "sql": "SELECT * FROM '/data/file.duckdb'.table_name LIMIT 10",
  "limit": 1000
}
```

**Response:**
```json
{
  "columns": ["id", "name", "value"],
  "rows": [[1, "test", 100], ...],
  "row_count": 10,
  "truncated": false
}
```

---

### Per-file Query (File Token)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/files/{filename}/query` | POST | Query specific file with permission validation |

- Token must be valid for the specified file
- SQL validated against token's permission level
- No file path syntax needed - queries run against the bound file

**Request:**
```json
{
  "sql": "SELECT * FROM my_table LIMIT 100",
  "limit": 1000
}
```

---

### Admin File Management (Admin Key)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/admin/files` | GET | List all files with metadata |
| `/api/v1/admin/files/{filename}/preview` | GET | Preview file contents (first 100 rows per table) |
| `/api/v1/admin/files/upload` | POST | Upload new file (.duckdb or .parquet) |
| `/api/v1/admin/files/{filename}` | DELETE | Delete file |
| `/api/v1/admin/files/{filename}/rename` | PUT | Rename file |
| `/api/v1/admin/files/{filename}/download` | GET | Download file |
| `/api/v1/admin/files/{filename}/refresh-metadata` | POST | Refresh cached metadata |

**List Files Response:**
```json
{
  "files": [
    {
      "name": "data.duckdb",
      "size_bytes": 1048576,
      "size_mb": 1.0,
      "row_count": 50000,
      "file_type": "duckdb",
      "tables": ["users", "orders"],
      "table_row_counts": {"users": 1000, "orders": 49000},
      "updated_at": "2026-01-02T10:00:00",
      "share_token": null
    },
    {
      "name": "analytics.parquet",
      "size_bytes": 2097152,
      "size_mb": 2.0,
      "row_count": 100000,
      "file_type": "parquet",
      "tables": ["_data"],
      "columns": ["event_id", "event_type", "timestamp"],
      "updated_at": "2026-01-02T10:00:00",
      "share_token": "abc123"
    }
  ],
  "count": 2
}
```

---

### Table Operations (Admin Key, DuckDB Only)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/admin/files/{filename}/tables/{table}/rename` | PUT | Rename table |
| `/api/v1/admin/files/{filename}/tables/{table}` | DELETE | Delete table |

---

### File Sharing

| Endpoint | Method | Auth | Description |
|----------|--------|------|-------------|
| `/api/v1/admin/files/{filename}/share` | POST | Admin | Generate share link |
| `/api/v1/admin/files/{filename}/share` | DELETE | Admin | Remove share link |
| `/s/{token}` | GET | None | Public download via share token |

---

### Token Management (Admin Key)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/tokens` | POST | Create new per-file token |
| `/api/v1/tokens` | GET | List all tokens |
| `/api/v1/tokens/{id}` | GET | Get token details |
| `/api/v1/tokens/{id}` | DELETE | Delete token |
| `/api/v1/tokens/{id}/rotate` | POST | Rotate token (generates new value) |

**Create Token Request:**
```json
{
  "name": "My App Token",
  "file_name": "mydata.duckdb",
  "permissions": "read",
  "expires_days": 30
}
```

**Create Token Response (token shown only once):**
```json
{
  "id": 1,
  "name": "My App Token",
  "token": "abc123...",
  "file_name": "mydata.duckdb",
  "permissions": "read",
  "expires_at": "2026-02-01T10:00:00"
}
```

---

### Direct Upload (For Large Files)

| Endpoint | Method | Auth | Description |
|----------|--------|------|-------------|
| `/api/v1/admin/upload-token` | POST | Admin | Generate upload token (10-min expiry) |
| `/api/v1/admin/upload-direct/{token}` | POST | Token | Upload using token |

Use `https://duckdb-upload.tigzig.com` for direct uploads to bypass Cloudflare's 100MB limit.

---

## Supported File Types

| Extension | Type | Description |
|-----------|------|-------------|
| `.duckdb` | DuckDB | Database files with multiple tables |
| `.parquet` | Parquet | Single-dataset columnar files |

For Parquet files:
- Tables list contains `["_data"]` (pseudo-table name)
- Additional `columns` field lists column names
- Preview shows data under `_data` key

---

## Query Safety (Per-file Tokens)

Permission levels control allowed SQL operations:

**Read Permission:**
- Only `SELECT` and `WITH` (CTE) statements

**Write Permission:**
- `SELECT`, `WITH`, `INSERT`, `UPDATE`

**Admin Permission:**
- All operations except `DROP DATABASE`

Row limit: 10,000 (configurable via `MAX_ROWS`)

---

## Environment Variables

```bash
API_KEY=your-master-api-key       # REQUIRED - Master admin key
DATA_DIR=./data                   # Directory for all files
MAX_UPLOAD_SIZE=524288000         # 500MB default
RATE_LIMIT=100/hour               # Rate limiting
MAX_ROWS=10000                    # Max rows per query
LOG_LEVEL=INFO
CORS_ORIGINS=*
```

---

## Rate Limiting

- Default: 100 requests/hour per IP
- Uses `slowapi` library
- Returns 429 when exceeded

---

## Metadata Caching

File metadata cached in `data/file_metadata.json`:
- File size, modification time
- Tables and their schemas (or columns for Parquet)
- Row counts per table

Auto-rebuilds on startup if missing or outdated.

---

## Token Storage

Per-file tokens stored in `data/tokens.db` (SQLite):
- Tokens bcrypt-hashed (never stored plain)
- Tracks: name, file_name, permissions, created_at, last_used_at, expires_at, enabled
- Raw token only returned on creation/rotation

---

## Deployment (Coolify)

**Coolify App UUID:** `b8ogo4k4ckwcckwos8ck4c4w`

**Start Command:**
```bash
uvicorn app:app --host 0.0.0.0 --port 8000 --timeout-keep-alive 180
```

**Required Environment Variables:**
- `API_KEY` - Master authentication key

**To redeploy:**
```bash
curl -s -k -X GET "https://hosting.tigzig.com/api/v1/deploy?uuid=b8ogo4k4ckwcckwos8ck4c4w" \
  -H "Authorization: Bearer <COOLIFY_TOKEN>" \
  -H "Accept: application/json"
```

---

## Adding New Endpoints

**Protected endpoints (Admin Key):**
```python
@app.get("/api/v1/admin/new-endpoint", dependencies=[Depends(verify_api_key)])
@limiter.limit(RATE_LIMIT)
async def new_admin_endpoint(request: Request):
    pass
```

**Protected endpoints (File Token):**
```python
@app.get("/api/v1/files/{filename}/new-endpoint", dependencies=[Depends(verify_file_token)])
async def new_file_endpoint(filename: str, request: Request):
    pass
```

**Public endpoints:**
```python
@app.get("/public-endpoint")
async def public_endpoint():
    pass
```

---

## Error Codes

| Code | Meaning |
|------|---------|
| 400 | Invalid SQL / bad request |
| 401 | Missing Authorization header |
| 403 | Invalid API key or token / insufficient permissions |
| 404 | Table/file not found |
| 413 | File too large |
| 429 | Rate limit exceeded |
| 500 | Server error |

---

## Client Apps

See [CLIENTS.md](./CLIENTS.md) for apps connecting to this backend.

| App | Auth Method |
|-----|-------------|
| DuckDB Admin | Master key via Vercel proxy |
| Client apps | Per-file tokens |
