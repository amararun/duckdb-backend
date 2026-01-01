# DuckDB Backend - Architecture

**Last Updated:** 2026-01-01

## Overview

A **generic FastAPI server** that provides REST API access to DuckDB databases. Not cricket-specific - can host any DuckDB files. Deployed on Hetzner VPS via Coolify, accessed through Vercel proxy.

```
Frontend (React/Vercel)
        ↓
Vercel Proxy (/api/duckdb.ts)  ← Holds BACKEND_API_KEY
        ↓
FastAPI Backend (Hetzner/Coolify)
        ↓
DuckDB Files (/data/*.duckdb)
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
├── app.py              # Main FastAPI application
├── requirements.txt    # Python dependencies
├── .env.example        # Environment variables template
├── README.md
├── docs/
│   ├── ARCHITECTURE.md # This file
│   └── CLIENTS.md      # Apps accessing this backend
└── data/
    ├── *.duckdb        # Database files
    ├── file_metadata.json    # Cached file/table info
    └── share_tokens.json     # Public share tokens
```

## Authentication

### API Key Authentication

All protected endpoints require Bearer token authentication:

```bash
Authorization: Bearer YOUR_API_KEY
```

**Current API Key:** `cricket-dashboard-2024-tigzig`

### Security Implementation

- Uses `secrets.compare_digest()` for constant-time comparison (prevents timing attacks)
- API key stored in environment variable `API_KEY`
- Frontend never sees the key - Vercel proxy holds it

### Platform Access Credentials

For Clerk, Cloudflare, and other platform credentials used by AI coders:

**Location:** `C:\AMARDATA\GITHUB\RBICC_AUTH_EMBED\projects\claude-platform-access\`

Files:
- `clerk.md` - Clerk authentication setup, API access, DNS requirements
- `cloudflare.md` - Cloudflare DNS API access, zone IDs, tokens
- `vercel.md` - Vercel deployment access
- `coolify.md` - Coolify server access

## API Endpoints

### Health Check (No Auth)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |

### Query Endpoints (Auth Required)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/tables` | GET | List all tables |
| `/api/v1/schema/{table}` | GET | Get table schema |
| `/api/v1/query` | POST | Execute SQL query (SELECT/WITH only) |

### Admin File Management (Auth Required)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/admin/files` | GET | List all DuckDB files |
| `/api/v1/admin/files/{filename}/preview` | GET | Preview file data |
| `/api/v1/admin/files/upload` | POST | Upload new file |
| `/api/v1/admin/files/{filename}` | DELETE | Delete file |
| `/api/v1/admin/files/{filename}/rename` | PUT | Rename file |
| `/api/v1/admin/files/{filename}/download` | GET | Download file |

### Table Operations (Auth Required)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/admin/files/{filename}/tables/{table}/rename` | PUT | Rename table |
| `/api/v1/admin/files/{filename}/tables/{table}` | DELETE | Delete table |

### File Sharing

| Endpoint | Method | Auth | Description |
|----------|--------|------|-------------|
| `/api/v1/admin/files/{filename}/share` | POST | Yes | Generate share link |
| `/api/v1/admin/files/{filename}/share` | DELETE | Yes | Remove share link |
| `/s/{token}` | GET | No | Download via share token |

### Direct Upload (For Large Files)

| Endpoint | Method | Auth | Description |
|----------|--------|------|-------------|
| `/api/v1/admin/upload-token` | POST | Yes | Generate upload token (10-min expiry) |
| `/api/v1/admin/upload-direct/{token}` | POST | No | Upload using token |

## Query Safety

Only allows read operations:
- `SELECT` statements
- `WITH` (CTE) statements

Blocks dangerous keywords:
- INSERT, UPDATE, DELETE, DROP, CREATE, ALTER, TRUNCATE

Row limit: 10,000 (configurable via `MAX_ROWS`)

## Environment Variables

```bash
API_KEY=your-secure-api-key       # REQUIRED
DATA_PATH=./data/cricket.duckdb   # Default database file
DATA_DIR=./data                   # Directory for all DB files
MAX_UPLOAD_SIZE=500000000         # 500MB default
RATE_LIMIT=100/hour               # Rate limiting
MAX_ROWS=10000                    # Max rows per query
LOG_LEVEL=INFO
CORS_ORIGINS=*
```

## Rate Limiting

- Default: 100 requests/hour per IP
- Uses `slowapi` library
- Returns 429 when exceeded

## Metadata Caching

File metadata cached in `data/file_metadata.json`:
- File size, modification time
- Tables and their schemas
- Row counts per table

Auto-rebuilds on startup if missing or outdated.

## Deployment (Coolify)

**Coolify App UUID:** `b8ogo4k4ckwcckwos8ck4c4w`

**Start Command:**
```bash
uvicorn app:app --host 0.0.0.0 --port 8000 --timeout-keep-alive 180
```

**Required Environment Variables:**
- `API_KEY` - Authentication key

**To redeploy:**
```bash
curl -s -k -X GET "https://hosting.tigzig.com/api/v1/deploy?uuid=b8ogo4k4ckwcckwos8ck4c4w" \
  -H "Authorization: Bearer <COOLIFY_TOKEN>" \
  -H "Accept: application/json"
```

## Adding New Endpoints

When adding new endpoints:

1. **Protected endpoints** - Use `Depends(verify_api_key)`:
```python
@app.get("/api/v1/new-endpoint")
async def new_endpoint(request: Request, _: None = Depends(verify_api_key)):
    pass
```

2. **Public endpoints** - No dependency:
```python
@app.get("/public-endpoint")
async def public_endpoint():
    pass
```

3. **Update CLIENTS.md** if endpoint is used by a client app

## Error Codes

| Code | Meaning |
|------|---------|
| 401 | Missing Authorization header |
| 403 | Invalid API key |
| 400 | Invalid SQL / bad request |
| 404 | Table/file not found |
| 413 | File too large |
| 429 | Rate limit exceeded |
| 500 | Server error |

## Sample Database Schema (cricket.duckdb)

### `ball_by_ball` (~4.4M rows)
Every ball in cricket matches:
- `match_id`, `start_date`, `match_type`
- `venue`, `batting_team`, `bowling_team`, `innings`
- `striker`, `non_striker`, `bowler`
- `runs_off_bat`, `extras`, `wides`, `noballs`
- `wicket_type`, `player_dismissed`

### `match_info` (~8,762 rows)
Match metadata:
- `match_id`, `match_type`, `start_date`, `venue`
- `team1`, `team2`, `winner`
- `winner_runs`, `winner_wickets`
