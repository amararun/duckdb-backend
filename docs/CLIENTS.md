# DuckDB Backend - Client Applications

**Last Updated:** 2026-01-01

This document tracks all applications that access this backend. **Update this when adding new clients or modifying how existing clients access the backend.**

---

## Current Clients

### 1. Cricket Dashboard App

| Property | Value |
|----------|-------|
| **Repo Location** | `C:\AMARDATA\GITHUB\DUCKDB_CRICKET_APP` |
| **Deployed URL** | `https://duckdb-cricket-app.tigzig.com` |
| **Vercel Project** | `duckdb-cricket-app` |

#### How It Accesses the Backend

**Flow:**
```
React Component → Vercel Proxy → Backend API
```

**Vercel Proxy File:** `api/duckdb.ts`

The proxy:
1. Receives requests from React frontend at `/api/duckdb`
2. Adds `Authorization: Bearer {DUCKDB_BACKEND_API_KEY}` header
3. Forwards to `https://duckdb-backend.tigzig.com`
4. Returns response to frontend

**Environment Variables (Vercel):**
```
DUCKDB_BACKEND_URL=https://duckdb-backend.tigzig.com
DUCKDB_BACKEND_API_KEY=cricket-dashboard-2024-tigzig
```

#### Endpoints Used

| Endpoint | Purpose |
|----------|---------|
| `POST /api/v1/query` | Execute SQL queries for dashboard charts |
| `GET /api/v1/tables` | List available tables |
| `GET /api/v1/schema/{table}` | Get table structure |

#### Frontend Service

**File:** `src/api/duckdb.ts`

Key functions:
- `executeQuery(sql)` - Run SQL queries
- `getTables()` - List tables
- `getSchema(tableName)` - Get column info

#### Impact of Backend Changes

When modifying backend, check:

1. **Query endpoint changes** - Will break all dashboard pages
2. **Schema endpoint changes** - Will break table explorer
3. **Response format changes** - Will break data parsing
4. **Auth changes** - Will need to update Vercel env vars

---

### 2. DuckDB Admin App

| Property | Value |
|----------|-------|
| **Repo Location** | `C:\AMARDATA\GITHUB\DUCKDB_ADMIN` |
| **Deployed URL** | `https://duckadmin.tigzig.com` |
| **Vercel Project** | `duckdb-admin` |

#### How It Accesses the Backend

Same proxy pattern as Cricket Dashboard.

**Vercel Proxy File:** `api/duckdb.ts`

**Environment Variables (Vercel):**
```
DUCKDB_BACKEND_URL=https://duckdb-backend.tigzig.com
DUCKDB_BACKEND_API_KEY=cricket-dashboard-2024-tigzig
```

#### Endpoints Used

| Endpoint | Purpose |
|----------|---------|
| `GET /api/v1/admin/files` | List all DuckDB files |
| `GET /api/v1/admin/files/{file}/preview` | Preview file contents |
| `POST /api/v1/admin/files/upload` | Upload new files |
| `DELETE /api/v1/admin/files/{file}` | Delete files |
| `PUT /api/v1/admin/files/{file}/rename` | Rename files |
| `GET /api/v1/admin/files/{file}/download` | Download files |
| `POST /api/v1/admin/files/{file}/share` | Generate share link |
| `DELETE /api/v1/admin/files/{file}/share` | Remove share link |
| `PUT /api/v1/admin/files/{file}/tables/{table}/rename` | Rename table |
| `DELETE /api/v1/admin/files/{file}/tables/{table}` | Delete table |
| `POST /api/v1/admin/upload-token` | Get direct upload token |

#### Authentication

Uses **Clerk** for user authentication (separate from API key auth):
- Only users with `role: "admin"` in Clerk metadata can access
- Clerk instance: `tigzig.com`
- See `claude-platform-access/clerk.md` for setup details

#### Impact of Backend Changes

When modifying backend, check:

1. **Admin file endpoints** - Will break file management UI
2. **Share link endpoints** - Will break file sharing feature
3. **Upload endpoints** - Will break file upload
4. **Response format changes** - Will break UI parsing

---

## Adding a New Client

When adding a new application that accesses this backend:

1. **Add entry to this document** with:
   - Repo location
   - Deployed URL
   - How it accesses the backend
   - Which endpoints it uses

2. **Set up Vercel proxy** (recommended):
   - Copy `api/duckdb.ts` from existing app
   - Add env vars: `DUCKDB_BACKEND_URL`, `DUCKDB_BACKEND_API_KEY`

3. **Or use direct access** (if needed):
   - Store API key securely (never in frontend code)
   - Add CORS origin to backend if needed

---

## API Key Management

**Current API Key:** `cricket-dashboard-2024-tigzig`

All current clients share the same API key. If you need per-client keys:

1. Modify `app.py` to support multiple keys
2. Update each client's Vercel env vars
3. Document which client uses which key

---

## Checking Impact Before Backend Changes

Before modifying the backend:

1. **Check this document** for all affected clients
2. **Search for endpoint usage** in each client repo
3. **Test changes** against each client
4. **Update clients** if response format changes
5. **Deploy backend first**, then update clients if needed
