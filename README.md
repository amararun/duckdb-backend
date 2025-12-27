# DuckDB Cricket Backend

Read-only FastAPI server for querying cricket analytics data stored in DuckDB.

## Endpoints

| Endpoint | Method | Auth | Description |
|----------|--------|------|-------------|
| `/health` | GET | No | Health check |
| `/api/v1/tables` | GET | Yes | List available tables |
| `/api/v1/schema/{table}` | GET | Yes | Get table schema |
| `/api/v1/query` | POST | Yes | Execute SQL query |

## Setup

1. Copy `.env.example` to `.env` and set your API key
2. Place your DuckDB file in `data/`
3. Install dependencies: `pip install -r requirements.txt`
4. Run: `uvicorn main:app --host 0.0.0.0 --port 8000`

## Coolify Deployment

**Start Command:**
```
uvicorn main:app --host 0.0.0.0 --port 8000
```

**Environment Variables:**
- `API_KEY` - Required
- `DATA_PATH` - Path to DuckDB file (default: `./data/odi_t20.duckdb`)

## Query Example

```bash
curl -X POST https://your-api.com/api/v1/query \
  -H "Authorization: Bearer YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM odi_t20 LIMIT 10"}'
```
