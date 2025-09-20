# `docs/DEVELOPER_GUIDE.md`

```markdown
# Developer Guide

> Comprehensive developer documentation for the MCP server project.
>
> Location: `docs/DEVELOPER_GUIDE.md`

---

## Contents

1. Project overview & goals  
2. High-level architecture  
3. File / package map (what each piece does)  
4. Local development & environment setup  
5. Running the app (dev & prod)  
6. FastMCP & Registry integration (how tools are exposed)  
7. How to add a new tool (complete example)  
8. Ingestion & RAG workflow (CSV lifecycle)  
9. API adapter (HTTP endpoints / example)  
10. LangGraph / Agent integration notes  
11. Testing & CI recommendations  
12. Docker & deployment checklist  
13. Troubleshooting & common fixes  
14. Appendix — code snippets and templates

---

# 1. Project overview & goals

This repository implements an MCP-style server that:

- Exposes **tools** (CSV RAG, Weather, etc.) to agents/clients via FastMCP.
- Supports tools implemented in Python and structured so they can wrap external-language tools later.
- Uses **Postgres** for metadata and **Chroma** for vector embeddings.
- Is compatible with LangGraph and other MCP-aware orchestrators.
- Is containerized for production use.

Primary goals:
- Minimal changes to existing tool internals during migration to FastMCP.
- Robust startup initialization (advisory locks for ingestion).
- Extensible registry so new tools can be added with minimal steps.

---

# 2. High-level architecture

```

+--------------------------+
\|  Agents / LangGraph      |
+------------+-------------+
|
HTTP / STDIO
|
+-------v--------+
\|   FastMCP /    |
\|   Registry     | <- wrapper around FastMCP (src/app/tool/registry.py)
+-------+--------+
|
Tool wrappers / endpoints
|
+------------+-------------+
\|   Tools (CSV RAG, Weather)|
\|   - CsvRagTool (rag.py)   |
\|   - WeatherTool (weather.py) |
+------------+-------------+
|
+----------+----------+
\|  Services / DB / VS |
\|  - Postgres + Alembic|
\|  - Chroma vectorstore|
+---------------------+

````

Key flow:
- Agent calls tool (via HTTP or MCP transport).
- Registry maps call to wrapper function (registered via `Registry` shim).
- Wrapper invokes methods on the tool instance (e.g., `csv_tool.run(query)`).
- Tools use DB/Chroma/embeddings to respond.

---

# 3. File / package map (what each piece does)

Top-level important files:
- `main.py` — application entrypoint (bootstraps DB, vector store, registry, registers tools, runs server).
- `Dockerfile`, `docker-compose.yml` — containerization configuration.
- `.env` — runtime environment variables (not committed to git).

Key directories (`src/`):
- `src/app/agent/`  
  - `local_client.py`, `agent_tool.py` — local agent helpers and example agent-tool glue.
- `src/app/api/`  
  - `api.py`, `schemas.py` — optional HTTP adapter (FastAPI) for simple REST endpoints.
- `src/app/tool/`  
  - `registry.py` — **compatibility Registry shim** (wraps FastMCP). This is crucial; it provides `Registry.instance()` and registration helpers.
  - `tools/` — tool implementations (CSV RAG, Weather).
    - `csv_rag/`  
      - `loader.py`, `models.py`, `rag.py`, `schema.py`, `managers/*` — ingestion, embedding and query logic.
    - `weather/`  
      - `weather.py`, `schema.py` — city index, OpenWeather integration, normalization.
- `src/base/`  
  - `base_tool.py`, `vector_store.py`, `llm_base.py` — base abstractions for tools, LLMs, and vector stores.
- `src/config/`  
  - `db.py`, `settings.py`, `logger.py`, `vector_store.py` — environment config/DB/session factory and logging.
- `src/services/`  
  - `chromadb.py`, `embedding.py` — adapter to Chroma and embeddings.
- `src/helpers/` — utility functions (file, normalize, advisory locks, etc).
- `alembic/` — migrations for CSV metadata tables.
- `static/`  
  - `csv/fixed_civil_places.csv` — example CSV used for ingest.
  - `json/iran_cities.json` — city index for WeatherTool.

---

# 4. Local development & environment setup

## Prereqs
- Python 3.11+ (project uses `pyproject.toml` / `requirements.txt`)
- Docker & docker-compose (for local services)
- Postgres (if not using docker-compose)

## Quick setup

```bash
# Clone & cd
git clone <repo-url>
cd <repo>

# Create virtualenv and install
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Create .env (see template below)
cp .env.template .env
# edit .env to put DB/keys

# Start dependent services (Postgres, optional)
docker-compose up -d db

# Run alembic migrations (if using Postgres)
alembic upgrade head
````

### `.env.template` sample (put this in repo as `.env.template`)

```
# Database
DATABASE_URL=postgresql+asyncpg://app:password@db:5432/mcp

# Chroma
CHROMA_PERSIST_DIRECTORY=./chroma_data
CHROMA_COLLECTION=csv_rows

# FastMCP / server
PORT=8000
HOST=0.0.0.0
MCP_TRANSPORT=http

# Weather
WEATHER_API_KEY=
WEATHER_URL=https://api.openweathermap.org/data/2.5/weather
CITIES_JSON_PATH=./static/json/iran_cities.json
```

---

# 5. Running the app (dev & prod)

## Run locally (dev)

```bash
# With env vars set (source .env)
python main.py
# or for stdio quick smoke
MCP_TRANSPORT=stdio python main.py
```

The `main.py` in this repo:

* builds `Database()` from `src.config.db`
* constructs vector store via `src.services.chromadb.ChromaVectorStore` (you may need adapter)
* instantiates `CsvRagTool` and `WeatherTool`
* registers their methods into Registry (e.g., `csv_rag.query`, `csv_rag.ingest_folder`, `weather.get`)
* initializes tools by calling `initialize()` concurrently
* starts FastMCP server using configured transport

## Run as ASGI (production, behind uvicorn/gunicorn)

Create `app_asgi.py` (see Appendix). Then run:

```bash
uvicorn app_asgi:app --host 0.0.0.0 --port 8000 --workers 1
```

Alternatively use Gunicorn + Uvicorn workers for production. See Docker section.

---

# 6. FastMCP & Registry integration (how tools are exposed)

We use a **Registry shim** at `src/app/tool/registry.py`. It wraps `FastMCP` but provides convenient API expected by older code:

Key features:

* `Registry.instance(name)` — singleton returning registry.
* `register_instance_method(instance, method_name, name)` — registers `instance.method_name` as a tool with the given name.
* `register_function(func, name)` — register a plain function.
* `initialize_instances(instances)` — calls `initialize()` on instances that have it.
* `http_app()` — return ASGI app for uvicorn.

**Why a shim?**

* avoids refactoring old code that expected `Registry` symbol from `fastmcp`.
* keeps the project stable while using canonical `FastMCP` internally.

**Common registration examples (in `main.py`)**

```python
reg = Registry.instance("mcp-server")
reg.register_instance_method(csv_tool, method_name="run", name="csv_rag.query")
reg.register_instance_method(csv_tool, method_name="ingest_folder", name="csv_rag.ingest_folder")
reg.register_instance_method(csv_tool, method_name="initialize", name="csv_rag.initialize")
reg.register_instance_method(weather_tool, method_name="run", name="weather.get")
```

**Tool naming convention suggestion**

* `package.tool_name` for clarity, e.g. `csv_rag.query`, `weather.get`.

---

# 7. How to add a new tool (complete example)

Follow these steps to add a new tool cleanly.

## 1) Create folder

`src/app/tool/tools/my_tool/`

Files:

* `__init__.py`
* `schema.py` (Pydantic model for input/output)
* `my_tool.py` (implementation)

## 2) Implement the tool (example)

```python
# src/app/tool/tools/my_tool/my_tool.py
from typing import Any
from src.base.base_tool import BaseTool

class MyTool(BaseTool):
    def __init__(self, some_dep):
        self.some_dep = some_dep
        self._ready = False

    @property
    def name(self) -> str:
        return "my_tool"

    @property
    def description(self) -> str:
        return "Does something awesome."

    async def initialize(self):
        # optional startup work
        self._ready = True

    async def run(self, input_str: str) -> Any:
        # business logic (can be sync or async)
        return {"result": f"echo: {input_str}"}
```

## 3) Register the tool (in `main.py` or new `fastmcp_registry.py`)

```python
my_tool_instance = MyTool(dep)
reg.register_instance_method(my_tool_instance, method_name="run", name="my_tool.run")
reg.register_instance_method(my_tool_instance, method_name="initialize", name="my_tool.initialize")
```

## 4) Add API endpoint if you want HTTP access (optional)

Use `src/app/api/api.py` to add a FastAPI endpoint that calls the instance method (see Appendix for snippet).

## 5) Test

* Unit test calling the instance directly.
* Integration test using `fastmcp.Client` in-memory (see Testing section).

---

# 8. Ingestion & RAG workflow (CSV lifecycle)

1. **Scan** — `CSVFileManager.scan_folder(folder_path)` finds CSV files in `static/csv` or provided folder.
2. **Register** — `get_or_register_file(session, path)` stores metadata (status PENDING).
3. **Ingest** — `IngestManager.ingest_rows(session, CSVLoader.stream_csv_async(path), ...)` reads rows, calls embedding service and writes to Chroma.
4. **Mark DONE** — after success `file_mgr.mark_file_as_done(session, file_meta)` updates DB.
5. **Query** — `QueryManager.search(query, top_k)` runs vector similarity + any reranking & returns results.

**Concurrency / safety**

* Ingestion uses `advisory_lock` (Postgres advisory locks) to avoid duplicate ingestion across processes. This is why `CsvRagTool.initialize()` obtains a lock optionally to 'reserve' startup ingestion behavior.

**Best practices**

* Keep `CHROMA_PERSIST_DIRECTORY` on a volume in production.
* If you have multiple workers, rely on database-level advisory locks to prevent concurrent ingestion of same file.

---

# 9. API adapter (HTTP endpoints / example)

There are two ways to expose HTTP endpoints:

A. **Expose FastMCP's HTTP transport** (FastMCP provides an HTTP endpoint) — recommended for LangGraph and MCP clients.

B. **Create a simple FastAPI adapter** that maps REST endpoints to internal tool methods (useful for curl / Postman).

### FastAPI adapter snippet (put in `src/app/api/api.py`)

```python
# src/app/api/api.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from src.app.tool.registry import Registry
from typing import Any

app = FastAPI(title="MCP HTTP Adapter")

class CSVQuery(BaseModel):
    query: str
    top_k: int = 5

class WeatherQuery(BaseModel):
    city: str

@app.post("/query/csv")
async def query_csv(body: CSVQuery):
    reg = Registry.instance()
    # either call the csv tool instance directly or use the registered FastMCP tool
    # directly calling instance: import CsvRagTool instance where you created it (main.py)
    # for this snippet we call the registered FastMCP call via internal mcp client
    async with reg.mcp.client() as client:
        return await client.call_tool("csv_rag.query", {"query": body.query, "top_k": body.top_k})

@app.post("/query/weather")
async def query_weather(body: WeatherQuery):
    reg = Registry.instance()
    async with reg.mcp.client() as client:
        return await client.call_tool("weather.get", {"city": body.city})
```

**Note**: This uses FastMCP client API (in-process) to call registered tools; if your registry uses a different client signature, adapt accordingly.

### Example curl

```bash
curl -X POST http://localhost:8000/query/csv \
  -H "Content-Type: application/json" \
  -d '{"query":"civil places near Azadi Square", "top_k": 5}'
```

---

# 10. LangGraph / Agent integration notes

* Use the **HTTP transport** of the MCP server when integrating with LangGraph — it’s more natural to orchestrate via HTTP endpoints.
* For LangGraph, create a node or task that calls your MCP HTTP adapter endpoints (e.g., `/query/csv`) or calls FastMCP’s HTTP interface if you prefer direct MCP client integration.
* LangGraph can orchestrate multiple tools: e.g., a graph node calls `csv_rag.query`, another node calls `weather.get`, and a final agent node composes the answers.

---

# 11. Testing & CI recommendations

## Unit tests

* Test tool logic in isolation: call `CsvQueryManager.search`, `CSVLoader.stream_csv_async`, `WeatherTool._guess_city`.
* Put tests in `src/tests/`. Use `pytest` and `pytest-asyncio` for async tests.

## Integration tests

* Use FastMCP `Client` in-process to list tools and call them.
* Example `tests/test_fastmcp_smoke.py` skeleton:

```python
# src/tests/test_fastmcp_smoke.py
import pytest
import asyncio
from src.app.tool.registry import Registry

@pytest.mark.asyncio
async def test_health_ping():
    reg = Registry.instance("test")
    async with reg.mcp.client() as client:
        res = await client.call_tool("health.ping", {})
        assert res == {"status": "ok"}
```

## CI (GitHub Actions sample)

* Lint, unit tests, build docker, run smoke tests.
* Add matrix for python 3.11.

---

# 12. Docker & deployment checklist

### Dockerfile tips

* Use multi-stage build for dependencies requiring build tools.
* Use a non-root user for runtime.
* Copy only what's needed.

### docker-compose

* `db` service (Postgres).
* `mcp-server` service uses service name `db` in `DATABASE_URL`.

### Production notes

* Use `uvicorn` with Gunicorn workers (e.g., `gunicorn -k uvicorn.workers.UvicornWorker app_asgi:app`) for resilience.
* Mount volume for `CHROMA_PERSIST_DIRECTORY`.
* Provide secrets via environment (or a secrets manager) — never commit `.env` in git.

---

# 13. Troubleshooting & common fixes

### Error: `ImportError: cannot import name 'Registry' from 'fastmcp'`

* Cause: code expected `Registry` exported from package. Fix: use local shim `src/app/tool/registry.py` (already included) — make sure it's in `PYTHONPATH` and that you didn't accidentally install a different `fastmcp` version.
* Ensure `requirements.txt` pins `fastmcp>=2.0.0`.

### Chroma not persisting / missing files

* Ensure `CHROMA_PERSIST_DIRECTORY` exists and is mounted to container.
* Check file permissions and that service user can write.

### Alembic / DB migrations failing

* Ensure `DATABASE_URL` is correct and Postgres is reachable.
* Run `alembic upgrade head` manually to see errors.

### Ingestion stuck / duplicate ingestion

* Ensure advisory locks are working (same DB connection type, Postgres supports advisory locks).
* Check logs: `logs/app.log`.

---

# 14. Appendix — code snippets & templates

## `app_asgi.py` (ASGI entrypoint for uvicorn/gunicorn)

```python
# app_asgi.py
from src.app.tool.registry import Registry
from src.config.db import Database
from src.services.chromadb import ChromaVectorStore
from src.app.tool.fastmcp_registry import register_tools  # optional if you have fastmcp_registry

# Build infra
db = Database()
vs = ChromaVectorStore(collection_name="csv_rows")

reg = Registry.instance("mcp-server")
# If you have register_tools utility that registers all tools, call it.
# Otherwise ensure tools are registered in main.py and Registry is populated.

app = reg.http_app()
```

## `main.py` (already in repo) — ensure it follows the shim pattern described above.

## Example unit test (pytest + asyncio)

(See section 11.)

---

# Final notes

* The design purposefully **keeps your tool internals untouched** and adds a thin wrapper (Registry shim + main entrypoint) so migrating to FastMCP is low-risk.
* For production, prefer ASGI transport + uvicorn/gunicorn. The MCP HTTP transport is useful for direct FastMCP clients and LangGraph.
* If you want, I can generate the actual `app_asgi.py` + `src/app/api/api.py` files, plus a GitHub Actions workflow and a production-ready `gunicorn` run command. Tell me which one you'd like first and I'll produce exact files.

````

---

# `README.md`
```markdown
# Municipality MCP Server

AI-powered MCP server for municipal services (CSV RAG + Weather + Tools).  
Exposes tools via FastMCP and an optional HTTP adapter. Ready for Docker deployment.

---

## Quick summary

- **CSV RAG**: Ingest municipal CSVs, compute embeddings, and query via vector similarity.  
- **Weather**: Lookup normalized city names and fetch current weather via OpenWeather API.
- **Vector store**: Chroma (duckdb+parquet) persisted under `chroma_data/`.
- **DB**: Postgres for file metadata; migrations via Alembic.
- **Server**: FastMCP with a `Registry` shim to keep compatibility with project structure.
- **Transport**: HTTP (recommended) or STDIO (dev/smoke test).
- **LangGraph**: Compatible (use HTTP interface to integrate).

---

## Quick start (local with Docker Compose)

1. Copy `.env.template` to `.env` and fill secrets:
   ```bash
   cp .env.template .env
   # edit .env to add DB credentials and WEATHER_API_KEY
````

2. Build and start:

   ```bash
   docker-compose up --build
   ```

3. Apply migrations (if needed):

   ```bash
   docker-compose exec mcp-server alembic upgrade head
   ```

4. Test endpoints:

   * REST (if FastAPI adapter is enabled):

     ```bash
     curl -X POST http://localhost:8000/query/csv \
       -H "Content-Type: application/json" \
       -d '{"query":"civil places near Azadi Square","top_k":5}'
     ```

   * FastMCP health:
     Use in-process client or the FastMCP HTTP endpoint. Example (in Python):

     ```python
     from fastmcp import Client
     # create a Client pointing to server or use Registry.instance().mcp for in-process
     ```

---

## Environment variables

Put these in `.env` (or pass through your container orchestration system).

* `DATABASE_URL` — Postgres (asyncpg) connection string.
* `CHROMA_PERSIST_DIRECTORY` — path for Chroma to persist duckdb/parquet files.
* `CHROMA_COLLECTION` — collection name (default `csv_rows`).
* `WEATHER_API_KEY` — OpenWeather API key (optional; WeatherTool will warn if missing).
* `WEATHER_URL` — OpenWeather endpoint (default `https://api.openweathermap.org/data/2.5/weather`).
* `CITIES_JSON_PATH` — path to `static/json/iran_cities.json`.
* `PORT`, `HOST`, `MCP_TRANSPORT` — server config. `MCP_TRANSPORT=http|stdio`.

---

## Adding CSV data

Place CSV files under `static/csv/` (or any folder you choose and pass path to `csv_rag.ingest_folder`). Example CSV included: `static/csv/fixed_civil_places.csv`.

To ingest:

* Call the tool endpoint `csv_rag.ingest_folder(folder_path="/app/static/csv", batch_size=512)` or use the HTTP adapter `/query/ingest` if available.

---

## How to add a tool (short)

1. Add folder `src/app/tool/tools/<name>/`.
2. Implement a class inheriting `BaseTool`, with `name`, `description`, `initialize()` (optional), and `run(...)`.
3. Register it in `main.py` using `Registry.instance().register_instance_method(...)`.
4. Optionally expose REST endpoint in `src/app/api/api.py`.

---

## Logs & monitoring

* Application logs are under `logs/app.log`.
* For production, integrate with a log collector (Fluentd/ELK/Prometheus).
* Add Prometheus metrics (instrument the FastMCP endpoints and heavy operations).

---

## Troubleshooting

* `ImportError: cannot import name 'Registry'` — ensure you have the project-local `src/app/tool/registry.py` shim and that `fastmcp>=2.0.0` is installed.
* Chroma persistence issues — verify `CHROMA_PERSIST_DIRECTORY` is mounted and writable.
* DB connection issues — check `DATABASE_URL` and that Postgres service is up.

---

## Development resources

* Developer guide: `docs/DEVELOPER_GUIDE.md` (detailed instructions, tests, CI)
* Tests: `src/tests/` — run `pytest` inside virtual env.

---

## Contact & maintenance

* Project owner: Municipality of Isfahan (internal)
* For quick changes, add a new branch and open a PR. Use CI checks (linters + tests).

---

## License

Proprietary (Municipality of Isfahan). Do not redistribute without permission.

```

