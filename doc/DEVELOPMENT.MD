# DEVELOPMENT.md — Developer & Scaling Guide

> Purpose: detailed developer handbook for the MCP Server.
> This document assumes you already read `README.md` (project intro). Here we go deep — how the system is implemented, how tools are added, how ingestion/embedding/worker flows work, how to test, deploy and scale safely, and common operational runbooks.

---

## Table of contents

1. [High-level architecture recap](#high-level-architecture-recap)
2. [Core concepts & contracts](#core-concepts--contracts)
3. [How tools are implemented (step-by-step)](#how-tools-are-implemented-step-by-step)
4. [Lazy loading & registry lifecycle](#lazy-loading--registry-lifecycle)
5. [CSV RAG pipeline internals](#csv-rag-pipeline-internals)
6. [Embedding pipeline & model considerations](#embedding-pipeline--model-considerations)
7. [Celery worker design (ingest & run tasks)](#celery-worker-design-ingest--run-tasks)
8. [Vector store (Chroma) & metadata linking (Postgres)](#vector-store-chroma--metadata-linking-postgres)
9. [Testing strategy (unit, integration, E2E)](#testing-strategy-unit-integration-e2e)
10. [Local dev & debugging recipes](#local-dev--debugging-recipes)
11. [Docker / Compose → Production recommendations](#docker--compose---production-recommendations)
12. [Kubernetes / Cloud deployment notes (scale)](#kubernetes--cloud-deployment-notes-scale)
13. [Database & vector store maintenance / backups](#database--vector-store-maintenance--backups)
14. [Security & access control](#security--access-control)
15. [CI / CD and testing pipelines](#ci--cd-and-testing-pipelines)
16. [Troubleshooting & FAQ](#troubleshooting--faq)
17. [Appendix: Useful commands & snippets](#appendix-useful-commands--snippets)

---

## High-level architecture recap

* **FastMCP server** (ASGI) exposes tools via MCP protocol (streaming SSE or stateless JSON mode).
* **Tools layer** (`src/app/tool/tools/*`) — each tool implements `BaseTool` contract (`initialize`, `run`, optionally `ingest`/`ingest_folder`).
* **Registry** (`src/app/tool/registry.py`) registers tool wrappers with FastMCP and provides an ASGI app.
* **LazyToolWrapper / factories** — tools are created on first use to save startup cost. Factories live in `src/app/tool/__init__.py`.
* **Celery + Redis** — perform heavy/background tasks: embeddings, large folder ingestion, long-running RAG jobs. Locking (RedisLock) ensures idempotency.
* **Postgres** — metadata store for files & rows (tables `csv_files`, `csv_rows`).
* **Chroma** — local persistent vector store, vectors contain metadata (row\_id).
* **LLM (Qwen via Ollama)** — used by agents; LangGraph can orchestrate LLM + tool flows using MCP SSE transport.

---

## Core concepts & contracts

### `BaseTool` contract (what every tool must support)

* `name` (property) — short unique tool name (e.g. `csv_rag`).
* `description` (property) — human readable.
* `async initialize(self)` — optional: warmup, index load, locks. Called periodically at startup or before first run.
* `async run(self, args: dict) -> dict` — central call entrypoint that tool wrappers register to MCP.
* Optionally `async ingest(self, **kwargs)` or `async ingest_folder(self, folder_path, ...)` for ingestion flows.

Follow this signature exactly so registry and Celery tasks can call tools generically.

---

## How tools are implemented (step-by-step)

To add a new tool (complete example):

1. **Create directory**: `src/app/tool/tools/<your_tool>/`

   * `__init__.py`, `<your_tool>.py`, `schemas.py` (pydantic), `README.md` (tool docs).

2. **Tool class** — inherit `BaseTool`:

```python
# src/app/tool/tools/mytool/mytool.py
from src.base.base_tool import BaseTool
from pydantic import BaseModel

class MyToolArgs(BaseModel):
    query: str

class MyTool(BaseTool):
    @property
    def name(self): return "mytool"

    @property
    def description(self): return "Describe tool"

    async def initialize(self):
        # optional: warmup (load index) or set _ready True
        self._ready = True

    async def run(self, args: dict):
        parsed = MyToolArgs(**args)
        # do actual logic
        return {"result": f"received {parsed.query}"}
```

3. **Add factory** to `src/app/tool/__init__.py` (TOOL\_FACTORIES):

```python
def _mytool_factory():
    return MyTool()

TOOL_FACTORIES["mytool"] = _mytool_factory
```

4. **Register** — registry code will create and register a wrapper during `init_tools()` using `LazyToolWrapper`.

5. **Tests** — add a unit test under `src/tests/test_mytool.py`.

---

## Lazy loading & registry lifecycle

* `TOOL_FACTORIES` provides lazy constructors. `LazyToolWrapper` holds a factory and only instantiates tool on first call (optionally runs `initialize()`).
* Registry exposes `.register_instance_method(instance, method_name="run", name="tool_name")` that wraps the instance method with a FastMCP tool decorator.
* `init_tools(registry)` function (in `src/app/tool/__init__.py`) wires factories → wrapper → registry. This is called at server boot (in `main.py`) or can be delayed for first-call lazy registration.
* **Important**: `initialize_instances` runs `initialize()` concurrently for instances that require warmup (e.g. building an in-memory index). Keep this idempotent.

---

## CSV RAG pipeline internals

Key modules:

* `CSVFileManager` — scans folders, computes checksums, register files in DB, mark status.
* `CSVIngestManager` — orchestrates streaming, DB bulk upsert, embeddings, vector store persistence.
* `CSVQueryManager` — embed query → query vector store → map to database rows → return results.

Important table schemas (short):

* `csv_files` — `id`, `path`, `checksum`, `status`, `last_row_index`.
* `csv_rows` — `id`, `external_id`, `file_id`, `content`, `checksum`, `fields`, `vector_id`, `embedding_status`.

Ingestion steps (detailed):

1. Acquire advisory lock (Postgres or Redis) to prevent concurrent ingestion.
2. Scan folder and register file metadata via `CSVFileManager.get_or_register_file`.
3. Stream rows via `RowStreamer` batching logic; compute checksums per row using `row_checksum`.
4. Bulk-upsert rows into Postgres (`bulk_upsert_rows`) returning DB ids per checksum.
5. Prepare unique texts and call embedding service `embed_texts_async` (or offload to Celery).
6. For each generated vector, create vector\_id (e.g. `CSVRow:{dbid}`), call `vs_add_and_persist_async` to Chroma, storing `row_id` in vector metadata.
7. Update `csv_rows` entries to `embedding_status = DONE` and set `vector_id`.
8. Update `csv_files.last_row_index` so next ingestion resumes properly.

Idempotency:

* Checksum unique constraint on `csv_rows.checksum` prevents duplicate records.
* If ingestion fails mid-batch, manager records failed checksums and resumes from `last_row_index`.

---

## Embedding pipeline & model considerations

### Model & offline policy

* Model recommended: `intfloat/multilingual-e5-base` (multilingual).
* Loading via `SentenceTransformer` and caching in `hf_cache` (mounted volume).
* For production: consider quantized / ONNX / GPU instances for speed.

### Batch sizing & normalization

* Use `embedding_batch_size` config (defaults: 128–512 depending on memory).
* Normalize vectors to unit length for cosine similarity (done in `embed_texts`).

### GPU vs CPU

* CPU: quantize model or use smaller models for throughput.
* GPU: large batches per request benefit from GPU; ensure GPU memory > batch\_size × embedding\_dim × 4 bytes.

### Service patterns

Two deployment patterns:

1. **In-process embedding** — simple, low ops, easier to test (current code). Good for small scale.
2. **Embedding microservice** — recommended at scale:

   * Dedicated service (container) with model loaded once.
   * Accepts HTTP/gRPC batch embed requests, returns vectors.
   * Autoscale separately (CPU/GPU resources).
   * Celery workers call this service (fewer model reloads, more efficient).

---

## Celery worker design (ingest & run tasks)

Key points in `src/services/worker.py`:

* Two main tasks:

  * `run_tool_task(tool_name, kwargs)` — executes `tool.run(...)` (sync or async safely).
  * `ingest_tool_task(tool_name, kwargs)` — robust ingest with RedisLock ID, auto-renew and safe release.
* RedisLock:

  * Stores `owner_id` (Celery task id) in Redis key.
  * Uses Lua script to release only by owner.
  * Auto-renew thread extends TTL every `renew_interval`.
* Celery config:

  * `task_acks_late=True`, `worker_prefetch_multiplier=1` recommended for long-running tasks.
  * Set `soft_time_limit` and `time_limit` sensible defaults (e.g., 5–30 minutes depending on job complexity).
* Idempotency:

  * Lock key derived from deterministic hash of `(tool_name + kwargs)`.
  * If a lock already exists, `ingest_tool_task` returns `{status: "running", running_task_id: ...}`.

Scaling tips:

* Run multiple Celery workers across hosts; tasks will be distributed.
* Use dedicated worker queue for heavy embedding jobs (e.g., `high_mem_queue`).
* Use autoscaling based on queue length (K8s HPA + custom metrics or Celery autoscale).

---

## Vector store (Chroma) & metadata linking (Postgres)

Linking strategy:

* Chroma vectors store metadata with a `row_id` or Postgres `id`.
* Vector ID uses deterministic prefix (`CSVRow:{dbid}`) so updating references is trivial.
* When returning search results, `CSVQueryManager` queries Chroma for nearest vectors, extracts vector IDs, and fetches DB rows using `select_rows_by_vector_ids(session, vector_ids)`.

Persistence & concurrency:

* Use `CHROMA_PERSIST_DIRECTORY` volume (mounted in docker-compose).
* Ensure only one write client persists at a time or the chosen Chromadb client supports concurrent writes.

When to migrate:

* Chroma is fine for single-node or moderate scale. For large-scale deployment, consider Milvus or Pinecone (if you need HA, distributed sharding).

---

## Testing strategy (unit, integration, E2E)

### Unit tests

* Place tests in `src/tests/`
* Mock external dependencies:

  * Replace `VectorStore` with in-memory stub in tests.
  * Use SQLite in-memory or `pytest-postgresql` for DB tests.
* Tests to write:

### Integration tests

* Use `docker-compose.test.yml` to bring up Postgres + Redis + Chroma + app.
* Test end-to-end ingestion + query:

  1. Start services.
  2. Call `csv_rag.ingest_folder` with small sample.
  3. Assert DB updated, chroma created vectors, and `csv_rag.query` returns expected entries.

### E2E tests (agent-level)

* Spin up small Ollama (or mock LLM) + FastMCP server.
* Use LangGraph or test client to run a graph that calls `csv_rag.query` → assert results and sequencing.

### CI best practices

* Run unit tests in parallel.
* Run integration tests in a separate job with docker-compose.
* Gate merges with passing tests.

---

## Local dev & debugging recipes

### Quick start (dev)

```bash
# build and run (dev)
docker-compose up --build
# or run locally without Docker
pip install -r requirements.txt
alembic upgrade head
python main.py
```

### Running Celery locally

```bash
# in one terminal: Redis + Postgres + Chroma are running via docker-compose
# start a worker:
docker-compose run --rm celery  # or `celery -A src.services.worker worker -l info`
```

### Debugging tips

* Inspect logs: `logs/app.log` and container logs `docker-compose logs app` / `docker-compose logs celery`.
* DB: connect via `psql` to inspect `csv_files`, `csv_rows`.
* Chroma: check `chroma_data` files (persistence directory).
* If ingestion stalls: check Redis lock info (use `redis-cli GET <key>`).
* To forcibly clear lock (for debugging): `redis-cli DEL <lock_key>` (do not do this in production unless you know the lock owner is dead).

---

## Docker / Compose → Production recommendations

Your current `docker-compose.yml` is a good dev baseline. For production:

1. **Separate images**:

   * `app` (FastMCP HTTP)
   * `worker` (Celery worker) — same base but different CMD
   * `ingester` (optional) — specialized for heavy embeddings on GPU

2. **Volumes**:

   * Postgres PV, Chroma persist dir, HF cache mount, logs.

3. **Environment / Secrets**:

   * Use secret store (Vault / cloud secret manager) — do not commit `.env`.
   * Ensure `DATABASE_URL`, `REDIS_URL`, `WEATHER_API_KEY`, and any LLM credentials are injected securely.

4. **Reverse proxy + TLS**:

   * Put NGINX/Traefik in front. Expose only the proxy to public network.
   * Use mTLS or JWT for internal agent auth if needed.

5. **Health checks**:

   * Expose `/health/live` and `/health/ready` in ASGI app; check DB and Redis connectivity.

6. **Pod replicas**:

   * Avoid running more than one process that writes to same Chroma DB concurrently unless Chroma supports multi-writer mode.

---

## Kubernetes / Cloud deployment notes (scale)

* Use StatefulSets for Postgres and Chroma (if they require persistence).
* Use Deployments for `app` and `worker`.
* Celery:

  * Use a separate Deployment for workers with `replicas` tuned to resources.
  * Use HPA based on queue length (custom metrics via Prometheus).
* Embedding service:

  * Put embeddings on GPU nodes; autoscale them independently.
* Storage:

  * Use fast block storage for Chroma and Postgres (IO-heavy).
* Load balancing:

  * For MCP streaming transport (SSE), prefer sticky sessions or ensure client reconnect logic.

---

## Database & vector store maintenance / backups

### Postgres

* Daily backups via `pg_dump` or cloud managed snapshots.
* WAL archiving if required.
* Vacuum & analyze on schedule.
* Monitor `pg_stat_activity` for long queries.

Example backup:

```bash
pg_dump -h dbhost -U user -Fc -f /backups/mcp_$(date +%F).dump dbname
```

### Chroma

* Chroma persistence directory contains the vector store: ensure regular backups by copying the directory or snapshotting the PV.
* If using DuckDB+parquet for chroma, periodically checkpoint and copy the files.

---

## Security & access control

* **Network**: Keep DB & Chroma in private network, only app & workers connect.
* **Auth**: Add an API gateway that validates JWT or mTLS for MCP requests if you expose to external agents.
* **Secrets**: Use Vault or cloud secret managers; mount as environment vars or files in the container at runtime.
* **Least privilege**: Postgres user privileges should be minimal (only needed tables).
* **Rate limiting**: Put rate limits on tools that call external APIs (weather) to avoid abuse.

---

## CI / CD and testing pipelines

Suggested GitHub Actions stages:

1. `lint` — flake8 / isort / black
2. `unit-tests` — run pytest with mocks
3. `integration-tests` — bring up docker-compose (Postgres, Redis, Chroma), run a subset of integration tests
4. `build-and-push` — build Docker images, push to registry
5. `deploy` — update k8s manifests via helm or argo

Ensure integration tests run against ephemeral resources and are isolated.

---

## Troubleshooting & FAQ

* **`Missing session id` or `406 Not Acceptable` in curl**:

  * You are hitting the streaming MCP transport; use the right client or switch to stateless JSON mode for curl testing.
  * Use the FastMCP Python client: `from fastmcp import Client`.

* **Ingestion tasks never start / lock held**:

  * Check Redis lock TTL, and inspect `RedisLock.get_lock_info`.
  * If a worker crashed while holding lock, it will auto-expire after TTL; reduce TTL for dev.

* **Vectors not found**:

  * Ensure `CSVIngestManager` persisted vector IDs to DB and Chroma.
  * Check `vector_id` column in `csv_rows`.

* **Slow embeddings / OOM**:

  * Reduce `embedding_batch_size`, add GPU workers, or shard ingestion.

* **Chroma file corruption**:

  * Stop all writers, restore from last backup, and re-ingest if needed.

---

## Appendix: Useful commands & snippets

### Run tests

```bash
# unit
pytest -q src/tests

# run integration (example)
docker-compose -f docker-compose.yml up -d postgres redis chroma
pytest -q src/tests/test_fastmcp_smoke.py
```

### Start dev stack

```bash
docker-compose up --build
# or
pip install -r requirements.txt
alembic upgrade head
python main.py
```

### Celery worker (direct)

```bash
celery -A src.services.worker worker -l info --concurrency=2
```

### Call tool via stateless JSON POST (Windows cmd)

```cmd
curl http://localhost:8000/mcp -X POST -H "Content-Type: application/json" -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"call_tool\",\"params\":{\"name\":\"health.ping\",\"arguments\":{}}}"
```

### Python test client (recommended)

```python
# test_mcp_client.py
import asyncio
from fastmcp import Client  

async def main():
    client = Client("http://localhost:8000/mcp")
    async with client:
        r = await client.call_tool("health.ping", {})
        print(r)

if __name__ == "__main__":
    asyncio.run(main())
```

---

## Final notes — operational checklist before going live

* [ ] Secret management in place (no `.env` in repo).
* [ ] Backups for Postgres and Chroma enabled and tested.
* [ ] Health & ready checks implemented in app.
* [ ] Monitoring and alerting configured (Prometheus + Grafana + Sentry).
* [ ] Load test ingestion and query paths (simulate many agents).
* [ ] Harden RBAC on database and internal services.
* [ ] Plan for model scaling (embedding service; GPU nodes).

---