## MCP Server

This project is a **production-ready MCP (Model Context Protocol) server** that powers **Retrieval-Augmented Generation (RAG)** with CSV and structured data sources.  
It integrates **PostgreSQL, ChromaDB, Redis, Celery, and FastMCP**, with a modular tool-based design, making it scalable and extendable for real-world AI workflows.

---

## Key Highlights

- **MCP-Native**: Fully compliant with [Model Context Protocol](https://modelcontextprotocol.io/), enabling agent-driven tool execution.  
- **CSV-based RAG**: Ingests and indexes CSV files for semantic search and question answering.  
- **Embeddings**: Uses [`intfloat/multilingual-e5-base`](https://huggingface.co/intfloat/multilingual-e5-base) to build multilingual dense embeddings.  
- **Vector Search**: [Chroma](https://www.trychroma.com/) handles vector similarity search, linked to Postgres IDs.  
- **Metadata Store**: PostgreSQL stores structured metadata, ingestion status, and row-level tracking.  
- **Async Distributed Processing**: [Celery](https://docs.celeryq.dev/) workers + Redis handle ingestion, embeddings, and heavy tasks with locking & retries.  
- **Streaming Agent Calls**: Tools are exposed via **FastMCP** and stream results back to LLMs with **Server-Sent Events (SSE)**.  
- **Resilient Design**: Safe ingestion with locks, retry policies, and idempotent embeddings.  
- **Scalable**: Supports parallel workers, containerized deployment, and pluggable tools.  

---

---
## Project Structure
```bash
        |   main.py                 # Entry point, starts MCP server
        |   agent.py                # Agent integration (LangGraph/LangChain)
        |   docker-compose.yml      # Runs DB + server stack
        |   Dockerfile              # Container build
        |   requirements.txt        # Python dependencies
        |   pyproject.toml          # uv
        |
        +---src
        |   +---app
        |   |   +---tool
        |   |   |   |   registry.py         # Registry (FastMCP wrapper)
        |   |   |   |   __init__.py         # Tool initialization (factories + lazy loading)
        |   |   |   \---tools               # Actual tool implementations
        |   |   |       +---csv_rag         # CSV RAG (loader, managers, crud, schemas)
        |   |   |       \---weather         # Weather API tool
        |   |   |
        |   |   +---api                     # REST API schemas & routes
        |   |   +---agent                   # Local client + agent_tool
        |   |
        |   +---base                        # Base classes (BaseTool, LLMBase, VectorStoreBase)
        |   +---config                      # Settings, DB, logger, Celery, vector store
        |   +---enum                        # Tool + CSV status enums
        |   +---helpers                     # Utils (pg lock, singleton, file ops, etc.)
        |   +---services                    # External services (Chroma, embeddings, Celery worker)
        |   +---tests                       # Unit tests
        |
        +---static                          # Example CSVs + City index JSON

```

---

## 🏗️ Architecture Overview

```text
                    ┌───────────────────────┐
                    │     LLM / Agent       │
                    │ (LangChain, LangGraph)│
                    └───────────┬───────────┘
                                │ MCP (SSE / JSON-RPC)
                                ▼
                        ┌───────────────┐
                        │   FastMCP     │
                        │   (HTTP App)  │
                        └──────┬────────┘
                               │
                  ┌────────────┴────────────┐
                  │        Tools Layer       │
                  │  (CsvRag, Weather, etc.) │
                  └────────────┬────────────┘
                               │
         ┌─────────────────────┴─────────────────────┐
         │                                           │
 ┌───────────────┐                          ┌────────────────┐
 │  PostgreSQL   │                          │   ChromaDB     │
 │ (metadata +   │     ◄───────────────►    │ (vector store) │                 
 │ ingestion log)│                          │ embeddings     │
 └───────────────┘                          └────────────────┘
         ▲                                           ▲
         │                                           │
         └─────────────── Celery + Redis ────────────┘
```
FastMCP: Exposes tools (RAG, weather, health) via MCP-compatible API.

CsvRagTool: Handles ingestion + querying of CSVs.

Celery Workers: Offload ingestion & embedding jobs from MCP HTTP server.

Postgres: Guarantees persistence and transactional safety for metadata.

Chroma: Stores normalized embeddings for semantic search.

## Features in Depth(Tools)
csv_rag – Ingests a folder of CSVs into Postgres + Chroma. Runs RAG pipeline over Chroma embeddings with Postgres metadata lookup.

weather.get – Example tool for city-based weather (JSON data).

health.ping – Lightweight system health check.

Each tool is self-contained and registered with MCP dynamically.

# Ingestion Flow

CSV uploaded → task scheduled in Celery.

Worker parses CSV rows → writes metadata into Postgres.

Worker generates embeddings in batches → stores vectors in Chroma.

Embedding process is idempotent (safe to re-run).

# Query Flow

User queries via MCP agent.

FastMCP routes request → CsvRagTool.

Embeddings generated for query → similarity search in Chroma.

Row IDs fetched from Postgres → reconstructed + returned.

Response streamed to agent via SSE.


| Layer                         | Technology                       | Reasoning                           |
| ----------------------------- | -------------------------------- | ----------------------------------- | 
| **Protocol Layer**            | FastMCP                          | MCP-native server, SSE support      |
| **App Server**                | FastAPI + Uvicorn                | Production-ready async HTTP         |
| **Orchestration**             | Celery + Redis                   | Async ingestion & embeddings        |
| **Database**                  | PostgreSQL                       | Metadata & ingestion status         |
| **Vector Store**              | ChromaDB                         | Fast semantic similarity search     |
| **LLM**                       | Qwen2.5 (via Ollama)             | Local inference, multilingual       |
| **Embeddings**                | HuggingFace E5                   | Multilingual dense vectors          |
| **Infra**                     | Docker Compose                   | Portable deployment                 |
| **Observability**             | Logging, retries, safe ingestion | Production resiliency               |
|**LangGraph & LangChain**      | Communication agent MCP server   | Testing MCP server & wrap embedding | 

## Getting Started
1️⃣ Clone & configure
```bash
git clone https://github.com/your-org/csv-rag-mcp.git
cd csv-rag-mcp
cp .env.example .env
```

2️⃣ Run full stack
```bash
docker-compose up --build
```

3️⃣ Access services
```bash
MCP Server → http://localhost:8000/mcp

PostgreSQL → localhost:5432

Redis → localhost:6379

ChromaDB → internal Docker network
```
## Scalability & Production

This project was designed with horizontal scaling and distributed execution in mind:

Multiple Celery workers can process ingestion & embedding in parallel.

Task queue partitioning ensures long-running embeddings don’t block queries.

Idempotent ingestion prevents duplicate work (safe retries).

Postgres transactions guarantee row-level consistency.

Chroma vectors normalized for fast similarity search under high load.

Dockerized stack allows scaling each component independently.

Observability: logs and retries make debugging production-ready.

## 👩‍💻 Developer Guide

See DEVELOPMENT.md for:

Adding new tools

Worker internals

Embedding pipeline

Local dev setup

Scaling strategies