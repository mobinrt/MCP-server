import os
import asyncio
from fastmcp import Registry

from src.config.logger import logging

logger = logging.getLogger(__name__)

registry = Registry(name="mcp-server")

_csv_rag_tool = None
_weather_tool = None
_agent_tool = None


async def _ensure_csv_rag_tool():
    global _csv_rag_tool
    if _csv_rag_tool is None:
        from src.config import db
        from src.config.vector_store import VectorStore
        from src.app.tool.tools.csv_rag.rag import CsvRagTool

        vs = VectorStore()
        _csv_rag_tool = CsvRagTool(db, vs)
        await _csv_rag_tool.initialize()
    return _csv_rag_tool


async def _ensure_weather_tool():
    global _weather_tool
    if _weather_tool is None:
        from src.app.tool.tools.weather.weather import WeatherTool

        _weather_tool = WeatherTool()
        await _weather_tool.initialize()
    return _weather_tool


async def _ensure_agent_tool():
    global _agent_tool
    if _agent_tool is None:
        from src.app.agent.agent_tool import GraphAgent

        _agent_tool = GraphAgent()
        await _agent_tool.initialize()
    return _agent_tool


@registry.tool(name="csv_rag")
async def csv_rag(query: str, top_k: int = 5):
    tool = await _ensure_csv_rag_tool()
    return await tool.run(query, top_k)


@registry.tool(name="csv_rag_ingest")
async def csv_rag_ingest(folder_path: str, batch_size: int = 512):
    """
    Start ingestion for a folder. By default this enqueues a Celery job (non-blocking).
    To run ingestion inline (for local testing), set USE_CELERY_INGEST=0 in env.
    """
    use_celery = os.getenv("USE_CELERY_INGEST", "1") != "0"
    if use_celery:
        from celery import Celery

        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        celery_app = Celery("mcp_client", broker=redis_url, backend=redis_url)

        def _send_task():
            async_result = celery_app.send_task(
                "ingest_tool_task",
                args=[
                    "csv_rag",
                    {"folder_path": folder_path, "batch_size": batch_size},
                ],
            )
            return async_result.get(
                timeout=int(os.getenv("TOOL_CELERY_TIMEOUT", "600"))
            )

        loop = asyncio.get_event_loop()

        result = await loop.run_in_executor(None, _send_task)
        return {"status": "enqueued", "celery_result": result}
    else:
        tool = await _ensure_csv_rag_tool()
        await tool.ingest_folder(folder_path, batch_size)
        return {"status": "ingestion_finished"}


@registry.tool(name="weather")
async def weather(city: str):
    tool = await _ensure_weather_tool()
    return await tool.run(city)


@registry.tool(name="agent_chat")
async def agent_chat(user_input: str):
    """
    Agent tool that uses the standalone GraphAgent.
    This is intentionally independent and will not touch csv_rag or weather modules.
    """
    agent = await _ensure_agent_tool()
    resp = await agent.run(user_input)
    return {"response": resp}


@registry.tool(name="health_ping")
async def health_ping():
    return {"status": "ok"}


async def initialize_tools(preload: bool = False):
    """
    Optionally preload all tools (set preload=True). Otherwise, tools will
    be instantiated lazily on the first call to each endpoint.
    Preload useful for warm workers or integration tests.
    """
    logger.info("Initializing tools (preload=%s)...", preload)
    if preload:
        await _ensure_agent_tool()
        await _ensure_weather_tool()
        await _ensure_csv_rag_tool()
    logger.info("Initialization complete.")
