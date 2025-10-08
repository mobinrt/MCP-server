from typing import Any, Callable, Dict

from src.config import db as config_db
from src.app.tool.tools.csv_rag.rag import CsvRagTool
from src.app.tool.tools.weather.weather import WeatherTool
from src.config.vector_store import VectorStore
from src.enum.tools import Tools
from src.app.tool.registry import Registry
from src.app.tool.tools.weather import cities_path
from src.helpers.lazy_wrapper import LazyToolWrapper
from src.app.tool.dispatcher import dispatch_tool

"""
TOOL_FACTORIES: mapping tool_name -> factory() that returns a tool *implementation instance*
"""


def _csv_factory() -> CsvRagTool:
    vs = VectorStore()
    return CsvRagTool(db=config_db, vector_store=vs)


def _weather_factory() -> WeatherTool:
    return WeatherTool(cities_path=cities_path)


TOOL_FACTORIES: Dict[str, Callable[[], Any]] = {
    Tools.CSV_RAG.value: _csv_factory,
    Tools.WEATHER.value: _weather_factory,
}


async def init_tools(reg: Registry):
    csv_wrapper = LazyToolWrapper(_csv_factory, name=Tools.CSV_RAG.value)
    reg.register_instance(csv_wrapper, name=Tools.CSV_RAG.value)

    weather_wrapper = LazyToolWrapper(_weather_factory, name=Tools.WEATHER.value)
    reg.register_instance(weather_wrapper, name=Tools.WEATHER.value)

    @reg.mcp.tool(name=Tools.CSV_RAG.value, description="CSV RAG query")
    async def csv_rag_entry(args: dict):
        return await dispatch_tool(Tools.CSV_RAG.value, args or {})

    @reg.mcp.tool(
        name=f"{Tools.CSV_RAG.value}.ingest_folder", description="CSV RAG ingest folder"
    )
    async def csv_rag_ingest_entry(args: dict):
        return await dispatch_tool(Tools.CSV_RAG.value, args or {})

    @reg.mcp.tool(name=Tools.WEATHER.value, description="Weather lookup")
    async def weather_entry(args: dict):
        print("payload: ", args)
        return await dispatch_tool(Tools.WEATHER.value, args or {})

    @reg.mcp.tool(name=Tools.HEALTH.value, description="Basic health check")
    async def health_ping(args: dict):
        return {"status": "ok"}

    # Do not call factories here and do not await their instantiation.
    # If you want to warm a tool at startup, call:
    # await reg.initialize_instances([csv_wrapper, weather_wrapper])
