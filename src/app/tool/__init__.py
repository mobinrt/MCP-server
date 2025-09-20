from typing import Any, Callable, Dict

from mcp import Tool

from src.config import db as config_db

from src.app.tool.tools.csv_rag.rag import CsvRagTool
from src.app.tool.tools.weather.weather import WeatherTool
from src.config.vector_store import VectorStore
from src.enum.tools import Tools
from src.app.tool.registry import Registry
from pydantic import BaseModel


class HealthPingInput(BaseModel):
    args: Dict = {}


"""
TOOL_FACTORIES: mapping tool_name -> factory() that returns a tool *implementation instance*
"""


def _csv_factory() -> Any:
    """
    Create and return a CsvRagTool instance.
    """

    vs = VectorStore()
    return CsvRagTool(db=config_db, vector_store=vs)


def _weather_factory() -> Any:
    return WeatherTool()


TOOL_FACTORIES: Dict[str, Callable[[], Any]] = {
    Tools.CSV_RAG.value: _csv_factory,
    Tools.WEATHER.value: _weather_factory,
}


async def init_tools(reg: Registry):
    reg.register_instance_method(
        TOOL_FACTORIES[Tools.CSV_RAG.value](), method_name="run", name="csv_rag"
    )

    reg.register_instance_method(
        TOOL_FACTORIES[Tools.WEATHER.value](), method_name="run", name="weather"
    )

    @reg.mcp.tool(name=Tools.HEALTH.value, description="Basic health check")
    async def health_ping(args: dict):
        return {"status": "ok"}

    await reg.initialize_instances(
        [
            TOOL_FACTORIES.get(Tools.CSV_RAG.value),
            TOOL_FACTORIES.get(Tools.WEATHER.value),
        ]
    )
