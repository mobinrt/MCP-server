from typing import Any, Callable, Dict

from src.config import db as config_db

from src.app.tool.tools.csv_rag.rag import CsvRagTool
from src.app.tool.tools.weather.weather import WeatherTool
from src.config.vector_store import VectorStore
from src.enum.tools import Tools
from src.app.tool.registry import Registry
from src.app.tool.tools.weather import cities_path
from src.base.base_tool import BaseTool

"""
Saves startup cost, tools only load when needed
First call to a tool will be slower.
"""


class LazyToolWrapper:
    def __init__(self, factory) -> BaseTool:
        self.factory = factory
        self._instance: BaseTool = None

    async def run(self):
        if self._instance is None:
            self._instance = self.factory()
            if hasattr(self._instance, "initialize"):
                await self._instance.initialize()
        return self._instance


"""
TOOL_FACTORIES: mapping tool_name -> factory() that returns a tool *implementation instance*
"""


def _csv_factory() -> CsvRagTool:
    """
    Create and return a CsvRagTool instance.
    """

    vs = VectorStore()
    return CsvRagTool(db=config_db, vector_store=vs)


def _weather_factory() -> WeatherTool:
    return WeatherTool(cities_path=cities_path)


TOOL_FACTORIES: Dict[str, Callable[[], Any]] = {
    Tools.CSV_RAG.value: _csv_factory,
    Tools.WEATHER.value: _weather_factory,
}


async def init_tools(reg: Registry):
    reg.register_instance_method(
        await LazyToolWrapper(TOOL_FACTORIES[Tools.CSV_RAG.value]).run(),
        method_name="run",
        name="csv_rag",
    )

    reg.register_instance_method(
        await LazyToolWrapper(TOOL_FACTORIES[Tools.WEATHER.value]).run(),
        method_name="run",
        name="weather",
    )

    @reg.mcp.tool(name=Tools.HEALTH.value, description="Basic health check")
    async def health_ping(args: dict):
        return {"status": "ok"}

    await reg.initialize_instances(
        [
            TOOL_FACTORIES[Tools.CSV_RAG.value](),
            TOOL_FACTORIES[Tools.WEATHER.value](),
        ]
    )
