from typing import Any, Callable, Dict

from src.config import db as config_db

from src.app.tool.tools.csv_rag.rag import CsvRagTool
from src.app.tool.tools.weather.weather import WeatherTool
from src.config.vector_store import VectorStore

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
    "csv_rag": _csv_factory,
    "weather": _weather_factory,
}
