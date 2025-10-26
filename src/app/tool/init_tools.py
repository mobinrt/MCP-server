from functools import partial

from src.base.vector_store import VectorStoreBase
from src.config import db
from src.app.tool.tools.rag.rag import CsvRagTool
from src.app.tool.tools.weather.weather import WeatherTool
from src.enum.tools import Tools
from src.app.tool.registry import Registry
from src.app.tool.tools.weather import cities_path
from src.helpers.lazy_wrapper import LazyToolWrapper
from src.app.tool.dispatcher import dispatch_tool
from src.app.tool.tools.rag.managers.tool_registry import ToolRegistryManager
from src.config.logger import logging


"""
TOOL_FACTORIES: mapping tool_name -> factory() that returns a tool *implementation instance*
"""
logger = logging.getLogger(__name__)


def _weather_factory() -> WeatherTool:
    return WeatherTool(cities_path=cities_path)


# TOOL_FACTORIES: Dict[str, Callable[[], Any]] = {
#     Tools.CSV_RAG.value: _csv_factory,
#     Tools.WEATHER.value: _weather_factory,
# }


async def init_tools(reg: Registry, vs: VectorStoreBase):
    weather_wrapper = LazyToolWrapper(
        lambda: WeatherTool(cities_path=cities_path), name=Tools.WEATHER.value
    )
    reg.register_instance(weather_wrapper, name=Tools.WEATHER.value)

    reg_mgr = ToolRegistryManager()

    async with db.session_read() as session:
        all_rags_tools = await reg_mgr.list_of_enabled_tools(session)

    for tool in all_rags_tools:
        try:
            tool_name = str(tool.get("name"))
            tool_type = str(tool.get("type"))
            logger.info("show me the fucking tool %s", tool_name)
            if tool_type == Tools.CSV_RAG.value:
                temp_instance = CsvRagTool(vs, name=tool_name)
                await temp_instance.set_metadata_from_json()
                description = temp_instance.description
                logger.info(f"add description {description}")

                tool_factory = partial(CsvRagTool, vector_store=vs, name=tool_name)
                wrapper = LazyToolWrapper(
                    tool_factory,
                    name=tool_name,
                )
                reg.register_instance(wrapper, name=tool_name)

                @reg.mcp.tool(
                    name=f"{tool_name}.ingest_folder",
                    description=f"Ingest folder for {tool_name}, description: {description}",
                )
                async def csv_ingest_entry(args: dict, tool_name=tool_name):
                    return await dispatch_tool(tool_name, args or {})

                @reg.mcp.tool(
                    name=tool_name,
                    description=f"Query {tool_name}, description: {description}",
                )
                async def csv_query_entry(args: dict, tool_name=tool_name):
                    return await dispatch_tool(tool_name, args or {})
            else:
                logger.warning(f"Unknown tool type '{tool_type}' for {tool_name}")

        except Exception as e:
            logger.error(f"Failed to register tool {tool_name}: {e}")

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

    # @reg.mcp.tool(name=Tools.CSV_RAG.value, description="CSV RAG query")
    # async def csv_rag_entry(args: dict):
    #     return await dispatch_tool(Tools.CSV_RAG.value, args or {})

    # @reg.mcp.tool(
    #     name=f"{Tools.CSV_RAG.value}.ingest_folder", description="CSV RAG ingest folder"
    # )
    # async def csv_rag_ingest_entry(args: dict):
    #     return await dispatch_tool(Tools.CSV_RAG.value, args or {})
