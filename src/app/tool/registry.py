from fastmcp import Registry
from src.config import db
from src.config.vector_store import VectorStore
from src.app.tool.tools.csv_rag.rag import CsvRagTool
from src.app.tool.tools.weather.weather import WeatherTool
from src.config.logger import logging

logger = logging.getLogger(__name__)


vs = VectorStore()


csv_rag_tool = CsvRagTool(db, vs)
weather_tool = WeatherTool()


registry = Registry(name="mcp-server")


@registry.tool(name="csv_rag")
async def csv_rag(query: str, top_k: int = 5):
    return await csv_rag_tool.run(query, top_k)


@registry.tool(name="csv_rag_ingest")
async def csv_rag_ingest(folder_path: str, batch_size: int = 512):
    await csv_rag_tool.ingest_folder(folder_path, batch_size)
    return {"status": "ingestion_started"}


@registry.tool(name="weather")
async def weather(city: str):
    return await weather_tool.run(city)


@registry.tool(name="health_ping")
async def health_ping():
    return {"status": "ok"}


async def initialize_tools():
    logger.info("Initializing CsvRagTool and WeatherTool...")
    await csv_rag_tool.initialize()
    await weather_tool.initialize()
    logger.info("All tools initialized successfully.")
