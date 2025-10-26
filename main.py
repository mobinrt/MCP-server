
from fastmcp import FastMCP
from src.config.logger import logging
from src.config import db
from src.app.tool.registry import registry
from src.app.tool.init_tools import init_tools
from src.services.chromadb import ChromaVectorStore

logger = logging.getLogger(__name__)


async def async_init() -> FastMCP:
    """Initialize tools before starting server."""
    vs = ChromaVectorStore()
    print("Initializing tools...")
    await db.init_db()
    await init_tools(registry, vs)
    print("Tools initialized successfully.")
    
    return registry.mcp

