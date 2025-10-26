import warnings
import asyncio
import uvicorn
from fastmcp import FastMCP
from src.config.logger import logging
from src.config import db
from src.app.tool.registry import registry
from src.config.settings import settings
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


    
# if __name__ == "__main__":
#     import asyncio

#     asyncio.run(async_init())
#     registry.run(host="0.0.0.0", port=8001, transport="http")

# app = registry.http_app()

# @app.on_event("startup")
# async def startup_event():
#     """Initialize tools and DB inside Uvicorn’s event loop."""
#     vs = VectorStore()
#     logger.info("Initializing tools...")

#     await db.init_db()

#     await init_tools(registry, vs)

#     loop = asyncio.get_running_loop()
#     logger.info("Server main loop_id=%s starting up", id(loop))
#     logger.info("Tools initialized successfully.")


# if __name__ == "__main__":
#     # warnings.filterwarnings("ignore", category=DeprecationWarning)

#     port = int(settings.port)
#     host = settings.host

#     uvicorn.run(
#         "main:app",
#         host=host,
#         port=8001,
#         log_level="info",
#         access_log=True,
#         use_colors=True,
#     )
