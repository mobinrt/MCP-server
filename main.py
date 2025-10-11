import warnings
import anyio
import uvicorn

from src.config.logger import logging
from src.config import db
from src.app.tool.registry import registry
from src.config.settings import settings
from src.app.tool import init_tools

logger = logging.getLogger(__name__)

# warnings.filterwarnings("ignore", category=DeprecationWarning)

app = registry.http_app()


async def async_init():
    """Initialize tools before starting server."""

    logger.info("Initializing tools...")
    await db.init_db()
    await init_tools(registry)
    logger.info("Tools initialized successfully.")


if __name__ == "__main__":
    anyio.run(async_init)

    port = int(settings.port)
    host = settings.host

    uvicorn.run(
        "main:app",
        host=host,
        port=8001,
        log_level="info",
        access_log=True,
        use_colors=True,
    )
    """
    for using Uvicorn, u should use FastAPI and lifespan
    """