import asyncio
from fastmcp import Server
from src.app.tool.registry import registry, initialize_tools
from src.config.logger import logging

logger = logging.getLogger(__name__)


async def main():
    await initialize_tools()

    server = Server(registry=registry, host="0.0.0.0", port=8000)
    logger.info("Starting MCP server on 0.0.0.0:8000")
    await server.start()


if __name__ == "__main__":
    asyncio.run(main())
