import os
import asyncio
from fastmcp import Server
from src.app.tool.registry import registry, initialize_tools
from src.config.logger import logging

logger = logging.getLogger(__name__)


async def main(preload: bool = False):
    await initialize_tools(preload=preload)

    server = Server(
        registry=registry, host="0.0.0.0", port=int(os.getenv("MCP_PORT", "8000"))
    )
    logger.info("Starting MCP server on 0.0.0.0:%s", os.getenv("MCP_PORT", "8000"))
    await server.start()


if __name__ == "__main__":
    asyncio.run(main(preload=False))
