from typing import Dict, Optional, List
import asyncio
from src.base.base_tool import BaseTool
from src.helpers.singleton import SingletonMeta

import logging
import time


logger = logging.getLogger(__name__)


class ToolRegistry(metaclass=SingletonMeta):
    """
    Registry for tool adapters.
    - Thread-safe register/unregister
    - Initialize all tools concurrently
    - Wait for them to report ready
    """

    def __init__(self) -> None:
        self._tools: Dict[str, BaseTool] = {}
        self._lock = asyncio.Lock()

    async def register(self, tool: BaseTool) -> None:
        """
        Register a tool adapter. Replaces existing tool if name already exists.
        """
        async with self._lock:
            if tool.name in self._tools:
                logger.warning("Tool %s already registered; replacing", tool.name)
            self._tools[tool.name] = tool
            logger.info("Registered tool: %s", tool.name)

    async def unregister(self, name: str) -> None:
        """
        Unregister a tool adapter by name.
        """
        async with self._lock:
            if name in self._tools:
                self._tools.pop(name)
                logger.info("Unregistered tool: %s", name)

    def get(self, name: str) -> Optional[BaseTool]:
        """
        Retrieve a tool by name (or None if not found).
        """
        return self._tools.get(name)

    def list(self) -> List[str]:
        """
        List all registered tool names.
        """
        return list(self._tools.keys())

    @property
    def tools(self) -> List[BaseTool]:
        return list(self._tools.values())

    async def initialize_all(self, timeout: float = 300.0) -> None:
        """
        Call initialize() concurrently on all registered tools.
        Raises the first exception encountered or TimeoutError.
        """
        tasks = [
            asyncio.create_task(t.initialize())
            for t in self.tools
            if hasattr(t, "initialize")
        ]
        if not tasks:
            return

        done, pending = await asyncio.wait(
            tasks, return_when=asyncio.FIRST_EXCEPTION, timeout=timeout
        )

        for d in done:
            exc = d.exception()
            if exc:
                for p in pending:
                    p.cancel()
                raise exc

        if pending:
            for p in pending:
                p.cancel()
            raise TimeoutError(
                f"Timeout initializing tools after {timeout:.1f} seconds"
            )

        logger.info("All tools initialized successfully.")

    async def wait_until_ready(self, timeout: float = 60.0, poll: float = 0.5) -> None:
        """
        Poll until every tool with a `.ready` attribute reports True.
        If `.ready` is missing, the tool is considered ready after initialize().
        """
        start = time.time()

        while True:
            not_ready = [
                t.name for t in self.tools if getattr(t, "ready", True) is False
            ]

            if not not_ready:
                logger.info("All tools are ready.")
                return

            if time.time() - start > timeout:
                raise TimeoutError(f"Tools not ready after {timeout:.1f}s: {not_ready}")

            await asyncio.sleep(poll)
