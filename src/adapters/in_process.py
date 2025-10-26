import asyncio
import inspect
from typing import Any

from src.base.adapter_base import AdapterBase
from src.config.logger import logging

logger = logging.getLogger(__name__)

"""
InProcessAdapter: runs tools in-process by calling their initialize() and run()
"""


class InProcessAdapter(AdapterBase):
    def __init__(self, impl: Any):
        
        self._impl = impl
        self._ready = False

    @property
    def name(self) -> str:
        return getattr(self._impl, "name")

    @property
    def description(self) -> str:
        return getattr(self._impl, "description", "")

    async def initialize(self) -> None:
        init = getattr(self._impl, "initialize", None)
        if init:
            if inspect.iscoroutinefunction(init):
                await init()
            else:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, init)

        self._ready = bool(getattr(self._impl, "ready", True))

    @property
    def ready(self) -> bool:
        return bool(self._ready)

    async def run(self, args: dict) -> Any:
        fn = getattr(self._impl, "run", None)
        if not fn:
            raise RuntimeError(f"Underlying impl for {self.name} has no run()")
        if inspect.iscoroutinefunction(fn):
            return await fn(args)
        else:
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, fn, args)
