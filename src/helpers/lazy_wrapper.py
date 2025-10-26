import asyncio
import inspect
from typing import Callable
from src.base.adapter_base import AdapterBase, BaseTool
from src.config.logger import logging

logger = logging.getLogger(__name__)


class LazyToolWrapper(AdapterBase):
    """
    Lazily instantiate a tool via factory() on first use.
    Thread- and async-safe, ensures initialize() only runs once.
    """

    def __init__(
        self, factory: Callable[[], BaseTool], name: str, description: str | None = None
    ):
        self.factory = factory
        self._instance: BaseTool | None = None
        self._lock: asyncio.Lock | None = None
        self._name = name or getattr(factory, "__name__", "lazy_tool")
        self._description = description
        self._ready = False

    async def _ensure_instance(self):
        if self._ready:
            return

        if self._lock is None:
            self._lock = asyncio.Lock()

        async with self._lock:
            if self._ready:
                return

            loop = asyncio.get_running_loop()
            logger.debug(
                "LazyToolWrapper._ensure_instance loop id=%s for %s",
                id(loop),
                self._name,
            )

            logger.info("LazyToolWrapper: creating instance for %s", self._name)
            inst = self.factory()
            self._instance = inst

            init = getattr(inst, "initialize", None)
            if callable(init):
                result = init()
                if inspect.isawaitable(result):
                    task = loop.create_task(result)
                    await task

            self._description = getattr(inst, "description", "") or ""
            self._ready = True
            logger.info("LazyToolWrapper: instance ready for %s", self._name)

    @property
    def name(self) -> str:
        if self._instance:
            return getattr(self._instance, "name", self._name)
        return self._name

    @property
    def description(self) -> str:
        if self._instance:
            return getattr(self._instance, "description", self._description)
        return self._description or ""

    async def initialize(self):
        await self._ensure_instance()

    @property
    def ready(self) -> bool:
        return self._ready

    async def run(self, payload: dict):
        await self._ensure_instance()
        if not self._ready or not self._instance:
            raise RuntimeError(f"Tool {self._name} not ready after initialization")

        method = getattr(self._instance, "run", None)
        if not method:
            raise RuntimeError(f"Underlying instance for {self._name} has no run()")

        args = payload.get("args", payload)
        return await method(args)

    async def ingest_folder(self, folder_path: str, **kwargs):
        await self._ensure_instance()
        if not self._ready or not self._instance:
            raise RuntimeError(f"Tool {self._name} not ready after initialization")

        method = getattr(self._instance, "ingest_folder", None)
        if not method:
            raise RuntimeError(
                f"Underlying instance for {self._name} has no ingest_folder()"
            )

        sig = inspect.signature(method)
        params = [p for p in sig.parameters.values() if p.name != "self"]
        if params and params[0].kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        ):
            return await method(folder_path, **kwargs)
        return await method(folder_path=folder_path, **kwargs)
