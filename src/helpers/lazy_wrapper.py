import asyncio
import inspect
from typing import Any, Callable, Optional
from src.base.base_tool import BaseTool
from src.config.logger import logging

logger = logging.getLogger(__name__)


class LazyToolWrapper(BaseTool):
    """
    Lazily instantiate a tool via factory() on first use.
    Exposes async initialize(), run(args: dict) and ingest_folder(folder_path, **kwargs) if underlying instance has them.

    The factory is expected to return an instance whose API is async (async def initialize/run/etc).
    """

    def __init__(self, factory: Callable[[], BaseTool], name: Optional[str] = None):
        self.factory = factory
        self._instance: Optional[BaseTool] = None
        self._lock = asyncio.Lock()
        self._name = name or getattr(factory, "__name__", "lazy_tool")
        self._description = ""
        self._ready = False

    async def _ensure_instance(self):
        if self._instance is None:
            async with self._lock:
                if self._instance is None:
                    logger.info("LazyToolWrapper: creating instance for %s", self._name)
                    inst = self.factory()
                    try:
                        self._description = getattr(inst, "description", "") or ""
                    except Exception:
                        self._description = ""

                    init = getattr(inst, "initialize", None)
                    if callable(init):
                        ret = init()
                        if inspect.isawaitable(ret):
                            await ret
                    self._instance = inst
                    self._ready = True

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
        method = getattr(self._instance, "run", None)
        if not method:
            raise RuntimeError(f"Underlying instance for {self._name} has no run()")

        args = payload.get("args", payload)
        return await method(args)

    async def ingest_folder(self, folder_path: str, **kwargs):
        await self._ensure_instance()
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
