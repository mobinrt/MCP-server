from __future__ import annotations
import inspect
import asyncio
from typing import Callable, Any, Optional, Iterable, Dict
from src.helpers.singleton import SingletonMeta
from src.base.base_tool import BaseTool
from src.adapters import InProcessAdapter, CeleryAdapter
from src.helpers.lazy_wrapper import LazyToolWrapper
from src.enum.executor import Executor
from src.config.logger import logging

from fastmcp import FastMCP

logger = logging.getLogger(__name__)


class Registry(metaclass=SingletonMeta):
    _singleton_instance: "Registry" = None

    @classmethod
    def instance(cls, *args, **kwargs) -> "Registry":
        if cls._singleton_instance is None:
            cls._singleton_instance = cls(*args, **kwargs)
        return cls._singleton_instance

    def __init__(
        self, name: str = "mcp-server", default_adapter: str = Executor.IN_PROCESS.value
    ):
        self.mcp = FastMCP(name=name)
        self.tools: Dict[str, Callable] = {}
        self.instances: Dict[str, Any] = {}
        self.default_adapter = default_adapter

    def get(self, name: str) -> Optional[Any]:
        """
        Return the underlying instance for `name`, if available.
        Falls back to returning the registered MCP callable if no instance exists.
        """
        inst = self.instances.get(name)
        if inst is not None:
            return inst
        return self.tools.get(name)

    def _select_adapter(self, tool: Any, adapter: Optional[str] = None):
        adapter_type = adapter or self.default_adapter
        if adapter_type == Executor.CELERY.value:
            return CeleryAdapter(tool)
        return InProcessAdapter(tool)

    def register_function(
        self,
        func: Callable,
        name: Optional[str] = None,
        adapter: Optional[str] = None,
        **tool_kwargs,
    ) -> Callable:
        """Register a plain function as an MCP tool (with adapter + lazy wrapper)."""
        adapter_inst = self._select_adapter(func, adapter)
        wrapper = LazyToolWrapper(lambda: adapter_inst, name=name or func.__name__)

        tool_name = name or getattr(func, "__name__", None)

        async def _tool_entry(args: dict):
            return await wrapper.run(args or {})

        decorated = self.mcp.tool(name=tool_name, **tool_kwargs)(_tool_entry)
        self.tools[tool_name] = decorated
        
        self.instances[tool_name] = wrapper
        return decorated

    def register_instance(
        self,
        instance: Any,
        method_name: str = "run",
        name: Optional[str] = None,
        adapter: Optional[str] = None,
        **tool_kwargs,
    ):
        """
        Register an *instance-like* object (usually LazyToolWrapper or a tool instance).
        We register an MCP callable that delegates to dispatch/adapter, AND we keep the
        instance object in self.instances so dispatch_tool / workers can call it in-process.
        """
        if not hasattr(instance, method_name):
            raise AttributeError(
                f"Instance {instance!r} has no attribute '{method_name}'"
            )

        tool_name = name or getattr(instance, "name", instance.__class__.__name__)

        async def _tool_entry(args: dict):
            """ When invoked via MCP, we expect args as {"args": {...}} or a simple dict
                The instance itself can decide how to handle the payload.
                We call instance.run(payload) so the instance (LazyToolWrapper) can
                extract payload["args"] if needed.
            """
            return await instance.run(args or {})

        decorated = self.mcp.tool(
            name=tool_name,
            description=getattr(instance, "description", ""),
            **tool_kwargs,
        )(_tool_entry)

        self.tools[tool_name] = decorated
        self.instances[tool_name] = instance
        return decorated

    async def initialize_instances(self, instances: Iterable[Any]) -> None:
        tasks = []
        for inst in instances:
            init = getattr(inst, "initialize", None)
            if callable(init):
                ret = init()
                if inspect.isawaitable(ret):
                    tasks.append(ret)
        if tasks:
            await asyncio.gather(*tasks)

    def http_app(self):
        return self.mcp.http_app()

    def run(
        self, host: str = "0.0.0.0", port: int = 8000, transport: Optional[str] = None
    ):
        if transport:
            self.mcp.run(transport=transport, host=host, port=port)
        else:
            self.mcp.run(host=host, port=port)


registry: Registry = Registry.instance(name="mcp-server")
