"""
This module provides:
- Registry.instance() -> singleton Registry that holds a FastMCP server
- register_function(func, name=None) -> registers a plain function as a FastMCP tool
- register_instance_method(instance, method_name='run', name=None) -> wraps an instance method and registers it
- initialize_instances(instances) -> calls async initialize() on the given instances (if present)
- run(...) -> starts the FastMCP server (transport + host + port)
- http_app() -> returns ASGI app for uvicorn/gunicorn if you want ASGI deployment
"""

from __future__ import annotations
import inspect
import asyncio
from typing import Callable, Any, Optional, Iterable, Dict
from src.helpers.singleton import SingletonMeta
from src.base.base_tool import BaseTool
from fastmcp import FastMCP
from fastmcp.server.http import create_streamable_http_app


def _method_param_info(method: Callable):
    """Return list of parameter names excluding `self` for bound/instance methods."""
    sig = inspect.signature(method)
    params = [p for p in sig.parameters.values() if p.name != "self"]
    return params


class Registry(metaclass=SingletonMeta):
    _singleton_instance: "Registry" = None

    @classmethod
    def instance(cls, *args, **kwargs) -> "Registry":
        if cls._singleton_instance is None:
            cls._singleton_instance = cls(*args, **kwargs)
        return cls._singleton_instance

    def __init__(self, name: str = "mcp-server"):
        self.mcp = FastMCP(name=name)
        self.tools: Dict[str, Callable] = {}

    def register_function(
        self, func: Callable, name: Optional[str] = None, **tool_kwargs
    ) -> Callable:
        """
        Register a plain function as an MCP tool.
        - func: the callable (sync or async)
        - name: optional explicit tool name (e.g. 'csv_rag.query')
        - tool_kwargs: passed to the FastMCP decorator (description, tags, etc.)
        Returns the original function (so it can be used as decorator style too).
        """
        if name:
            decorator = self.mcp.tool(name=name, **tool_kwargs)
        else:
            decorator = self.mcp.tool(**tool_kwargs)

        decorated = decorator(func)
        tool_name = name or getattr(func, "__name__", None)
        self.tools[tool_name] = decorated
        return decorated

    def register_instance_method(
        self,
        instance: BaseTool,
        method_name: str = "run",
        name: Optional[str] = None,
        **tool_kwargs,
    ):
        if not hasattr(instance, method_name):
            raise AttributeError(
                f"Instance {instance!r} has no attribute {method_name}"
            )

        method = getattr(instance, method_name)

        if inspect.iscoroutinefunction(method):

            async def wrapper(args: dict):
                return await method(args)

        else:

            def wrapper(args: dict):
                return method(args)

        """
        tool_name = name or f"{instance.__class__.__name__}.{method_name}
        U can add method name, if the logic changed.
        """
        tool_name = name or f"{instance.__class__.__name__}"
        tool_description = f"{instance.description}"
        decorated = self.mcp.tool(
            name=tool_name, **tool_kwargs, description=tool_description
        )(wrapper)
        self.tools[tool_name] = decorated
        return decorated

    async def initialize_instances(self, instances: Iterable[Any]) -> None:
        """
        Call `initialize()` on all provided instances that have it. Run concurrently.
        """
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
        """
        Return an ASGI app (FastAPI/Starlette) suitable for ASGI servers (uvicorn/gunicorn).
        """
        return self.mcp.http_app()

    def run(
        self, host: str = "0.0.0.0", port: int = 8000, transport: Optional[str] = None
    ):
        """
        Start the FastMCP server. transport can be 'http' or 'stdio' (None uses default).
        """
        if transport:
            self.mcp.run(transport=transport, host=host, port=port)
        else:
            self.mcp.run(host=host, port=port)


registry = Registry.instance(name="mcp-server")
