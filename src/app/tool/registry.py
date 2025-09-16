from typing import Dict, Optional
import asyncio
from src.base.base_tool import BaseTool
from src.helpers.singleton import SingletonMeta


class ToolRegistry(metaclass=SingletonMeta):
    """
    Simple registry of BaseTool instances keyed by name.
    """

    def __init__(self) -> None:
        self._tools: Dict[str, BaseTool] = {}
        self._lock = asyncio.Lock()

    def register(self, tool: BaseTool) -> None:
        if tool.name in self._tools:
            raise ValueError(f"Tool '{tool.name}' already registered")
        self._tools[tool.name] = tool

    def unregister(self, name: str) -> None:
        self._tools.pop(name, None)

    def get(self, name: str) -> Optional[BaseTool]:
        return self._tools.get[name]

    def list(self) -> list[str]:
        return list(self._tools.keys())

    def all(self) -> list[BaseTool]:
        return list(self._tools.values())

    @property
    def tools(self):
        return list(self._tools.values())
