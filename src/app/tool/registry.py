from typing import Dict, Optional

from src.base.base_tool import BaseTool
from src.helpers.singleton import SingletonMeta


class ToolRegistry(metaclass=SingletonMeta):
    """
    Simple registry of BaseTool instances keyed by name.
    """

    def __init__(self) -> None:
        self._tools: Dict[str, BaseTool] = {}

    def register(self, tool: BaseTool) -> None:
        if tool.name in self._tools:
            raise ValueError(f"Tool '{tool.name}' already registered")
        self._tools[tool.name] = tool

    def get(self, name: str) -> Optional[BaseTool]:
        return self._tools.get[name]

    def list(self) -> list[str]:
        return list(self._tools.keys())

    def all(self) -> list[BaseTool]:
        return list(self._tools.values())
