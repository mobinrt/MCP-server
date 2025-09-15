from abc import ABC, abstractmethod
from typing import Any, Dict


class BaseTool(ABC):
    """
    Abstract base class for all tools.
    Every tool must implement `name`, `description`, and `run`.
    """

    def __init__(self, config: Dict[str, Any] | None = None):
        self.config = config or {}
        self._ready = False

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique name of the tool (used by the agent)."""
        pass

    @property
    @abstractmethod
    def description(self) -> str:
        """Human-readable description of what this tool does."""
        pass

    @abstractmethod
    async def run(self, payload: Dict[str, Any]) -> Any:
        pass

    async def initialize(self) -> None:
        """
        Optional startup hook. Called by the server before serving requests.
        Should set self._ready = True on success.
        """
        self._ready = True

    async def shutdown(self) -> None:
        """Optional cleanup hook."""
        return
