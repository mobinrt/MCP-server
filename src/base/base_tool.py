from abc import ABC, abstractmethod
from typing import Any, Dict

class BaseTool(ABC):
    """
    Abstract base class for all tools.
    Every tool must implement `name`, `description`, and `run`.
    """

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
    async def run(self, query: str, **kwargs: Dict[str, Any]) -> Any:
        """
        Execute the tool with a query (from the user or LLM).
        Must be async to support API calls / DB queries.

        Args:
            query: The user input or processed LLM instruction.
            kwargs: Extra parameters (e.g., config, session, context).
        
        Returns:
            Any result (string, dict, etc.)
        """
        pass
