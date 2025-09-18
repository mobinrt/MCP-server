from .base_tool import BaseTool
from abc import ABC


class AdapterBase(BaseTool, ABC):
    """
    Base class for tool adapters.
    Adapters *look like tools* but only delegate work to
    a real tool (local or remote).
    Adapters MUST NOT implement business logic.
    """
