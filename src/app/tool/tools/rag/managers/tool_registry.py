from typing import Optional, Any, Dict, List
import logging

from src.config.db import Database

from src.app.tool.tools.rag.crud.crud_tool import (
    get_all_tools,
    get_tool_registry,
    set_tool_enable_status,
    create_tool_registry,
    change_tool_adapter,
    delete_tool_registry,
)
from src.app.tool.tools.rag.crud.crud_file import get_csv_file_by_id
from src.enum.executor import Executor
from src.app.tool.tools.rag.models import ToolRegistry

logger = logging.getLogger(__name__)


class ToolRegistryManager:
    """
    Thin async wrapper around the csv_rag tool registry CRUD functions.
    Exposes CRUD + a couple convenience helpers used by higher-level code.
    """

    def __init__(self, db: Database):
        self.db = db

   
    async def list_of_enabled_tools(self, session) -> List[ToolRegistry.to_dict]:
        """Return list of registry entries."""
        return await get_all_tools(session)

    async def get_tool(self, session, name: str) -> Optional[Dict[str, Any]]:
        """Fetch a single registry entry by name (or None)."""
        return await get_tool_registry(session, name)

    async def create_tool(
        self,
        session,
        name: str,
        file_id: int,
        adapter: str = Executor.CELERY.value,
        enabled: bool = True,
        description: str | None = None,
        type: str = "csv_rag"
    ) -> Dict[str, Any]:
        """Create a tool registry entry."""
        
        return await create_tool_registry(
            session=session,
            name=name,
            adapter=adapter,
            enabled=enabled,
            description=description,
            type=type,
            file_id=file_id
        )

    async def set_enable_status(self, session, name: str, enabled: bool) -> Dict[str, Any]:
        """Enable/disable a registry entry."""
        return await set_tool_enable_status(session=session, name=name, enabled=enabled)

    async def change_adapter(self, session, name: str, adapter: str) -> Dict[str, Any]:
        """Change the adapter for a registry entry (e.g. 'celery' / 'in_process')."""
        return await change_tool_adapter(session=session, name=name, adapter=adapter)

    async def delete_tool(self, session, name: str) -> None:
        """Delete a registry entry."""
        return await delete_tool_registry(session=session, name=name)

   
    async def validate_and_prepare_tool(self, session, tool_name: str):
        """
        Ensure a registry entry exists for tool_name. Returns (True, tool_dict) on success.
        - This function intentionally does not mutate CSV files; it only ensures a registry
          row exists so higher-level code can decide what to do.
        """
        tool = await get_tool_registry(session, tool_name)
        if tool:
            return True, tool

        logger.exception("Tool %s not found. It will be created after ingestion.", tool_name)
        return False, f"Tool '{tool_name}' not found. It will be created after ingestion."

    async def initialize_tool(self, session, tool_name: str, file_id: Optional[int] = None) -> Dict[str, Any]:
        """
        Optional helper for initialization tasks that may need file metadata.
        Returns a dict with at least the tool entry; if file_id provided and found,
        the file dict is also returned as 'file'.
        """
        tool = await get_tool_registry(session, tool_name)
        if not tool:
            raise KeyError(f"Tool '{tool_name}' not found in tool registry")

        out = {"tool": tool}
        if file_id:
            file_meta = await get_csv_file_by_id(session, file_id)
            out["file"] = file_meta
        return out
