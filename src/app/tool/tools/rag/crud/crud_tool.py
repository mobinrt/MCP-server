import asyncio
from typing import Optional, List
from sqlalchemy import select, update, delete
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError

from src.app.tool.tools.rag.models import ToolRegistry  
from src.config.logger import logging
from src.enum.executor import Executor
from src.helpers.object_to_dict import model_to_dict

logger = logging.getLogger(__name__)

async def create_tool_registry(
    session: AsyncSession,
    name: str,
    file_id: int,
    description: Optional[str] = None,
    adapter: str = Executor.CELERY.value,
    type: str = "csv_rag",
    enabled: bool = True,
) -> ToolRegistry:
    """Create a new ToolRegistry entry."""
    try:
        tool = ToolRegistry(
            name=name,
            description=description,
            file_id=file_id,
            adapter=adapter,
            type=type,
            enabled=enabled,
        )
        session.add(tool)
        await session.commit()
        return model_to_dict(tool) 
    except SQLAlchemyError:
        await session.rollback()
        raise


async def delete_tool_registry(session: AsyncSession, tool_name: str) -> bool:
    """Delete a ToolRegistry entry by name."""
    try:
        stmt = delete(ToolRegistry).where(ToolRegistry.name == tool_name)
        result = await session.execute(stmt)
        await session.commit()
        
        if result.rowcount:
            return True
        logger.warning(f"ToolRegistry entry not found: {tool_name}")
        return False
    except SQLAlchemyError as e:
        await session.rollback()
        logger.error(f"Failed to delete ToolRegistry {tool_name}: {e}")
        raise


async def set_tool_enable_status(
    session: AsyncSession, tool_name: str, enabled: bool
) -> Optional[ToolRegistry]:
    """Enable or disable a ToolRegistry entry."""
    try:
        stmt = (
            update(ToolRegistry)
            .where(ToolRegistry.name == tool_name)
            .values(enabled=enabled)
            .returning(ToolRegistry)
        )
        result = await session.execute(stmt)
        row = result.scalar_one_or_none()
        await session.commit()
        
        if row:
            logger.info(f"Updated {tool_name} enabled={enabled}")
        else:
            logger.warning(f"Tool {tool_name} not found for enable status change")
        return model_to_dict(row)
    except SQLAlchemyError as e:
        await session.rollback()
        logger.error(f"Failed to update enable status for {tool_name}: {e}")
        raise


async def change_tool_adapter(
    session: AsyncSession, tool_name: str, adapter: str
) -> Optional[ToolRegistry]:
    """Change adapter (e.g., in_process, celery, http) for a tool."""
    try:
        stmt = (
            update(ToolRegistry)
            .where(ToolRegistry.name == tool_name)
            .values(adapter=adapter)
            .returning(ToolRegistry)
        )
        result = await session.execute(stmt)
        row = result.scalar_one_or_none()
        await session.commit()
        
        if row:
            logger.info(f"Changed adapter for {tool_name} → {adapter}")
        else:
            logger.warning(f"Tool {tool_name} not found for adapter change")
        return model_to_dict(row)
    except SQLAlchemyError as e:
        await session.rollback()
        logger.error(f"Failed to change adapter for {tool_name}: {e}")
        raise


async def get_tool_registry(
    session: AsyncSession, tool_name: str
) -> Optional[ToolRegistry]:
    """Fetch a single ToolRegistry entry by name."""
    stmt = select(ToolRegistry).where(ToolRegistry.name == tool_name)
    result = await session.execute(stmt)

    tool = result.scalar_one_or_none()
    return model_to_dict(tool) if tool else None


async def get_all_tools(
    session: AsyncSession, only_enabled: bool = True
) -> List[ToolRegistry.to_dict]:
    """Fetch all tools, optionally only enabled ones."""
    stmt = select(ToolRegistry)
    if only_enabled:
        stmt = stmt.where(ToolRegistry.enabled.is_(True))
    result = await session.execute(stmt)
    tools = result.scalars().all()
    return [model_to_dict(t) for t in tools]