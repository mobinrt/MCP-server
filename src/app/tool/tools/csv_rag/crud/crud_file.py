# src/app/tool/tools/csv_rag/crud_file.py
from typing import Optional, Dict, Any, List
from sqlalchemy import select, update, insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.app.tool.tools.csv_rag.models import CSVFile



async def get_csv_file(session: AsyncSession, path: str) -> Optional[Dict[str, Any]]:
    """
    Return a mapping for CSVFile row or None.
    """
    stmt = select(CSVFile).where(CSVFile.path == path)
    res = await session.execute(stmt)
    row = res.mappings().one_or_none()
    return dict(row) if row else None


async def create_csv_file(session: AsyncSession, path: str, checksum: str) -> Dict[str, Any]:
    """
    Insert new CSVFile record and return mapping.
    """
    stmt = insert(CSVFile).values(path=path, checksum=checksum).returning(CSVFile.id)
    res = await session.execute(stmt)
    await session.commit()
    return res


async def update_csv_file_checksum(session: AsyncSession, file_id: int, checksum: str) -> Dict[str, Any]:
    """
    Update checksum for existing CSVFile and return mapping.
    """
    await session.execute(update(CSVFile).where(CSVFile.id == file_id).values(checksum=checksum))
    await session.commit()
   
    sel = select(CSVFile).where(CSVFile.id == file_id)
    res = await session.execute(sel)
    return dict(res.mappings().one())


async def list_csv_files(session: AsyncSession) -> List[Dict[str, Any]]:
    stmt = select(CSVFile)
    res = await session.execute(stmt)
    return [dict(r._mapping) for r in res.all()]

