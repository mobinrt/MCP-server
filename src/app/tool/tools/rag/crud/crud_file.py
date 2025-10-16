import os
from typing import Optional, Dict, Any, List
from sqlalchemy import select, update, insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.expression import func

from src.app.tool.tools.rag.models import CSVFile
from src.helpers.object_to_dict import model_to_dict
from src.enum.csv_status import FileStatus


async def get_csv_file(session: AsyncSession, path: str) -> Optional[Dict[str, Any]]:
    """
    Return a mapping for CSVFile row or None.
    """
    stmt = select(CSVFile).where(CSVFile.path == path)
    res = await session.execute(stmt)
    obj = res.scalar_one_or_none()
    return model_to_dict(obj) if obj else None

async def get_csv_file_by_id(session: AsyncSession, id: int) -> Optional[Dict[str, Any]]:
    stmt = select(CSVFile).where(CSVFile.id == id)
    res = await session.execute(stmt)
    obj = res.scalar_one_or_none()
    return model_to_dict(obj) if obj else None

async def create_csv_file(
    session: AsyncSession,
    path: str,
    checksum: str,
    status: FileStatus,
    last_row_index: int,
) -> Dict[str, Any]:
    """
    Insert new CSVFile record and return mapping.
    """
    normalized_path = os.path.normpath(path).replace("\\", "/")
    stmt = (
        insert(CSVFile)
        .values(
            path=normalized_path,
            checksum=checksum,
            status=status.value,
            last_row_index=last_row_index,
        )
        .returning(CSVFile)
    )
    res = await session.execute(stmt)
    obj = res.scalar_one_or_none()
    dict = model_to_dict(obj) if obj else None
    await session.commit()
    return dict


async def update_csv_file_checksum(
    session: AsyncSession,
    file_id: int,
    new_checksum: str,
    status: FileStatus,
    last_row_index: int,
) -> Dict[str, Any]:
    """
    Update checksum for existing CSVFile and return mapping.
    """
    await session.execute(
        update(CSVFile)
        .where(CSVFile.id == file_id)
        .values(
            checksum=new_checksum, status=status.value, last_row_index=last_row_index
        )
    )
    sel = select(CSVFile).where(CSVFile.id == file_id)
    res = await session.execute(sel)
    obj = res.scalar_one_or_none()
    await session.commit()
    return model_to_dict(obj) if obj else None


async def update_csv_file_status(
    session, file_id: int, new_status: FileStatus, total_rows: int
) -> dict:
    stmt = (
        update(CSVFile)
        .where(CSVFile.id == file_id)
        .values(
            status=new_status.value,
            last_row_index=total_rows,
            updated_at=func.now(),
        )
        .returning(CSVFile)
    )
    res = await session.execute(stmt)
    obj = res.scalar_one_or_none()
    await session.commit()
    return model_to_dict(obj) if obj else None


async def list_csv_files(session: AsyncSession) -> List[Dict[str, Any]]:
    stmt = select(CSVFile)
    res = await session.execute(stmt)
    objs = res.scalars().all()
    return [model_to_dict(o) for o in objs]
