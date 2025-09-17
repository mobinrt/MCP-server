from typing import Dict, Any, List
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from src.app.tool.tools.csv_rag.models import CSVRow


async def bulk_upsert_rows(
    session: AsyncSession, rows: List[Dict[str, Any]]
) -> Dict[str, int]:
    """
    Insert or update rows into csv_rows table.
    Returns mapping {checksum -> id} (IDs are ints).
    Uses ON CONFLICT DO UPDATE + RETURNING so we get canonical ids for existing & new rows.
    """
    if not rows:
        return {}

    stmt = (
        insert(CSVRow)
        .values(rows)
        .on_conflict_do_update(
            index_elements=[CSVRow.checksum],
            set_={
                "external_id": insert(CSVRow).excluded.external_id,
                "file_id": insert(CSVRow).excluded.file_id,
                "content": insert(CSVRow).excluded.content,
                "fields": insert(CSVRow).excluded.fields,
            },
        )
        .returning(CSVRow.id, CSVRow.checksum)
    )

    res = await session.execute(stmt)
    await session.commit()

    mapping: Dict[str, int] = {}
    
    for db_id, checksum in res.fetchall():
        mapping[str(checksum)] = int(db_id)
    return mapping


async def select_rows_by_ids(
    session: AsyncSession, ids: List[int]
) -> List[Dict[str, Any]]:
    """
    Fetch rows by ids (returns list of dict mappings with column keys).
    """
    if not ids:
        return []

    sel = select(CSVRow).where(CSVRow.id.in_(ids))
    res = await session.execute(sel)
    return [{str(k): v for k, v in r._mapping.items()} for r in res.all()]

async def select_rows_by_vector_ids(session, vector_ids: List[str]):
    stmt = select(CSVRow).where(CSVRow.vector_id.in_(vector_ids))
    res = await session.execute(stmt)
    return [row.to_dict() for row in res.scalars().all()]
