# src/crud.py
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Dict, Any
from .models import csv_rows


async def bulk_upsert_rows(
    session: AsyncSession, rows: List[Dict[str, Any]]
) -> Dict[str, int]:
    """
    Insert or update rows into csv_rows table.
    Returns mapping {checksum -> id} for both inserted and existing rows.
    Uses ON CONFLICT DO UPDATE trick so RETURNING works for existing rows too.
    """

    if not rows:
        return {}

    stmt = (
        insert(csv_rows)
        .values(rows)
        .on_conflict_do_update(
            index_elements=[csv_rows.c.checksum],
            set_={"checksum": insert(csv_rows).excluded.checksum}, 
        )
        .returning(csv_rows.c.id, csv_rows.c.checksum)
    )

    res = await session.execute(stmt)
    await session.commit()

    return {chk: id_ for id_, chk in res.fetchall()}
