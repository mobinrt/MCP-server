from typing import List, Dict, Any
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from .models import CSVRow


async def bulk_upsert_rows(
    session: AsyncSession, rows: List[Dict[str, Any]]
) -> List[int]:
    """
    Upsert rows into CSVRow table.
    - Uses ON CONFLICT (checksum) DO NOTHING
    - Returns list of ids (for inserted or existing rows).
    """

    if not rows:
        return []

    stmt = insert(CSVRow).values(rows)
    stmt = stmt.on_conflict_do_nothing(index_elements=[CSVRow.checksum])

    await session.execute(stmt)
    await session.commit()

    # fetch ids for all provided checksums
    checksums = [r["checksum"] for r in rows]
    sel = select(CSVRow.id, CSVRow.checksum).where(CSVRow.checksum.in_(checksums))
    res = await session.execute(sel)

    chk_to_id = {row.checksum: row.id for row in res.mappings().all()}

    return [chk_to_id[c] for c in checksums if c in chk_to_id]
