from typing import List, Dict
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from .models import CSVRow


async def bulk_upsert_rows(session: AsyncSession, rows: List[Dict]) -> List[int]:
    """
    Inserts rows and returns mapping checksum -> id (string).
    Uses ON CONFLICT DO UPDATE SET checksum=EXCLUDED.checksum so RETURNING returns id
    for both inserted and existing rows.
    """
    if not rows:
        return []

    stmt = insert(CSVRow).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=[CSVRow.c.checksum],
        set_={"checksum": stmt.excluded.checksum},
    ).returning(CSVRow.c.id, CSVRow.c.checksum)

    res = await session.execute(stmt)
    await session.commit()

    chk_to_id = {chk: row_id for row_id, chk in res.fetchall()}
    return [chk_to_id[r["checksum"]] for r in rows if r["checksum"] in chk_to_id]
