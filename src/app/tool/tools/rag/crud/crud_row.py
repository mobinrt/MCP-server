from typing import Dict, Any, List, Sequence
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy import case, update

from src.app.tool.tools.rag.models import CSVRow
from src.enum.csv_status import EmbeddingStatus


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

    for row in res.fetchall():
        db_id = row.id
        checksum = row.checksum
        mapping[str(checksum)] = int(db_id)
    return mapping


async def mark_rows_done_with_vector(
    session: AsyncSession,
    row_ids: Sequence[int],
    vector_ids: Sequence[str],
):
    """
    Bulk update all given row_ids with DONE status and their vector_ids
    using a single SQL UPDATE.
    """
    if not row_ids:
        return

    case_expr = case(
        (CSVRow.id == int(row_id), vec_id)
        for row_id, vec_id in zip(row_ids, vector_ids)
    )

    stmt = (
        update(CSVRow)
        .where(CSVRow.id.in_(list(map(int, row_ids))))
        .values(
            embedding_status=EmbeddingStatus.DONE.value,
            vector_id=case_expr,
        )
    )

    await session.execute(stmt)
    await session.commit()


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
