from typing import Dict, Any, List, Iterable, AsyncIterable, Union, Tuple, Sequence
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from src.app.tool.tools.csv_rag.models import CSVFile, CSVRow
from src.config.logger import logging
from src.enum.csv_status import EmbeddingStatus
from src.app.tool.tools.csv_rag.crud.crud_row import bulk_upsert_rows
from src.services.embedding import (
    embed_texts_async,
    prepare_text_for_embedding,
)
from src.services.chromadb import vs_add_and_persist_async
from src.helpers.row_util import row_checksum
from src.app.tool.tools.csv_rag.schemas import IncomingRow, PreparedRow, FileMeta


logger = logging.getLogger(__name__)

"""
- Internals split into:
    - RowStreamer: yields batches from sync/async iterables, preserves last_row_index logic
    - RowRepository: wraps DB upsert + update operations (uses existing CRUD helpers)
    - EmbeddingService: wraps embed_texts_async and error handling delegation
    - VectorStoreClient: wraps vs_add_and_persist_async
    - CSVIngestManager: orchestration glue (small, testable)
"""


class RowStreamer:
    """
    Streams rows (sync or async iterable) and yields batches of prepared row dicts,
    checksums, texts and metas. Keeps track of the row_counter (starting from
    file_meta.last_row_index).
    """

    def __init__(self, start_index: int = 0):
        self.start_index = start_index

    async def stream_batches(
        self,
        rows: Union[Iterable[IncomingRow], AsyncIterable[IncomingRow]],
        file_id: int,
        batch_size: int = 512,
    ) -> AsyncIterable[
        Tuple[List[PreparedRow], List[str], List[str], List[Dict[str, Any]], int]
    ]:
        async def _aiter():
            if hasattr(rows, "__aiter__"):
                async for r in rows:
                    yield r
            else:
                for r in rows:
                    yield r

        row_counter = self.start_index
        buffer: List[PreparedRow] = []
        checksums: List[str] = []
        texts: List[str] = []
        metas: List[Dict[str, Any]] = []

        async for r in _aiter():
            original_row = r["metadata"]
            original_row["file_id"] = file_id

            row_counter += 1
            if row_counter <= self.start_index:
                continue

            chk = row_checksum(original_row)
            content = prepare_text_for_embedding(original_row)

            buffer.append(
                {
                    "file_id": original_row.get("file_id"),
                    "external_id": int(original_row.get("external_id")),
                    "content": content,
                    "checksum": chk,
                    "fields": dict(original_row),
                }
            )
            checksums.append(chk)
            texts.append(content)
            metas.append({"row_checksum": chk})

            if len(buffer) >= batch_size:
                yield buffer, checksums, texts, metas, row_counter
                buffer, checksums, texts, metas = [], [], [], []

        if buffer:
            yield buffer, checksums, texts, metas, row_counter


class RowRepository:
    """
    Abstracts DB interactions so CSVIngestManager doesn't need to know update SQL details.
    Uses existing CRUD helpers where appropriate (bulk_upsert_rows).
    """

    async def bulk_upsert(
        self, session: AsyncSession, buffer: List[PreparedRow]
    ) -> Dict[str, int]:
        return await bulk_upsert_rows(session, buffer) or {}

    async def mark_checksums_failed(
        self, session: AsyncSession, checksums: Sequence[str], error_text: str
    ):
        if not checksums:
            return
        await session.execute(
            update(CSVRow)
            .where(CSVRow.checksum.in_(list(checksums)))
            .values(
                embedding_status=EmbeddingStatus.FAILED.value,
                embedding_error=error_text,
            )
        )
        await session.commit()

    async def mark_rows_done_with_vector(
        self, session: AsyncSession, row_ids: Sequence[int], vector_ids: Sequence[str]
    ):
        for row_id, vec_id in zip(row_ids, vector_ids):
            await session.execute(
                update(CSVRow)
                .where(CSVRow.id == int(row_id))
                .values(embedding_status=EmbeddingStatus.DONE.value, vector_id=vec_id)
            )
        await session.commit()

    async def update_last_row_index(
        self, session: AsyncSession, file_id: int, last_row_index: int
    ):
        await session.execute(
            update(CSVFile)
            .where(CSVFile.id == file_id)
            .values(last_row_index=last_row_index)
        )
        await session.commit()


class EmbeddingService:
    async def embed_texts(self, texts: List[str]) -> List[List[float]]:
        return await embed_texts_async(texts)


class VectorStoreClient:
    def __init__(self, vs_client):
        self.vs = vs_client

    async def add_and_persist(
        self, ids: List[str], embs: List[List[float]], metas: List[Dict[str, Any]]
    ):
        await vs_add_and_persist_async(self.vs, ids, embs, metas)


class CSVIngestManager:
    """
    Orchestrates streaming, DB upsert, embeddings and vector store persistence.
    Public API preserved: ingest_rows(session, rows, file_meta, batch_size=512)
    """

    def __init__(self, db, vector_store):
        self.db = db
        self.vs = vector_store
        self.repo = RowRepository()
        self.embed_srv = EmbeddingService()
        self.vs_client = VectorStoreClient(self.vs)

    async def ingest_rows(
        self,
        session: AsyncSession,
        rows: Union[Iterable[IncomingRow], AsyncIterable[IncomingRow]],
        file_meta: FileMeta,
        batch_size: int = 512,
    ):
        """
        Stream rows (sync iterable or async iterable), bulk-upsert into DB, compute embeddings
        for unique checksums per batch, push vectors to VS and set row embedding status.
        Behavior should match original implementation exactly.
        """
        
        start_index = file_meta.get("last_row_index", 0)
        file_id = file_meta.get("id")
        streamer = RowStreamer(start_index=start_index)

        async for (
            buffer,
            checksums,
            texts,
            metas,
            current_row_counter,
        ) in streamer.stream_batches(rows, file_id, batch_size=batch_size):
            try:
                chk_to_dbid = await self.repo.bulk_upsert(session, buffer)
            except Exception:
                logger.exception("bulk_upsert_rows failed for file_id=%s", file_id)
                await self.repo.update_last_row_index(
                    session, file_id, current_row_counter
                )
                continue

            unique_checksums = list(dict.fromkeys(checksums))
            chk_to_text = {}
            for i, chk in enumerate(checksums):
                if chk not in chk_to_text:
                    chk_to_text[chk] = texts[i]
            unique_texts = [chk_to_text[c] for c in unique_checksums]

            try:
                unique_embs = await self.embed_srv.embed_texts(unique_texts)
            except Exception as e:
                await self.repo.mark_checksums_failed(session, unique_checksums, str(e))
                logger.exception(
                    "Embedding error for batch (file_id=%s): %s", file_id, e
                )
                await self.repo.update_last_row_index(
                    session, file_id, current_row_counter
                )
                continue

            vs_batch = []
            row_ids_for_vs: List[int] = []
            vec_ids_for_db_update: List[str] = []
            for i, chk in enumerate(unique_checksums):
                dbid = chk_to_dbid.get(chk)
                if not dbid:
                    continue
                vec_id = f"CSVRow:{dbid}"
                vs_batch.append(
                    (vec_id, unique_embs[i], {"row_id": dbid, "row_checksum": chk})
                )
                row_ids_for_vs.append(dbid)
                vec_ids_for_db_update.append(vec_id)

            if vs_batch:
                ids_vs, embs_vs, metas_vs = zip(*vs_batch)
                try:
                    await self.vs_client.add_and_persist(
                        list(ids_vs), list(embs_vs), list(metas_vs)
                    )
                except Exception:
                    failed_checksums = [m["row_checksum"] for m in metas_vs]
                    await self.repo.mark_checksums_failed(
                        session, failed_checksums, "vector_store_error"
                    )
                    logger.exception(
                        "Vector store persistence failed for file_id=%s", file_id
                    )
                    await self.repo.update_last_row_index(
                        session, file_id, current_row_counter
                    )
                    continue

                await self.repo.mark_rows_done_with_vector(
                    session, row_ids_for_vs, vec_ids_for_db_update
                )

            await self.repo.update_last_row_index(session, file_id, current_row_counter)

        logger.info("Completed ingest_rows for file_id=%s", file_meta.get("id"))
