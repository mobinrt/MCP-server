import asyncio
import hashlib
from typing import (
    Dict,
    Any,
    List,
    Iterable,
    AsyncIterable,
    Union,
    Tuple,
    Sequence,
    Optional,
)

from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from langchain.schema import Document
from langchain.text_splitter import RecursiveCharacterTextSplitter

from src.app.tool.tools.rag.models import CSVFile, CSVRow
from src.config.logger import logging
from src.enum.csv_status import EmbeddingStatus
from src.app.tool.tools.rag.crud.crud_row import bulk_upsert_rows
from src.services.embedding import row_checksum
from src.services.embedding import prepare_text_for_embedding
from src.app.tool.tools.rag.schemas import IncomingRow, PreparedRow, FileMeta
from src.config import Database, db as global_db

logger = logging.getLogger(__name__)


# ---------------- RowStreamer ----------------
class RowStreamer:
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


# ---------------- RowRepository ----------------
class RowRepository:
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


# ---------------- VectorStore adapter ----------------
class VectorStoreAdapter:
    """
    Adapter that:
      - preferred: async LangChain-style add (aadd_documents)
      - fallback: sync add_documents in a threadpool
    """

    def __init__(self, vs_client):
        self.vs = vs_client

    async def add_documents(
        self, docs: List[Document], ids: Optional[List[str]] = None
    ):
        add_async = getattr(self.vs, "aadd_documents", None)
        if callable(add_async):
            try:
                try:
                    await add_async(docs, ids=ids)
                except TypeError:
                    await add_async(docs)
                return
            except Exception as e:
                logger.debug("vs.aadd_documents failed, falling back: %s", e)

        add_sync = getattr(self.vs, "add_documents", None)
        if callable(add_sync):
            loop = asyncio.get_running_loop()

            def _call():
                try:
                    if ids is not None:
                        return add_sync(docs, ids=ids)
                    return add_sync(docs)
                except TypeError:
                    return add_sync(docs)

            await loop.run_in_executor(None, _call)
            return

        raise RuntimeError("Vector store does not support add_documents/aadd_documents")


# ---------------- CSVIngestManager (main) ----------------
class CSVIngestManager:
    """
    - chunk per-row into smaller pieces with RecursiveCharacterTextSplitter
    - persist chunk vectors with deterministic ids "CSVRow:{row_id}:{chunk_idx}"
    - keep CSVRow.vector_id = "CSVRow:{row_id}" for backward compatibility
    """

    def __init__(self, vector_store):
        self.db: Database = global_db
        self.vs = vector_store
        self.repo = RowRepository()
        self.vs_adapter = VectorStoreAdapter(self.vs)
        self.splitter = RecursiveCharacterTextSplitter(chunk_size=800, chunk_overlap=64)

    def _chunk_checksum(self, text: str) -> str:
        return hashlib.sha256(text.encode("utf-8")).hexdigest()

    async def ingest_rows(
        self,
        session: AsyncSession,
        rows: Union[Iterable[IncomingRow], AsyncIterable[IncomingRow]],
        file_meta: FileMeta,
        batch_size: int = 512,
    ):
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
                # 1) Upsert rows (one DB row per original CSV row)
                chk_to_dbid = await self.repo.bulk_upsert(session, buffer)
            except Exception:
                logger.exception("bulk_upsert_rows failed for file_id=%s", file_id)
                await self.repo.update_last_row_index(
                    session, file_id, current_row_counter
                )
                continue

            # 2) Build Documents (one doc per original row) and run splitter to produce chunks
            docs_for_split = []
            row_checksum_map = {}
            for row in buffer:
                dbid = chk_to_dbid.get(row["checksum"])
                if not dbid:
                    continue
                doc = Document(
                    page_content=row["content"],
                    metadata={"row_id": dbid, **row["fields"]},
                )
                docs_for_split.append(doc)
                row_checksum_map[dbid] = row["checksum"]

            if not docs_for_split:
                await self.repo.update_last_row_index(
                    session, file_id, current_row_counter
                )
                continue

            chunk_docs = self.splitter.split_documents(docs_for_split)

            # 3) Construct deterministic ids + metadata
            row_chunk_counters: Dict[int, int] = {}
            vs_docs: List[Document] = []
            vs_ids: List[str] = []
            row_ids_for_vs: List[int] = []
            vec_ids_for_db_update: List[str] = []

            for cd in chunk_docs:
                row_id = cd.metadata.get("row_id")
                if row_id is None:
                    continue
                idx = row_chunk_counters.get(row_id, 0)
                vec_id = f"CSVRow:{row_id}:{idx}"

                meta = {
                    "row_id": row_id,
                    "row_checksum": row_checksum_map.get(row_id),
                    "chunk_index": idx,
                }

                vs_docs.append(Document(page_content=cd.page_content, metadata=meta))
                vs_ids.append(vec_id)

                if row_id not in row_ids_for_vs:
                    row_ids_for_vs.append(row_id)
                    vec_ids_for_db_update.append(f"CSVRow:{row_id}")

                row_chunk_counters[row_id] = idx + 1

            if not vs_docs:
                await self.repo.update_last_row_index(
                    session, file_id, current_row_counter
                )
                continue

            # 4) Persist to vector store (LangChain will embed internally)
            try:
                await self.vs_adapter.add_documents(vs_docs, ids=vs_ids)
            except Exception as e:
                failed_checksums = [
                    self._chunk_checksum(d.page_content) for d in vs_docs
                ]
                await self.repo.mark_checksums_failed(session, failed_checksums, str(e))
                logger.exception(
                    "Vector store persistence failed for file_id=%s: %s", file_id, e
                )
                await self.repo.update_last_row_index(
                    session, file_id, current_row_counter
                )
                continue

            # 5) Mark rows done and set parent vector ids in DB (CSVRow.vector_id = 'CSVRow:<row_id>')
            try:
                await self.repo.mark_rows_done_with_vector(
                    session, row_ids_for_vs, vec_ids_for_db_update
                )
            except Exception as e:
                logger.exception(
                    "Failed to mark rows done for file_id=%s: %s", file_id, e
                )

            await self.repo.update_last_row_index(session, file_id, current_row_counter)

        logger.info("Completed ingest_rows for file_id=%s", file_meta.get("id"))
