import asyncio
import hashlib
from typing import Dict, Any, List, Iterable, AsyncIterable, Union, Tuple, Sequence, Optional

from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from langchain.schema import Document
from langchain.text_splitter import RecursiveCharacterTextSplitter

from src.app.tool.tools.rag.models import CSVFile, CSVRow
from src.config.logger import logging
from src.enum.csv_status import EmbeddingStatus
from src.app.tool.tools.rag.crud.crud_row import bulk_upsert_rows
from src.services.embedding import embed_texts_async, prepare_text_for_embedding
from src.services.chromadb import vs_add_and_persist_async
from src.helpers.row_util import row_checksum
from src.app.tool.tools.rag.schemas import IncomingRow, PreparedRow, FileMeta
from src.app.tool.tools.rag.managers.tool_registry import ToolRegistryManager
from src.config import Database, db as global_db

logger = logging.getLogger(__name__)


# ---------------- RowStreamer (unchanged) ----------------
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


# ---------------- RowRepository (unchanged) ----------------
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
        # keep same behavior: set CSVRow.vector_id to parent id (e.g. 'CSVRow:123')
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
    Adapter that supports:
      - preferred: async langchain-style add with explicit ids (aadd_documents)
      - fallback: sync add_documents (run in threadpool)
      - final fallback: compute embeddings and call vs_add_and_persist_async(ids, embs, metas)
    """

    def __init__(self, vs_client):
        self.vs = vs_client

    async def add_documents(self, docs: List[Document], ids: Optional[List[str]] = None):
        """
        docs: list of langchain Document
        ids: optional list of vector ids (same length as docs).
        """
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
            try:
                def _call():
                    try:
                        if ids is not None:
                            return add_sync(docs, ids=ids)
                        return add_sync(docs)
                    except TypeError:
                        return add_sync(docs)

                await loop.run_in_executor(None, _call)
                return
            except Exception as e:
                logger.debug("vs.add_documents failed, falling back: %s", e)

        texts = [d.page_content for d in docs]
        metas = [d.metadata or {} for d in docs]

        embs = await embed_texts_async(texts)
        if ids is None:
            ids = [m.get("row_id") or f"doc:{i}" for i, m in enumerate(metas)]

        await vs_add_and_persist_async(self.vs, ids, embs, metas)

    async def get_relevant_documents(self, query: str, k: int = 5, filter: Optional[Dict[str, Any]] = None):
        """
        Tries to use many possible retriever shapes, but final fallback returns a dict
        with raw vectorstore output (ids/distances) for legacy path.
        """
        aget = getattr(self.vs, "aget_relevant_documents", None)
        if callable(aget):
            return await aget(query, k=k, filter=filter)

        as_ret = getattr(self.vs, "as_retriever", None)
        if callable(as_ret):
            retr = self.vs.as_retriever(search_kwargs={"k": k})
            aget_ret = getattr(retr, "aget_relevant_documents", None)
            if callable(aget_ret):
                return await aget_ret(query)
            get_ret = getattr(retr, "get_relevant_documents", None)
            if callable(get_ret):
                loop = asyncio.get_running_loop()
                return await loop.run_in_executor(None, lambda: get_ret(query))

        # Final: compute query embedding and call vs.query (legacy)
        query_embs = await embed_texts_async([query])
        emb = query_embs[0]
        res = self.vs.query(emb, top_k=k, filter=filter)
        return res


# ---------------- CSVIngestManager (main) ----------------
class CSVIngestManager:
    """
    Reworked to:
      - chunk per-row into smaller pieces with RecursiveCharacterTextSplitter
      - dedupe duplicate chunk texts by checksum per batch
      - persist chunk vectors with deterministic ids "CSVRow:{row_id}:{chunk_idx}"
      - keep CSVRow.vector_id set to "CSVRow:{row_id}" (parent) for backward compatibility
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
                await self.repo.update_last_row_index(session, file_id, current_row_counter)
                continue

            # 2) Build Documents (one doc per original row) and run splitter to produce chunks
            docs_for_split = []
            # keep mapping from row_dbid -> original checksum (for metadata)
            row_checksum_map = {}
            for row in buffer:
                dbid = chk_to_dbid.get(row["checksum"])
                if not dbid:
                    continue
                doc = Document(page_content=row["content"], metadata={"row_id": dbid, **row["fields"]})
                docs_for_split.append(doc)
                row_checksum_map[dbid] = row["checksum"]

            if not docs_for_split:
                await self.repo.update_last_row_index(session, file_id, current_row_counter)
                continue

            chunk_docs = self.splitter.split_documents(docs_for_split)

            # 3) Create chunk-level checksums, chunk_ids and group them
            row_chunk_counters: Dict[int, int] = {}
            chunk_entries: List[Dict[str, Any]] = []
            for cd in chunk_docs:
                row_id = cd.metadata.get("row_id")
                if row_id is None:
                    continue
                idx = row_chunk_counters.get(row_id, 0)
                chunk_id = f"CSVRow:{row_id}:{idx}"
                chunk_text = cd.page_content
                chunk_chk = self._chunk_checksum(chunk_text)
                chunk_entries.append(
                    {
                        "row_id": row_id,
                        "chunk_index": idx,
                        "chunk_id": chunk_id,
                        "text": chunk_text,
                        "chunk_checksum": chunk_chk,
                        "meta": {"row_id": row_id, "row_checksum": row_checksum_map.get(row_id)},
                    }
                )
                row_chunk_counters[row_id] = idx + 1

            if not chunk_entries:
                await self.repo.update_last_row_index(session, file_id, current_row_counter)
                continue

            # 4) Deduplicate chunk texts (by checksum) before embedding
            unique_chk_to_text = {}
            chunk_order = []
            for ce in chunk_entries:
                chk = ce["chunk_checksum"]
                if chk not in unique_chk_to_text:
                    unique_chk_to_text[chk] = ce["text"]
                chunk_order.append(chk)

            unique_checksums = list(unique_chk_to_text.keys())
            unique_texts = [unique_chk_to_text[c] for c in unique_checksums]

            # 5) Compute embeddings
            try:
                unique_embs = await embed_texts_async(unique_texts)
            except Exception as e:
                logger.exception("Embedding error for batch (file_id=%s): %s", file_id, e)
                failed_checksums = [ce["chunk_checksum"] for ce in chunk_entries]
                await self.repo.mark_checksums_failed(session, failed_checksums, str(e))
                await self.repo.update_last_row_index(session, file_id, current_row_counter)
                continue

            # build mapping chunk_checksum -> embedding
            chk_to_emb = {chk: emb for chk, emb in zip(unique_checksums, unique_embs)}

            # 6) Prepare vectorstore docs and ids in the same order as chunk_entries
            vs_docs: List[Document] = []
            vs_ids: List[str] = []
            row_ids_for_vs: List[int] = []
            vec_ids_for_db_update: List[str] = []
            metas_for_vs: List[Dict[str, Any]] = []

            for ce in chunk_entries:
                row_id = ce["row_id"]
                chunk_idx = ce["chunk_index"]
                chk = ce["chunk_checksum"]
                text = unique_chk_to_text[chk]
                vec_id = ce["chunk_id"]  

                meta = {
                    "row_id": row_id,
                    "row_checksum": ce["meta"].get("row_checksum"),
                    "chunk_index": chunk_idx,
                }

                vs_docs.append(Document(page_content=text, metadata=meta))
                vs_ids.append(vec_id)
                metas_for_vs.append(meta)

                if row_id not in row_ids_for_vs:
                    row_ids_for_vs.append(row_id)
                    vec_ids_for_db_update.append(f"CSVRow:{row_id}")

            # 7) Persist to vector store via adapter (attempt to pass explicit ids)
            try:
                await self.vs_adapter.add_documents(vs_docs, ids=vs_ids)
            except Exception as e:
                failed_checksums = [ce["chunk_checksum"] for ce in chunk_entries]
                await self.repo.mark_checksums_failed(session, failed_checksums, str(e))
                logger.exception("Vector store persistence failed for file_id=%s: %s", file_id, e)
                await self.repo.update_last_row_index(session, file_id, current_row_counter)
                continue

            # 8) Mark rows done and set parent vector ids in DB (CSVRow.vector_id = 'CSVRow:<row_id>')
            try:
                await self.repo.mark_rows_done_with_vector(session, row_ids_for_vs, vec_ids_for_db_update)
            except Exception as e:
                logger.exception("Failed to mark rows done for file_id=%s: %s", file_id, e)

            await self.repo.update_last_row_index(session, file_id, current_row_counter)
        logger.info("Completed ingest_rows for file_id=%s", file_meta.get("id"))
