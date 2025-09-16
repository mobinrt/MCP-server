"""
CSVIngestManager (rows → db + embeddings + vs)
"""

import json
from typing import Dict, Any, List, Iterable, AsyncIterable, Union
from sqlalchemy import update

from src.app.tool.tools.csv_rag.models import CSVFile
from sqlalchemy.ext.asyncio import AsyncSession
from src.config.logger import logging

from src.enum.embedding_status import EmbeddingStatus
from src.app.tool.tools.csv_rag.crud.crud_row import bulk_upsert_rows
from src.app.tool.tools.csv_rag.models import CSVRow
from src.app.tool.tools.csv_rag.embedding import embed_texts_async
from src.app.tool.tools.csv_rag.chromadb import vs_add_and_persist_async
from src.enum.embedding_status import embeddingStatus
from src.helpers.row_checksum import row_checksum

logger = logging.getLogger(__name__)


class CSVIngestManager:
    def __init__(self, db, vector_store):
        self.db = db
        self.vs = vector_store
    async def ingest_rows(
        self,
        rows: Union[Iterable[Dict[str, Any]], AsyncIterable[Dict[str, Any]]],
        file_meta: Dict,
        batch_size: int = 512,
    ):
        """
        Stream rows (sync iterable or async iterable), bulk-upsert into DB, compute embeddings
        for unique checksums per batch, push vectors to VS and set row embedding status.
        """
        self.row_counter = file_meta.get("last_row_index", 0)
        async def _aiter():
            if hasattr(rows, "__aiter__"):
                async for r in rows:
                    r["file_id"] = file_meta["id"] 
                    self.row_counter += 1
                    if self.row_counter <= file_meta.get("last_row_index", 0):
                        continue
                    yield r
            else:
                for r in rows:
                    r["file_id"] = file_meta["id"]
                    self.row_counter += 1
                    if self.row_counter <= file_meta.get("last_row_index", 0):
                        continue
                    yield r

        buffer: List[Dict[str, Any]] = []
        checksums: List[str] = []
        texts: List[str] = []
        metas: List[Dict[str, Any]] = []

        async with self.db.SessionLocal() as session:
            async for row in _aiter():
                chk = row_checksum(row)
                content = json.dumps(row, ensure_ascii=False)

                buffer.append(
                    {
                        "file_id": row.get("file_id"),
                        "external_id": row.get("external_id"),
                        "content": content,
                        "checksum": chk,
                        "fields": {k: v for k, v in row.items()},
                        "extra": None,
                    }
                )
                checksums.append(chk)
                texts.append(content)
                metas.append({"row_checksum": chk})

                if len(buffer) >= batch_size:
                    await self._flush_insert_stream(
                        session, buffer, checksums, texts, metas
                    )
                    buffer, checksums, texts, metas = [], [], [], []

            if buffer:
                await self._flush_insert_stream(
                    session, buffer, checksums, texts, metas
                )
        self._mark_as_done(file_meta)

    async def _flush_insert_stream(
        self,
        session: AsyncSession,
        buffer: List[Dict[str, Any]],
        checksums: List[str],
        texts: List[str],
        metas: List[Dict[str, Any]],
    ):
        if not buffer:
            return

        # 1) Upsert rows (single DB op) -> mapping checksum -> db id
        chk_to_dbid = await bulk_upsert_rows(session, buffer)

        # 2) Deduplicate by checksum for embedding work
        unique_checksums = list(dict.fromkeys(checksums))
        chk_to_text = {}
        for i, chk in enumerate(checksums):
            if chk not in chk_to_text:
                chk_to_text[chk] = texts[i]
        unique_texts = [chk_to_text[c] for c in unique_checksums]

        # 3) Compute embeddings for unique_texts
        try:
            unique_embs = await embed_texts_async(unique_texts)
        except Exception as e:
            await session.execute(
                update(CSVRow)
                .where(CSVRow.c.checksum.in_(unique_checksums))
                .values(
                    embedding_status=embeddingStatus.FAILED.value,
                    embedding_error=str(e),
                )
            )
            await session.commit()
            logger.exception("Embedding error for batch: %s", e)
            return

        # 4) Prepare vector store batch (only for checksums that have DB ids)
        vs_batch = []
        for i, chk in enumerate(unique_checksums):
            dbid = chk_to_dbid.get(chk)
            if not dbid:
                continue
            vec_id = f"CSVRow:{dbid}"
            vs_batch.append(
                (vec_id, unique_embs[i], {"row_id": dbid, "row_checksum": chk})
            )

        # 5) Add to vector store and update DB rows
        if vs_batch:
            ids_vs, embs_vs, metas_vs = zip(*vs_batch)
            await vs_add_and_persist_async(
                self.vs, list(ids_vs), list(embs_vs), list(metas_vs)
            )

            for vec_id, _, meta in vs_batch:
                await session.execute(
                    update(CSVRow)
                    .where(CSVRow.c.id == int(meta["row_id"]))
                    .values(
                        embedding_status=embeddingStatus.DONE.value, vector_id=vec_id
                    )
                )
            await session.commit()

        file_id = buffer[0].get("file_id")
        if file_id:
            await session.execute(
                update(CSVFile)
                .where(CSVFile.id == file_id)
                .values(last_row_index=self.row_counter)
            )
            await session.commit()

    async def _mark_as_done(self, file_meta):
        async with self.db.SessionLocal() as session:
            if file_meta.get("id"):
                await session.execute(
                    update(CSVFile)
                    .where(CSVFile.id == file_meta["id"])
                    .values(status=EmbeddingStatus.DONE.value, last_row_index=self.row_counter)
                )
                await session.commit()
