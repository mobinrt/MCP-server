import json
from typing import List, Dict, Iterable, AsyncIterable, Union, Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import update

from src.base.vector_store import VectorStoreBase
from src.base.base_tool import BaseTool
from src.enum.embedding_status import embeddingStatus
from src.config import db
from .models import CSVRow
from src.app.tool.tools.csv_rag.embedding import embed_texts_async
from .crud import bulk_upsert_rows
from src.app.tool.tools.csv_rag.chromadb import vs_add_and_persist_async
from src.helpers.row_checksum import row_checksum


import logging

logger = logging.getLogger(__name__)

logger.info("Ingest started")
logger.error("Something failed", exc_info=True)


class CsvRagTool(BaseTool):
    def __init__(self, db: db, vector_store: VectorStoreBase):
        self.db = db
        self.vs = vector_store

    @property
    def name(self) -> str:
        return "csv_rag"

    @property
    def description(self) -> str:
        return "Searches ingested CSV data via embeddings and vector similarity."

    async def ingest(
        self,
        rows: Union[Iterable[Dict[str, Any]], AsyncIterable[Dict[str, Any]]],
        batch_size: int = 512,
    ):
        """
        Stream ingestion with async-friendly batching.
        - Rows can be sync iterable or async iterable
        - Buffers into batch_size chunks
        - Flushes each batch via _flush_insert_stream
        """

        async def _aiter():
            if hasattr(rows, "__aiter__"):
                async for r in rows:
                    yield r
            else:
                for r in rows:
                    yield r

        buffer, checksums, texts, metas = [], [], [], []

        async with self.db.SessionLocal() as session:
            async for row in _aiter():
                chk = row_checksum(row)
                content = json.dumps(row, ensure_ascii=False)

                buffer.append(
                    {
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

        # 1) Upsert rows, get checksum->dbid mapping
        chk_to_dbid = await bulk_upsert_rows(session, buffer)

        # 2) Deduplicate by checksum
        unique_checksums = list(dict.fromkeys(checksums))
        chk_to_text = {}
        for i, chk in enumerate(checksums):
            if chk not in chk_to_text:
                chk_to_text[chk] = texts[i]
        unique_texts = [chk_to_text[c] for c in unique_checksums]

        # 3) Compute embeddings in threadpool
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
            return

        # 4) Prepare vector store entries
        vs_batch = []
        for i, chk in enumerate(unique_checksums):
            dbid = chk_to_dbid.get(chk)
            if not dbid:
                continue
            vec_id = f"CSVRow:{dbid}"
            vs_batch.append(
                (vec_id, unique_embs[i], {"row_id": dbid, "row_checksum": chk})
            )

        # 5) Add to vector store in a blocking thread
        if vs_batch:
            ids_vs, embs_vs, metas_vs = zip(*vs_batch)
            await vs_add_and_persist_async(
                self.vs, list(ids_vs), list(embs_vs), list(metas_vs)
            )

            # 6) Update DB with embedding_status + vector_id
            for i, (vec_id, _, meta) in enumerate(vs_batch):
                await session.execute(
                    update(CSVRow)
                    .where(CSVRow.c.id == meta["row_id"])
                    .values(
                        embedding_status=embeddingStatus.DONE.value, vector_id=vec_id
                    )
                )
            await session.commit()

    async def run(self, query: str, top_k: int = 5) -> List[Dict[str, Any]]:
        """
        Return rows from Postgres joined with similarity scores.
        """
        embs = await embed_texts_async([query], batch_size=None)
        emb = embs[0]
        res = self.vs.query(emb, top_k=top_k)

        ids = [r for r in res["ids"][0]] if "ids" in res else res["ids"]
        scores = res["distances"][0] if "distances" in res else [None] * len(ids)

        async with self.db.SessionLocal() as session:
            sel = select(CSVRow).where(CSVRow.id.in_(ids))
            result = await session.execute(sel)
            rows = result.scalars().all()

        id_to_row = {str(row.id): row for row in rows}
        out = []
        for i, rid in enumerate(ids):
            row = id_to_row.get(str(rid))
            if row:
                out.append(
                    {
                        "id": row.id,
                        "external_id": row.external_id,
                        "content": row.content,
                        "fields": row.fields,
                        "score": scores[i],
                    }
                )
        return out
