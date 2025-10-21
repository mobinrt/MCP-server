"""
CSVQueryManager (query → embed → vs → db)
"""

from typing import List, Dict, Any, Optional
import asyncio
from langchain.schema import Document

from src.config.logger import logging
from src.base.vector_store import VectorStoreBase
from src.app.tool.tools.rag.crud.crud_row import select_rows_by_vector_ids
from src.services.embedding import embed_texts_async
from src.config import Database, db as global_db

logger = logging.getLogger(__name__)


class CSVQueryManager:
    """
    Use vectorstore retriever when available. Keeps DB fetch path as fallback if needed.
    """

    def __init__(self, vector_store: VectorStoreBase):
        self.db: Database = global_db
        self.vs = vector_store
        try:
            self.retriever = getattr(self.vs, "as_retriever")(search_kwargs={"k": 5})
        except Exception:
            self.retriever = None

    async def search(
        self, query: str, top_k: int = 5, filter: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Returns list of rows like:
        [
          {"id": row_id, "external_id": ..., "content": "...", "fields": {...}, "score": ...},
          ...
        ]
        """

        # 1) Preferred: async retriever that returns Documents
        if self.retriever is not None:
            aget = getattr(self.retriever, "aget_relevant_documents", None)
            if callable(aget):
                try:
                    docs: List[Document] = await aget(query)
                except Exception as e:
                    logger.exception("Async retriever failed: %s", e)
                    docs = []
            else:
                get_docs = getattr(self.retriever, "get_relevant_documents", None)
                if callable(get_docs):
                    loop = asyncio.get_running_loop()
                    docs = await loop.run_in_executor(None, lambda: get_docs(query))
                else:
                    docs = []

            out = []
            for d in docs[:top_k]:
                meta = d.metadata or {}
                out.append(
                    {
                        "id": meta.get("row_id"),
                        "external_id": meta.get("external_id"),
                        "content": d.page_content,
                        "fields": meta,
                        "score": meta.get("score"),
                    }
                )
            return out

        # 2) Fallback: compute embedding + call legacy vs.query (sync)
        try:
            embs = await embed_texts_async([query])
            emb = embs[0]
            res = self.vs.query(emb, top_k=top_k, filter=filter)
            vector_ids = res.get("ids", [[]])[0] if "ids" in res else res.get("ids", [])
            scores = (
                res.get("distances", [[]])[0]
                if "distances" in res
                else [None] * len(vector_ids)
            )

            parent_ids = [
                vid.rsplit(":", 1)[0] if (isinstance(vid, str) and ":" in vid) else vid
                for vid in vector_ids
            ]
            seen = set()
            unique_parent_ids = []
            for p in parent_ids:
                if p not in seen:
                    seen.add(p)
                    unique_parent_ids.append(p)

            async with self.db.session() as session:
                rows = await select_rows_by_vector_ids(session, unique_parent_ids)

            id_to_row = {r.get("vector_id"): r for r in rows}
            out = []
            for i, vid in enumerate(vector_ids):
                parent = parent_ids[i]
                r = id_to_row.get(parent)
                if not r:
                    logger.warning(f"No DB row found for parent vector_id={parent}")
                    continue
                out.append(
                    {
                        "id": r.get("id"),
                        "external_id": r.get("external_id"),
                        "content": r.get("content"),
                        "fields": r.get("fields"),
                        "score": scores[i] if i < len(scores) else None,
                    }
                )
            return out

        except Exception as e:
            logger.exception("Vectorstore query failed: %s", e)
            return []
