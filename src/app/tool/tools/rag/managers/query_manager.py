from typing import List, Dict, Any, Optional, TypeVar
import asyncio
from langchain.schema import Document
from src.config.logger import logging
from src.base.vector_store import VectorStoreBase
from src.app.tool.tools.rag.crud.crud_row import select_rows_by_vector_ids
from src.config import Database, db as global_db

from src.services.embedding import get_embeddings

logger = logging.getLogger(__name__)

V = TypeVar("V", bound=VectorStoreBase)


class CSVQueryManager:
    """
    CSVQueryManager (query → embed → vs → db)
    Uses vectorstore retriever when available, falls back to vector-by-vector search + DB join.
    """

    def __init__(self, vector_store: V):
        self.db: Database = global_db
        self.vs = vector_store
        self.retriever = self._init_retriever()

    def _init_retriever(self) -> Optional[object]:
        """Try to initialize a default retriever."""
        if hasattr(self.vs, "as_retriever") and callable(self.vs.as_retriever):
            try:
                retriever = self.vs.as_retriever(k=5)
                logger.debug(f"Retriever initialized: {type(retriever).__name__}")
                return retriever
            except Exception as e:
                logger.warning("Retriever init failed: %s", e)
        else:
            logger.info("Vector store has no retriever interface.")
        return None

    async def _embed_query(self, query: str) -> List[float]:
        """Non-blocking wrapper around HuggingFace embed_query."""
        emb = await asyncio.to_thread(get_embeddings().embed_query, query)
        return emb

    async def search(
        self, query: str, top_k: int = 5, filter: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Query vector store (retriever if available), fallback to vector search + DB join."""
        docs: List[Document] = []

        if hasattr(self.vs, "as_retriever") and callable(self.vs.as_retriever):
            try:
                retriever = self.vs.as_retriever(k=top_k, filter=filter)
                aget = getattr(retriever, "ainvoke", None)
                get_docs = getattr(retriever, "get_relevant_documents", None)
                if callable(aget):
                    docs = await aget(query)
                elif callable(get_docs):
                    loop = asyncio.get_running_loop()
                    docs = await loop.run_in_executor(None, lambda: get_docs(query))
            except Exception as e:
                logger.exception("Retriever failed: %s", e)

            if docs:
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

        try:
            emb = await self._embed_query(query)

            results = self.vs.similarity_search_by_vector_with_score(
                query_vector=emb, k=top_k, filter=filter
            )

            vector_ids = []
            scores = []
            for doc, score in results:
                m = doc.metadata or {}
                row_id = m.get("row_id")
                if row_id is not None:
                    vector_ids.append(f"CSVRow:{int(row_id)}:0")
                    scores.append(float(score))

            parent_ids = [vid.split(":", 2)[:2] for vid in vector_ids]
            parent_ids = [":".join(parts) for parts in parent_ids]

            unique_parent_ids = []
            seen = set()
            for p in parent_ids:
                if p not in seen:
                    seen.add(p)
                    unique_parent_ids.append(p)

            async with self.db.session() as session:
                rows = await select_rows_by_vector_ids(session, unique_parent_ids)

            id_to_row = {r.get("vector_id"): r for r in rows}
            out = []
            for i, parent_vec_id in enumerate(parent_ids):
                r = id_to_row.get(parent_vec_id)
                if not r:
                    logger.warning("No DB row found for vector_id=%s", parent_vec_id)
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
