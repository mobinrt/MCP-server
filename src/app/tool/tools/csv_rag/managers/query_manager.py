"""
CSVQueryManager (query → embed → vs → db)
"""

from typing import List, Dict, Any

from src.config import Database
from src.config.logger import logging
from src.base.vector_store import VectorStoreBase
from src.app.tool.tools.csv_rag.crud.crud_row import select_rows_by_vector_ids
from src.app.tool.tools.csv_rag.embedding import embed_texts_async

logger = logging.getLogger(__name__)


class CSVQueryManager:
    def __init__(self, db: Database, vector_store: VectorStoreBase):
        self.db = db
        self.vs = vector_store

    async def search(self, query: str, top_k: int = 5, filter: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        # 1) embed query
        embs = await embed_texts_async([query], batch_size=None)
        emb = embs[0]

        # 2) query vector store
        res = self.vs.query(emb, top_k=top_k, filter=filter)

        # 3) extract ids and distances
        vector_ids = [r for r in res["ids"][0]] if "ids" in res else res["ids"]
        scores = res["distances"][0] if "distances" in res else [None] * len(vector_ids)

        # 4) fetch rows by ids from DB
        async with self.db.SessionLocal() as session:
            rows = await select_rows_by_vector_ids(session, vector_ids)

        # 5) order results according to VS order
        id_to_row = {r.get("vector_id"): r for r in rows}
        out = []
        for i, vector_id in enumerate(vector_ids):
            r = id_to_row.get(vector_id)
            if not r:
                logger.warning(f"No DB row found for vector_id={vector_id}")
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
