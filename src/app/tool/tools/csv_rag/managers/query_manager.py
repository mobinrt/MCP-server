"""
CSVQueryManager (query → embed → vs → db)
"""


from typing import List, Dict, Any

from src.config.logger import logging
from src.app.tool.tools.csv_rag.crud.crud_row import select_rows_by_ids
from src.app.tool.tools.csv_rag.embedding import embed_texts_async

logger = logging.getLogger(__name__)


class CSVQueryManager:
    def __init__(self, db, vector_store):
        self.db = db
        self.vs = vector_store

    async def search(self, query: str, top_k: int = 5) -> List[Dict[str, Any]]:
        # 1) embed query 
        embs = await embed_texts_async([query], batch_size=None)
        emb = embs[0]

        # 2) query vector store 
        res = self.vs.query(emb, top_k=top_k)

        # 3) extract ids and distances
        ids = [r for r in res["ids"][0]] if "ids" in res else res["ids"]
        scores = res["distances"][0] if "distances" in res else [None] * len(ids)

        # 4) fetch rows by ids from DB
        async with self.db.SessionLocal() as session:
            rows = await select_rows_by_ids(session, [int(i) for i in ids])

        # 5) order results according to VS order
        id_to_row = {str(r["id"]): r for r in rows}
        out = []
        for i, rid in enumerate(ids):
            r = id_to_row.get(str(rid))
            if not r:
                continue
            out.append(
                {
                    "id": r["id"],
                    "external_id": r.get("external_id"),
                    "content": r.get("content"),
                    "fields": r.get("fields"),
                    "score": scores[i] if i < len(scores) else None,
                }
            )
        return out
