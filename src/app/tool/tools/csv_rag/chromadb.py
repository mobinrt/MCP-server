import asyncio
from chromadb import Client
from chromadb.config import Settings as ChromaSettings
from src.base.vector_store import VectorStoreBase
from typing import List, Dict, Any
from src.config.settings import settings


class ChromaVectorStore(VectorStoreBase):
    def __init__(self, collection_name: str = "csv_rows"):
        self.client = Client(
            ChromaSettings(
                is_persistent=True,
                persist_directory=settings.chroma_persist_directory
            )
        )
        self.collection = self.client.get_or_create_collection(name=collection_name)

    def add(
        self,
        ids: List[str],
        embeddings: List[List[float]],
        metadatas: List[Dict[str, Any]],
    ):
        # ids should be strings
        self.collection.add(ids=ids, embeddings=embeddings, metadatas=metadatas)

    def query(
        self, embedding: List[float], top_k: int = 10, filter: Dict[str, Any] = None
    ):
        res = self.collection.query(
            query_embeddings=[embedding], n_results=top_k, where=filter
        )
        return res

    def persist(self):
        self.client.persist()

    def delete(self, ids: List[str]):
        self.collection.delete(ids=ids)


async def vs_add_and_persist_async(vs, ids, embeddings, metadatas):
    def _add_and_persist():
        vs.add(ids=ids, embeddings=embeddings, metadatas=metadatas)
        vs.persist()

    await asyncio.to_thread(_add_and_persist)
