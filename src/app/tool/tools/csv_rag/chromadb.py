import asyncio
from typing import List, Dict, Any
from chromadb import PersistentClient

from src.base.vector_store import VectorStoreBase
from src.config.settings import settings
from src.config.logger import logging

logger = logging.getLogger(__name__)


class ChromaVectorStore(VectorStoreBase):
    """
    A persistent vector store using ChromaDB.
    """

    def __init__(self, collection_name: str = "csv_rows"):
        try:
            self.client = PersistentClient(path=settings.chroma_persist_directory)
            self.collection = self.client.get_or_create_collection(name=collection_name)
        except Exception as e:
            logger.error(f"Failed to initialize ChromaDB client: {e}")
            raise RuntimeError("Could not initialize ChromaDB client.") from e

    def add(
        self,
        ids: List[str],
        embeddings: List[List[float]],
        metadatas: List[Dict[str, Any]],
    ):
        """Adds documents to the collection."""
        try:
            self.collection.add(ids=ids, embeddings=embeddings, metadatas=metadatas)
        except Exception as e:
            logger.error(f"Failed to add documents to collection: {e}")
            raise

    def query(
        self, embedding: List[float], top_k: int = 10, filter: Dict[str, Any] = None
    ):
        """Queries the collection for similar documents."""
        try:
            res = self.collection.query(
                query_embeddings=[embedding], n_results=top_k, where=filter
            )
            return res
        except Exception as e:
            logger.error(f"Failed to query collection: {e}")
            raise

    def persist(self):
        """
        Persists the collection data to disk.
        This method is not strictly necessary for PersistentClient.
        The PersistentClient automatically handles persistence in the background.
        """
        logger.info("ChromaDB is persisting data...")

    def delete(self, ids: List[str]):
        """Deletes documents from the collection by ID."""
        try:
            self.collection.delete(ids=ids)
        except Exception as e:
            logger.error(f"Failed to delete documents from collection: {e}")
            raise


async def vs_add_and_persist_async(vs: ChromaVectorStore, ids, embeddings, metadatas):
    """
    Asynchronously adds documents to ChromaDB in a separate thread.
    This is for CPU-bound tasks.
    """
    try:
        await asyncio.to_thread(
            vs.add, ids=ids, embeddings=embeddings, metadatas=metadatas
        )
    except Exception as e:
        logger.error(f"Asynchronous add operation failed: {e}")
        raise
