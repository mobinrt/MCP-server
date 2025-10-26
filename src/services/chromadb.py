import os
from typing import List, Dict, Any, Iterable, Optional, Sequence, Tuple

try:
    from langchain_chroma import Chroma
except Exception:
    from langchain_community.vectorstores import Chroma  # type: ignore

from langchain.schema import Document
from langchain.embeddings.base import Embeddings

from src.config.logger import logging
from src.config.settings import settings
from src.services.embedding import get_embeddings

logger = logging.getLogger(__name__)


class ChromaVectorStore:
    """
    Minimal, production-ready wrapper around LangChain's Chroma integration.
    - Persistent store on disk
    - Add texts or precomputed embeddings
    - Query by text or by vector
    - Supports metadata filters
    - Exposes a LangChain Retriever
    """

    def __init__(
        self,
        collection_name: Optional[str] = None,
        embedding_function: Optional[Embeddings] = None,
        persist_directory: Optional[str] = None,
    ):
        collection = collection_name or settings.chroma_collection_name
        persist_dir = persist_directory or settings.chroma_persist_directory
        os.makedirs(persist_dir, exist_ok=True)

        self._emb: Embeddings = embedding_function or get_embeddings()

        logger.info(
            "Initializing LangChain Chroma at %s (collection=%s)",
            persist_dir,
            collection,
        )
        self.vs = Chroma(
            collection_name=collection,
            persist_directory=persist_dir,
            embedding_function=self._emb,
        )

    def add_texts(
        self,
        texts: Sequence[str],
        metadatas: Optional[Sequence[Dict[str, Any]]] = None,
        ids: Optional[Sequence[str]] = None,
    ) -> List[str]:
        """
        Add raw texts + optional metadata. Returns assigned ids.
        """
        return self.vs.add_texts(texts=list(texts), metadatas=metadatas, ids=ids)

    def add_documents(
        self,
        docs: Iterable[Document],
        ids: Optional[Sequence[str]] = None,
    ) -> List[str]:
        """
        Add pre-constructed LangChain Documents (page_content + metadata).
        """
        return self.vs.add_documents(list(docs), ids=ids)

    def add_embeddings(
        self,
        embeddings: Sequence[Sequence[float]],
        metadatas: Optional[Sequence[Dict[str, Any]]] = None,
        ids: Optional[Sequence[str]] = None,
    ) -> List[str]:
        """
        Add precomputed vectors with metadata and ids (useful if you embed elsewhere).
        """
        return self.vs.add_embeddings(
            embeddings=list(embeddings), metadatas=metadatas, ids=ids
        )

    def persist(self) -> None:
        """Flush in-memory state to disk."""
        self.vs.persist()

    def similarity_search(
        self,
        query_text: str,
        k: int = 5,
        filter: Optional[Dict[str, Any]] = None,
    ) -> List[Document]:
        """
        Search by query text. Returns List[Document] (with metadata).
        """
        return self.vs.similarity_search(query_text, k=k, filter=filter)

    def similarity_search_with_score(
        self,
        query_text: str,
        k: int = 5,
        filter: Optional[Dict[str, Any]] = None,
    ) -> List[Tuple[Document, float]]:
        """
        Search by text and also return the store-specific score (distance).
        """
        return self.vs.similarity_search_with_score(query_text, k=k, filter=filter)

    def similarity_search_by_vector(
        self,
        query_vector: Sequence[float],
        k: int = 5,
        filter: Optional[Dict[str, Any]] = None,
    ) -> List[Document]:
        """
        Search using a precomputed embedding vector.
        """
        return self.vs.similarity_search_by_vector(
            embedding=list(query_vector), k=k, filter=filter
        )

    def similarity_search_by_vector_with_score(
        self,
        query_vector: Sequence[float],
        k: int = 5,
        filter: Optional[Dict[str, Any]] = None,
    ) -> List[Tuple[Document, float]]:
        """
        Vector search that also returns (Document, score) pairs.
        """
        return self.vs.similarity_search_with_score(
            embedding=list(query_vector), k=k, filter=filter
        )

    def delete(
        self,
        ids: Optional[Sequence[str]] = None,
        where: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Delete by ids and/or filter ('where').
        """
        self.vs.delete(ids=ids, where=where)

    def as_retriever(self, k: int = 5, filter: Optional[Dict[str, Any]] = None):
        """
        Returns a LangChain retriever for use with chains/agents.
        """
        search_kwargs = {"k": k}
        if filter is not None:
            search_kwargs["filter"] = filter
        return self.vs.as_retriever(search_kwargs=search_kwargs)
