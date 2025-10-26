# base/vector_store.py
from abc import ABC, abstractmethod
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from langchain.schema import Document


class VectorStoreBase(ABC):
    @abstractmethod
    def add_texts(
        self,
        texts: Sequence[str],
        metadatas: Optional[Sequence[Dict[str, Any]]] = None,
        ids: Optional[Sequence[str]] = None,
    ) -> List[str]:
        ...

    @abstractmethod
    def add_documents(
        self, docs: Iterable[Document], ids: Optional[Sequence[str]] = None
    ) -> List[str]:
        ...

    @abstractmethod
    def add_embeddings(
        self,
        embeddings: Sequence[Sequence[float]],
        metadatas: Optional[Sequence[Dict[str, Any]]] = None,
        ids: Optional[Sequence[str]] = None,
    ) -> List[str]:
        ...

    @abstractmethod
    def similarity_search(
        self, query_text: str, k: int = 5, filter: Optional[Dict[str, Any]] = None
    ) -> List[Document]:
        ...

    @abstractmethod
    def similarity_search_with_score(
        self, query_text: str, k: int = 5, filter: Optional[Dict[str, Any]] = None
    ) -> List[Tuple[Document, float]]:
        ...

    @abstractmethod
    def similarity_search_by_vector(
        self, query_vector: Sequence[float], k: int = 5, filter: Optional[Dict[str, Any]] = None
    ) -> List[Document]:
        ...

    @abstractmethod
    def similarity_search_by_vector_with_score(
        self, query_vector: Sequence[float], k: int = 5, filter: Optional[Dict[str, Any]] = None
    ) -> List[Tuple[Document, float]]:
        ...

    @abstractmethod
    def as_retriever(self, k: int = 5, filter: Optional[Dict[str, Any]] = None):
        ...

    @abstractmethod
    def delete(self, ids: Optional[Sequence[str]] = None, where: Optional[Dict[str, Any]] = None) -> None:
        ...

    @abstractmethod
    def persist(self) -> None:
        ...
