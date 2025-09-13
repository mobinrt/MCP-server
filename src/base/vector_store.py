from typing import List, Dict, Any
from abc import ABC, abstractmethod


class VectorStoreBase(ABC):
    @abstractmethod
    def add(
        self,
        ids: List[str],
        embeddings: List[List[float]],
        metadatas: List[Dict[str, Any]],
    ):
        raise NotImplementedError()

    @abstractmethod
    def query(
        self, embedding: List[float], top_k: int = 10, filter: Dict[str, Any] = None
    ):
        raise NotImplementedError()

    @abstractmethod
    def persist(self):
        raise NotImplementedError()

    @abstractmethod
    def delete(self, ids: List[str]):
        raise NotImplementedError()
