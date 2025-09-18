import os

from src.config.logger import logging
from src.services.chromadb import ChromaVectorStore
from src.config.settings import settings

logger = logging.getLogger(__name__)

telemetry_enabled = os.environ["CHROMA_TELEMETRY_ENABLED"] = (
    settings.chroma_telemetry_enabled
)


class VectorStore:
    def __init__(self):
        self._vs = None

    def get(self) -> ChromaVectorStore:
        if self._vs is None:
            persist_dir = settings.chroma_persist_directory
            collection = settings.chroma_collection_name

            os.makedirs(persist_dir, exist_ok=True)
            logger.info(
                "Initializing ChromaVectorStore at %s (collection=%s)",
                persist_dir,
                collection,
            )

            self._vs = ChromaVectorStore(collection_name=collection)
        return self._vs
