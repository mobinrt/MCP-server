# src/app/tool/tools/csv_rag/rag.py
from src.config.logger import logging
from src.base.base_tool import BaseTool
from src.helpers.pg_lock import advisory_lock

from src.app.tool.tools.csv_rag.managers.file_manager import CSVFileManager
from src.app.tool.tools.csv_rag.managers.ingest_manager import CSVIngestManager
from src.app.tool.tools.csv_rag.managers.query_manager import CSVQueryManager
from src.app.tool.tools.csv_rag.loader import CSVLoader
from src.config.db import Database
from src.base.vector_store import VectorStoreBase
from src.enum.embedding_status import EmbeddingStatus

logger = logging.getLogger(__name__)


class CsvRagTool(BaseTool):
    def __init__(self, db: Database, vector_store: VectorStoreBase):
        self.db = db
        self.vs = vector_store
        self.file_mgr = CSVFileManager(db)
        self.ingest_mgr = CSVIngestManager(db, vector_store)
        self.query_mgr = CSVQueryManager(db, vector_store)
        self._ready = False

    @property
    def name(self) -> str:
        return "csv_rag"

    @property
    def description(self) -> str:
        return "Searches ingested CSV data via embeddings and vector similarity."

    async def initialize(self):
        """
        Acquire advisory lock to avoid duplicate ingestion race if desired by startup flow.
        This does NOT auto-ingest; it only ensures readiness and optionally prevents
        concurrent ingestion if you choose to call ingest_folder right away.
        """
        lock_key = 42
        async with self.db.SessionLocal() as session:
            async with advisory_lock(
                session, lock_key, wait=False, retries=5, delay=0.1
            ) as acquired:
                if not acquired:
                    logger.info(
                        "CsvRagTool initialize: another process holds the lock; proceeding (ready)."
                    )
                    self._ready = True
                    return
                logger.info(
                    "CsvRagTool initialize: acquired lock (no auto-ingest performed)."
                )
                self._ready = True

    async def ingest_folder(self, folder_path: str, batch_size: int = 512):
        """
        Scan folder and ingest new/changed CSVs.
        It acquires an advisory lock for the lifetime of ingestion so multiple workers
        don't ingest the same files concurrently.
        """
        lock_key = 42
        async with self.db.SessionLocal() as session:
            async with advisory_lock(session, lock_key) as acquired:
                if not acquired:
                    logger.info(
                        "ingest_folder: another process holds lock, skipping ingestion."
                    )
                    return
                file_paths = await self.file_mgr.scan_folder(folder_path)
                for p in file_paths:
                    file_meta = await self.file_mgr.get_or_register_file(session, p)
                    if file_meta.get("status") == EmbeddingStatus.PENDING.value:
                        logger.info("Ingesting file: %s", p)

                        await self.ingest_mgr.ingest_rows(
                            CSVLoader.stream_csv_async(p),
                            batch_size=batch_size,
                            file_meta=file_meta,
                        )
                    else:
                        logger.info("Skipping unchanged file: %s", p)

    async def run(self, query: str, top_k: int = 5):
        return await self.query_mgr.search(query, top_k)
