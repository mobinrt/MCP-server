from pathlib import Path

from src.config.logger import logging
from src.base.base_tool import BaseTool
from src.helpers.pg_lock import advisory_lock

from src.app.tool.tools.csv_rag.managers.file_manager import CSVFileManager
from src.app.tool.tools.csv_rag.managers.ingest_manager import CSVIngestManager
from src.app.tool.tools.csv_rag.managers.query_manager import CSVQueryManager
from src.app.tool.tools.csv_rag.managers.tool_registry import ToolRegistryManager
from src.app.tool.tools.csv_rag.loader import CSVLoader
from src.config.db import Database
from src.base.vector_store import VectorStoreBase
from src.enum.csv_status import FileStatus
from .schemas import RagArgs

logger = logging.getLogger(__name__)


class CsvRagTool(BaseTool):
    """
    Tool for CSV Retrieval-Augmented Generation (RAG).
    Handles ingestion of CSV files into a vector store and querying them via embeddings.
    """

    def __init__(self, db: Database, vector_store: VectorStoreBase):
        self.db = db
        self.vs = vector_store
        self.file_mgr: CSVFileManager = CSVFileManager(db)
        self.ingest_mgr: CSVIngestManager = CSVIngestManager(db, vector_store)
        self.query_mgr: CSVQueryManager = CSVQueryManager(db, vector_store)
        self.registry_mgr: ToolRegistryManager = ToolRegistryManager(db)
        self._ready = False

    @property
    def name(self) -> str:
        return "csv_rag"

    @property
    def description(self) -> str:
        return "Search ingested CSV data via embeddings and vector similarity."

    async def initialize(self):
        """
        Initialization phase.
        Now integrates ToolRegistry validation.
        Still acquires an advisory lock to avoid duplicate ingestion races.
        """
        async with self.db.SessionLocal() as session:
            valid, tool_or_msg = await self.registry_mgr.validate_and_prepare_tool(
                session, self.name
            )
            if not valid:
                logger.warning(
                    f"Tool {self.name} not found. Creating it automatically."
                )
                tool_or_msg = await self.registry_mgr.create_tool(
                    session,
                    name=self.name,
                    description="Global CSV RAG root tool",
                    file_id=None,
                )
                valid = True

            tool = tool_or_msg
            lock_key = 1000

            async with advisory_lock(
                session, lock_key, wait=False, retries=5, delay=0.1
            ) as acquired:
                if not acquired:
                    logger.info(
                        f"CsvRagTool initialize: another process holds the lock (tool={tool.name}). Proceeding (ready)."
                    )
                else:
                    await self.registry_mgr.initialize_tool(
                        session, tool.name, tool.file_id
                    )
                    logger.info(
                        f"CsvRagTool initialize: acquired lock and validated tool '{tool.name}'."
                    )
                self._ready = True

    async def ingest_folder(self, folder_path: str, batch_size: int = 512):
        """
        Scan a folder and ingest new/changed CSVs into the vector store.
        Protected by an advisory lock to avoid concurrent ingestion.
        Heavy ingestion should be delegated to Celery (if used in production).
        """
        if not self._ready:
            logger.warning("CsvRagTool not initialized. Call initialize() first.")
            return

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

                    status = file_meta.get("status")
                    if status == FileStatus.DONE.value:
                        logger.info("Skipping already ingested file: %s", p)
                        continue

                    file_stem = Path(file_meta["path"]).stem
                    subtool_name = f"{self.name}:{file_stem}"

                    if status in [FileStatus.PENDING.value, FileStatus.FAILED.value]:
                        logger.info(f"Skipping already ingested file: {p}")
                        try:
                            await self.ingest_mgr.ingest_rows(
                                session,
                                CSVLoader.stream_csv_async(p),
                                batch_size=batch_size,
                                file_meta=file_meta,
                            )
                            await self.file_mgr.mark_file_as_done(session, file_meta)

                            await self.registry_mgr.create_tool(
                                session,
                                name=f"{self.name}:{Path(file_meta.get('path')).stem}",
                                file_id=file_meta.get("id"),
                            )

                            logger.info(f"Registered new subtool: {subtool_name}")

                        except Exception as e:
                            await self.file_mgr.mark_file_as_failed(session, file_meta)
                            logger.error("Ingestion failed for file %s: %s", p, e)
                            await session.rollback()
                    else:
                        logger.info("Skipping unchanged file: %s", p)

    async def run(self, args: dict):
        """
        Run a search query against ingested CSV data.
        """
        if not self._ready:
            logger.warning("CsvRagTool not initialized. Call initialize() first.")
            return {"error": "Tool not initialized."}

        try:
            parsed = RagArgs(**args)
            res = await self.query_mgr.search(parsed.query, parsed.top_k)
            return {"result": res}
        except Exception as e:
            logger.exception("CsvRagTool run failed")
            return {"error": str(e)}
