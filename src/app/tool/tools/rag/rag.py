from pathlib import Path
import aiofiles
import json
from typing import TypeVar
from src.config.logger import logging
from src.base.base_tool import BaseTool
from src.helpers.pg_lock import advisory_lock

from src.app.tool.tools.rag.managers.file_manager import CSVFileManager
from src.app.tool.tools.rag.managers.ingest_manager import CSVIngestManager
from src.app.tool.tools.rag.managers.query_manager import CSVQueryManager
from src.app.tool.tools.rag.managers.tool_registry import ToolRegistryManager
from src.app.tool.tools.rag.loader import CSVLoader
from src.config import db as global_db, Database
from src.base.vector_store import VectorStoreBase
from src.enum.csv_status import FileStatus
from .schemas import RagArgs

logger = logging.getLogger(__name__)

V=TypeVar("V", bound=VectorStoreBase)

class CsvRagTool(BaseTool):
    """
    Tool for CSV Retrieval-Augmented Generation (RAG).
    Handles ingestion of CSV files into a vector store and querying them via embeddings.
    """

    def __init__(
        self,
        vector_store: V,
        name: str,
    ):
        self.db: Database = global_db
        self.vs = vector_store
        self.file_mgr: CSVFileManager = CSVFileManager()
        self.ingest_mgr: CSVIngestManager = CSVIngestManager(vector_store)
        self.query_mgr: CSVQueryManager = CSVQueryManager(vector_store)
        self.registry_mgr: ToolRegistryManager = ToolRegistryManager()
        self._ready = False
        self._description = "General RAG"
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, name: str):
        self._name = name

    @property
    def description(self) -> str:
        return self._description

    @description.setter
    def description(self, value: str):
        self._description = value

    async def initialize(self):
        """
        Initialization phase.
        Now integrates ToolRegistry validation.
        Still acquires an advisory lock to avoid duplicate ingestion races.
        """

        async with self.db.session() as session:
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
            # async with self.db.session() as session:
            #     tool = await self.registry_mgr.get_tool(session, self.name)
            #     if not tool:
            #         tool = await self.registry_mgr.create_tool(
            #             session,
            #             name=self.name,
            #             description="Global CSV RAG root tool",
            #             file_id=None,
            #         )

            lock_key = 1000
            
            async with advisory_lock(
                session, lock_key, wait=False, retries=5, delay=0.1
            ) as acquired:
                if not acquired:
                    logger.info(
                        f"CsvRagTool initialize: another process holds the lock (tool={tool.get("name")}). Proceeding (ready)."
                    )
                else:
                    await self.registry_mgr.initialize_tool(
                        session, tool.get("name"), tool.get("file_id")
                    )
                    logger.info(
                        f"CsvRagTool initialize: acquired lock and validated tool '{tool.get("name")}'."
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
        async with self.db.session() as session:
            async with advisory_lock(session, lock_key) as acquired:
                if not acquired:
                    logger.info(
                        "ingest_folder: another process holds lock, skipping ingestion."
                    )
                    return

            file_paths = await self.file_mgr.scan_folder(folder_path)
            for p in file_paths:
                file_meta = await self.file_mgr.get_or_register_file(
                    session, p
                )

                status = file_meta.get("status")
                if status == FileStatus.DONE.value:
                    logger.info("Skipping already ingested file: %s", p)
                    continue

                file_stem = Path(file_meta["path"]).stem
                subtool_name = f"{self.name}:{file_stem}"

                if status in [
                    FileStatus.PENDING.value,
                    FileStatus.FAILED.value,
                ]:
                    logger.info(f"Processing pending or failed file {p}")
                    try:
                        await self.ingest_mgr.ingest_rows(
                            session,
                            CSVLoader.stream_csv_async(p),
                            batch_size=batch_size,
                            file_meta=file_meta,
                        )
                        await self.file_mgr.mark_file_as_done(
                            session, file_meta
                        )

                        await self.registry_mgr.create_tool(
                            session,
                            name=f"{self.name}:{Path(file_meta.get('path')).stem}",
                            file_id=file_meta.get("id"),
                        )

                        logger.info(f"Registered new subtool: {subtool_name}")

                    except Exception as e:
                        await self.file_mgr.mark_file_as_failed(
                            session, file_meta
                        )
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

    async def set_metadata_from_json(self) -> None:
        try:
            async with aiofiles.open(
                "static/csv/csv_description.json", "r", encoding="utf_8"
            ) as f:
                content = await f.read()
                meta_map: dict = json.loads(content)
        except FileNotFoundError:
            logger.warning("csv_description.json not found. Skipping metadata update.")
            return

        base_name = self.name.split(":")[1] if ":" in self.name else None

        if base_name is None:
            logger.warning(
                f"Tool {self.name} is a parent tool, skipping metadata update."
            )
            return

        meta: dict = meta_map.get(base_name)
        if not meta:
            logger.warning(f"No metadata record in json for tool: {base_name}")
            return

        self.description = f"{meta.get('description')}, categories of records are: {meta.get('category')}"
