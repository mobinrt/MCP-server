import asyncio
from typing import List, Dict


from src.config import Database
from src.config.logger import logging
from sqlalchemy.ext.asyncio import AsyncSession
from src.enum.csv_status import FileStatus
from src.helpers.file_util import (
    compute_file_checksum_sync,
    scan_folder_sync,
    normalized_path,
    count_total_rows,
)
from src.app.tool.tools.rag.crud.crud_file import (
    get_csv_file,
    create_csv_file,
    update_csv_file_checksum,
    update_csv_file_status,
)

logger = logging.getLogger(__name__)


class CSVFileManager:
    def __init__(self, db: Database):
        self.db = db

    async def compute_file_checksum(self, file_path: str) -> str:
        norm_path = normalized_path(file_path)
        return await asyncio.to_thread(compute_file_checksum_sync, norm_path)

    async def scan_folder(self, folder_path: str) -> List[str]:
        return await asyncio.to_thread(scan_folder_sync, folder_path)

    async def get_or_register_file(self, session: AsyncSession, file_path: str) -> Dict:
        norm_path = normalized_path(file_path)
        checksum = await self.compute_file_checksum(norm_path)
        existing = await get_csv_file(session, norm_path)

        if not existing:
            created = await create_csv_file(
                session,
                path=norm_path,
                checksum=checksum,
                status=FileStatus.PENDING,
                last_row_index=0,
            )
            logger.info("Registered new CSV file: %s", norm_path)
            return created

        if existing.get("checksum") != checksum:
            updated = await update_csv_file_checksum(
                session,
                file_id=existing["id"],
                new_checksum=checksum,
                status=FileStatus.PENDING,
                last_row_index=0,
            )
            logger.info("CSV file changed, will re-ingest: %s", norm_path)
            return updated

        logger.info(
            "CSV file unchanged: %s (status=%s)",
            norm_path,
            existing.get("status"),
        )
        return existing

    async def mark_file_as_done(self, session: AsyncSession, file_meta: Dict):
        total_rows = await count_total_rows(file_meta.get("path"))
        return await update_csv_file_status(
            session, file_meta.get("id"), FileStatus.DONE, total_rows
        )

    async def mark_file_as_failed(self, session: AsyncSession, file_meta: Dict):
        current_last_row_index = file_meta.get("last_row_index")
        return await update_csv_file_status(
            session, file_meta.get("id"), FileStatus.FAILED, current_last_row_index
        )
