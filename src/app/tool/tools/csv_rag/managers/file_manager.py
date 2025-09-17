import os
import hashlib
import asyncio
import csv
from typing import List, Dict


from src.config import Database
from src.config.logger import logging
from sqlalchemy.ext.asyncio import AsyncSession
from src.enum.csv_status import FileStatus
from src.app.tool.tools.csv_rag.crud.crud_file import (
    get_csv_file,
    create_csv_file,
    update_csv_file_checksum,
    update_csv_file_status,
)

logger = logging.getLogger(__name__)


def _compute_file_checksum_sync(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(8192)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _scan_folder_sync(folder_path: str) -> List[str]:
    out = []
    for root, _, files in os.walk(folder_path):
        for fname in files:
            if fname.lower().endswith(".csv"):
                out.append(os.path.join(root, fname))
    return out


class CSVFileManager:
    def __init__(self, db: Database):
        self.db = db

    async def compute_file_checksum(self, file_path: str) -> str:
        norm_path = self._normalized_path(file_path)
        return await asyncio.to_thread(_compute_file_checksum_sync, norm_path)

    async def scan_folder(self, folder_path: str) -> List[str]:
        return await asyncio.to_thread(_scan_folder_sync, folder_path)

    async def get_or_register_file(self, session: AsyncSession, file_path: str) -> Dict:
        norm_path = self._normalized_path(file_path)
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
        total_rows = await self.count_total_rows(file_meta.get("path"))
        return await update_csv_file_status(
            session, file_meta.get("id"), FileStatus.DONE, total_rows
        )

    async def mark_file_as_failed(self, session: AsyncSession, file_meta: Dict):
        current_last_row_index = file_meta.get("last_row_index")
        return await update_csv_file_status(
            session, file_meta.get("id"), FileStatus.FAILED, current_last_row_index
        )

    async def count_total_rows(self, file_path: str) -> int:
        """
        Count the number of data rows in a CSV file (excluding the header).
        """

        def _count_rows_sync():
            row_count = 0
            try:
                with open(file_path, "r", encoding="utf-8", newline="") as f:
                    reader = csv.reader(f)

                    next(reader, None)
                    for _ in reader:
                        row_count += 1
            except FileNotFoundError:
                logger.error(f"File not found: {file_path}")
                return 0
            except Exception as e:
                logger.error(f"Error counting rows in file {file_path}: {e}")
                return 0
            return row_count

        return await asyncio.to_thread(_count_rows_sync)

    def _normalized_path(self, path: str) -> str:
        return os.path.normpath(path).replace("\\", "/")
