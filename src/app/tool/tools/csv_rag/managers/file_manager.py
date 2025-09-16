import os
import hashlib
import asyncio
import logging
from typing import List, Dict

from sqlalchemy.ext.asyncio import AsyncSession

from src.app.tool.tools.csv_rag.crud.crud_file import (
    get_csv_file,
    create_csv_file,
    update_csv_file_checksum,
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
    def __init__(self, db):
        self.db = db

    async def compute_file_checksum(self, file_path: str) -> str:
        return await asyncio.to_thread(_compute_file_checksum_sync, file_path)

    async def scan_folder(self, folder_path: str) -> List[str]:
        return await asyncio.to_thread(_scan_folder_sync, folder_path)

    async def get_or_register_file(self, session: AsyncSession, file_path: str) -> Dict:
        """
        Ensure a CSVFile record exists and return a mapping. Also attach transient
        boolean key '_needs_reingest' (True if new or checksum changed).
        """
        checksum = await self.compute_file_checksum(file_path)
        existing = await get_csv_file(session, file_path)
        if not existing:
            created = await create_csv_file(session, file_path, checksum)
            created["_needs_reingest"] = True
            logger.info("Registered new CSV file: %s", file_path)
            return created
        if existing.get("checksum") != checksum:
            updated = await update_csv_file_checksum(session, existing["id"], checksum)
            updated["_needs_reingest"] = True
            logger.info("CSV file changed, will re-ingest: %s", file_path)
            return updated
        existing["_needs_reingest"] = False
        return existing
