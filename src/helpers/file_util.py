import os
import hashlib
import csv
import asyncio
from typing import List


def compute_file_checksum_sync(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(8192)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def scan_folder_sync(folder_path: str) -> List[str]:
    out = []
    for root, _, files in os.walk(folder_path):
        for fname in files:
            if fname.lower().endswith(".csv"):
                out.append(os.path.join(root, fname))
    return out


def normalized_path(path: str) -> str:
    return os.path.normpath(path).replace("\\", "/")


async def count_total_rows(file_path: str) -> int:
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
            return 0
        except Exception:
            return 0
        return row_count

    return await asyncio.to_thread(_count_rows_sync)
