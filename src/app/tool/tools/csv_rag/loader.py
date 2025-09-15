import csv
import aiofiles
import aiocsv
from typing import List, Dict, Iterable, AsyncIterable


class CSVLoader:
    @staticmethod
    def _format_row(idx: int, row: Dict) -> Dict:
        """
        Convert a raw CSV row dict into a standardized document format.
        """
        text = " | ".join(f"{k}: {v}" for k, v in row.items())
        return {"id": idx, "text": text, "metadata": row}

    @classmethod
    def load_csv(cls, file_path: str) -> List[Dict]:
        """
        Synchronous: load entire CSV into memory as a list of dicts.
        """
        docs = []
        with open(file_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for idx, row in enumerate(reader):
                docs.append(cls._format_row(idx, row))
        return docs

    @classmethod
    def stream_csv(cls, file_path: str) -> Iterable[Dict]:
        """
        Synchronous streaming generator over CSV rows.
        """
        with open(file_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for idx, row in enumerate(reader):
                yield cls._format_row(idx, row)

    @classmethod
    async def stream_csv_async(cls, file_path: str) -> AsyncIterable[Dict]:
        """
        Asynchronous streaming generator using aiofiles + aiocsv.
        """
        async with aiofiles.open(file_path, mode="r", encoding="utf-8") as f:
            reader = aiocsv.AsyncDictReader(f)
            async for idx, row in enumerate(reader):
                yield cls._format_row(idx, row)
