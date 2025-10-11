import asyncio

from src.config import db
from src.app.tool.tools.csv_rag.rag import CsvRagTool
from src.config.vector_store import VectorStore
from src.config.logger import logging

logger = logging.getLogger(__name__)


async def main():
    # --- 1. Setup DB + Vector store
    vs = VectorStore().get()
    # --- 2. Init CsvRagTool
    tool = CsvRagTool(db, vs)
    await tool.initialize()

    # --- 3. Ingest folder
    folder = "static/csv"  # <- change to where your test CSVs are
    logger.info("Ingesting folder: %s", folder)
    await tool.ingest_folder(folder_path=folder, batch_size=128)

    # --- 4. Run query
    query = "بیمارستان"
    args = {"query": query, "top_k": 5*3}
    results = await tool.run(args)
    print("\n=== Query Results ===")
    for r in results["result"]:
        print(f"[score={r['score']}] id={r['id']} external_id={r['external_id']}")
        print(f"fields={r['fields']}")
        print("---")


if __name__ == "__main__":
    asyncio.run(main())
