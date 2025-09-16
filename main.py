import asyncio
import logging
import sys
import os

# # Ensure src is in sys.path
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.app.tool.tools.csv_rag.rag import CsvRagTool
from src.config.db import db
from src.config.vector_store import VectorStore

logging.basicConfig(level=logging.INFO)


async def main():
    # --- 1. Setup DB + Vector store (adapt these imports to your repo config)
    vs = VectorStore().get()

    # --- 2. Init CsvRagTool
    tool = CsvRagTool(db, vs)
    await tool.initialize()

    # --- 3. Ingest folder
    folder = "static/csv"   # <- change to where your test CSVs are
    logging.info("Ingesting folder: %s", folder)
    await tool.ingest_folder(folder_path=folder, batch_size=128)

    # --- 4. Run query
    query = "test query"
    results = await tool.run(query, top_k=5)
    print("\n=== Query Results ===")
    for r in results:
        print(f"[score={r['score']}] id={r['id']} external_id={r['external_id']}")
        print(f"fields={r['fields']}")
        print("---")


if __name__ == "__main__":
    asyncio.run(main())
