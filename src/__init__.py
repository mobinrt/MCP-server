from fastapi import FastAPI
from contextlib import asynccontextmanager
import logging

import src.config.logger as _  # noqa: F401  # type: ignore[reportUnusedImport]
from src.config.db import db
from app.tool.tools.csv_rag.rag import CsvRagTool
from app.tool.tools.csv_rag.loader import CSVLoader
from src.app.tool.tools.csv_rag.chromadb import ChromaVectorStore

logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting up... preparing RAG tool")

    try:
        vector_store = ChromaVectorStore(persist_path="static/chroma")
        app.state.csv_rag_tool = CsvRagTool(db, vector_store)

        csv_path = "static/csv/fixed_civil_places.csv"

        rows = CSVLoader.stream_csv_async(csv_path)

        await app.state.csv_rag_tool.ingest(rows, batch_size=256)

        logger.info("CSV ingestion complete, tool is ready")

    except Exception:
        logger.exception("Failed to initialize RAG tool during startup")
        app.state.csv_rag_tool = None

    yield
    
    logger.info("Shutting down...")


app = FastAPI(lifespan=lifespan)


