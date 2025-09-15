from fastapi import HTTPException, APIRouter, status
from typing import Optional
import logging

from app.tool.tools.csv_rag.rag import CsvRagTool

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/rag",
    tags=["rag"],
)


@router.get(
    "/",
    status_code=status.HTTP_200_OK,
    responses={status.HTTP_404_NOT_FOUND: {"description": "RAG tool not initialized "}},
)
async def rag_query(query: str, top_k: int = 5):
    tool: Optional[CsvRagTool] = getattr(router.state, "csv_rag_tool", None)

    if not tool:
        logger.error("RAG tool is not initialized — cannot serve request")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="RAG tool not initialized"
        )
    try:
        results = await tool.run(query, top_k=top_k)
        logger.info(
            f"Handled RAG query='{query}' (top_k={top_k}) -> {len(results)} results"
        )
        return {"ok": True, "results": results}
    except Exception as e:
        logger.exception("Error while processing RAG query")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )
