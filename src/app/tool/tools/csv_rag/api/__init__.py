from fastapi import APIRouter

from .router import router as rag_api
rag_router = APIRouter()

rag_router.include_router(rag_api)