
import uvicorn
from fastapi import status, Request

from src import app  # noqa: F401  # type: ignore[reportUnusedImport]
from src.app.tool.tools.csv_rag.api import rag_router
@app.get("/")
def start():
    return "this is my Mini GPT project!!"


app.include_router(rag_router)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

