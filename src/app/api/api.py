from fastapi import FastAPI, HTTPException
from .schemas import CsvQuery, CsvIngest, WeatherQuery
from src.app.tool.registry import registry

app = FastAPI(title="MCP HTTP Shim")


@app.post("/tool/csv_rag")
async def csv_rag_endpoint(payload: CsvQuery):
    try:
        return await registry.call("csv_rag", payload.query, payload.top_k)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/tool/csv_rag_ingest")
async def csv_rag_ingest_endpoint(payload: CsvIngest):
    try:
        return await registry.call(
            "csv_rag_ingest", payload.folder_path, payload.batch_size
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/tool/weather")
async def weather_endpoint(payload: WeatherQuery):
    try:
        return await registry.call("weather", payload.city)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
async def health():
    return await registry.call("health_ping")
