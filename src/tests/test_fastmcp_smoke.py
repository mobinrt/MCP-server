import pytest
from src.app.tool.registry import csv_rag_tool, weather_tool


@pytest.mark.asyncio
async def test_csv_rag_initialize():
    await csv_rag_tool.initialize()
    assert csv_rag_tool._ready


@pytest.mark.asyncio
async def test_weather_initialize():
    await weather_tool.initialize()
    assert weather_tool._ready


@pytest.mark.asyncio
async def test_csv_rag_run_empty():
    result = await csv_rag_tool.run("nonexistent query", top_k=3)
    assert isinstance(result, list)


@pytest.mark.asyncio
async def test_weather_run_invalid_city():
    with pytest.raises(Exception):
        await weather_tool.run("unknown_city")
