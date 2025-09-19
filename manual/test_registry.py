import pytest
import  asyncio
from src.app.tool.registry import ToolRegistry

class DummyTool:
    def __init__(self, name):
        self.name = name
        self.ready = False
    async def initialize(self):
        await asyncio.sleep(0.01)
        self.ready = True
    async def run(self, **kw):
        return {"ok": True, "kw": kw}

@pytest.mark.asyncio
async def test_registry_init_and_invoke():
    r = ToolRegistry()
    t1 = DummyTool("t1")
    await r.register(t1, concurrency_limit=2)
    await r.initialize_all(timeout=1.0)
    await r.wait_until_ready(timeout=1.0)
    assert r.get("t1") is t1
    async with await r.invoke_slot("t1"):
        adapter = r.get("t1")
        result = await adapter.run(x=1)
        assert result["ok"] is True
