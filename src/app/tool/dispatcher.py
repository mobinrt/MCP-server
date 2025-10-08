from typing import Dict, Any

from src.adapters import InProcessAdapter, CeleryAdapter
from src.config.settings import settings
from src.app.tool.registry import registry
from src.config.logger import logging

logger = logging.getLogger(__name__)

TOOLS_RUN_WITH_CELERY = settings.tools_run_with_celery


async def dispatch_tool(tool_name: str, args: dict):
    """
    Decide whether to run tool in-process or via Celery.
    `tool_name` should be the *base* tool name, e.g. "csv_rag".
    """
    args = args or {}
    if getattr(settings, "use_celery", False) and tool_name in TOOLS_RUN_WITH_CELERY:
        adapter = CeleryAdapter(tool_name)
        payload = args if "args" in args else {"args": args}
        logger.info("payload: %s", payload)
        return await adapter.run(payload)


    impl = registry.get(tool_name)
    if impl is None:
        raise ValueError(f"Unknown tool (no instance registered): {tool_name}")

    adapter = InProcessAdapter(impl)
    payload = args if "args" in args else {"args": args}
    logger.info("payload: %s", payload)
        
    return await adapter.run(payload)
