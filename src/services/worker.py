# src/worker.py
"""
Features:
- lazy imports of tool factories to reduce startup cost
- runs both sync and async tool methods safely (asyncio.run)
- supports run_tool_task, ingest_tool_task and folder ingestion via ingest_folder
- robust task config: acks_late, soft/hard time limits, retries with backoff
"""

import os
import traceback
import asyncio
from typing import Any, Dict, Optional

from celery import Celery, Task

from src.config.settings import settings
from src.config.celery import CELERY_CONFIG
from src.config.logger import logging

logger = logging.getLogger(__name__)


celery_app = Celery("mcp_worker", broker=settings.redis_url, backend=settings.redis_url)

celery_app.conf.update(CELERY_CONFIG)

DEFAULT_SOFT_TIME_LIMIT = settings.worker_task_soft_time_limit
DEFAULT_TIME_LIMIT = settings.worker_task_time_limit
DEFAULT_MAX_RETRIES = settings.worker_max_retries

celery_app.conf.update(
    task_acks_late=True,
    worker_prefetch_multiplier=1,  # avoid large prefetch
    result_expires=int(os.getenv("CELERY_RESULT_EXPIRES", "3600")),
    task_annotations={
        "*": {"rate_limit": os.getenv("CELERY_RATE_LIMIT", None) or None}
    },
    enable_utc=True,
    task_track_started=True,
)


DEFAULT_SOFT_TIME_LIMIT = int(os.getenv("WORKER_TASK_SOFT_TIME_LIMIT", "300"))
DEFAULT_TIME_LIMIT = int(os.getenv("WORKER_TASK_TIME_LIMIT", "360"))
DEFAULT_MAX_RETRIES = int(os.getenv("WORKER_MAX_RETRIES", "3"))


class BaseToolTask(Task):
    """Common Task base to provide unified failure logging."""

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        logger.error(
            "Task %s failed. exc=%s task_id=%s args=%s kwargs=%s",
            self.name,
            exc,
            task_id,
            args,
            kwargs,
        )
        logger.error("Traceback: %s", einfo)

    def on_success(self, retval, task_id, args, kwargs):
        logger.info("Task %s succeeded task_id=%s", self.name, task_id)


def _safe_call_sync_or_async(fn, *args, **kwargs):
    """
    Helper to call a function which may be sync or async.
    If async -> run via asyncio.run(); if sync -> call directly.
    """
    if asyncio.iscoroutinefunction(fn):
        return asyncio.run(fn(*args, **kwargs))
    else:
        return fn(*args, **kwargs)


def _invoke_tool_method(
    tool, method_name: str, kwargs: Optional[Dict[str, Any]] = None
):
    """
    Invoke tool.<method_name>(**kwargs) where method may be sync or async.
    Returns result or raises.
    """
    kwargs = kwargs or {}
    method = getattr(tool, method_name, None)
    if method is None:
        raise AttributeError(f"Tool missing method: {method_name}")

    return _safe_call_sync_or_async(method, **kwargs)


# ---- Tasks ----


@celery_app.task(
    name="ingest_tool_task",
    bind=True,
    base=BaseToolTask,
    acks_late=True,
    soft_time_limit=settings.worker_ingest_soft_time_limit,
    time_limit=settings.worker_ingest_time_limit,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_kwargs={"max_retries": settings.worker_ingest_max_retries},
)
def run_tool_task(self, tool_name: str, kwargs: Optional[Dict[str, Any]] = None):
    """
    Execute tool.run(**kwargs) inside Celery worker.
    Returns a JSON-serializable payload with either "result" or "error".
    This task auto-retries on exceptions (configurable).
    """
    kwargs = kwargs or {}
    try:
        from src.app.tool.adapters import TOOL_FACTORIES

        factory = TOOL_FACTORIES.get(tool_name)
        if factory is None:
            return {"tool": tool_name, "error": "unknown tool"}

        tool = factory()

        init = getattr(tool, "initialize", None)
        if init:
            _safe_call_sync_or_async(init)

        run_fn = getattr(tool, "run", None)
        if run_fn is None:
            return {"tool": tool_name, "error": "missing run()"}

        result = _safe_call_sync_or_async(run_fn, **(kwargs or {}))
        return {"tool": tool_name, "result": result}

    except Exception as e:
        tb = traceback.format_exc()
        logger.exception("run_tool_task: error running %s: %s\n%s", tool_name, e, tb)
        raise


@celery_app.task(
    name="ingest_tool_task",
    bind=True,
    base=BaseToolTask,
    acks_late=True,
    soft_time_limit=settings.worker_ingest_soft_time_limit,
    time_limit=settings.worker_ingest_time_limit,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_kwargs={"max_retries": settings.worker_ingest_max_retries},
)
def ingest_tool_task(self, tool_name: str, kwargs: Optional[Dict[str, Any]] = None):
    """
    Ingest task - calls tool.ingest(...) or tool.ingest_folder(folder_path=...).
    kwargs may be:
      - {"rows": [...]}  -> calls tool.ingest(rows=...)
      - {"folder_path": "/path/to/csvs", ...} -> calls tool.ingest_folder(folder_path=..., **rest)
      - any mapping depending on tool implementation
    """
    kwargs = kwargs or {}
    try:
        from src.app.tool.adapters import TOOL_FACTORIES

        factory = TOOL_FACTORIES.get(tool_name)
        if factory is None:
            return {"tool": tool_name, "error": "unknown tool"}

        tool = factory()

        init = getattr(tool, "initialize", None)
        if init:
            _safe_call_sync_or_async(init)

        if (
            isinstance(kwargs, dict)
            and "folder_path" in kwargs
            and hasattr(tool, "ingest_folder")
        ):
            folder_path = kwargs.pop("folder_path")

            res = _safe_call_sync_or_async(
                getattr(tool, "ingest_folder"), folder_path, **(kwargs or {})
            )
            return {"tool": tool_name, "result": res}

        ingest_fn = getattr(tool, "ingest", None)
        if ingest_fn is None:
            return {
                "tool": tool_name,
                "error": "missing ingest() and no ingest_folder provided",
            }

        res = _safe_call_sync_or_async(ingest_fn, **(kwargs or {}))
        return {"tool": tool_name, "result": res}

    except Exception as e:
        tb = traceback.format_exc()
        logger.exception("ingest_tool_task: error for %s: %s\n%s", tool_name, e, tb)
        raise
