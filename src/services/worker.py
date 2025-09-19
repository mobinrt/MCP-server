"""
FastMCP-compatible Celery worker for MCP server (production-ready).

Key production features:
- Safe execution of sync/async tool methods
- Redis-backed idempotent ingestion locks with task-id stored
- Safe lock release (only the owner can release) via Lua script
- Lock auto-renewal (background thread) to support long-running ingests
- Consistent responses, better retry defaults, logging
- Uses FastMCP registry dynamically (no hard-coded tool map)
"""

import os
import asyncio
import json
import traceback
import threading
import hashlib
import uuid
from typing import Any, Dict, Optional

from celery import Celery, Task
from celery.exceptions import SoftTimeLimitExceeded
import redis

from src.config.logger import logging
from src.config.settings import settings
from src.config.celery import CELERY_CONFIG

from src.app.tool.fastmcp_registry import registry

logger = logging.getLogger(__name__)

celery_app = Celery("mcp_worker", broker=settings.redis_url, backend=settings.redis_url)
celery_app.conf.update(CELERY_CONFIG)

DEFAULT_SOFT_TIME_LIMIT = int(os.getenv("WORKER_TASK_SOFT_TIME_LIMIT", "300"))
DEFAULT_TIME_LIMIT = int(os.getenv("WORKER_TASK_TIME_LIMIT", "360"))
DEFAULT_MAX_RETRIES = int(os.getenv("WORKER_MAX_RETRIES", "3"))

celery_app.conf.update(
    task_acks_late=True,
    task_acks_on_failure_or_timeout=False,
    worker_prefetch_multiplier=1,
    result_expires=int(os.getenv("CELERY_RESULT_EXPIRES", "3600")),
    task_annotations={"*": {"rate_limit": os.getenv("CELERY_RATE_LIMIT", None)}},
    enable_utc=True,
    task_track_started=True,
)

redis_client = redis.from_url(settings.redis_url, decode_responses=True)

#  Lock Utilities
# Use a compare-and-delete Lua script to safely release locks
_RELEASE_LUA = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
else
    return 0
end
"""


def _make_lock_key(tool_name: str, kwargs: Dict[str, Any]) -> str:
    """
    Deterministic lock key for tool+kwargs; uses sha256 to prevent overlong keys and collisions.
    """
    payload = json.dumps(
        {"tool": tool_name, "kwargs": kwargs}, sort_keys=True, separators=(",", ":")
    )
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return f"fastmcp:ingest_lock:{digest}"


class RedisLock:
    """
    Redis-based lock that stores owner_id (we use Celery task_id).
    Provides:
    - acquire (nx + ex)
    - auto-renewal in a background thread while acquired
    - release using Lua script that only deletes if owner matches
    """

    def __init__(
        self,
        redis_client,
        key: str,
        owner_id: str,
        ttl: int = 900,
        renew_interval: int = 60,
    ):
        """
        ttl: seconds lock expiration (default 15m)
        renew_interval: how often renewal runs (default 60s)
        """
        self.redis = redis_client
        self.key = key
        self.owner_id = owner_id
        self.ttl = int(ttl)
        self.renew_interval = int(renew_interval)
        self._renew_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._is_acquired = False

    def acquire(self) -> bool:
        """
        Attempt to acquire lock. Stores owner_id as the value.
        Returns True if acquired, False otherwise.
        """
        try:
            acquired = self.redis.set(self.key, self.owner_id, nx=True, ex=self.ttl)
            if acquired:
                self._is_acquired = True
                # start renewal thread
                self._stop_event.clear()
                self._renew_thread = threading.Thread(
                    target=self._renew_loop, daemon=True
                )
                self._renew_thread.start()
                logger.debug("Lock acquired: %s by %s", self.key, self.owner_id)
                return True
            else:
                return False
        except Exception:
            logger.exception("Error acquiring lock %s", self.key)
            return False

    def _renew_loop(self):
        try:
            while not self._stop_event.wait(self.renew_interval):
                try:
                    cur = self.redis.get(self.key)
                    if cur != self.owner_id:
                        logger.warning(
                            "Lock %s no longer owned by this task (owner=%s, me=%s). Stopping renew.",
                            self.key,
                            cur,
                            self.owner_id,
                        )
                        break
                    self.redis.expire(self.key, self.ttl)
                    logger.debug(
                        "Lock %s renewed by %s (ttl=%s)",
                        self.key,
                        self.owner_id,
                        self.ttl,
                    )
                except Exception:
                    logger.exception("Error renewing lock %s", self.key)
        finally:
            logger.debug("Renew loop exiting for lock %s", self.key)

    def release(self):
        """
        Release lock only if owner matches (atomic via Lua).
        Stops renewal thread too.
        """
        try:
            self._stop_event.set()
            if self._renew_thread and self._renew_thread.is_alive():
                self._renew_thread.join(timeout=1.0)

            try:
                res = self.redis.eval(_RELEASE_LUA, 1, self.key, self.owner_id)
                if res == 1:
                    logger.debug(
                        "Lock %s released by owner %s", self.key, self.owner_id
                    )
                else:
                    logger.debug(
                        "Lock %s not released (not owned by %s)",
                        self.key,
                        self.owner_id,
                    )
            except redis.RedisError:
                logger.exception(
                    "Error releasing lock via Lua script for %s; falling back to delete",
                    self.key,
                )
                try:
                    cur = self.redis.get(self.key)
                    if cur == self.owner_id:
                        self.redis.delete(self.key)
                except Exception:
                    logger.exception(
                        "Fallback delete also failed for lock %s", self.key
                    )
        finally:
            self._is_acquired = False

    def is_owner(self) -> bool:
        try:
            return self.redis.get(self.key) == self.owner_id
        except Exception:
            return False

    @classmethod
    def get_lock_info(cls, redis_client, key: str) -> Optional[Dict[str, Any]]:
        """
        Return lock info dict: {'owner_id': str, 'ttl': int} or None
        """
        try:
            owner = redis_client.get(key)
            if owner is None:
                return None
            ttl = redis_client.ttl(key)
            return {"owner_id": owner, "ttl": int(ttl) if ttl is not None else -1}
        except Exception:
            logger.exception("Error fetching lock info for %s", key)
            return None


#  Helpers
def _safe_call_sync_or_async(fn, *args, **kwargs):
    """
    Safely call function that may be sync or async:
    - If coroutine function and no running loop -> asyncio.run()
    - If coroutine function and a running loop exists -> create task and run until complete via loop.run_until_complete
    """
    if asyncio.iscoroutinefunction(fn):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(fn(*args, **kwargs))
        else:
            coro = fn(*args, **kwargs)
            return loop.run_until_complete(coro)
    return fn(*args, **kwargs)


def _get_tool(tool_name: str):
    """Resolve tool from FastMCP registry."""
    tool = registry.get(tool_name)
    if not tool:
        raise ValueError(f"Unknown tool: {tool_name}")
    return tool


def _standard_response(tool: str, status: str, **extra):
    """Uniform API for task responses."""
    return {"tool": tool, "status": status, **extra}


#  Base Task
class BaseToolTask(Task):
    """
    Common base for logging and sane retry defaults.
    Keep retries explicit in code (self.retry) for fine control.
    """

    autoretry_for = (IOError, OSError)
    retry_backoff = True
    retry_kwargs = {"max_retries": DEFAULT_MAX_RETRIES}

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        logger.error(
            "Task %s failed (task_id=%s) exc=%s args=%s kwargs=%s",
            self.name,
            task_id,
            exc,
            args,
            kwargs,
        )
        logger.error("Traceback:\n%s", einfo)

    def on_success(self, retval, task_id, args, kwargs):
        logger.info("Task %s succeeded (task_id=%s)", self.name, task_id)


#  Celery Tasks
@celery_app.task(
    name="run_tool_task",
    bind=True,
    base=BaseToolTask,
    acks_late=True,
    soft_time_limit=DEFAULT_SOFT_TIME_LIMIT,
    time_limit=DEFAULT_TIME_LIMIT,
)
def run_tool_task(self, tool_name: str, kwargs: Optional[Dict[str, Any]] = None):
    kwargs = kwargs or {}
    try:
        tool = _get_tool(tool_name)

        if hasattr(tool, "initialize"):
            _safe_call_sync_or_async(tool.initialize)

        if not hasattr(tool, "run"):
            return _standard_response(
                tool_name, "error", message="Missing run() method"
            )

        result = _safe_call_sync_or_async(tool.run, **kwargs)
        return _standard_response(tool_name, "ok", result=result)

    except SoftTimeLimitExceeded:
        logger.error(
            "run_tool_task exceeded soft time limit for tool %s (task=%s)",
            tool_name,
            self.request.id,
        )
        raise

    except Exception as e:
        tb = traceback.format_exc()
        logger.exception("run_tool_task failed for %s: %s\n%s", tool_name, e, tb)
        raise


@celery_app.task(
    name="ingest_tool_task",
    bind=True,
    base=BaseToolTask,
    acks_late=True,
    soft_time_limit=DEFAULT_SOFT_TIME_LIMIT * 2,
    time_limit=DEFAULT_TIME_LIMIT * 2,
)
def ingest_tool_task(self, tool_name: str, kwargs: Optional[Dict[str, Any]] = None):
    """
    Ingestion task with robust Redis locking (idempotency).

    Behavior:
    - Lock key derived from (tool_name + kwargs).
    - Lock value = Celery task id (self.request.id).
    - If lock exists and belongs to another task, we return status "running" and the running_task_id.
    - If lock exists but owner is dead / expired, the new task may acquire it (normal Redis TTL behavior).
    - While running, the lock is auto-renewed periodically until task completes.
    - Lock released safely using Lua script that ensures only owner deletes it.
    """
    kwargs = kwargs or {}
    task_id = str(self.request.id or uuid.uuid4())
    lock_key = _make_lock_key(tool_name, kwargs)
    lock_ttl = int(os.getenv("INGEST_LOCK_TTL", "900"))  # default 15 minutes
    renew_interval = int(
        os.getenv("INGEST_LOCK_RENEW", "60")
    )  # default renew every 60s

    lock = RedisLock(
        redis_client,
        lock_key,
        owner_id=task_id,
        ttl=lock_ttl,
        renew_interval=renew_interval,
    )

    # try acquire
    acquired = lock.acquire()
    if not acquired:
        # fetch existing owner info
        info = RedisLock.get_lock_info(redis_client, lock_key)
        if info:
            running_task_id = info.get("owner_id")
            ttl_remaining = info.get("ttl")
            logger.warning(
                "Ingest skipped: tool=%s kwargs=%s already running (owner=%s ttl=%s)",
                tool_name,
                kwargs,
                running_task_id,
                ttl_remaining,
            )
            return _standard_response(
                tool_name, "running", running_task_id=running_task_id, ttl=ttl_remaining
            )
        else:
            # race condition: key disappeared between set and get, try once more to acquire
            acquired = lock.acquire()
            if not acquired:
                logger.warning(
                    "Ingest cannot acquire lock (race) for %s; skipping", tool_name
                )
                return _standard_response(
                    tool_name, "skipped", message="Could not acquire lock"
                )

    try:
        tool = _get_tool(tool_name)

        if hasattr(tool, "initialize"):
            _safe_call_sync_or_async(tool.initialize)

        if "folder_path" in kwargs and hasattr(tool, "ingest_folder"):
            folder_path = kwargs.pop("folder_path")
            result = _safe_call_sync_or_async(tool.ingest_folder, folder_path, **kwargs)
            return _standard_response(tool_name, "ok", task_id=task_id, result=result)

        if hasattr(tool, "ingest"):
            result = _safe_call_sync_or_async(tool.ingest, **kwargs)
            return _standard_response(tool_name, "ok", task_id=task_id, result=result)

        return _standard_response(
            tool_name,
            "error",
            message="No ingest_folder() or ingest() available",
            task_id=task_id,
        )

    except SoftTimeLimitExceeded:
        logger.error(
            "ingest_tool_task exceeded soft time limit for tool %s (task=%s)",
            tool_name,
            task_id,
        )
        raise

    except Exception as e:
        tb = traceback.format_exc()
        logger.exception(
            "ingest_tool_task failed for %s (task=%s): %s\n%s",
            tool_name,
            task_id,
            e,
            tb,
        )
        raise

    finally:
        try:
            lock.release()
        except Exception:
            logger.exception("Failed to release lock %s (owner=%s)", lock_key, task_id)


def get_ingest_lock_status(
    tool_name: str, kwargs: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    kwargs = kwargs or {}
    key = _make_lock_key(tool_name, kwargs)
    info = RedisLock.get_lock_info(redis_client, key)
    if not info:
        return {"tool": tool_name, "status": "free"}
    return {
        "tool": tool_name,
        "status": "running",
        "running_task_id": info.get("owner_id"),
        "ttl": info.get("ttl"),
    }
