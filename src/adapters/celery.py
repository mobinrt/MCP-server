import asyncio
from typing import Any, Optional

from src.config.settings import settings
from src.config.logger import logging
from src.base.adapter_base import AdapterBase
import src.services.worker as worker 

logger = logging.getLogger(__name__)


class CeleryAdapter(AdapterBase):
    """
    Uses Celery to send a task named 'run_tool_task' to run the requested tool.
    """

    def __init__(self, name: str, redis_url: Optional[str] = None, timeout: int = 60):
        self._name = name
        self._redis_url = redis_url or settings.redis_url
        self._timeout = int(settings.tool_celery_timeout)
        self._ready = True
        self._celery = worker.celery_app

    @property
    def name(self) -> str:
        return self._name

    @property
    def description(self) -> str:
        return f"Remote Celery-executed tool ({self._name})"

    async def initialize(self) -> None:
        self._ready = True

    @property
    def ready(self) -> bool:
        return bool(self._ready)

    async def run(self, args: dict) -> Any:
        def _send_and_get():
            try:
                async_result = self._celery.send_task(
                    "run_tool_task", args=[self._name, args]
                )
                return async_result.get(timeout=self._timeout)
            except Exception as e:
                logger.exception("Celery task for %s failed: %s", self._name, e)
                raise RuntimeError(f"Celery task for {self._name} failed") from e

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, _send_and_get)
