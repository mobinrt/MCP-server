from typing import Any, Optional

import aiohttp
from src.base.adapter_base import AdapterBase
from src.config.logger import logging

logger = logging.getLogger(__name__)


class HttpToolAdapter(AdapterBase):
    def __init__(
        self, name: str, base_url: str, token: Optional[str] = None, timeout: int = 10
    ):
        self._name = name
        self._base_url = base_url.rstrip("/")
        self._token = token
        self._timeout = timeout
        self._ready = False

    @property
    def name(self) -> str:
        return self._name

    @property
    def description(self) -> str:
        return f"Remote HTTP tool at {self._base_url}"

    async def initialize(self) -> None:
        headers = {}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(
                    f"{self._base_url}/health", headers=headers, timeout=self._timeout
                ) as resp:
                    if resp.status == 200:
                        self._ready = True
                        logger.info("HttpToolAdapter %s healthy", self._name)
                    else:
                        logger.warning(
                            "HttpToolAdapter %s health returned %s",
                            self._name,
                            resp.status,
                        )
                        self._ready = False
        except Exception as e:
            logger.warning("HttpToolAdapter %s health check failed: %s", self._name, e)
            self._ready = False

    @property
    def ready(self) -> bool:
        return bool(self._ready)

    async def run(self, **kwargs) -> Any:
        headers = {"Content-Type": "application/json"}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        async with aiohttp.ClientSession() as s:
            async with s.post(
                f"{self._base_url}/invoke",
                json={"input": kwargs},
                headers=headers,
                timeout=self._timeout,
            ) as resp:
                text = await resp.text()
                if resp.status != 200:
                    logger.error(
                        "Remote tool %s error: %s %s", self._name, resp.status, text
                    )
                    raise RuntimeError(f"Remote tool error: {resp.status} {text}")
                try:
                    return await resp.json()
                except Exception:
                    return text
