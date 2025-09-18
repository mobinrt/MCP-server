from src.config.logger import logging
from src.base.adapter_base import AdapterBase
from src.app.adapters import CeleryAdapter, InProcessAdapter, HttpToolAdapter
from src.app.tool.factories import TOOL_FACTORIES
from src.enum.executor import Executor

logger = logging.getLogger(__name__)


def create_adapter_for(
    tool_name: str, executor: str = Executor.IN_PROCESS.value, **executor_kwargs
) -> AdapterBase:
    """
    executor: "in_process" | "http" | "celery"
    executor_kwargs for http: base_url, token, timeout
    executor_kwargs for celery: redis_url, timeout
    """
    factory = TOOL_FACTORIES.get(tool_name)
    if factory is None:
        raise KeyError(f"Unknown tool: {tool_name}")
    if executor == Executor.IN_PROCESS.value:
        impl = factory()
        return InProcessAdapter(impl)
    if executor == Executor.HTTP.value:
        base_url = executor_kwargs.get("base_url")
        token = executor_kwargs.get("token")
        timeout = executor_kwargs.get("timeout", 10)
        return HttpToolAdapter(
            tool_name, base_url=base_url, token=token, timeout=timeout
        )
    if executor == Executor.CELERY.value:
        redis_url = executor_kwargs.get("redis_url")
        timeout = executor_kwargs.get("timeout", 60)
        return CeleryAdapter(tool_name, redis_url=redis_url, timeout=timeout)
    raise ValueError(f"Unknown executor: {executor}")
