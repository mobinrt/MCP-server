"""
Features:
- register(adapter, concurrency_limit=None)
- unregister(name)
- get(name)
- list() and list_with_meta()
- initialize_all(timeout) with concurrent initialization and first-exception behavior
- wait_until_ready(timeout) -> waits until all tools with .ready expose True
- listeners/subscriptions for lifecycle events
- per-tool concurrency limits (asyncio.Semaphore) and an async context manager to acquire invocation slot
- call registry + cancellation support (call_id -> asyncio.Task)
"""

from __future__ import annotations
import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Iterable, Tuple
import time
import functools
import uuid

logger = logging.getLogger(__name__)


Listener = Callable[[str, Dict[str, Any]], Any]


@dataclass
class ToolEntry:
    name: str
    adapter: Any
    ready: bool = False
    initializing_task: Optional[asyncio.Task] = None
    semaphore: Optional[asyncio.Semaphore] = None
    running_count: int = 0
    concurrency_limit: Optional[int] = None
    last_error: Optional[str] = None


class RegistryError(Exception):
    pass


class ToolRegistry:
    def __init__(self) -> None:
        self._tools: Dict[str, ToolEntry] = {}
        self._lock = asyncio.Lock()
        self._state_lock = asyncio.Lock()
        self._listeners: List[Listener] = []
        # call_id -> asyncio.Task (for cancellation)
        self._calls: Dict[str, asyncio.Task] = {}

    async def register(
        self, adapter: Any, concurrency_limit: Optional[int] = None
    ) -> None:
        """
        Register an adapter. adapter must expose .name and optionally .description, .initialize(), .ready, .run().
        concurrency_limit: if provided, limits concurrent run() invocations for this tool.
        """
        name = getattr(adapter, "name", None)
        if not name:
            raise RegistryError("Adapter must have a .name property")

        async with self._lock:
            if name in self._tools:
                logger.warning("Replacing existing tool registration: %s", name)
            entry = ToolEntry(name=name, adapter=adapter)
            if concurrency_limit is not None:
                entry.concurrency_limit = concurrency_limit
                entry.semaphore = asyncio.Semaphore(concurrency_limit)
            self._tools[name] = entry
            logger.info("Tool registered: %s (concurrency=%s)", name, concurrency_limit)
        await self._emit_event(
            "tool_registered", {"name": name, "concurrency_limit": concurrency_limit}
        )

    async def unregister(self, name: str) -> None:
        async with self._lock:
            entry = self._tools.pop(name, None)
        if entry:
            logger.info("Tool unregistered: %s", name)
            await self._emit_event("tool_unregistered", {"name": name})

    def get(self, name: str) -> Optional[Any]:
        entry = self._tools.get(name)
        return entry.adapter if entry else None

    def list(self) -> List[str]:
        return list(self._tools.keys())

    def list_with_meta(self) -> List[Dict[str, Any]]:
        result = []
        for name, entry in self._tools.items():
            result.append(
                {
                    "name": name,
                    "ready": entry.ready,
                    "concurrency_limit": entry.concurrency_limit,
                    "running_count": entry.running_count,
                    "description": getattr(entry.adapter, "description", None),
                    "last_error": entry.last_error,
                }
            )
        return result

    async def initialize_all(self, timeout: float = 300.0) -> None:
        """
        Call initialize() concurrently on all registered adapters.
        Behavior:
          - Run initialize() for each tool concurrently.
          - If any initialize() raises, cancel pending inits and re-raise the first exception.
          - If timeout expires before all inits finish, cancel pending tasks and raise TimeoutError.
        """
        async with self._lock:
            entries = list(self._tools.values())

        tasks_map: Dict[asyncio.Task, ToolEntry] = {}

        for entry in entries:
            init_fn = getattr(entry.adapter, "initialize", None)
            if not init_fn:
                entry.ready = bool(getattr(entry.adapter, "ready", True))
                continue

            # define coroutine wrapper
            async def _init_wrapper(e: ToolEntry):
                try:
                    fn = getattr(e.adapter, "initialize")
                    if asyncio.iscoroutinefunction(fn):
                        await fn()
                    else:
                        loop = asyncio.get_running_loop()
                        await loop.run_in_executor(None, fn)

                    e.ready = bool(getattr(e.adapter, "ready", True))
                    e.last_error = None
                    logger.info("Initialized tool %s -> ready=%s", e.name, e.ready)
                    await self._emit_event(
                        "tool_ready", {"name": e.name, "ready": e.ready}
                    )
                except Exception as exc:
                    e.ready = False
                    e.last_error = str(exc)
                    logger.exception(
                        "Initialization failed for tool %s: %s", e.name, exc
                    )
                    await self._emit_event(
                        "tool_not_ready", {"name": e.name, "error": str(exc)}
                    )
                    raise

            task = asyncio.create_task(_init_wrapper(entry))
            entry.initializing_task = task
            tasks_map[task] = entry

        if not tasks_map:
            return

        done, pending = await asyncio.wait(
            tasks_map.keys(), return_when=asyncio.FIRST_EXCEPTION, timeout=timeout
        )

        for d in done:
            if d.cancelled():
                continue
            exc = d.exception()
            if exc:
                for p in pending:
                    p.cancel()
                raise exc

        if pending:
            for p in pending:
                p.cancel()
            raise TimeoutError(f"Timeout initializing tools after {timeout} seconds")

        return

    async def initialize_tool(self, name: str, timeout: Optional[float] = None) -> None:
        """
        Initialize a single tool by name.
        """
        entry = self._tools.get(name)
        if not entry:
            raise RegistryError(f"Tool not found: {name}")
        fn = getattr(entry.adapter, "initialize", None)
        if not fn:
            entry.ready = bool(getattr(entry.adapter, "ready", True))
            return
        try:
            if asyncio.iscoroutinefunction(fn):
                if timeout:
                    await asyncio.wait_for(fn(), timeout=timeout)
                else:
                    await fn()
            else:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, fn)
            entry.ready = bool(getattr(entry.adapter, "ready", True))
            entry.last_error = None
            await self._emit_event(
                "tool_ready", {"name": entry.name, "ready": entry.ready}
            )
        except Exception as exc:
            entry.ready = False
            entry.last_error = str(exc)
            await self._emit_event(
                "tool_not_ready", {"name": entry.name, "error": str(exc)}
            )
            raise

    async def wait_until_ready(self, timeout: float = 60.0, poll: float = 0.5) -> None:
        """
        Wait until all registered tools with a `.ready` property are True.
        Tools without a `.ready` attribute are considered ready after initialize returned.
        """
        start = time.time()
        while True:
            not_ready = []
            async with self._lock:
                for entry in self._tools.values():
                    adapter_ready = getattr(entry.adapter, "ready", None)
                    if adapter_ready is None:
                        ok = entry.ready
                    else:
                        ok = bool(adapter_ready)
                    if not ok:
                        not_ready.append(entry.name)
            if not not_ready:
                return
            if time.time() - start > timeout:
                raise TimeoutError(f"Tools not ready after {timeout}s: {not_ready}")
            await asyncio.sleep(poll)

    class _InvocationCtx:
        def __init__(
            self, registry: "ToolRegistry", entry: ToolEntry, call_id: Optional[str]
        ):
            self._registry = registry
            self._entry = entry
            self._call_id = call_id

        async def __aenter__(self):
            if self._entry.semaphore is not None:
                await self._entry.semaphore.acquire()
            async with self._registry._state_lock:
                self._entry.running_count += 1
                await self._registry._emit_event(
                    "call_started",
                    {
                        "name": self._entry.name,
                        "running_count": self._entry.running_count,
                        "call_id": self._call_id,
                    },
                )
            return self

        async def __aexit__(self, exc_type, exc, tb):
            async with self._registry._state_lock:
                self._entry.running_count = max(0, self._entry.running_count - 1)
                await self._registry._emit_event(
                    "call_finished",
                    {
                        "name": self._entry.name,
                        "running_count": self._entry.running_count,
                        "call_id": self._call_id,
                    },
                )
            # release semaphore if any
            if self._entry.semaphore is not None:
                try:
                    self._entry.semaphore.release()
                except ValueError:
                    # semaphore release error shouldn't crash
                    logger.exception(
                        "Semaphore release failed for %s", self._entry.name
                    )

    async def invoke_slot(self, name: str, call_id: Optional[str] = None):
        """
        Async context manager to acquire an invocation slot for `name`.
        Usage:
            async with registry.invoke_slot("csv_rag", call_id=cid):
                result = await adapter.run(...)
        """
        entry = self._tools.get(name)
        if not entry:
            raise RegistryError(f"Tool not found: {name}")
        return ToolRegistry._InvocationCtx(self, entry, call_id)

    async def set_concurrency_limit(self, name: str, limit: Optional[int]) -> None:
        """
        Set or update concurrency limit for a tool.
        If limit is None -> unlimited (remove semaphore).
        """
        async with self._state_lock:
            entry = self._tools.get(name)
            if not entry:
                raise RegistryError(f"Tool not found: {name}")
            entry.concurrency_limit = limit
            if limit is None:
                entry.semaphore = None
                return
            # create a new semaphore with available slots = max(limit - running_count, 0)
            available = max(limit - entry.running_count, 0)
            entry.semaphore = asyncio.Semaphore(available)

    def register_call(self, call_id: Optional[str], task: asyncio.Task) -> str:
        """
        Register a running asyncio.Task with a call_id. If call_id is None, create one.
        Returns the call_id used.
        """
        if call_id is None:
            call_id = str(uuid.uuid4())
        self._calls[call_id] = task
        return call_id

    async def cancel_call(self, call_id: str) -> bool:
        """
        Cancel a registered call by id. Returns True if cancelled (or already done).
        """
        task = self._calls.get(call_id)
        if not task:
            return False
        cancelled = task.cancel()
        try:
            await asyncio.wait_for(task, timeout=2.0)
        except asyncio.CancelledError:
            pass
        except Exception:
            pass
        self._calls.pop(call_id, None)
        await self._emit_event("call_cancelled", {"call_id": call_id})
        return cancelled

    # Listeners / events
    def add_listener(self, listener: Listener) -> Callable[[], None]:
        """
        Add a listener callable(event_name, payload). It may be sync or async.
        Returns an unsubscribe function.
        """
        self._listeners.append(listener)

        def _unsubscribe():
            try:
                self._listeners.remove(listener)
            except ValueError:
                pass

        return _unsubscribe

    async def _emit_event(self, event_name: str, payload: Dict[str, Any]) -> None:
        """
        Emit event to all listeners. Schedule async listeners via create_task,
        run sync listeners in threadpool to avoid blocking.
        """
        for listener in list(self._listeners):
            try:
                if asyncio.iscoroutinefunction(listener):
                    asyncio.create_task(listener(event_name, payload))
                else:
                    loop = asyncio.get_running_loop()
                    loop.run_in_executor(
                        None, functools.partial(listener, event_name, payload)
                    )
            except Exception:
                logger.exception("Listener failure for event %s", event_name)

    # Status / diagnostics
    def get_status(self) -> Dict[str, Any]:
        """
        Return dict with aggregated status and detail per tool.
        {
          "all_ready": bool,
          "tools": {
              "<name>": { "ready": bool, "running_count": int, "concurrency_limit": int|None, "last_error": str|None }
          }
        }
        """
        tools = {}
        all_ready = True
        for name, entry in self._tools.items():
            adapter_ready = getattr(entry.adapter, "ready", None)
            ready = (
                bool(adapter_ready) if adapter_ready is not None else bool(entry.ready)
            )
            if not ready:
                all_ready = False
            tools[name] = {
                "ready": ready,
                "running_count": entry.running_count,
                "concurrency_limit": entry.concurrency_limit,
                "last_error": entry.last_error,
                "description": getattr(entry.adapter, "description", None),
            }
        return {"all_ready": all_ready, "tools": tools}
