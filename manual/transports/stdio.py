# src/transports/stdio_transport.py
"""
- Reads messages from stdin using Content-Length framing.
- Writes JSON-RPC responses and notifications to stdout (protocol only).
- All logs go to stderr (via standard logging configuration).
- Methods supported:
    - "tools/list" -> returns list of tools
    - "tools/status" -> registry.get_status()
    - "tools/call" -> { "tool": <name>, "input": {...}, "call_id": optional } -> returns result or streams notifications
    - "tools/cancel" -> { "call_id": <id> } -> cancels in-flight call
- Streaming: if adapter.run returns an async generator, we send notifications:
    {"jsonrpc":"2.0","method":"tools/event","params":{"call_id":..., "event": <partial>}}
  Final answer is sent as a result message (if there is an id) or as a final "tools/event" notification.
"""

from __future__ import annotations

import sys
import asyncio
import json
import logging
from typing import Any, Dict, Optional
import threading
import traceback

from src.app.tool.registry import ToolRegistry, RegistryError

logger = logging.getLogger(__name__)

_write_lock = threading.Lock()


def _write_stdout_message(msg: Dict[str, Any]):
    b = json.dumps(msg, ensure_ascii=False).encode("utf-8")
    header = f"Content-Length: {len(b)}\r\n\r\n".encode("utf-8")
    with _write_lock:
        sys.stdout.buffer.write(header)
        sys.stdout.buffer.write(b)
        sys.stdout.buffer.flush()


def _make_jsonrpc_result(req_id: Any, result: Any) -> Dict[str, Any]:
    return {"jsonrpc": "2.0", "id": req_id, "result": result}


def _make_jsonrpc_error(req_id: Any, code: int, message: str, data: Optional[Any] = None) -> Dict[str, Any]:
    err = {"code": code, "message": message}
    if data is not None:
        err["data"] = data
    return {"jsonrpc": "2.0", "id": req_id, "error": err}


def _make_notification(method: str, params: Any) -> Dict[str, Any]:
    return {"jsonrpc": "2.0", "method": method, "params": params}


def _read_content_length_prefixed_message() -> Optional[bytes]:
    """
    Blocking read from stdin using Content-Length framing.
    Returns bytes payload or None if EOF.
    """
    headers = {}
    while True:
        line = sys.stdin.buffer.readline()
        if not line:
            return None  
        if line in (b"\r\n", b"\n", b""):
            break
        try:
            k, v = line.decode("utf-8").split(":", 1)
            headers[k.strip().lower()] = v.strip()
        except Exception:
            continue
    length = headers.get("content-length")
    if not length:
        return None
    try:
        n = int(length)
    except Exception:
        return None
    data = sys.stdin.buffer.read(n)
    return data


class StdioTransport:
    def __init__(self, registry: ToolRegistry):
        self.registry = registry
        self._loop = asyncio.get_event_loop()
        self._stop = False

    def start(self):
        """
        Start a background thread to read stdin and schedule handling on event loop.
        """
        t = threading.Thread(target=self._reader_thread, daemon=True)
        t.start()
        logger.info("STDIO transport started (listening on stdin)")

    def stop(self):
        self._stop = True

    def _reader_thread(self):
        while not self._stop:
            try:
                payload = _read_content_length_prefixed_message()
                if payload is None:
                    logger.info("STDIN closed or invalid framing; stopping stdio transport")
                    break
                try:
                    obj = json.loads(payload.decode("utf-8"))
                except Exception as e:
                    logger.exception("Failed to parse JSON from stdin: %s", e)
                    continue
                asyncio.run_coroutine_threadsafe(self._handle_message(obj), self._loop)
            except Exception as e:
                logger.exception("Exception in stdio reader thread: %s", e)
                traceback.print_exc(file=sys.stderr)
                break

    async def _handle_message(self, msg: Dict[str, Any]):
        req_id = msg.get("id")
        method = msg.get("method")
        params = msg.get("params", {})

        try:
            if method == "tools/list":
                res = {"tools": self.registry.list()}
                if req_id is not None:
                    _write_stdout_message(_make_jsonrpc_result(req_id, res))
            elif method == "tools/status":
                res = self.registry.get_status()
                if req_id is not None:
                    _write_stdout_message(_make_jsonrpc_result(req_id, res))
            elif method == "tools/call":
                # params expected: {"tool": name, "input": {...}, "call_id": optional}
                tool = params.get("tool")
                input_obj = params.get("input", {})
                call_id = params.get("call_id") or None
                if not tool:
                    if req_id is not None:
                        _write_stdout_message(_make_jsonrpc_error(req_id, -32602, "Missing 'tool' in params"))
                    return

                # create a task to run adapter.run and stream if generator
                asyncio.create_task(self._run_tool_and_stream(tool, input_obj, req_id, call_id))
            elif method == "tools/cancel":
                call_id = params.get("call_id")
                if not call_id:
                    if req_id is not None:
                        _write_stdout_message(_make_jsonrpc_error(req_id, -32602, "Missing 'call_id'"))
                    return
                ok = await self.registry.cancel_call(call_id)
                if req_id is not None:
                    _write_stdout_message(_make_jsonrpc_result(req_id, {"cancelled": bool(ok)}))
            else:
                if req_id is not None:
                    _write_stdout_message(_make_jsonrpc_error(req_id, -32601, f"Unknown method {method}"))
        except Exception as e:
            logger.exception("Error handling stdin message: %s", e)
            if req_id is not None:
                _write_stdout_message(_make_jsonrpc_error(req_id, -32000, str(e)))

    async def _run_tool_and_stream(self, tool_name: str, input_obj: Dict[str, Any], req_id: Optional[Any], call_id: Optional[str]):
        """
        Run tool and stream partial results if tool returns async generator.
        Final result will be sent as JSON-RPC result message (if req_id provided).
        Partial events use method 'tools/event' notifications with params {"call_id":..., "event": ...}
        """
        adapter = self.registry.get(tool_name)
        if adapter is None:
            if req_id is not None:
                _write_stdout_message(_make_jsonrpc_error(req_id, -32004, f"Tool not found: {tool_name}"))
            return

        ctx = await self.registry.invoke_slot(tool_name, call_id=call_id)
        async with ctx:
            async def _runner():
                try:
                    run_fn = getattr(adapter, "run")
                    is_coro = asyncio.iscoroutinefunction(run_fn)
                    
                    result = await run_fn(**input_obj) if is_coro else await asyncio.get_running_loop().run_in_executor(None, run_fn, **input_obj)
                    # if result is an async generator, we will handle elsewhere
                    return result
                except asyncio.CancelledError:
                    logger.info("Tool run cancelled: %s call_id=%s", tool_name, call_id)
                    raise
                except Exception as e:
                    logger.exception("Tool run error: %s", e)
                    return {"error": str(e)}

            task = asyncio.create_task(_runner())
            real_call_id = self.registry.register_call(call_id, task)

            try:
                res = await task
                # if result is an async generator, handle streaming
                if hasattr(res, "__aiter__"):
                    async for part in res:
                        _write_stdout_message(_make_notification("tools/event", {"call_id": real_call_id, "event": part}))
    
                    _write_stdout_message(_make_notification("tools/event", {"call_id": real_call_id, "event": {"status": "done"}}))
                    if req_id is not None:
                        _write_stdout_message(_make_jsonrpc_result(req_id, {"call_id": real_call_id, "status": "done"}))
                else:
                    if req_id is not None:
                        _write_stdout_message(_make_jsonrpc_result(req_id, res))
                    else:
                        # send notification with final result
                        _write_stdout_message(_make_notification("tools/event", {"call_id": real_call_id, "event": {"result": res}}))
            except asyncio.CancelledError:
                # send cancelled notification
                _write_stdout_message(_make_notification("tools/event", {"call_id": real_call_id, "event": {"status": "cancelled"}}))
                if req_id is not None:
                    _write_stdout_message(_make_jsonrpc_error(req_id, -32800, "Request cancelled"))
            except Exception as e:
                logger.exception("Unhandled exception running tool %s: %s", tool_name, e)
                if req_id is not None:
                    _write_stdout_message(_make_jsonrpc_error(req_id, -32000, str(e)))
            finally:
                # unregister call if still present
                try:
                    await self.registry.cancel_call(real_call_id)
                except Exception:
                    pass
