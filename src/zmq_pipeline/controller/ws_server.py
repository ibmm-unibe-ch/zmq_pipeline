"""
controller/ws_server.py
~~~~~~~~~~~~~~~~~~~~~~~
WebSocket connection manager for the Controller observability plane.

Responsibilities:
    - Track all connected WebSocket clients in a set
    - Broadcast JSON messages to every connected client
    - Provide a thread-safe bridge so the telemetry background thread can
      push events into the asyncio event loop where WebSocket sends live

Wire protocol (per PROJECT.md §6):
    - status_update  — translated from a StatusEvent
    - log_event      — translated from a LogEvent
    - job_complete   — synthetic event emitted when sink reports done

Usage in controller.py::

    from controller.ws_server import ws_manager

    def on_event(event):
        registry.on_status_event(event)          # existing
        ws_manager.broadcast_from_thread(event)  # new — forwards to all WS clients
"""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from typing import Any, Dict, Optional, Set

from fastapi import WebSocket
from starlette.websockets import WebSocketDisconnect

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from shared.protocol import LogEvent, StatusEvent


class WebSocketManager:
    """Manages connected WebSocket clients and broadcasts telemetry events.

    Thread-safety model
    -------------------
    ``broadcast_from_thread()`` is called from the telemetry background thread.
    All WebSocket I/O must happen in the asyncio event loop.  The bridge uses
    ``asyncio.run_coroutine_threadsafe()`` which is explicitly safe to call from
    outside the loop.

    The loop reference is captured in ``set_loop()`` which is called from a
    FastAPI startup hook (inside the event loop) before any events arrive.
    """

    def __init__(self) -> None:
        self._clients: Set[WebSocket] = set()
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    # ------------------------------------------------------------------
    # Loop registration (called once from FastAPI startup hook)
    # ------------------------------------------------------------------

    def set_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop

    # ------------------------------------------------------------------
    # Client lifecycle
    # ------------------------------------------------------------------

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        self._clients.add(ws)

    def disconnect(self, ws: WebSocket) -> None:
        self._clients.discard(ws)

    # ------------------------------------------------------------------
    # Broadcast helpers
    # ------------------------------------------------------------------

    async def broadcast(self, message: Dict[str, Any]) -> None:
        """Send *message* to every connected client (runs in the event loop)."""
        if not self._clients:
            return
        text = json.dumps(message)
        dead: Set[WebSocket] = set()
        for ws in list(self._clients):
            try:
                await ws.send_text(text)
            except Exception:
                dead.add(ws)
        for ws in dead:
            self._clients.discard(ws)

    def broadcast_from_thread(self, event: StatusEvent | LogEvent) -> None:
        """Thread-safe bridge: translate *event* and schedule a broadcast.

        Called from the telemetry background thread.  Does nothing if no
        event loop has been registered yet (e.g. before any client connects).
        """
        if self._loop is None or self._loop.is_closed():
            return

        msg = _translate(event)
        if msg is None:
            return

        asyncio.run_coroutine_threadsafe(self.broadcast(msg), self._loop)

    # ------------------------------------------------------------------
    # Convenience: emit a synthetic job_complete event
    # ------------------------------------------------------------------

    def emit_job_complete_from_thread(
        self,
        job_id: str,
        timestamp: str,
        tasks_total: int,
        tasks_ok: int,
        tasks_error: int,
    ) -> None:
        if self._loop is None or self._loop.is_closed():
            return
        msg: Dict[str, Any] = {
            "event":     "job_complete",
            "job_id":    job_id,
            "timestamp": timestamp,
            "data": {
                "tasks_total": tasks_total,
                "tasks_ok":    tasks_ok,
                "tasks_error": tasks_error,
            },
        }
        asyncio.run_coroutine_threadsafe(self.broadcast(msg), self._loop)


# ---------------------------------------------------------------------------
# Translation helpers
# ---------------------------------------------------------------------------

def _translate(event: StatusEvent | LogEvent) -> Optional[Dict[str, Any]]:
    """Convert a pipeline event to the WS wire format."""
    if isinstance(event, StatusEvent):
        data: Dict[str, Any] = {"status": event.status}
        if event.task_id:
            data["task_id"] = event.task_id
        if event.completed_count is not None:
            data["completed_count"] = event.completed_count
        if event.error_count is not None:
            data["error_count"] = event.error_count
        return {
            "event":        "status_update",
            "component":    event.component,
            "component_id": event.component_id,
            "job_id":       event.job_id,
            "timestamp":    event.timestamp,
            "data":         data,
        }

    if isinstance(event, LogEvent):
        return {
            "event":        "log_event",
            "component":    event.component,
            "component_id": event.component_id,
            "job_id":       event.job_id,
            "timestamp":    event.timestamp,
            "data": {
                "level":   event.level,
                "message": event.message,
            },
        }

    return None


# Module-level singleton — imported by http_server.py and controller.py
ws_manager = WebSocketManager()