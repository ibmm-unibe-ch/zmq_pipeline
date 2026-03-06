"""
controller/controller.py
~~~~~~~~~~~~~~~~~~~~~~~~
Main entry point for the Controller process.

Starts two concurrent components in the same process:

1. **Telemetry thread** (daemon) — binds SUB socket on :5558, receives all
   pipeline events, prints them, updates the ``JobRegistry``, and forwards
   events to connected WebSocket clients via ``WebSocketManager``.

2. **HTTP + WS server** (main thread) — FastAPI + uvicorn on :5001, exposes
   ``POST /jobs``, ``GET /jobs/{job_id}``, ``WS /ws``, and ``GET /`` (frontend).

Startup order matters: the telemetry SUB socket must bind *before* any
pipeline component PUB socket connects.  The telemetry thread is started
and given 300 ms to bind before uvicorn begins accepting connections.

Usage::

    python -m controller.controller

Environment variables: see ``shared/config.py``.
"""

from __future__ import annotations

import sys
import threading
import time
from pathlib import Path

import uvicorn

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ..shared.config import cfg
from ..shared.protocol import StatusEvent
from .http_server import app, registry
from .ws_server import ws_manager
from .telemetry import run as run_telemetry


def _on_event(event: object) -> None:
    """Telemetry callback: update registry + broadcast to WebSocket clients."""
    # 1. Update job registry
    if isinstance(event, StatusEvent):
        registry.on_status_event(event)

    # 2. Forward the raw event to WS clients FIRST so sink/done arrives
    #    before the synthetic job_complete that follows.
    ws_manager.broadcast_from_thread(event) # pyright: ignore[reportArgumentType]

    # 3. Emit synthetic job_complete AFTER the triggering status_update
    if isinstance(event, StatusEvent):
        if event.component == "sink" and event.status in {"done", "error"}:
            entry = registry.get(event.job_id)
            if entry is not None:
                ws_manager.emit_job_complete_from_thread(
                    job_id=event.job_id,
                    timestamp=event.timestamp,
                    tasks_total=entry.task_count,
                    # Use task_count - error_count: completed_count from worker
                    # status events may lag behind the sink's ground truth.
                    tasks_ok=entry.task_count - entry.error_count,
                    tasks_error=entry.error_count,
                )


def main() -> None:
    # ------------------------------------------------------------------ telemetry thread
    stop_event = threading.Event()
    tel_thread = threading.Thread(
        target=run_telemetry,
        kwargs={"on_event": _on_event, "stop_event": stop_event},
        name="telemetry",
        daemon=True,
    )
    tel_thread.start()

    time.sleep(0.3)
    print(f"[controller] Telemetry thread started.")

    # ------------------------------------------------------------------ HTTP + WS server
    print(f"[controller] Starting HTTP/WS server on port {cfg.http_port} …")
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=cfg.http_port,
        log_level="warning",
    )

    # ------------------------------------------------------------------ shutdown
    print("[controller] HTTP server stopped. Signalling telemetry thread …")
    stop_event.set()
    tel_thread.join(timeout=3)
    print("[controller] Shut down.")


if __name__ == "__main__":
    main()