"""
controller/http_server.py
~~~~~~~~~~~~~~~~~~~~~~~~~
FastAPI HTTP server for the Controller command plane.

Endpoints
---------
POST /jobs
    Accept a job submission from the Frontend (or curl/scripts).
    Validates the body against ``JobSubmission``, converts it to a
    ``JobDispatch``, dispatches synchronously to the Ventilator via
    ``VentilatorClient``, registers the job in the registry, and returns
    202 (accepted) or 503 (rejected / ventilator unreachable).

GET /jobs/{job_id}
    Return the current state of a job from the registry.

GET /jobs
    List all registered jobs.

GET /
    Serve the frontend dashboard HTML.

WS /ws
    WebSocket connection for real-time telemetry events.

The FastAPI app is a module-level singleton (``app``) so it can be imported
by ``controller.py`` and mounted under uvicorn.  The ``JobRegistry`` is also
a module-level singleton shared with the telemetry thread.

``VentilatorClient.dispatch()`` is synchronous/blocking.  It is run in
asyncio's default thread-pool executor so it does not block the event loop.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import Any, Dict, List

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from pydantic import ValidationError

from ..shared.config import cfg
from ..shared.protocol import (
    JobAcceptedResponse,
    JobRejectedResponse,
    JobSubmission,
)
from .zmq_client import VentilatorClient
from .job_registry import JobRegistry
from .ws_server import ws_manager

_FRONTEND_PATH = Path(__file__).resolve().parents[1] / "frontend" / "templates" / "index.html"

# ---------------------------------------------------------------------------
# Singletons shared with controller.py / telemetry thread
# ---------------------------------------------------------------------------

registry = JobRegistry()
_client  = VentilatorClient()

app = FastAPI(title="ZMQ Pipeline Controller", version="0.6.0")


# ---------------------------------------------------------------------------
# Lifecycle: capture the event loop for the WS thread-safe bridge
# ---------------------------------------------------------------------------

@app.on_event("startup")
async def _startup() -> None:
    ws_manager.set_loop(asyncio.get_event_loop())


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/")
async def frontend() -> FileResponse:
    """Serve the pipeline dashboard."""
    return FileResponse(str(_FRONTEND_PATH), media_type="text/html")


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket) -> None:
    """Accept a WebSocket connection and stream telemetry events to it."""
    await ws_manager.connect(ws)
    try:
        while True:
            try:
                await asyncio.wait_for(ws.receive_text(), timeout=30)
            except asyncio.TimeoutError:
                # Client is listen-only — silence is normal, keep connection alive
                continue
    except (WebSocketDisconnect, Exception):
        pass
    finally:
        ws_manager.disconnect(ws)


@app.post("/jobs", status_code=202)
async def submit_job(body: JobSubmission) -> Dict[str, Any]:
    """Accept a job, dispatch to Ventilator, return 202 or 503."""
    dispatch_payload: Dict[str, Any] = {
        "job_id":       body.job_id,
        "submitted_at": body.submitted_at,
        "task_count":   len(body.inputs),
        "inputs":       [{"task_id": t.task_id, "data": t.data} for t in body.inputs],
    }

    loop = asyncio.get_event_loop()
    try:
        reply = await loop.run_in_executor(None, _client.dispatch, dispatch_payload)
    except TimeoutError:
        raise HTTPException(
            status_code=503,
            detail=JobRejectedResponse(
                job_id=body.job_id, reason="ventilator_timeout"
            ).model_dump(),
        )
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=JobRejectedResponse(
                job_id=body.job_id, reason=f"dispatch_error: {exc}"
            ).model_dump(),
        )

    if reply.status == "nack":
        reason = reply.reason or "ventilator_busy"
        raise HTTPException(
            status_code=503,
            detail=JobRejectedResponse(job_id=body.job_id, reason=reason).model_dump(),
        )

    registry.register(
        job_id=body.job_id,
        task_count=len(body.inputs),
        submitted_at=body.submitted_at,
    )

    return JobAcceptedResponse(job_id=body.job_id).model_dump()


@app.get("/jobs/{job_id}")
async def get_job(job_id: str) -> Dict[str, Any]:
    entry = registry.get(job_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"Job {job_id!r} not found")
    return entry.to_dict()


@app.get("/jobs")
async def list_jobs() -> List[Dict[str, Any]]:
    return [e.to_dict() for e in registry.all()]


@app.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "ok"}