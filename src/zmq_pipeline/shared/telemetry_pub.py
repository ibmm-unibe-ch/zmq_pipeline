"""
shared/telemetry_pub.py
~~~~~~~~~~~~~~~~~~~~~~~
Thin helper that wraps a ZMQ PUB socket for telemetry publishing.

Every pipeline component (Ventilator, Worker, Sink) creates one
``TelemetryPublisher`` instance.  All telemetry flows to the Controller's
SUB socket which binds ``cfg.telemetry_sub_bind``.

Wire format (per PROJECT.md §5):
    Frame 1 — topic bytes:  b"status"  or  b"log"
    Frame 2 — UTF-8 JSON:   StatusEvent  or  LogEvent payload

The publisher connects; the Controller SUB binds.  ZMQ handles reconnects
automatically if the Controller restarts.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

import zmq

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from .config import cfg
from .protocol import LogEvent, StatusEvent


class TelemetryPublisher:
    """Publish ``status`` and ``log`` events to the Controller SUB socket."""

    def __init__(
        self,
        component: str,
        component_id: str,
        context: zmq.Context,
        endpoint: Optional[str] = None,
    ) -> None:
        self._component = component
        self._component_id = component_id

        self._pub: zmq.Socket = context.socket(zmq.PUB)
        self._pub.setsockopt(zmq.LINGER, 2000)
        self._pub.setsockopt(zmq.SNDHWM, 0)
        self._pub.connect(endpoint or cfg.telemetry_pub_endpoint)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def status(
        self,
        job_id: str,
        status: str,
        task_id: Optional[str] = None,
        completed_count: Optional[int] = None,
        error_count: Optional[int] = None,
    ) -> None:
        """Publish a ``status`` event."""
        event = StatusEvent(
            component=self._component,
            component_id=self._component_id,
            job_id=job_id,
            status=status,
            task_id=task_id,
            completed_count=completed_count,
            error_count=error_count,
        )
        self._pub.send_multipart([b"status", event.model_dump_json().encode("utf-8")])

    def log(self, job_id: str, level: str, message: str) -> None:
        """Publish a ``log`` event.  *level* must be INFO, WARN, or ERROR."""
        event = LogEvent(
            component=self._component,
            component_id=self._component_id,
            job_id=job_id,
            level=level,
            message=message,
        )
        self._pub.send_multipart([b"log", event.model_dump_json().encode("utf-8")])

    def close(self) -> None:
        self._pub.close()