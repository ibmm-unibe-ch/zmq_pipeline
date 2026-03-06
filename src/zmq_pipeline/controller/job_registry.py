"""
controller/job_registry.py
~~~~~~~~~~~~~~~~~~~~~~~~~~
Thread-safe in-memory registry of all jobs submitted to the Controller.

The registry is the authoritative source of job state for the HTTP layer.
It is updated from two places:

* ``register()``          — called by the HTTP handler when a job is accepted
* ``on_status_event()``   — called by the telemetry thread on every StatusEvent

``JobEntry`` tracks enough state for the HTTP response and a future dashboard:
    status           string life-cycle:
        "accepted"       → HTTP accepted, not yet confirmed by telemetry
        "distributing"   → Ventilator confirmed it is pushing tasks
        "running"        → at least one Worker task has been seen as done/error
        "done"           → Sink reported all tasks complete
        "partial"        → Sink reported job with some errors
        "error"          → all tasks errored, or Sink reported error
    completed_count  increments on worker "done" status events
    error_count      increments on worker "error" status events

Transitions are driven by StatusEvent.component + StatusEvent.status:
    ventilator / distributing  → "distributing"
    ventilator / done          → (no change; Sink drives final state)
    worker     / done          → completed_count++; status="running"
    worker     / error         → error_count++;    status="running"
    sink       / done          → resolve final status ("done" or "partial")
    sink       / error         → status="error"
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

from ..shared.protocol import StatusEvent


def _utcnow() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


@dataclass
class JobEntry:
    job_id: str
    status: str                 # see docstring for valid values
    submitted_at: str
    task_count: int
    completed_count: int = 0
    error_count: int = 0
    accepted_at: str = field(default_factory=_utcnow)
    updated_at: str = field(default_factory=_utcnow)

    def to_dict(self) -> Dict:
        return {
            "job_id":          self.job_id,
            "status":          self.status,
            "submitted_at":    self.submitted_at,
            "task_count":      self.task_count,
            "completed_count": self.completed_count,
            "error_count":     self.error_count,
            "accepted_at":     self.accepted_at,
            "updated_at":      self.updated_at,
        }


class JobRegistry:
    """Thread-safe in-memory job state store."""

    def __init__(self) -> None:
        self._jobs: Dict[str, JobEntry] = {}
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Write API
    # ------------------------------------------------------------------

    def register(self, job_id: str, task_count: int, submitted_at: str) -> None:
        """Record a newly accepted job.  Called from the HTTP handler thread."""
        with self._lock:
            self._jobs[job_id] = JobEntry(
                job_id=job_id,
                status="accepted",
                submitted_at=submitted_at,
                task_count=task_count,
            )

    def on_status_event(self, event: StatusEvent) -> None:
        """Update job state from a telemetry StatusEvent.

        Called from the telemetry background thread — must be lock-safe.
        Events with an empty job_id (startup events) are silently ignored.
        """
        if not event.job_id:
            return

        with self._lock:
            entry = self._jobs.get(event.job_id)
            if entry is None:
                # Telemetry arrived before HTTP registration (race) — create entry.
                entry = JobEntry(
                    job_id=event.job_id,
                    status="accepted",
                    submitted_at="",
                    task_count=0,
                )
                self._jobs[event.job_id] = entry

            entry.updated_at = _utcnow()

            comp = event.component
            st   = event.status

            if comp == "ventilator":
                if st == "distributing":
                    entry.status = "distributing"
                # ventilator "done" / "idle" — no state change; Sink drives completion

            elif comp == "worker":
                if st == "done":
                    if entry.status not in {"done", "partial", "error"}:
                        entry.completed_count += 1
                        entry.status = "running"
                elif st == "error":
                    if entry.status not in {"done", "partial", "error"}:
                        entry.error_count += 1
                        entry.status = "running"

            elif comp == "sink":
                if st == "done":
                    # Use actual counts from sink if available (sink has
                    # ground truth via reliable PUSH/PULL).
                    if event.completed_count is not None:
                        entry.completed_count = event.completed_count
                    if event.error_count is not None:
                        entry.error_count = event.error_count
                    # Resolve final status based on error count
                    if entry.error_count == 0:
                        entry.status = "done"
                    elif entry.error_count < entry.task_count:
                        entry.status = "partial"
                    else:
                        entry.status = "error"
                elif st == "error":
                    entry.status = "error"

    # ------------------------------------------------------------------
    # Read API
    # ------------------------------------------------------------------

    def get(self, job_id: str) -> Optional[JobEntry]:
        with self._lock:
            return self._jobs.get(job_id)

    def all(self) -> List[JobEntry]:
        with self._lock:
            return list(self._jobs.values())