"""
shared/protocol.py
~~~~~~~~~~~~~~~~~~
Canonical message schemas for every wire-format in the ZMQ Pipeline Framework.

Two representations are provided for each schema:

* **TypedDict** — zero-overhead, used at runtime for dict construction /
  annotation.  Always named ``<Concept>Dict``.
* **Pydantic BaseModel** — used for *validation* at the boundary where
  untrusted JSON enters the system (Controller HTTP endpoint, Ventilator REP
  socket).  Always named ``<Concept>``.

The goal is that every component refers to these types instead of building
ad-hoc dicts, so a schema change is caught at a single point.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from typing_extensions import TypedDict

from pydantic import BaseModel, Field, field_validator, model_validator
import uuid


# ============================================================================
# Helpers
# ============================================================================

def _utcnow() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


# ============================================================================
# 1. Job Submission  (Frontend → Controller)
# ============================================================================

class TaskInputDict(TypedDict):
    task_id: str
    data: Dict[str, Any]


class PipelineMetaDict(TypedDict):
    name: str
    description: str


class JobSubmissionDict(TypedDict):
    job_id: str
    submitted_at: str
    pipeline: PipelineMetaDict
    inputs: List[TaskInputDict]


# --- Pydantic validators ----------------------------------------------------

class TaskInput(BaseModel):
    task_id: str = Field(..., min_length=1)
    data: Dict[str, Any] = Field(default_factory=dict)


class PipelineMeta(BaseModel):
    name: str = Field(..., min_length=1)
    description: str = ""


class JobSubmission(BaseModel):
    """Validates an incoming job from the Frontend (HTTP POST /jobs)."""

    job_id: str
    submitted_at: str
    pipeline: PipelineMeta
    inputs: List[TaskInput] = Field(..., min_length=1)

    @field_validator("job_id")
    @classmethod
    def _validate_uuid(cls, v: str) -> str:
        try:
            parsed = uuid.UUID(v, version=4)
        except (ValueError, AttributeError) as exc:
            raise ValueError(f"job_id must be a valid UUID v4, got {v!r}") from exc
        if str(parsed) != v.lower():
            raise ValueError(f"job_id must be a canonical lowercase UUID v4, got {v!r}")
        return v

    @field_validator("submitted_at")
    @classmethod
    def _validate_iso8601(cls, v: str) -> str:
        try:
            datetime.fromisoformat(v.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"submitted_at must be a valid ISO 8601 timestamp, got {v!r}") from exc
        return v

    @model_validator(mode="after")
    def _unique_task_ids(self) -> "JobSubmission":
        ids = [t.task_id for t in self.inputs]
        if len(ids) != len(set(ids)):
            raise ValueError("All task_id values in inputs must be unique")
        return self


# ============================================================================
# 2. Job Dispatch  (Controller → Ventilator)
# ============================================================================

class JobDispatchDict(TypedDict):
    job_id: str
    submitted_at: str
    task_count: int
    inputs: List[TaskInputDict]


class JobDispatch(BaseModel):
    """Validated form of the dispatch message sent over REQ/REP."""

    job_id: str
    submitted_at: str
    task_count: int = Field(..., gt=0)
    inputs: List[TaskInput] = Field(..., min_length=1)

    @model_validator(mode="after")
    def _task_count_matches(self) -> "JobDispatch":
        if self.task_count != len(self.inputs):
            raise ValueError(
                f"task_count={self.task_count} does not match len(inputs)={len(self.inputs)}"
            )
        return self


# ============================================================================
# 3. Ventilator ACK / NACK  (Ventilator → Controller)
# ============================================================================

class VentilatorReplyDict(TypedDict):
    job_id: str
    status: str   # "ack" | "nack"
    reason: Optional[str]


class VentilatorReply(BaseModel):
    job_id: str
    status: str   # "ack" | "nack"
    reason: Optional[str] = None

    @field_validator("status")
    @classmethod
    def _valid_status(cls, v: str) -> str:
        if v not in {"ack", "nack"}:
            raise ValueError(f"status must be 'ack' or 'nack', got {v!r}")
        return v


# ============================================================================
# 4. Task Envelope  (Ventilator → Workers)
# ============================================================================

class TaskEnvelopeDict(TypedDict):
    job_id: str
    task_id: str
    task_count: int
    payload: Dict[str, Any]


class TaskEnvelope(BaseModel):
    job_id: str
    task_id: str
    task_count: int = Field(..., gt=0)
    payload: Dict[str, Any] = Field(default_factory=dict)


# ============================================================================
# 5. Result Metadata Frame  (Workers → Sink, Frame 1)
# ============================================================================

class ArrayMetaDict(TypedDict):
    dtype: str
    shape: List[int]
    byte_offset: int


class ResultMetaDict(TypedDict):
    job_id: str
    task_id: str
    worker_id: str
    completed_at: str
    status: str   # "ok" | "error"
    arrays: Dict[str, ArrayMetaDict]


class ArrayMeta(BaseModel):
    dtype: str
    shape: List[int]
    byte_offset: int = Field(..., ge=0)


class ResultMeta(BaseModel):
    """Frame 1 of the Worker → Sink multipart result message."""

    job_id: str
    task_id: str
    worker_id: str
    completed_at: str
    status: str         # "ok" | "error"
    arrays: Dict[str, ArrayMeta] = Field(default_factory=dict)
    error: Optional[str] = None
    task_count: Optional[int] = None

    @field_validator("status")
    @classmethod
    def _valid_status(cls, v: str) -> str:
        if v not in {"ok", "error"}:
            raise ValueError(f"status must be 'ok' or 'error', got {v!r}")
        return v


# ============================================================================
# 6. Telemetry  (All components → Controller)
# ============================================================================

# Valid component types
COMPONENT_TYPES = {"ventilator", "worker", "sink", "controller"}

# Valid status values per component
WORKER_STATUSES = {"idle", "busy", "error", "done"}
VENTILATOR_STATUSES = {"idle", "distributing", "done"}
SINK_STATUSES = {"idle", "busy", "error", "done"}
ALL_STATUSES = WORKER_STATUSES | VENTILATOR_STATUSES | SINK_STATUSES

LOG_LEVELS = {"INFO", "WARN", "ERROR"}


class StatusEventDict(TypedDict):
    topic: str          # always "status"
    component: str
    component_id: str
    job_id: str
    status: str
    task_id: Optional[str]
    timestamp: str


class LogEventDict(TypedDict):
    topic: str          # always "log"
    component: str
    component_id: str
    job_id: str
    level: str
    message: str
    timestamp: str


class StatusEvent(BaseModel):
    topic: str = "status"
    component: str
    component_id: str
    job_id: str
    status: str
    task_id: Optional[str] = None
    timestamp: str = Field(default_factory=_utcnow)
    completed_count: Optional[int] = None
    error_count: Optional[int] = None

    @field_validator("component")
    @classmethod
    def _valid_component(cls, v: str) -> str:
        if v not in COMPONENT_TYPES:
            raise ValueError(f"component must be one of {COMPONENT_TYPES}, got {v!r}")
        return v

    @field_validator("status")
    @classmethod
    def _valid_status(cls, v: str) -> str:
        if v not in ALL_STATUSES:
            raise ValueError(f"status must be one of {ALL_STATUSES}, got {v!r}")
        return v


class LogEvent(BaseModel):
    topic: str = "log"
    component: str
    component_id: str
    job_id: str
    level: str
    message: str
    timestamp: str = Field(default_factory=_utcnow)

    @field_validator("component")
    @classmethod
    def _valid_component(cls, v: str) -> str:
        if v not in COMPONENT_TYPES:
            raise ValueError(f"component must be one of {COMPONENT_TYPES}, got {v!r}")
        return v

    @field_validator("level")
    @classmethod
    def _valid_level(cls, v: str) -> str:
        if v not in LOG_LEVELS:
            raise ValueError(f"level must be one of {LOG_LEVELS}, got {v!r}")
        return v


# ============================================================================
# 7. WebSocket events  (Controller → Frontend)
# ============================================================================

class WSStatusUpdateDict(TypedDict):
    event: str          # "status_update"
    component: str
    component_id: str
    job_id: str
    timestamp: str
    data: Dict[str, Any]


class WSLogEventDict(TypedDict):
    event: str          # "log_event"
    component: str
    component_id: str
    job_id: str
    timestamp: str
    data: Dict[str, Any]


class WSJobCompleteDict(TypedDict):
    event: str          # "job_complete"
    job_id: str
    timestamp: str
    data: Dict[str, Any]   # tasks_total, tasks_ok, tasks_error


# ============================================================================
# 8. HTTP response schemas  (Controller → Frontend)
# ============================================================================

class JobAcceptedResponse(BaseModel):
    job_id: str
    status: str = "accepted"
    reason: Optional[str] = None


class JobRejectedResponse(BaseModel):
    job_id: str
    status: str = "rejected"
    reason: str


# ============================================================================
# 9. STOP sentinel  (Ventilator → Workers)
# ============================================================================

STOP_SENTINEL = b'{"__sentinel__": "STOP"}'