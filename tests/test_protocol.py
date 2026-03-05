"""
tests/test_protocol.py
~~~~~~~~~~~~~~~~~~~~~~
Unit tests for shared/protocol.py.

Validates that Pydantic models accept well-formed inputs and raise
``ValidationError`` for every class of invalid input.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from zmq_pipeline.shared.protocol import (
    JobSubmission, JobDispatch, VentilatorReply, TaskEnvelope,
    ResultMeta, StatusEvent, LogEvent,
)


VALID_JOB_ID = "550e8400-e29b-41d4-a716-446655440000"
VALID_TS = "2024-01-15T10:30:00Z"
VALID_TS2 = "2024-01-15T10:30:00+00:00"


# ============================================================================
# JobSubmission
# ============================================================================

class TestJobSubmission:

    def _valid(self, **overrides):
        base = {
            "job_id": VALID_JOB_ID,
            "submitted_at": VALID_TS,
            "pipeline": {"name": "test_pipeline", "description": "desc"},
            "inputs": [
                {"task_id": "t-1", "data": {"x": 1}},
                {"task_id": "t-2", "data": {"x": 2}},
            ],
        }
        base.update(overrides)
        return base

    def test_valid_job_accepted(self):
        job = JobSubmission.model_validate(self._valid())
        assert job.job_id == VALID_JOB_ID

    def test_invalid_uuid_rejected(self):
        with pytest.raises(ValidationError, match="UUID v4"):
            JobSubmission.model_validate(self._valid(job_id="not-a-uuid"))

    def test_invalid_timestamp_rejected(self):
        with pytest.raises(ValidationError, match="ISO 8601"):
            JobSubmission.model_validate(self._valid(submitted_at="not-a-date"))

    def test_empty_inputs_rejected(self):
        with pytest.raises(ValidationError):
            JobSubmission.model_validate(self._valid(inputs=[]))

    def test_duplicate_task_ids_rejected(self):
        with pytest.raises(ValidationError, match="unique"):
            JobSubmission.model_validate(self._valid(inputs=[
                {"task_id": "dup", "data": {}},
                {"task_id": "dup", "data": {}},
            ]))

    def test_missing_pipeline_name_rejected(self):
        data = self._valid()
        data["pipeline"] = {"description": "only desc"}
        with pytest.raises(ValidationError):
            JobSubmission.model_validate(data)

    def test_iso8601_with_offset_accepted(self):
        job = JobSubmission.model_validate(self._valid(submitted_at=VALID_TS2))
        assert job.submitted_at == VALID_TS2


# ============================================================================
# JobDispatch
# ============================================================================

class TestJobDispatch:

    def _valid(self, **overrides):
        base = {
            "job_id": VALID_JOB_ID,
            "submitted_at": VALID_TS,
            "task_count": 2,
            "inputs": [
                {"task_id": "t-1", "data": {}},
                {"task_id": "t-2", "data": {}},
            ],
        }
        base.update(overrides)
        return base

    def test_valid_dispatch(self):
        d = JobDispatch.model_validate(self._valid())
        assert d.task_count == 2

    def test_task_count_mismatch_rejected(self):
        with pytest.raises(ValidationError, match="task_count"):
            JobDispatch.model_validate(self._valid(task_count=99))

    def test_zero_task_count_rejected(self):
        with pytest.raises(ValidationError):
            JobDispatch.model_validate(self._valid(task_count=0, inputs=[]))


# ============================================================================
# VentilatorReply
# ============================================================================

class TestVentilatorReply:

    def test_ack(self):
        r = VentilatorReply(job_id=VALID_JOB_ID, status="ack")
        assert r.reason is None

    def test_nack_with_reason(self):
        r = VentilatorReply(job_id=VALID_JOB_ID, status="nack", reason="ventilator_busy")
        assert r.reason == "ventilator_busy"

    def test_invalid_status_rejected(self):
        with pytest.raises(ValidationError):
            VentilatorReply(job_id=VALID_JOB_ID, status="maybe")


# ============================================================================
# TaskEnvelope
# ============================================================================

class TestTaskEnvelope:

    def test_valid_envelope(self):
        env = TaskEnvelope(
            job_id=VALID_JOB_ID,
            task_id="task-001",
            task_count=10,
            payload={"path": "/data/img.tif", "scale": 0.5},
        )
        assert env.task_count == 10

    def test_zero_task_count_rejected(self):
        with pytest.raises(ValidationError):
            TaskEnvelope(job_id=VALID_JOB_ID, task_id="t", task_count=0, payload={})


# ============================================================================
# ResultMeta
# ============================================================================

class TestResultMeta:

    def _valid(self, **overrides):
        base = {
            "job_id": VALID_JOB_ID,
            "task_id": "task-001",
            "worker_id": "worker-node04-12345",
            "completed_at": VALID_TS,
            "status": "ok",
            "arrays": {
                "output_image": {"dtype": "<f4", "shape": [1024, 1024], "byte_offset": 0},
            },
        }
        base.update(overrides)
        return base

    def test_valid_ok_result(self):
        r = ResultMeta.model_validate(self._valid())
        assert r.status == "ok"

    def test_valid_error_result(self):
        r = ResultMeta.model_validate(self._valid(
            status="error", error="boom", arrays={}
        ))
        assert r.error == "boom"

    def test_invalid_status_rejected(self):
        with pytest.raises(ValidationError):
            ResultMeta.model_validate(self._valid(status="unknown"))

    def test_negative_byte_offset_rejected(self):
        data = self._valid()
        data["arrays"]["output_image"]["byte_offset"] = -1
        with pytest.raises(ValidationError):
            ResultMeta.model_validate(data)


# ============================================================================
# StatusEvent
# ============================================================================

class TestStatusEvent:

    def test_worker_busy(self):
        e = StatusEvent(
            component="worker",
            component_id="w-1",
            job_id=VALID_JOB_ID,
            status="busy",
            task_id="task-001",
        )
        assert e.topic == "status"

    def test_invalid_component_rejected(self):
        with pytest.raises(ValidationError):
            StatusEvent(component="unknown", component_id="x",
                        job_id=VALID_JOB_ID, status="idle")

    def test_invalid_status_rejected(self):
        with pytest.raises(ValidationError):
            StatusEvent(component="worker", component_id="x",
                        job_id=VALID_JOB_ID, status="running")


# ============================================================================
# LogEvent
# ============================================================================

class TestLogEvent:

    def test_info_log(self):
        e = LogEvent(
            component="sink",
            component_id="sink-0",
            job_id=VALID_JOB_ID,
            level="INFO",
            message="Job complete",
        )
        assert e.topic == "log"

    def test_invalid_level_rejected(self):
        with pytest.raises(ValidationError):
            LogEvent(component="sink", component_id="s",
                     job_id=VALID_JOB_ID, level="DEBUG", message="x")

    def test_invalid_component_rejected(self):
        with pytest.raises(ValidationError):
            LogEvent(component="frontend", component_id="f",
                     job_id=VALID_JOB_ID, level="INFO", message="x")
