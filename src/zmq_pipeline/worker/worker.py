"""
worker/worker.py
~~~~~~~~~~~~~~~~
Phase-4 worker: pulls task envelopes from the Ventilator, calls a handler,
pushes multipart result frames to the Sink, and publishes status + log
telemetry to the Controller SUB socket.

Telemetry published:
    status:idle   — on startup
    status:busy   — when a task is pulled (includes task_id)
    log:INFO      — task started
    status:done   — after successful result push (includes task_id)
    log:INFO      — task completed with timing
    status:error  — after a failed task (includes task_id)
    log:ERROR     — exception message
    status:idle   — returning to idle after each task
"""

from __future__ import annotations

import os
import socket
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import zmq

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from shared.config import cfg
from shared.protocol import TaskEnvelope, STOP_SENTINEL
from shared.serialization import pack_result, pack_error_result
from shared.telemetry_pub import TelemetryPublisher
from worker.handlers.base_handler import BaseHandler
from worker.handlers.echo_handler import EchoHandler


def _make_worker_id() -> str:
    worker_id = cfg.worker_id
    if not worker_id:
        worker_id = f"worker-{socket.gethostname()}-{os.getpid()}"
    return worker_id


def run(handler: Optional[BaseHandler] = None, worker_id: Optional[str] = None, idle_timeout: float = 5.0) -> None:
    """Run the worker loop until idle timeout."""
    if handler is None:
        handler = EchoHandler()
    if worker_id is None:
        worker_id = _make_worker_id()

    context = zmq.Context()

    # PULL — receives task envelopes from Ventilator
    pull = context.socket(zmq.PULL)
    pull.setsockopt(zmq.LINGER, 0)
    pull.connect(cfg.ventilator_push_endpoint)

    # PUSH — sends result frames to Sink
    push = context.socket(zmq.PUSH)
    push.setsockopt(zmq.LINGER, 0)
    push.connect(cfg.sink_pull_endpoint)

    # PUB — telemetry to Controller SUB
    tel = TelemetryPublisher("worker", worker_id, context)

    print(f"[{worker_id}] Connected. Waiting for tasks …")
    tel.status(job_id="", status="idle")
    tel.log(job_id="", level="INFO", message=f"{worker_id} started, waiting for tasks")

    poller = zmq.Poller()
    poller.register(pull, zmq.POLLIN)

    idle_timeout_s = idle_timeout
    last_task_time = time.monotonic()

    try:
        while True:
            socks = dict(poller.poll(timeout=500))

            if pull not in socks:
                if time.monotonic() - last_task_time > idle_timeout_s:
                    print(f"[{worker_id}] Idle timeout — exiting.")
                    tel.log(job_id="", level="INFO",
                            message=f"{worker_id} idle timeout, shutting down")
                    break
                continue

            raw = pull.recv()
            last_task_time = time.monotonic()

            if raw == STOP_SENTINEL:
                print(f"[{worker_id}] Received STOP sentinel — exiting.")
                tel.log(job_id="", level="INFO",
                        message=f"{worker_id} received STOP sentinel, shutting down")
                break

            try:
                envelope = TaskEnvelope.model_validate_json(raw)
            except Exception as exc:
                print(f"[{worker_id}] ERROR deserialising task envelope: {exc}")
                tel.log(job_id="", level="ERROR",
                        message=f"Failed to deserialise task envelope: {exc}")
                continue

            job_id = envelope.job_id
            task_id = envelope.task_id

            print(f"[{worker_id}] Processing task {task_id} …")
            tel.status(job_id=job_id, status="busy", task_id=task_id)
            tel.log(job_id=job_id, level="INFO",
                    message=f"Started task {task_id}")

            t0 = time.monotonic()
            try:
                arrays = handler.process(task_id, envelope.payload)
                elapsed_ms = (time.monotonic() - t0) * 1000

                meta = {
                    "job_id": job_id,
                    "task_id": task_id,
                    "worker_id": worker_id,
                    "completed_at": datetime.now(tz=timezone.utc).isoformat(),
                    "status": "ok",
                    "task_count": envelope.task_count,
                }
                frame1, frame2 = pack_result(meta, arrays)
                push.send_multipart([frame1, frame2])

                print(f"[{worker_id}] Task {task_id} done ({elapsed_ms:.1f} ms) — result pushed.")
                tel.status(job_id=job_id, status="done", task_id=task_id)
                tel.log(job_id=job_id, level="INFO",
                        message=f"Completed task {task_id} in {elapsed_ms:.1f} ms")

            except Exception as exc:
                elapsed_ms = (time.monotonic() - t0) * 1000
                print(f"[{worker_id}] Task {task_id} FAILED: {exc}")

                frame1, frame2 = pack_error_result(
                    job_id=job_id,
                    task_id=task_id,
                    worker_id=worker_id,
                    error_message=str(exc),
                    task_count=envelope.task_count,
                )
                push.send_multipart([frame1, frame2])

                tel.status(job_id=job_id, status="error", task_id=task_id)
                tel.log(job_id=job_id, level="ERROR",
                        message=f"Task {task_id} failed after {elapsed_ms:.1f} ms: {exc}")

            # Return to idle between tasks
            tel.status(job_id=job_id, status="idle")

    except KeyboardInterrupt:
        pass
    finally:
        tel.close()
        pull.close()
        push.close()
        context.term()
        print(f"[{worker_id}] Shut down.")


if __name__ == "__main__":
    run()