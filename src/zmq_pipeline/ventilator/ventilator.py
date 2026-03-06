"""
ventilator/ventilator.py
~~~~~~~~~~~~~~~~~~~~~~~~
Phase-4 ventilator: REP socket waits for a job dispatch from the Controller,
replies ACK (or NACK if already busy), fans tasks out over PUSH, and
publishes status + log telemetry to the Controller SUB socket.

Telemetry published:
    status:idle          — on startup and after each job completes
    status:distributing  — immediately after ACK, before first PUSH
    log:INFO             — one message per task pushed
    status:done          — after all tasks pushed
    log:INFO             — job summary

State machine:
    IDLE ──(job received)──► DISTRIBUTING ──(all tasks pushed)──► IDLE
              └──(already busy)──► NACK → IDLE
"""

from __future__ import annotations

import signal
import sys
from pathlib import Path
from typing import Optional

import zmq

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ..shared.config import cfg
from ..shared.protocol import JobDispatch, TaskEnvelope, VentilatorReply, STOP_SENTINEL
from ..shared.telemetry_pub import TelemetryPublisher

_COMPONENT = "ventilator"
_COMPONENT_ID = "ventilator-0"

_SHUTDOWN = False


def _handle_signal(signum, frame) -> None:  # noqa: ANN001
    global _SHUTDOWN
    _SHUTDOWN = True


def run(max_jobs: Optional[int] = None, num_workers: int = 0) -> None:
    """Run the ventilator REP + PUSH + PUB loop.

    After the loop exits (max_jobs reached or shutdown signal), sends
    *num_workers* STOP sentinels so connected workers exit cleanly.
    """
    global _SHUTDOWN
    _SHUTDOWN = False
    signal.signal(signal.SIGTERM, _handle_signal)

    context = zmq.Context()

    # REP — receives job dispatch from Controller, replies ACK/NACK
    rep = context.socket(zmq.REP)
    rep.setsockopt(zmq.LINGER, 0)
    rep.bind(cfg.ventilator_rep_bind)

    # PUSH — fans task envelopes out to Workers
    push = context.socket(zmq.PUSH)
    push.setsockopt(zmq.LINGER, 0)
    push.bind(cfg.ventilator_push_bind)

    # PUB — telemetry to Controller SUB
    tel = TelemetryPublisher(_COMPONENT, _COMPONENT_ID, context)

    print(f"[ventilator] REP  bound to {cfg.ventilator_rep_bind}")
    print(f"[ventilator] PUSH bound to {cfg.ventilator_push_bind}")
    print(f"[ventilator] PUB  connected to {cfg.telemetry_pub_endpoint}")
    print("[ventilator] Waiting for job dispatch …")

    # Publish initial idle status (job_id="" — no job yet)
    tel.status(job_id="", status="idle")
    tel.log(job_id="", level="INFO", message="Ventilator started, waiting for job dispatch")

    jobs_processed = 0
    busy = False

    poller = zmq.Poller()
    poller.register(rep, zmq.POLLIN)

    try:
        while not _SHUTDOWN:
            if max_jobs is not None and jobs_processed >= max_jobs:
                print(f"[ventilator] Processed {jobs_processed} job(s) — exiting.")
                break

            socks = dict(poller.poll(timeout=500))
            if rep not in socks:
                continue

            raw = rep.recv()

            try:
                job = JobDispatch.model_validate_json(raw)
            except Exception as exc:
                _reply_nack(rep, "", f"invalid_payload: {exc}")
                continue

            if busy:
                print(f"[ventilator] NACK — already busy (job {job.job_id})")
                tel.log(job_id=job.job_id, level="WARN",
                        message=f"Rejected job {job.job_id}: ventilator_busy")
                _reply_nack(rep, job.job_id, "ventilator_busy")
                continue

            # Mark busy and ACK *before* distributing — keeps REQ/REP fast.
            busy = True
            _reply_ack(rep, job.job_id)
            print(f"[ventilator] ACK  job={job.job_id}  tasks={job.task_count}")

            tel.status(job_id=job.job_id, status="distributing")
            tel.log(job_id=job.job_id, level="INFO",
                    message=f"Accepted job {job.job_id}, distributing {job.task_count} tasks")

            _distribute(push, tel, job)

            tel.status(job_id=job.job_id, status="done")
            tel.log(job_id=job.job_id, level="INFO",
                    message=f"All {job.task_count} tasks pushed for job {job.job_id}")

            jobs_processed += 1
            busy = False
            print("[ventilator] Idle — ready for next job.")

            tel.status(job_id=job.job_id, status="idle")

    except KeyboardInterrupt:
        pass
    finally:
        # Send STOP sentinels so workers exit cleanly
        for _ in range(num_workers):
            push.send(STOP_SENTINEL)
        if num_workers:
            print(f"[ventilator] Sent {num_workers} STOP sentinel(s).")
        tel.close()
        rep.close()
        push.close()
        context.term()
        print("[ventilator] Shut down.")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _reply_ack(rep: zmq.Socket, job_id: str) -> None:
    reply = VentilatorReply(job_id=job_id, status="ack")
    rep.send(reply.model_dump_json().encode("utf-8"))


def _reply_nack(rep: zmq.Socket, job_id: str, reason: str) -> None:
    reply = VentilatorReply(job_id=job_id, status="nack", reason=reason)
    rep.send(reply.model_dump_json().encode("utf-8"))


def _distribute(push: zmq.Socket, tel: TelemetryPublisher, job: JobDispatch) -> None:
    """Fan out all task envelopes for *job* over the PUSH socket."""
    print(f"[ventilator] Distributing {job.task_count} tasks …")
    for inp in job.inputs:
        envelope = TaskEnvelope(
            job_id=job.job_id,
            task_id=inp.task_id,
            task_count=job.task_count,
            payload=inp.data,
        )
        push.send(envelope.model_dump_json().encode("utf-8"))
        tel.log(job_id=job.job_id, level="INFO",
                message=f"Pushed task {inp.task_id}")

    print(f"[ventilator] All {job.task_count} tasks pushed for job {job.job_id}")


if __name__ == "__main__":
    run()