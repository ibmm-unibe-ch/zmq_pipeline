"""
sink/sink.py
~~~~~~~~~~~~
Phase-7 sink: binds a PULL socket, receives multipart result frames from
workers, reconstructs numpy arrays via ``unpack_result()``, persists results
to disk, and publishes status + log telemetry to the Controller SUB socket.

Task count auto-detection: if ``expected_tasks`` is 0, the sink learns the
task count per job from the ``task_count`` field in the result metadata
(set by workers from the task envelope).
"""

from __future__ import annotations

import signal
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Set

import numpy as np
import zmq

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from shared.config import cfg
from shared.protocol import ResultMeta
from shared.serialization import unpack_result
from shared.telemetry_pub import TelemetryPublisher

_COMPONENT = "sink"
_COMPONENT_ID = "sink-0"

_SHUTDOWN = False


def _handle_signal(signum, frame) -> None:  # noqa: ANN001
    global _SHUTDOWN
    _SHUTDOWN = True


def run(expected_tasks: int = 0, output_dir: str = "", idle_timeout: float = 10.0) -> None:
    """Receive results and write them to disk.

    Parameters
    ----------
    expected_tasks:
        If > 0, exit after receiving this many results globally.
        If 0 (default), auto-detect task count per job from result metadata
        and exit when all detected jobs are complete (or on idle timeout).
    output_dir:
        Directory to write output ``.npz`` files.
    """
    global _SHUTDOWN
    _SHUTDOWN = False
    signal.signal(signal.SIGTERM, _handle_signal)

    out_path = Path(output_dir or cfg.sink_output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    context = zmq.Context()

    pull = context.socket(zmq.PULL)
    pull.setsockopt(zmq.LINGER, 0)
    pull.bind(cfg.sink_pull_bind)

    tel = TelemetryPublisher(_COMPONENT, _COMPONENT_ID, context)

    print(f"[sink] Bound PULL to {cfg.sink_pull_bind}")
    print(f"[sink] PUB  connected to {cfg.telemetry_pub_endpoint}")
    if expected_tasks:
        print(f"[sink] Expecting {expected_tasks} results …")
    else:
        print("[sink] Auto-detect mode — learning task counts from results …")

    tel.status(job_id="", status="idle")
    tel.log(job_id="", level="INFO", message="Sink started")

    results: Dict[str, Dict[str, Any]] = defaultdict(dict)
    meta_store: Dict[str, Dict[str, ResultMeta]] = defaultdict(dict)
    task_counts: Dict[str, int] = {}  # job_id → expected count
    completed_jobs: Set[str] = set()

    received = 0
    last_recv = time.monotonic()

    poller = zmq.Poller()
    poller.register(pull, zmq.POLLIN)

    try:
        while not _SHUTDOWN:
            # Global stop condition when expected_tasks is explicit
            if expected_tasks and received >= expected_tasks:
                break

            socks = dict(poller.poll(timeout=500))

            if pull not in socks:
                if time.monotonic() - last_recv > idle_timeout:
                    print(f"[sink] Idle timeout — received {received} results total.")
                    tel.log(job_id="", level="WARN",
                            message=f"Idle timeout: received {received} results")
                    break
                continue

            frames = pull.recv_multipart()
            last_recv = time.monotonic()

            if len(frames) != 2:
                print(f"[sink] Unexpected frame count {len(frames)} — skipping.")
                tel.log(job_id="", level="ERROR",
                        message=f"Received malformed message with {len(frames)} frames")
                continue

            try:
                meta, arrays = unpack_result(frames[0], frames[1])
            except Exception as exc:
                print(f"[sink] ERROR unpacking result: {exc}")
                tel.log(job_id="", level="ERROR",
                        message=f"Failed to unpack result frame: {exc}")
                continue

            received += 1
            job_id = meta.job_id

            # Learn task_count for this job
            if job_id not in task_counts:
                if expected_tasks:
                    task_counts[job_id] = expected_tasks
                elif meta.task_count and meta.task_count > 0:
                    task_counts[job_id] = meta.task_count
                    print(f"[sink] Learned task_count={meta.task_count} for job {job_id}")
                # else: unknown — will flush on shutdown/timeout
                tel.status(job_id=job_id, status="busy")

            if meta.status == "error":
                print(f"[sink] Task {meta.task_id} ERROR: {meta.error}")
                results[job_id][meta.task_id] = {}
                tel.log(job_id=job_id, level="ERROR",
                        message=f"Task {meta.task_id} reported error: {meta.error}")
            else:
                results[job_id][meta.task_id] = arrays
                array_summary = ", ".join(
                    f"{k}:{v.shape}" for k, v in arrays.items()
                )
                print(f"[sink] Received task {meta.task_id} from {meta.worker_id} — arrays: {{{array_summary}}}")
                tel.log(job_id=job_id, level="INFO",
                        message=f"Received task {meta.task_id} from {meta.worker_id} "
                                f"({len(arrays)} arrays: {array_summary})")

            meta_store[job_id][meta.task_id] = meta

            # Flush when all tasks for a job are collected
            if (job_id in task_counts
                    and len(results[job_id]) == task_counts[job_id]
                    and job_id not in completed_jobs):
                _persist(job_id, results[job_id], meta_store[job_id], out_path, tel)
                ok_count = sum(1 for m in meta_store[job_id].values() if m.status == "ok")
                err_count = sum(1 for m in meta_store[job_id].values() if m.status == "error")
                tel.status(job_id=job_id, status="done",
                           completed_count=ok_count, error_count=err_count)
                completed_jobs.add(job_id)

    except KeyboardInterrupt:
        pass
    finally:
        # Flush any remaining partial jobs
        for job_id, task_results in results.items():
            if job_id in completed_jobs:
                continue
            count = task_counts.get(job_id, 0)
            print(f"[sink] Partial job {job_id} — flushing {len(task_results)} results.")
            tel.log(job_id=job_id, level="WARN",
                    message=f"Partial flush: {len(task_results)}/{count} results")
            _persist(job_id, task_results, meta_store[job_id], out_path, tel)

        tel.close()
        pull.close()
        context.term()
        print(f"[sink] Done. Total results received: {received}")


def _persist(
    job_id: str,
    task_results: Dict[str, Dict[str, np.ndarray]],
    meta_store: Dict[str, ResultMeta],
    out_path: Path,
    tel: TelemetryPublisher,
) -> None:
    """Write all arrays for *job_id* to a single ``.npz`` file."""
    npz_arrays: Dict[str, np.ndarray] = {}

    for task_id, arrays in task_results.items():
        for array_name, arr in arrays.items():
            key = f"{task_id}__{array_name}"
            npz_arrays[key] = arr

    if not npz_arrays:
        print(f"[sink] Job {job_id} had no array data to persist.")
        tel.log(job_id=job_id, level="WARN",
                message=f"Job {job_id} had no array data to persist")
        return

    out_file = out_path / f"job-{job_id}.npz"
    np.savez(str(out_file), **npz_arrays) # pyright: ignore[reportArgumentType]

    ok_count = sum(1 for m in meta_store.values() if m.status == "ok")
    err_count = sum(1 for m in meta_store.values() if m.status == "error")
    summary = f"{ok_count} ok / {err_count} error results → {out_file}"
    print(f"[sink] Job {job_id} complete. Wrote {summary}")
    tel.log(job_id=job_id, level="INFO",
            message=f"Job {job_id} complete. Wrote {summary}")


if __name__ == "__main__":
    run()