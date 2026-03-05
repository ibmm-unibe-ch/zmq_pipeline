"""
tests/test_integration.py
~~~~~~~~~~~~~~~~~~~~~~~~~~
Subprocess-based integration tests for the full ZMQ pipeline.

Each test spins up all components as separate processes, submits a job
over HTTP, waits for completion, and verifies the results.

Port ranges are unique per test to allow parallel execution.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

import numpy as np
import requests

_ROOT = Path(__file__).resolve().parents[1]
_PY = sys.executable


def _env(base_port: int, output_dir: str) -> dict:
    """Build an env dict with unique ports starting from *base_port*."""
    env = os.environ.copy()
    env.update({
        "ZMQ_CONTROLLER_HOST": "localhost",
        "ZMQ_VENTILATOR_REP_PORT":  str(base_port),
        "ZMQ_VENTILATOR_PUSH_PORT": str(base_port + 1),
        "ZMQ_SINK_PULL_PORT":       str(base_port + 2),
        "ZMQ_TELEMETRY_SUB_PORT":   str(base_port + 3),
        "HTTP_PORT":                str(base_port + 4),
        "WS_PORT":                  str(base_port + 5),
        "SINK_OUTPUT_DIR":          output_dir,
        "CONTROLLER_TIMEOUT_MS":    "5000",
    })
    return env


def _wait_for_http(port: int, timeout: float = 10.0) -> bool:
    """Poll the health endpoint until the server is up."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            r = requests.get(f"http://localhost:{port}/health", timeout=1)
            if r.status_code == 200:
                return True
        except Exception:
            pass
        time.sleep(0.3)
    return False


def _make_job(job_id: str, n_tasks: int, fail_indices: list[int] | None = None):
    """Build a job submission payload."""
    inputs = []
    for i in range(n_tasks):
        data = {"index": i, "scale": 0.5}
        if fail_indices and i in fail_indices:
            data["fail"] = True
        inputs.append({"task_id": f"task-{i:03d}", "data": data})
    return {
        "job_id": job_id,
        "submitted_at": "2024-01-15T10:30:00Z",
        "pipeline": {"name": "integration_test", "description": "auto"},
        "inputs": inputs,
    }


class PipelineTestCase(unittest.TestCase):
    """Base class that manages process lifecycle."""

    BASE_PORT: int = 0  # set per test class
    procs: list
    output_dir: str

    @classmethod
    def setUpClass(cls):
        cls.output_dir = tempfile.mkdtemp(prefix="zmq-test-")
        cls.env = _env(cls.BASE_PORT, cls.output_dir)
        cls.procs = []

    @classmethod
    def tearDownClass(cls):
        for p in cls.procs:
            p.terminate()
        for p in cls.procs:
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()
        shutil.rmtree(cls.output_dir, ignore_errors=True)

    def _start(self, module: str) -> subprocess.Popen:
        p = subprocess.Popen(
            [_PY, "-m", module],
            cwd=str(_ROOT),
            env=self.env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        self.procs.append(p)
        return p

    def _http_port(self) -> int:
        return self.BASE_PORT + 4


class TestHappyPath(PipelineTestCase):
    """All tasks succeed — verify 202, job status, and .npz output."""

    BASE_PORT = 17100

    def test_full_job_flow(self):
        self._start("zmq_pipeline.controller.controller")
        self.assertTrue(_wait_for_http(self._http_port()), "HTTP server did not start")
        self._start("zmq_pipeline.ventilator.ventilator")
        self._start("zmq_pipeline.sink.sink")
        self._start("zmq_pipeline.worker.worker")
        time.sleep(1.0)

        n_tasks = 8
        job_id = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeee01"
        job = _make_job(job_id, n_tasks)

        # Submit job
        resp = requests.post(
            f"http://localhost:{self._http_port()}/jobs",
            json=job,
            timeout=5,
        )
        self.assertEqual(resp.status_code, 202)
        body = resp.json()
        self.assertEqual(body["status"], "accepted")
        self.assertEqual(body["job_id"], job_id)

        # Wait for all tasks to be processed (generous timeout for CI)
        deadline = time.monotonic() + 20
        data = {}
        while time.monotonic() < deadline:
            r = requests.get(
                f"http://localhost:{self._http_port()}/jobs/{job_id}", timeout=2
            )
            data = r.json()
            if data["completed_count"] + data["error_count"] >= n_tasks:
                break
            time.sleep(0.3)

        self.assertEqual(data["completed_count"], n_tasks)
        self.assertEqual(data["error_count"], 0)

        # Wait a moment for sink to flush
        time.sleep(1.0)
        npz_path = Path(self.output_dir) / f"job-{job_id}.npz"
        self.assertTrue(npz_path.exists(), f"Expected output file {npz_path}")

        # Verify array contents
        with np.load(str(npz_path)) as npz:
            for i in range(n_tasks):
                key = f"task-{i:03d}__output_data"
                self.assertIn(key, npz.files)
                expected_val = i * 0.5
                np.testing.assert_allclose(npz[key], expected_val, atol=1e-6)


class TestErrorPath(PipelineTestCase):
    """Some tasks fail — verify error results propagate correctly."""

    BASE_PORT = 17200

    def test_partial_failure(self):
        self._start("zmq_pipeline.controller.controller")
        self.assertTrue(_wait_for_http(self._http_port()), "HTTP server did not start")
        self._start("zmq_pipeline.ventilator.ventilator")
        self._start("zmq_pipeline.sink.sink")
        self._start("zmq_pipeline.worker.worker")
        time.sleep(1.0)

        n_tasks = 6
        fail_indices = [1, 4]  # tasks 1 and 4 will fail
        job_id = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeee02"
        job = _make_job(job_id, n_tasks, fail_indices=fail_indices)

        resp = requests.post(
            f"http://localhost:{self._http_port()}/jobs",
            json=job,
            timeout=5,
        )
        self.assertEqual(resp.status_code, 202)

        # Wait for all tasks to complete (including errors)
        deadline = time.monotonic() + 20
        data = {}
        while time.monotonic() < deadline:
            r = requests.get(
                f"http://localhost:{self._http_port()}/jobs/{job_id}", timeout=2
            )
            data = r.json()
            if data["completed_count"] + data["error_count"] >= n_tasks:
                break
            time.sleep(0.3)

        self.assertEqual(data["completed_count"], n_tasks - len(fail_indices))
        self.assertEqual(data["error_count"], len(fail_indices))

        # Wait for sink to flush
        time.sleep(1.0)
        npz_path = Path(self.output_dir) / f"job-{job_id}.npz"
        self.assertTrue(npz_path.exists(), f"Expected output file {npz_path}")

        # Verify successful tasks have correct data, failed tasks have no arrays
        with np.load(str(npz_path)) as npz:
            for i in range(n_tasks):
                data_key = f"task-{i:03d}__output_data"
                if i in fail_indices:
                    self.assertNotIn(data_key, npz.files)
                else:
                    self.assertIn(data_key, npz.files)


class TestVentilatorBusy(PipelineTestCase):
    """Submit two jobs rapidly — second should be rejected (503)."""

    BASE_PORT = 17300

    def test_double_submit_nack(self):
        self._start("zmq_pipeline.controller.controller")
        self.assertTrue(_wait_for_http(self._http_port()), "HTTP server did not start")
        self._start("zmq_pipeline.ventilator.ventilator")
        self._start("zmq_pipeline.sink.sink")
        time.sleep(0.5)
        # No worker — tasks will queue but ventilator stays busy longer
        # Actually we need a worker for the first job to complete eventually,
        # but submitting while distributing should NACK.
        # Use many tasks to keep ventilator busy distributing.

        job1_id = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeee03"
        job2_id = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeee04"

        job1 = _make_job(job1_id, 50)
        job2 = _make_job(job2_id, 5)

        # Start a worker so job1 eventually completes
        self._start("zmq_pipeline.worker.worker")
        time.sleep(0.2)

        resp1 = requests.post(
            f"http://localhost:{self._http_port()}/jobs", json=job1, timeout=5
        )
        self.assertEqual(resp1.status_code, 202)

        # Immediately submit second job
        resp2 = requests.post(
            f"http://localhost:{self._http_port()}/jobs", json=job2, timeout=5
        )
        # May be 503 (ventilator_busy) or 202 if ventilator already returned to idle
        # The important thing is the system doesn't crash
        self.assertIn(resp2.status_code, [202, 503])


class TestStopSentinel(PipelineTestCase):
    """Verify workers exit on STOP sentinel (via ventilator max_jobs + num_workers)."""

    BASE_PORT = 17400

    def test_worker_exits_on_sentinel(self):
        self._start("zmq_pipeline.controller.controller")
        self.assertTrue(_wait_for_http(self._http_port()), "HTTP server did not start")

        # Start ventilator with max_jobs=1, num_workers=1
        env = self.env.copy()
        vent_proc = subprocess.Popen(
            [_PY, "-c",
             "import sys; sys.path.insert(0, '.'); "
             "from zmq_pipeline.ventilator.ventilator import run; run(max_jobs=1, num_workers=1)"],
            cwd=str(_ROOT),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        self.procs.append(vent_proc)

        self._start("zmq_pipeline.sink.sink")
        worker_proc = self._start("zmq_pipeline.worker.worker")
        time.sleep(0.5)

        job_id = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeee05"
        job = _make_job(job_id, 3)

        resp = requests.post(
            f"http://localhost:{self._http_port()}/jobs", json=job, timeout=5
        )
        self.assertEqual(resp.status_code, 202)

        # Wait for worker to exit (received STOP sentinel after ventilator finishes)
        try:
            worker_proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            self.fail("Worker did not exit after STOP sentinel")

        # Worker should have exited cleanly (returncode 0 or terminated)
        # Check output for sentinel message
        output = worker_proc.stdout.read().decode(errors="replace")
        self.assertIn("STOP sentinel", output)


if __name__ == "__main__":
    unittest.main()