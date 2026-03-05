#!/usr/bin/env python3
"""
run_demo.py
~~~~~~~~~~~
Launch every component of the ZMQ Pipeline and open the browser dashboard.

Usage:
    python run_demo.py                  # 2 workers, default ports
    python run_demo.py --workers 4      # 4 workers
    python run_demo.py --port 8080      # HTTP dashboard on port 8080

Press Ctrl+C to shut everything down.
"""

from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import tempfile
import time
import webbrowser
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
_PY = sys.executable

# ---------------------------------------------------------------------------
# Component launcher helpers
# ---------------------------------------------------------------------------

def _launch(label: str, code: str, env: dict) -> subprocess.Popen:
    """Spawn a Python subprocess running *code* inline."""
    proc = subprocess.Popen(
        [_PY, "-c", code],
        cwd=str(_ROOT),
        env=env,
        # Let component output flow to our terminal
        stdout=None,
        stderr=None,
    )
    print(f"  [{label:>14}]  PID {proc.pid}")
    return proc


def _wait_for_health(port: int, timeout: float = 12.0) -> bool:
    """Poll the /health endpoint until the Controller HTTP server is up."""
    import urllib.request
    import urllib.error

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            r = urllib.request.urlopen(f"http://localhost:{port}/health", timeout=2)
            if r.status == 200:
                return True
        except (urllib.error.URLError, OSError):
            pass
        time.sleep(0.3)
    return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Launch the ZMQ Pipeline demo")
    parser.add_argument("--workers", "-w", type=int, default=2,
                        help="Number of worker processes (default: 2)")
    parser.add_argument("--port", "-p", type=int, default=5001,
                        help="HTTP dashboard port (default: 5001)")
    parser.add_argument("--no-browser", action="store_true",
                        help="Don't auto-open the browser")
    args = parser.parse_args()

    n_workers = max(1, args.workers)
    http_port = args.port
    output_dir = tempfile.mkdtemp(prefix="zmq-demo-output-")

    # Compute unique port assignments starting from the HTTP port
    env = os.environ.copy()
    env.update({
        "ZMQ_CONTROLLER_HOST":      "localhost",
        "ZMQ_VENTILATOR_REP_PORT":  str(http_port + 10),   # e.g. 5011
        "ZMQ_VENTILATOR_PUSH_PORT": str(http_port + 11),   # e.g. 5012
        "ZMQ_SINK_PULL_PORT":       str(http_port + 12),   # e.g. 5013
        "ZMQ_TELEMETRY_SUB_PORT":   str(http_port + 13),   # e.g. 5014
        "HTTP_PORT":                str(http_port),
        "WS_PORT":                  str(http_port + 1),
        "SINK_OUTPUT_DIR":          output_dir,
        "CONTROLLER_TIMEOUT_MS":    "5000",
    })

    print()
    print("  ╔═══════════════════════════════════════════════╗")
    print("  ║         ZMQ Pipeline — Interactive Demo       ║")
    print("  ╚═══════════════════════════════════════════════╝")
    print()
    print(f"  Workers:    {n_workers}")
    print(f"  Dashboard:  http://localhost:{http_port}")
    print(f"  Output dir: {output_dir}")
    print()
    print("  Starting components …")
    print()

    procs: list[subprocess.Popen] = []

    try:
        # 1. Controller  — binds SUB (telemetry) + HTTP + WS
        #    Must start FIRST so the SUB socket is bound before any
        #    component PUB socket connects (ZMQ "slow joiner" problem).
        procs.append(_launch("controller", (
            "import sys; sys.path.insert(0, '.');"
            "from controller.controller import main;"
            "main()"
        ), env))

        # Wait for HTTP server to come up (SUB socket binds ~300 ms before HTTP)
        sys.stdout.write("  Waiting for HTTP server ")
        sys.stdout.flush()
        if not _wait_for_health(http_port):
            print("FAILED")
            print("\n  ERROR: Controller HTTP server did not start. Check output above.")
            return
        print("OK")
        print()

        # 2. Ventilator  — binds REP + PUSH, PUB connects to Controller SUB
        procs.append(_launch("ventilator", (
            "import sys; sys.path.insert(0, '.');"
            "from ventilator.ventilator import run;"
            f"run(num_workers={n_workers})"
        ), env))

        # 3. Sink  — binds PULL, PUB connects to Controller SUB
        procs.append(_launch("sink", (
            "import sys; sys.path.insert(0, '.');"
            "from sink.sink import run;"
            "run(idle_timeout=600)"      # 10 min — won't timeout during demo
        ), env))

        # Give ventilator + sink sockets time to bind before workers connect
        time.sleep(0.8)

        # 4. Workers  — connect to ventilator PUSH + sink PULL
        for i in range(n_workers):
            worker_env = env.copy()
            worker_env["WORKER_ID"] = f"worker-{i}"
            procs.append(_launch(f"worker-{i}", (
                "import sys; sys.path.insert(0, '.');"
                "from worker.worker import run;"
                "run(idle_timeout=600)"  # 10 min
            ), worker_env))

        time.sleep(0.5)

        url = f"http://localhost:{http_port}"
        print()
        print("  ┌───────────────────────────────────────────────┐")
        print(f"  │  Dashboard ready:  {url:<25}  │")
        print("  │  Press Ctrl+C to shut down                    │")
        print("  └───────────────────────────────────────────────┘")
        print()

        if not args.no_browser:
            webbrowser.open(url)

        # Block until Ctrl+C
        signal.pause()

    except KeyboardInterrupt:
        print()
        print("  Shutting down …")
    finally:
        # Terminate all children
        for p in procs:
            try:
                p.terminate()
            except OSError:
                pass
        # Give them a moment to flush
        deadline = time.monotonic() + 3
        for p in procs:
            remaining = max(0.1, deadline - time.monotonic())
            try:
                p.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                p.kill()
        print(f"  All processes stopped.  Results in: {output_dir}")
        print()


if __name__ == "__main__":
    main()