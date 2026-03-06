"""
controller/telemetry.py
~~~~~~~~~~~~~~~~~~~~~~~
Observability plane of the Controller: binds a SUB socket, subscribes to
all telemetry topics (``status`` and ``log``), validates incoming events
against the protocol schemas, and prints a formatted live stream to stdout.

In Phase 4 this module is intentionally standalone — it runs as a separate
process alongside the pipeline and can be imported or run directly:

    python -m controller.telemetry

In Phase 5+ it will be wired into the Controller's asyncio event loop and
fan events out over WebSocket to the Frontend.

Wire format (per PROJECT.md §5):
    Frame 1 — topic bytes:  b"status"  or  b"log"
    Frame 2 — UTF-8 JSON:   StatusEvent  or  LogEvent payload

The Controller SUB socket **binds**; all component PUB sockets connect.
"""

from __future__ import annotations

import json
import signal
import sys
import threading
from pathlib import Path
from typing import Any, Callable, Optional

import zmq

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ..shared.config import cfg
from ..shared.protocol import LogEvent, StatusEvent

# ANSI colour codes for readable live output
_RESET  = "\033[0m"
_BOLD   = "\033[1m"
_CYAN   = "\033[36m"
_GREEN  = "\033[32m"
_YELLOW = "\033[33m"
_RED    = "\033[31m"
_GREY   = "\033[90m"

_LEVEL_COLOUR = {"INFO": _GREEN, "WARN": _YELLOW, "ERROR": _RED}
_STATUS_COLOUR = {
    "idle":         _GREY,
    "busy":         _CYAN,
    "distributing": _CYAN,
    "done":         _GREEN,
    "error":        _RED,
}

_SHUTDOWN = False


def _handle_signal(signum, frame) -> None:  # noqa: ANN001
    global _SHUTDOWN
    _SHUTDOWN = True


def _fmt_status(event: StatusEvent) -> str:
    colour = _STATUS_COLOUR.get(event.status, _RESET)
    task_part = f"  task={event.task_id}" if event.task_id else ""
    job_short = event.job_id[:8] if event.job_id else "--------"
    return (
        f"{_BOLD}[STATUS]{_RESET} "
        f"{_CYAN}{event.component:<12}{_RESET} "
        f"id={_GREY}{event.component_id}{_RESET} "
        f"job={_GREY}{job_short}…{_RESET} "
        f"→ {colour}{event.status}{_RESET}"
        f"{_GREY}{task_part}{_RESET}"
    )


def _fmt_log(event: LogEvent) -> str:
    colour = _LEVEL_COLOUR.get(event.level, _RESET)
    job_short = event.job_id[:8] if event.job_id else "--------"
    return (
        f"{_BOLD}[LOG]  {_RESET} "
        f"{_CYAN}{event.component:<12}{_RESET} "
        f"id={_GREY}{event.component_id}{_RESET} "
        f"job={_GREY}{job_short}…{_RESET} "
        f"{colour}{event.level:<5}{_RESET} "
        f"{event.message}"
    )


def run(
    max_events: Optional[int] = None,
    on_event: Optional[Any] = None,
    stop_event: Optional[Any] = None,
) -> int:
    """Subscribe to all telemetry and print events until shutdown.

    Parameters
    ----------
    max_events:
        If set, stop after receiving this many valid events.
        Used by the integration test runner.
    on_event:
        Optional callable ``(event: StatusEvent | LogEvent) -> None``.
        Called after each successfully parsed event, *after* printing.
        Used by the Controller to drive ``JobRegistry`` updates.
        Exceptions raised inside the callback are caught and printed.
    stop_event:
        Optional ``threading.Event``.  When set, the loop exits cleanly.
        Use this when running telemetry as a background thread so you can
        stop it without sending SIGTERM to the whole process.

    Returns
    -------
    int
        Number of valid events received.
    """
    global _SHUTDOWN
    _SHUTDOWN = False
    # signal.signal() only works in the main thread; skip when running as a
    # background thread inside the Controller process.
    if threading.current_thread() is threading.main_thread():
        signal.signal(signal.SIGTERM, _handle_signal)

    context = zmq.Context()
    sub = context.socket(zmq.SUB)
    sub.setsockopt(zmq.LINGER, 0)
    sub.setsockopt(zmq.RCVHWM, 0)
    sub.bind(cfg.telemetry_sub_bind)

    # Subscribe to both topics
    sub.setsockopt(zmq.SUBSCRIBE, b"status")
    sub.setsockopt(zmq.SUBSCRIBE, b"log")

    print(f"[telemetry] SUB bound to {cfg.telemetry_sub_bind}")
    print(f"[telemetry] Subscribed to topics: status, log")
    print(f"[telemetry] Waiting for events …\n")

    poller = zmq.Poller()
    poller.register(sub, zmq.POLLIN)

    events_received = 0

    def _should_stop() -> bool:
        if _SHUTDOWN:
            return True
        if stop_event is not None and stop_event.is_set():
            return True
        return False

    try:
        while not _should_stop():
            if max_events is not None and events_received >= max_events:
                break

            socks = dict(poller.poll(timeout=500))
            if sub not in socks:
                continue

            frames = sub.recv_multipart()
            if len(frames) != 2:
                print(f"[telemetry] WARN: unexpected frame count {len(frames)}, skipping")
                continue

            topic = frames[0]
            payload = frames[1]

            try:
                if topic == b"status":
                    event = StatusEvent.model_validate_json(payload)
                    print(_fmt_status(event))
                elif topic == b"log":
                    event = LogEvent.model_validate_json(payload)
                    print(_fmt_log(event))
                else:
                    print(f"[telemetry] WARN: unknown topic {topic!r}")
                    continue
            except Exception as exc:
                raw_preview = payload[:120].decode("utf-8", errors="replace")
                print(f"[telemetry] ERROR parsing event on topic {topic!r}: {exc}")
                print(f"[telemetry] Raw: {raw_preview}")
                continue

            # Drive external observer (e.g. JobRegistry) after successful parse
            if on_event is not None:
                try:
                    on_event(event)
                except Exception as cb_exc:
                    print(f"[telemetry] WARN: on_event callback raised: {cb_exc}")

            events_received += 1

    except KeyboardInterrupt:
        pass
    finally:
        sub.close()
        context.term()
        print(f"\n[telemetry] Shut down. Total events received: {events_received}")

    return events_received


if __name__ == "__main__":
    run()