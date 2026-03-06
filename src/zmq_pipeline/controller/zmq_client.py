"""
controller/zmq_client.py
~~~~~~~~~~~~~~~~~~~~~~~~
REQ socket client that dispatches a job to the Ventilator and blocks until
it receives an ACK or NACK (or times out).

This is the *only* ZMQ socket the Controller uses for the command plane.
It is intentionally a thin, synchronous wrapper — the async HTTP layer
around it (Phase 4) will run it in a thread executor to avoid blocking the
event loop.

Usage (standalone test)::

    from controller.zmq_client import VentilatorClient
    client = VentilatorClient()
    reply = client.dispatch(job_dispatch_dict)
    print(reply)   # {"job_id": "...", "status": "ack", "reason": null}
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict

import zmq

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ..shared.config import cfg
from ..shared.protocol import JobDispatch, VentilatorReply


class VentilatorClient:
    """One-shot REQ/REP client for job dispatch.

    A fresh socket is created per ``dispatch()`` call.  This is intentional:
    REQ sockets are strictly send→recv stateful; reusing a socket that saw a
    timeout leaves it in an undefined state.  Creating a new socket is cheap
    and makes error recovery trivial.
    """

    def __init__(
        self,
        endpoint: str | None = None,
        timeout_ms: int | None = None,
    ) -> None:
        self._endpoint = endpoint or cfg.ventilator_rep_endpoint
        self._timeout_ms = timeout_ms if timeout_ms is not None else cfg.controller_timeout_ms

    def dispatch(self, job: Dict[str, Any]) -> VentilatorReply:
        """Send *job* to the Ventilator and return its reply.

        Parameters
        ----------
        job:
            A dict matching the ``JobDispatch`` schema.  Validated by Pydantic
            before sending so schema errors surface at the caller, not as
            wire-level garbage.

        Returns
        -------
        VentilatorReply
            Validated reply with ``status="ack"`` or ``status="nack"``.

        Raises
        ------
        TimeoutError
            Ventilator did not respond within ``timeout_ms``.
        ValueError
            *job* failed schema validation, or the reply was malformed.
        zmq.ZMQError
            Low-level socket error (e.g. connection refused).
        """
        # Validate before sending — surface schema issues at the client.
        validated_job = JobDispatch.model_validate(job)

        context = zmq.Context.instance()
        req = context.socket(zmq.REQ)
        req.setsockopt(zmq.LINGER, 0)
        req.setsockopt(zmq.RCVTIMEO, self._timeout_ms)
        req.setsockopt(zmq.SNDTIMEO, self._timeout_ms)
        req.connect(self._endpoint)

        try:
            req.send(validated_job.model_dump_json().encode("utf-8"))

            try:
                raw = req.recv()
            except zmq.Again:
                raise TimeoutError(
                    f"Ventilator at {self._endpoint} did not respond "
                    f"within {self._timeout_ms} ms"
                )

            return VentilatorReply.model_validate_json(raw)

        finally:
            req.close()
