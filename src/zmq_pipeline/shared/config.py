"""
shared/config.py
~~~~~~~~~~~~~~~~
Centralised configuration loaded from environment variables (or a .env file).

Every component imports `cfg` and reads attributes from it — no component
hard-codes port numbers or hostnames.

Environment variables are read once at import time.  If you need to reload
(e.g. in tests) call ``Config.from_env()`` to get a fresh instance.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"Config error: {name}={raw!r} is not a valid integer") from exc


def _str(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass(frozen=True)
class Config:
    # ------------------------------------------------------------------ hosts
    controller_host: str = field(default_factory=lambda: _str("ZMQ_CONTROLLER_HOST", "localhost"))

    # ------------------------------------------------------------------ ports
    ventilator_rep_port: int = field(default_factory=lambda: _int("ZMQ_VENTILATOR_REP_PORT", 5555))
    ventilator_push_port: int = field(default_factory=lambda: _int("ZMQ_VENTILATOR_PUSH_PORT", 5556))
    sink_pull_port: int = field(default_factory=lambda: _int("ZMQ_SINK_PULL_PORT", 5557))
    telemetry_sub_port: int = field(default_factory=lambda: _int("ZMQ_TELEMETRY_SUB_PORT", 5558))
    http_port: int = field(default_factory=lambda: _int("HTTP_PORT", 5001))
    ws_port: int = field(default_factory=lambda: _int("WS_PORT", 5002))

    # ------------------------------------------------------------------ worker
    worker_id: str = field(default_factory=lambda: _str("WORKER_ID", ""))

    # ------------------------------------------------------------------ sink
    sink_output_dir: str = field(default_factory=lambda: _str("SINK_OUTPUT_DIR", "./output"))

    # ------------------------------------------------------------------ tuning
    controller_timeout_ms: int = field(
        default_factory=lambda: _int("CONTROLLER_TIMEOUT_MS", 5000)
    )
    ws_buffer_size: int = field(default_factory=lambda: _int("WS_BUFFER_SIZE", 500))

    def __post_init__(self) -> None:
        errors = []
        # Validate host
        if not self.controller_host.strip():
            errors.append("ZMQ_CONTROLLER_HOST must not be empty")
        # Validate port ranges
        port_fields = [
            ("ZMQ_VENTILATOR_REP_PORT", self.ventilator_rep_port),
            ("ZMQ_VENTILATOR_PUSH_PORT", self.ventilator_push_port),
            ("ZMQ_SINK_PULL_PORT", self.sink_pull_port),
            ("ZMQ_TELEMETRY_SUB_PORT", self.telemetry_sub_port),
            ("HTTP_PORT", self.http_port),
            ("WS_PORT", self.ws_port),
        ]
        for name, port in port_fields:
            if not (1 <= port <= 65535):
                errors.append(f"{name}={port} is outside valid range 1–65535")
        # Check for duplicate ports
        ports = [p for _, p in port_fields]
        seen = set()
        for name, port in port_fields:
            if port in seen:
                errors.append(f"{name}={port} collides with another port")
            seen.add(port)
        # Validate positive tuning values
        if self.controller_timeout_ms <= 0:
            errors.append(f"CONTROLLER_TIMEOUT_MS={self.controller_timeout_ms} must be > 0")
        if self.ws_buffer_size <= 0:
            errors.append(f"WS_BUFFER_SIZE={self.ws_buffer_size} must be > 0")
        if errors:
            # frozen dataclass — raise instead of fixing
            raise ValueError("Configuration errors:\n  " + "\n  ".join(errors))

    # ------------------------------------------------------------------ convenience methods

    @classmethod
    def from_env(cls) -> "Config":
        """Return a fresh Config instance read from the current environment."""
        return cls()

    # ---- pre-built ZMQ endpoint strings ---------------------------------

    @property
    def ventilator_rep_endpoint(self) -> str:
        return f"tcp://{self.controller_host}:{self.ventilator_rep_port}"

    @property
    def ventilator_push_endpoint(self) -> str:
        return f"tcp://{self.controller_host}:{self.ventilator_push_port}"

    @property
    def sink_pull_endpoint(self) -> str:
        return f"tcp://{self.controller_host}:{self.sink_pull_port}"

    @property
    def telemetry_pub_endpoint(self) -> str:
        """Endpoint that PUB sockets (Ventilator, Workers, Sink) connect to."""
        return f"tcp://{self.controller_host}:{self.telemetry_sub_port}"

    # ---- bind strings (used by the components that own the socket) ------

    @property
    def ventilator_rep_bind(self) -> str:
        return f"tcp://*:{self.ventilator_rep_port}"

    @property
    def ventilator_push_bind(self) -> str:
        return f"tcp://*:{self.ventilator_push_port}"

    @property
    def sink_pull_bind(self) -> str:
        return f"tcp://*:{self.sink_pull_port}"

    @property
    def telemetry_sub_bind(self) -> str:
        return f"tcp://*:{self.telemetry_sub_port}"


# Module-level singleton — import this everywhere.
cfg: Config = Config.from_env()