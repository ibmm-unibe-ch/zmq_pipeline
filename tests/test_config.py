"""tests/test_config.py — unit tests for shared/config.py"""

from __future__ import annotations

import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
from zmq_pipeline.shared.config import Config


class TestConfigDefaults:
    def test_default_ports(self):
        cfg = Config.from_env()
        assert cfg.ventilator_rep_port == 5555
        assert cfg.ventilator_push_port == 5556
        assert cfg.sink_pull_port == 5557
        assert cfg.telemetry_sub_port == 5558
        assert cfg.http_port == 5001
        assert cfg.ws_port == 5002

    def test_default_host(self):
        assert Config.from_env().controller_host == "localhost"

    def test_default_timeout(self):
        assert Config.from_env().controller_timeout_ms == 5000

    def test_default_ws_buffer(self):
        assert Config.from_env().ws_buffer_size == 500


class TestConfigFromEnv:
    def test_custom_port(self, monkeypatch):
        monkeypatch.setenv("ZMQ_VENTILATOR_REP_PORT", "9999")
        cfg = Config.from_env()
        assert cfg.ventilator_rep_port == 9999

    def test_custom_host(self, monkeypatch):
        monkeypatch.setenv("ZMQ_CONTROLLER_HOST", "head-node-01")
        cfg = Config.from_env()
        assert cfg.controller_host == "head-node-01"

    def test_invalid_port_raises(self, monkeypatch):
        monkeypatch.setenv("HTTP_PORT", "not_a_number")
        with pytest.raises(ValueError, match="not a valid integer"):
            Config.from_env()


class TestEndpointProperties:
    def test_ventilator_rep_endpoint(self):
        cfg = Config.from_env()
        assert cfg.ventilator_rep_endpoint == "tcp://localhost:5555"

    def test_sink_pull_bind(self):
        cfg = Config.from_env()
        assert cfg.sink_pull_bind == "tcp://*:5557"

    def test_telemetry_pub_endpoint(self):
        cfg = Config.from_env()
        assert cfg.telemetry_pub_endpoint == "tcp://localhost:5558"
