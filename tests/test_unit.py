"""
tests/test_unit.py
~~~~~~~~~~~~~~~~~~
Unit tests for config validation, protocol schemas, and serialization.
"""

from __future__ import annotations

import os
import unittest

import numpy as np


class TestConfigValidation(unittest.TestCase):

    def _make_config(self, **overrides):
        """Create a Config with custom env vars, then restore originals."""
        from zmq_pipeline.shared.config import Config

        old = {}
        env_map = {
            "controller_host": "ZMQ_CONTROLLER_HOST",
            "ventilator_rep_port": "ZMQ_VENTILATOR_REP_PORT",
            "ventilator_push_port": "ZMQ_VENTILATOR_PUSH_PORT",
            "sink_pull_port": "ZMQ_SINK_PULL_PORT",
            "telemetry_sub_port": "ZMQ_TELEMETRY_SUB_PORT",
            "http_port": "HTTP_PORT",
            "ws_port": "WS_PORT",
            "controller_timeout_ms": "CONTROLLER_TIMEOUT_MS",
            "ws_buffer_size": "WS_BUFFER_SIZE",
        }
        try:
            for key, val in overrides.items():
                env_key = env_map[key]
                old[env_key] = os.environ.get(env_key)
                os.environ[env_key] = str(val)
            return Config.from_env()
        finally:
            for env_key, orig in old.items():
                if orig is None:
                    os.environ.pop(env_key, None)
                else:
                    os.environ[env_key] = orig

    def test_valid_defaults(self):
        from zmq_pipeline.shared.config import Config
        cfg = Config.from_env()
        self.assertIsNotNone(cfg)

    def test_port_out_of_range(self):
        with self.assertRaises(ValueError) as ctx:
            self._make_config(ventilator_rep_port=0)
        self.assertIn("outside valid range", str(ctx.exception))

    def test_port_too_high(self):
        with self.assertRaises(ValueError) as ctx:
            self._make_config(http_port=70000)
        self.assertIn("outside valid range", str(ctx.exception))

    def test_duplicate_ports(self):
        with self.assertRaises(ValueError) as ctx:
            self._make_config(ventilator_rep_port=5556, ventilator_push_port=5556)
        self.assertIn("collides", str(ctx.exception))

    def test_negative_timeout(self):
        with self.assertRaises(ValueError) as ctx:
            self._make_config(controller_timeout_ms=-1)
        self.assertIn("must be > 0", str(ctx.exception))

    def test_non_integer_port(self):
        old = os.environ.get("HTTP_PORT")
        try:
            os.environ["HTTP_PORT"] = "not_a_number"
            from zmq_pipeline.shared.config import Config
            with self.assertRaises(ValueError) as ctx:
                Config.from_env()
            self.assertIn("not a valid integer", str(ctx.exception))
        finally:
            if old is None:
                os.environ.pop("HTTP_PORT", None)
            else:
                os.environ["HTTP_PORT"] = old


class TestSerialization(unittest.TestCase):

    def test_pack_unpack_roundtrip(self):
        from zmq_pipeline.shared.serialization import pack_result, unpack_result

        arrays = {
            "data": np.arange(12, dtype=np.float32).reshape(3, 4),
            "mask": np.ones((3, 4), dtype=np.uint8),
        }
        meta = {
            "job_id": "test-job",
            "task_id": "task-001",
            "worker_id": "worker-test",
            "completed_at": "2024-01-01T00:00:00Z",
            "status": "ok",
        }
        f1, f2 = pack_result(meta, arrays)
        meta_out, arrays_out = unpack_result(f1, f2)

        self.assertEqual(meta_out.job_id, "test-job")
        self.assertEqual(meta_out.status, "ok")
        np.testing.assert_array_equal(arrays_out["data"], arrays["data"])
        np.testing.assert_array_equal(arrays_out["mask"], arrays["mask"])

    def test_pack_error_result(self):
        from zmq_pipeline.shared.serialization import pack_error_result, unpack_result

        f1, f2 = pack_error_result(
            job_id="test-job",
            task_id="task-fail",
            worker_id="worker-test",
            error_message="something broke",
            task_count=5,
        )
        meta, arrays = unpack_result(f1, f2)

        self.assertEqual(meta.status, "error")
        self.assertEqual(meta.error, "something broke")
        self.assertEqual(meta.task_count, 5)
        self.assertEqual(len(arrays), 0)
        self.assertEqual(len(f2), 0)


class TestStopSentinel(unittest.TestCase):

    def test_sentinel_is_valid_json(self):
        import json
        from zmq_pipeline.shared.protocol import STOP_SENTINEL
        data = json.loads(STOP_SENTINEL)
        self.assertEqual(data["__sentinel__"], "STOP")


if __name__ == "__main__":
    unittest.main()
