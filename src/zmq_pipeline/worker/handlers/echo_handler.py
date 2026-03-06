"""
worker/handlers/echo_handler.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Minimal concrete handler used for integration testing.

Returns two numpy arrays whose values are derived from the task payload so
the Sink can verify that the right task produced the right arrays.
"""

from __future__ import annotations

from typing import Any, Dict

import random
import time

import numpy as np

from .base_handler import BaseHandler


class EchoHandler(BaseHandler):
    """Echo the task index back as array data so tests can verify correctness."""

    ARRAY_SHAPE = (4, 4)   # small — fast to allocate and compare

    def process(self, task_id: str, payload: Dict[str, Any]) -> Dict[str, np.ndarray]:
        if payload.get("fail"):
            raise RuntimeError(f"Deliberate failure for task {task_id}")

        index: int = int(payload.get("index", 0))
        scale: float = float(payload.get("scale", 1.0))

        # output_data: float32 array filled with index * scale
        output_data = np.full(self.ARRAY_SHAPE, index * scale, dtype=np.float32)

        # output_index: int32 array filled with the raw index value
        output_index = np.full(self.ARRAY_SHAPE, index, dtype=np.int32)

        time.sleep(random.random()*2)

        return {
            "output_data": output_data,
            "output_index": output_index,
        }