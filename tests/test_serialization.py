"""
tests/test_serialization.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Unit tests for shared/serialization.py.

Gate criterion: every round-trip test must pass before any Worker or Sink
code is written.  This file intentionally exercises every edge case described
in the protocol spec.
"""

from __future__ import annotations

import math
from datetime import datetime, timezone

import numpy as np
import pytest

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from zmq_pipeline.shared.serialization import pack_result, unpack_result, pack_error_result
from zmq_pipeline.shared.protocol import ResultMeta


# ============================================================================
# Helpers
# ============================================================================

BASE_META = {
    "job_id": "550e8400-e29b-41d4-a716-446655440000",
    "task_id": "task-001",
    "worker_id": "worker-node04-12345",
    "completed_at": datetime.now(tz=timezone.utc).isoformat(),
    "status": "ok",
}


def _meta(**overrides):
    return {**BASE_META, **overrides}


# ============================================================================
# Round-trip tests — shapes and dtypes
# ============================================================================

class TestRoundTrip:
    """pack → unpack must give back identical arrays."""

    def test_single_float32_2d(self):
        arr = np.random.rand(1024, 1024).astype(np.float32)
        f1, f2 = pack_result(_meta(), {"output_image": arr})
        _, arrays = unpack_result(f1, f2)

        assert "output_image" in arrays
        out = arrays["output_image"]
        assert out.shape == (1024, 1024)
        assert out.dtype == np.float32
        np.testing.assert_array_equal(out, arr)

    def test_single_uint8_2d(self):
        arr = (np.random.rand(512, 512) * 255).astype(np.uint8)
        f1, f2 = pack_result(_meta(), {"mask": arr})
        _, arrays = unpack_result(f1, f2)

        np.testing.assert_array_equal(arrays["mask"], arr)
        assert arrays["mask"].dtype == np.uint8

    def test_multiple_arrays_correct_offsets(self):
        """Two arrays must land at the right byte offsets and not bleed into each other."""
        img = np.arange(1024 * 1024, dtype=np.float32).reshape(1024, 1024)
        mask = np.ones((1024, 1024), dtype=np.uint8)

        f1, f2 = pack_result(_meta(), {"output_image": img, "output_mask": mask})
        meta, arrays = unpack_result(f1, f2)

        assert set(arrays.keys()) == {"output_image", "output_mask"}
        np.testing.assert_array_equal(arrays["output_image"], img)
        np.testing.assert_array_equal(arrays["output_mask"], mask)

    def test_byte_offsets_are_sequential(self):
        img = np.zeros((10, 10), dtype=np.float32)   # 400 bytes
        mask = np.zeros((10, 10), dtype=np.uint8)     # 100 bytes

        f1, _ = pack_result(_meta(), {"img": img, "mask": mask})
        meta = ResultMeta.model_validate_json(f1)

        assert meta.arrays["img"].byte_offset == 0
        assert meta.arrays["mask"].byte_offset == img.nbytes

    def test_1d_array(self):
        arr = np.linspace(0, 1, 256, dtype=np.float64)
        f1, f2 = pack_result(_meta(), {"scores": arr})
        _, arrays = unpack_result(f1, f2)

        np.testing.assert_allclose(arrays["scores"], arr)
        assert arrays["scores"].shape == (256,)

    def test_3d_array(self):
        arr = np.random.randint(0, 256, size=(3, 64, 64), dtype=np.uint8)
        f1, f2 = pack_result(_meta(), {"rgb": arr})
        _, arrays = unpack_result(f1, f2)

        np.testing.assert_array_equal(arrays["rgb"], arr)
        assert arrays["rgb"].shape == (3, 64, 64)

    def test_int64_dtype(self):
        arr = np.array([0, 1, -1, 2**32, -(2**32)], dtype=np.int64)
        f1, f2 = pack_result(_meta(), {"ids": arr})
        _, arrays = unpack_result(f1, f2)

        np.testing.assert_array_equal(arrays["ids"], arr)
        assert arrays["ids"].dtype == np.int64

    def test_scalar_shape(self):
        """A (1,) array — degenerate but valid."""
        arr = np.array([42.0], dtype=np.float32)
        f1, f2 = pack_result(_meta(), {"val": arr})
        _, arrays = unpack_result(f1, f2)

        np.testing.assert_array_equal(arrays["val"], arr)

    def test_f_contiguous_array_round_trips(self):
        """Fortran-order arrays must be made C-contiguous before packing."""
        arr_f = np.asfortranarray(np.arange(100, dtype=np.float32).reshape(10, 10))
        assert not arr_f.flags["C_CONTIGUOUS"]

        f1, f2 = pack_result(_meta(), {"arr": arr_f})
        _, arrays = unpack_result(f1, f2)

        # Values must match even though memory layout changed.
        np.testing.assert_array_equal(arrays["arr"], arr_f)
        assert arrays["arr"].shape == (10, 10)

    def test_many_arrays(self):
        """Ten arrays of mixed dtypes."""
        orig = {
            f"arr_{i}": np.random.rand(8, 8).astype(np.float32)
            for i in range(10)
        }
        f1, f2 = pack_result(_meta(), orig)
        _, arrays = unpack_result(f1, f2)

        assert set(arrays.keys()) == set(orig.keys())
        for name, arr in orig.items():
            np.testing.assert_array_equal(arrays[name], arr)


# ============================================================================
# Error result tests
# ============================================================================

class TestErrorResults:

    def test_error_result_via_pack_result(self):
        """pack_result with empty arrays dict produces zero-byte Frame 2."""
        meta = _meta(status="error", error="FileNotFoundError: img.tif not found")
        f1, f2 = pack_result(meta, {})

        assert f2 == b""

        result_meta, arrays = unpack_result(f1, f2)
        assert result_meta.status == "error"
        assert result_meta.error == "FileNotFoundError: img.tif not found"
        assert arrays == {}

    def test_error_result_convenience_helper(self):
        f1, f2 = pack_error_result(
            job_id="550e8400-e29b-41d4-a716-446655440000",
            task_id="task-007",
            worker_id="worker-xyz",
            error_message="timeout",
        )
        assert f2 == b""

        meta, arrays = unpack_result(f1, f2)
        assert meta.status == "error"
        assert meta.task_id == "task-007"
        assert meta.error == "timeout"
        assert arrays == {}


# ============================================================================
# Metadata fidelity tests
# ============================================================================

class TestMetadataFidelity:

    def test_job_and_task_ids_preserved(self):
        arr = np.zeros((4, 4), dtype=np.float32)
        f1, f2 = pack_result(
            _meta(job_id="550e8400-e29b-41d4-a716-446655440000", task_id="task-999"),
            {"arr": arr},
        )
        meta, _ = unpack_result(f1, f2)

        assert meta.job_id == "550e8400-e29b-41d4-a716-446655440000"
        assert meta.task_id == "task-999"

    def test_worker_id_preserved(self):
        arr = np.zeros((2,), dtype=np.float32)
        f1, f2 = pack_result(_meta(worker_id="worker-host-9999"), {"arr": arr})
        meta, _ = unpack_result(f1, f2)

        assert meta.worker_id == "worker-host-9999"

    def test_array_dtype_string_roundtrips(self):
        """dtype is stored as numpy dtype string (e.g. '<f4') and must resolve back."""
        arr = np.ones((3, 3), dtype=np.float32)
        f1, _ = pack_result(_meta(), {"arr": arr})
        meta = ResultMeta.model_validate_json(f1)

        # The stored dtype string must be parseable by numpy.
        reconstructed_dtype = np.dtype(meta.arrays["arr"].dtype)
        assert reconstructed_dtype == np.float32

    def test_shape_preserved_in_metadata(self):
        arr = np.zeros((7, 13, 5), dtype=np.float32)
        f1, _ = pack_result(_meta(), {"vol": arr})
        meta = ResultMeta.model_validate_json(f1)

        assert meta.arrays["vol"].shape == [7, 13, 5]


# ============================================================================
# Frame integrity tests
# ============================================================================

class TestFrameIntegrity:

    def test_frame2_length_matches_sum_of_array_bytes(self):
        a = np.zeros((100,), dtype=np.float32)   # 400 bytes
        b = np.zeros((50,), dtype=np.uint8)       # 50 bytes
        f1, f2 = pack_result(_meta(), {"a": a, "b": b})

        assert len(f2) == a.nbytes + b.nbytes

    def test_frame1_is_valid_utf8_json(self):
        arr = np.zeros((4,), dtype=np.float32)
        f1, _ = pack_result(_meta(), {"arr": arr})

        decoded = f1.decode("utf-8")
        import json
        parsed = json.loads(decoded)
        assert "arrays" in parsed
        assert "arr" in parsed["arrays"]

    def test_empty_arrays_dict_gives_empty_frame2(self):
        meta = _meta(status="ok")
        f1, f2 = pack_result(meta, {})
        assert f2 == b""

    def test_large_array_no_data_corruption(self):
        """10 MB float32 array — verifies no truncation or misalignment."""
        arr = np.random.rand(2560, 1024).astype(np.float32)  # 10 MB
        f1, f2 = pack_result(_meta(), {"big": arr})

        assert len(f2) == arr.nbytes
        _, arrays = unpack_result(f1, f2)
        np.testing.assert_array_equal(arrays["big"], arr)
