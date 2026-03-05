"""
shared/serialization.py
~~~~~~~~~~~~~~~~~~~~~~~
Serialization helpers for the performance-critical Worker → Sink result link.

Results are dictionaries of named numpy arrays.  JSON or pickle serialization
of large arrays is too slow.  Instead each result travels as exactly two ZMQ
frames:

    Frame 1  UTF-8 JSON   — ResultMeta (metadata describing each array)
    Frame 2  raw bytes    — all array buffers concatenated in declaration order

``pack_result``   builds the two frames from a metadata dict + array dict.
``unpack_result`` reconstructs the original arrays from the two frames.

The API is intentionally thin: both functions work with plain Python ``bytes``
objects so they are decoupled from any ZMQ socket.  The caller is responsible
for sending / receiving the frames over the wire.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

import numpy as np

from .protocol import ArrayMeta, ResultMeta


# ============================================================================
# Public API
# ============================================================================

def pack_result(
    meta: Dict[str, Any],
    arrays: Dict[str, np.ndarray],
) -> Tuple[bytes, bytes]:
    """Serialise a result into two ZMQ frames.

    Parameters
    ----------
    meta:
        A dict with at least the keys required by ``ResultMeta``
        (``job_id``, ``task_id``, ``worker_id``, ``completed_at``,
        ``status``).  The ``arrays`` key will be **overwritten** by this
        function to ensure it matches the supplied ``arrays`` dict.
    arrays:
        ``{name: ndarray}`` mapping to pack.  Arrays are made C-contiguous
        before packing to guarantee predictable byte layout.  Passing an
        already-contiguous array incurs no copy.

    Returns
    -------
    frame1 : bytes
        UTF-8 encoded JSON containing the full ``ResultMeta``.
    frame2 : bytes
        Concatenated raw array buffers.  Zero-length if *arrays* is empty
        (e.g. error results).
    """
    if not arrays:
        # Error result or explicit empty — Frame 2 is zero bytes.
        arrays_meta: Dict[str, Any] = {}
        frame2 = b""
    else:
        buffers: List[bytes] = []
        arrays_meta = {}
        offset = 0

        for name, arr in arrays.items():
            # Ensure C-contiguous layout so frombuffer works on the receive side.
            if not arr.flags["C_CONTIGUOUS"]:
                arr = np.ascontiguousarray(arr)

            raw: bytes = arr.tobytes()   # copy only when layout changes
            arrays_meta[name] = {
                "dtype": arr.dtype.str,  # e.g. "<f4" — portable across platforms
                "shape": list(arr.shape),
                "byte_offset": offset,
            }
            buffers.append(raw)
            offset += len(raw)

        frame2 = b"".join(buffers)

    # Overwrite (or set) the arrays key with what we actually packed.
    full_meta = dict(meta)
    full_meta["arrays"] = arrays_meta

    # Validate via Pydantic before serialising — catches schema violations early.
    validated = ResultMeta.model_validate(full_meta)
    frame1: bytes = validated.model_dump_json().encode("utf-8")

    return frame1, frame2


def unpack_result(
    frame1: bytes,
    frame2: bytes,
) -> Tuple[ResultMeta, Dict[str, np.ndarray]]:
    """Reconstruct a result from two ZMQ frames.

    Parameters
    ----------
    frame1 : bytes
        UTF-8 encoded JSON ``ResultMeta`` (as produced by ``pack_result``).
    frame2 : bytes
        Concatenated raw array buffers.  May be empty for error results.

    Returns
    -------
    meta : ResultMeta
        Validated metadata object.
    arrays : dict
        ``{name: ndarray}`` mapping.  Empty dict for error results.
    """
    meta = ResultMeta.model_validate_json(frame1)

    arrays: Dict[str, np.ndarray] = {}

    if not meta.arrays or not frame2:
        return meta, arrays

    for name, array_meta in meta.arrays.items():
        dtype = np.dtype(array_meta.dtype)
        shape = tuple(array_meta.shape)
        offset = array_meta.byte_offset
        count = int(np.prod(shape)) if shape else 1

        arr = np.frombuffer(frame2, dtype=dtype, count=count, offset=offset)
        arrays[name] = arr.reshape(shape)

    return meta, arrays


# ============================================================================
# Convenience helpers for error results
# ============================================================================

def pack_error_result(
    job_id: str,
    task_id: str,
    worker_id: str,
    error_message: str,
    task_count: int = 0,
) -> Tuple[bytes, bytes]:
    """Convenience wrapper that builds a two-frame error result."""
    meta: Dict[str, Any] = {
        "job_id": job_id,
        "task_id": task_id,
        "worker_id": worker_id,
        "completed_at": datetime.now(tz=timezone.utc).isoformat(),
        "status": "error",
        "error": error_message,
        "arrays": {},
        "task_count": task_count or None,
    }
    return pack_result(meta, {})