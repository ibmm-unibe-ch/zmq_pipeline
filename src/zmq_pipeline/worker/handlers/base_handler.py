"""
worker/handlers/base_handler.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Abstract interface every domain handler must implement.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict

import numpy as np


class BaseHandler(ABC):
    """Process a single task payload and return named numpy arrays."""

    @abstractmethod
    def process(self, task_id: str, payload: Dict[str, Any]) -> Dict[str, np.ndarray]:
        """
        Parameters
        ----------
        task_id : str
            Unique identifier of the task being processed.
        payload : dict
            Verbatim ``data`` field from the original job input.

        Returns
        -------
        dict
            ``{array_name: ndarray}`` — at least one entry for a successful
            result.  Raise an exception to signal failure; the Worker will
            catch it and send an error result frame.
        """
        ...
