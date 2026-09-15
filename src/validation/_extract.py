"""Shared helpers for pulling a scalar metric out of heterogeneous results.

Strategy scripts in this repo return metrics in many shapes: a bare float, a
flat dict, a nested dict, or an object with a `.metrics` attribute.  The
validation helpers accept all of these so they can wrap existing scripts with
zero refactoring.
"""

from __future__ import annotations

import numbers
from typing import Any

import numpy as np


def dig(obj: Any, path: str, default: Any = None) -> Any:
    """Look up a dotted path in nested dicts (lists indexed by [i])."""
    cur = obj
    for part in str(path).split("."):
        if isinstance(cur, dict):
            if part not in cur:
                return default
            cur = cur[part]
        elif isinstance(cur, (list, tuple)):
            try:
                cur = cur[int(part)]
            except (ValueError, IndexError):
                return default
        else:
            return default
    return cur


def as_float(value: Any) -> float | None:
    """Best-effort conversion to a finite float (None on failure/NaN)."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, numbers.Number):
        f = float(value)
        return f if np.isfinite(f) else None
    if isinstance(value, dict):
        # Report leaves are often {"value": ..} or metric blobs.
        for key in ("value", "metric", "sharpe", "mean", "total"):
            if key in value:
                return as_float(value[key])
    return None


def find_numeric(obj: Any, key: str, _depth: int = 0) -> float | None:
    """Depth-first search for the first finite number stored under `key`."""
    if _depth > 8:
        return None
    if isinstance(obj, dict):
        if key in obj:
            val = as_float(obj[key])
            if val is not None:
                return val
        for v in obj.values():
            found = find_numeric(v, key, _depth + 1)
            if found is not None:
                return found
    elif isinstance(obj, (list, tuple)):
        for v in obj:
            found = find_numeric(v, key, _depth + 1)
            if found is not None:
                return found
    return None


def extract_metric(result: Any, metric: Any) -> float | None:
    """Extract `metric` from an arbitrary strategy result.

    `metric` may be:
      - a callable: called with the result,
      - a dotted path ("a.b.c"),
      - a plain key name (searched depth-first as a fallback).
    """
    if callable(metric):
        try:
            return as_float(metric(result))
        except Exception:
            return None
    if isinstance(metric, str) and "." in metric:
        val = dig(result, metric)
        if val is not None:
            return as_float(val)
    if isinstance(result, dict):
        val = dig(result, metric)
        if val is not None:
            return as_float(val)
        return find_numeric(result, metric)
    for attr in ("metrics",):
        inner = getattr(result, attr, None)
        if isinstance(inner, dict):
            val = dig(inner, metric)
            if val is not None:
                return as_float(val)
    return as_float(result)
