"""Lossless JSON encoding for typed Hail-like fixture values."""
from __future__ import annotations

import math
from typing import Any


class _Missing:
    def __repr__(self) -> str:
        return "MISSING"


MISSING = _Missing()


def encode_value(value: Any) -> Any:
    """Encode fixture values; missing and non-JSON float values use explicit tags."""
    if value is MISSING:
        return {"$missing": True}
    if isinstance(value, float):
        if math.isnan(value):
            return {"$float64": "NaN"}
        if math.isinf(value):
            return {"$float64": "+Infinity" if value > 0 else "-Infinity"}
        if value == 0.0 and math.copysign(1.0, value) < 0:
            return {"$float64": "-0"}
        return value
    if isinstance(value, dict):
        return {str(k): encode_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [encode_value(v) for v in value]
    if value is None:
        raise TypeError("Use MISSING for a Hail missing value; JSON null is not a Hail value")
    if isinstance(value, (bool, int, str)):
        return value
    raise TypeError(f"Unsupported fixture value type: {type(value).__name__}")


def decode_value(value: Any) -> Any:
    if value is None:
        raise ValueError("JSON null is not a fixture value; encode Hail missing as {$missing: true}")
    if isinstance(value, dict):
        if set(value) == {"$missing"}:
            if value["$missing"] is not True:
                raise ValueError("$missing tag must be true")
            return MISSING
        if set(value) == {"$float64"}:
            tag = value["$float64"]
            if tag == "NaN":
                return float("nan")
            if tag == "+Infinity":
                return float("inf")
            if tag == "-Infinity":
                return float("-inf")
            if tag == "-0":
                return -0.0
            raise ValueError(f"Unknown $float64 tag: {tag!r}")
        return {k: decode_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [decode_value(v) for v in value]
    return value
