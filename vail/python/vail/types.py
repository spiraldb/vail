"""Small recursive type and value codecs for Vail's Hail-shaped frontend."""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import builtins
import math


@dataclass(frozen=True)
class _PrimitiveType:
    name: str

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.name


@dataclass(frozen=True)
class tarray:
    element_type: object

    def __post_init__(self):
        if not is_type(self.element_type):
            raise TypeError("tarray requires a Vail type")

    def __str__(self):
        return f"array<{self.element_type}>"

    def __repr__(self):
        return f"tarray({self.element_type!r})"


tint32 = _PrimitiveType("int32")
tint64 = _PrimitiveType("int64")
tfloat32 = _PrimitiveType("float32")
tfloat64 = _PrimitiveType("float64")
tbool = _PrimitiveType("bool")
tstr = _PrimitiveType("str")
tint = tint32
tfloat = tfloat64


class tstruct(Mapping):
    def __init__(self, **fields):
        for name, dtype in fields.items():
            if not isinstance(name, str):
                raise TypeError("struct field names must be strings")
            if not is_type(dtype):
                raise TypeError(f"unsupported type for {name!r}: {dtype}")
        self._fields = dict(fields)

    def __iter__(self):
        return iter(self._fields)

    def __len__(self):
        return len(self._fields)

    def __getitem__(self, name):
        return self._fields[name]

    def __repr__(self):
        return "struct{" + ", ".join(f"{k}: {v}" for k, v in self.items()) + "}"

    def __eq__(self, other):
        return isinstance(other, tstruct) and tuple(self.items()) == tuple(other.items())


class Struct(Mapping):
    def __init__(self, **fields):
        object.__setattr__(self, "_fields", dict(fields))

    def __iter__(self):
        return iter(self._fields)

    def __len__(self):
        return len(self._fields)

    def __getitem__(self, name):
        return self._fields[name]

    def __getattr__(self, name):
        try:
            return self._fields[name]
        except KeyError:
            raise AttributeError(name) from None

    def __setattr__(self, name, value):
        raise TypeError("Struct is immutable")

    def __delattr__(self, name):
        raise TypeError("Struct is immutable")

    def __repr__(self):
        return "Struct(" + ", ".join(f"{k}={v!r}" for k, v in self.items()) + ")"

    def __eq__(self, other):
        return isinstance(other, Mapping) and dict(self) == dict(other)


def is_type(value):
    return value in (tint32, tint64, tfloat32, tfloat64, tbool, tstr) or isinstance(value, (tstruct, tarray))


def dtype(type_string):
    """Parse the primitive, array, and struct subset used by the frontend."""
    if not builtins.isinstance(type_string, builtins.str):
        raise TypeError("dtype expects a string")
    return _parse_type(type_string.strip())


def _split_type_fields(body):
    fields = []
    start = 0
    depth = 0
    for i, char in enumerate(body):
        if char in "<{[":
            depth += 1
        elif char in ">}]":
            depth -= 1
        elif char == "," and depth == 0:
            fields.append(body[start:i].strip())
            start = i + 1
    tail = body[start:].strip()
    if tail:
        fields.append(tail)
    return fields


def _parse_type(text):
    aliases = {
        "int": tint32, "int32": tint32, "int64": tint64,
        "float": tfloat64, "float32": tfloat32, "float64": tfloat64,
        "bool": tbool, "str": tstr,
    }
    if text in aliases:
        return aliases[text]
    if text.startswith("array<") and text.endswith(">"):
        return tarray(_parse_type(text[6:-1].strip()))
    if text.startswith("struct{") and text.endswith("}"):
        fields = {}
        for decl in _split_type_fields(text[7:-1]):
            depth = 0
            colon = None
            for i, char in enumerate(decl):
                if char in "<{[": depth += 1
                elif char in ">}]": depth -= 1
                elif char == ":" and depth == 0:
                    colon = i
                    break
            if colon is None:
                raise ValueError(f"invalid struct field type declaration: {decl!r}")
            name = decl[:colon].strip()
            if not name or name in fields:
                raise ValueError(f"invalid or duplicate struct field name: {name!r}")
            fields[name] = _parse_type(decl[colon + 1:].strip())
        return tstruct(**fields)
    raise NotImplementedError(f"unsupported type string: {text!r}")


def type_to_wire(dtype):
    if isinstance(dtype, _PrimitiveType):
        return dtype.name
    if isinstance(dtype, tarray):
        return {"array": type_to_wire(dtype.element_type)}
    if isinstance(dtype, tstruct):
        return {"struct": [[name, type_to_wire(field_type)] for name, field_type in dtype.items()]}
    raise TypeError(f"not a Vail type: {dtype!r}")


def type_from_wire(value):
    if isinstance(value, str):
        primitives = {t.name: t for t in (tint32, tint64, tfloat32, tfloat64, tbool, tstr)}
        try:
            return primitives[value]
        except KeyError:
            raise ValueError(f"unknown Vail type: {value}") from None
    if isinstance(value, dict) and set(value) == {"array"}:
        return tarray(type_from_wire(value["array"]))
    if isinstance(value, dict) and set(value) == {"struct"}:
        return tstruct(**{name: type_from_wire(field_type) for name, field_type in value["struct"]})
    raise ValueError(f"invalid Vail type encoding: {value!r}")


def infer_type(value):
    if value is None:
        raise ValueError("cannot infer a type from a missing value; provide a type")
    if type(value) is bool:
        return tbool
    if type(value) is int:
        if -(2**31) <= value < 2**31:
            return tint32
        if -(2**63) <= value < 2**63:
            return tint64
        raise OverflowError("integer does not fit int64")
    if isinstance(value, float):
        return tfloat64
    if isinstance(value, str):
        return tstr
    if isinstance(value, Mapping):
        if any(field is None for field in value.values()):
            raise ValueError("cannot infer a struct field type from a missing value; provide a type")
        return tstruct(**{name: infer_type(field) for name, field in value.items()})
    if isinstance(value, list):
        if not value:
            raise ValueError("cannot infer an array element type from an empty array")
        types = [infer_type(x) for x in value if x is not None]
        if not types:
            raise ValueError("cannot infer an array element type from only missing values")
        dtype = types[0]
        for other in types[1:]:
            dtype = unify_types(dtype, other)
            if dtype is None:
                raise TypeError("array elements have incompatible types")
        return tarray(dtype)
    if isinstance(value, tuple):
        raise TypeError("tuple literals require a tuple type, which is not supported")
    raise TypeError(f"cannot infer a Vail type for {type(value).__name__}")


def unify_types(left, right):
    if left == right:
        return left
    numeric = (tint32, tint64, tfloat32, tfloat64)
    if left in numeric and right in numeric:
        if tfloat64 in (left, right) or {left, right} == {tfloat32, tint64}:
            return tfloat64
        if tfloat32 in (left, right):
            return tfloat32
        if tint64 in (left, right):
            return tint64
        return tint32
    if isinstance(left, tarray) and isinstance(right, tarray):
        element = unify_types(left.element_type, right.element_type)
        return None if element is None else tarray(element)
    return None


def encode_value(value):
    if isinstance(value, Struct):
        value = dict(value)
    if isinstance(value, Mapping):
        return {name: encode_value(item) for name, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [encode_value(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        return {"$float": "nan" if math.isnan(value) else ("inf" if value > 0 else "-inf")}
    return value


def decode_value(value, dtype=None):
    if value is None:
        return None
    if dtype in (tfloat32, tfloat64) and isinstance(value, dict) and set(value) == {"$float"}:
        return {"nan": float("nan"), "inf": float("inf"), "-inf": float("-inf")}[value["$float"]]
    if isinstance(dtype, tstruct) and isinstance(value, dict):
        return Struct(**{name: decode_value(value.get(name), field_type) for name, field_type in dtype.items()})
    if isinstance(dtype, tarray) and isinstance(value, list):
        return [decode_value(item, dtype.element_type) for item in value]
    if isinstance(value, dict):
        return Struct(**{name: decode_value(item) for name, item in value.items()})
    if isinstance(value, list):
        return [decode_value(item) for item in value]
    return value
