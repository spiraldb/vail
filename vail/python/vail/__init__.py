"""Experimental Hail-shaped Table frontend over Vail's native Rust execution.

This initial slice supports primitive constructors, keys, ordering and collection.
It is not yet a replacement for the full Hail frontend; unsupported operations fail
explicitly. The implementation never imports Hail, Spark or Java.
"""
from __future__ import annotations

import builtins
from collections.abc import Mapping
import json
import re

from ._native import NativeSession
from .expressions import (
    Expression,
    Field as _Field,
    StructExpression as _StructExpression,
    array,
    array_max as max,
    array_min as min,
    array_product as product,
    array_sum as sum,
    as_expr,
    bool_ as _bool_cast,
    cast,
    float32,
    float64,
    if_else,
    int32,
    int64,
    is_missing,
    literal,
    missing,
    str_ as _str_cast,
    struct,
)
from .types import (
    Struct,
    decode_value,
    dtype,
    encode_value,
    infer_type,
    tarray,
    tbool,
    tfloat,
    tfloat32,
    tfloat64,
    tint,
    tint32,
    tint64,
    tstr,
    tstruct,
    type_from_wire,
    type_to_wire,
)


class _Sort:
    def __init__(self, col, descending):
        self.col = col
        self.descending = descending


def asc(col):
    return _Sort(col, False)


def desc(col):
    return _Sort(col, True)


_session = None


def _native_session():
    global _session
    if _session is None:
        _session = NativeSession()
    return _session


def _validate_n(value, name, *, positive=False):
    if isinstance(value, builtins.bool) or not isinstance(value, builtins.int):
        raise TypeError(f"{name} must be an integer")
    if value < int(positive):
        raise ValueError(f"{name} must be {'positive' if positive else 'non-negative'}")


def _infer_type(values):
    present = [v for v in values if v is not None]
    if not present:
        raise ValueError("cannot infer a type from only missing values; provide schema")
    result = infer_type(present[0])
    for value in present[1:]:
        inferred = infer_type(value)
        if inferred != result:
            from .types import unify_types

            result = unify_types(result, inferred)
            if result is None:
                raise NotImplementedError("initial bridge cannot infer a common type for these values")
    return result


def _check_value(value, dtype, name):
    if value is None:
        return
    if dtype in (tint32, tint64):
        if type(value) is not builtins.int:
            raise TypeError(f"field {name!r} requires {dtype}")
        bits = 32 if dtype == tint32 else 64
        if not -(2 ** (bits - 1)) <= value < 2 ** (bits - 1):
            raise OverflowError(f"field {name!r} does not fit {dtype}")
    elif dtype in (tfloat32, tfloat64):
        if type(value) not in (builtins.int, builtins.float):
            raise TypeError(f"field {name!r} requires {dtype}")
    elif dtype == tbool and type(value) is not builtins.bool:
        raise TypeError(f"field {name!r} requires bool")
    elif dtype == tstr and not isinstance(value, builtins.str):
        raise TypeError(f"field {name!r} requires str")
    elif isinstance(dtype, tarray):
        if not isinstance(value, (list, tuple)):
            raise TypeError(f"field {name!r} requires {dtype}")
        for item in value:
            _check_value(item, dtype.element_type, name)
    elif isinstance(dtype, tstruct):
        if not isinstance(value, Mapping):
            raise TypeError(f"field {name!r} requires {dtype}")
        if set(value) != set(dtype):
            raise ValueError(f"nested fields for {name!r} must match schema")
        for field_name, field_type in dtype.items():
            _check_value(value[field_name], field_type, f"{name}.{field_name}")


def _parse_primitive_struct(schema):
    """Accept the flat primitive struct spelling used by Hail's Table.parallelize."""
    match = re.fullmatch(r"\s*struct\s*\{(.*)\}\s*", schema, re.DOTALL)
    if match is None:
        raise NotImplementedError("only flat primitive struct schema strings are supported")
    body = match.group(1).strip()
    if not body:
        return tstruct()
    types = {dtype.name: dtype for dtype in (tint32, tint64, tbool, tstr)}
    fields = {}
    for declaration in body.split(","):
        field = re.fullmatch(r"\s*([A-Za-z_][A-Za-z_0-9]*)\s*:\s*(\w+)\s*", declaration)
        if field is None or field.group(2) not in types:
            raise NotImplementedError("only flat primitive struct schema strings are supported")
        name, dtype = field.groups()
        if name in fields:
            raise ValueError(f"duplicate field {name!r}")
        fields[name] = types[dtype]
    return tstruct(**fields)


class Table:
    """A lazy native Table for the explicitly supported initial operation subset."""

    def __init__(self, native, schema, keys=()):
        object.__setattr__(self, "_native", native)
        object.__setattr__(self, "_schema", schema)
        object.__setattr__(self, "_keys", tuple(keys))

    def __setattr__(self, name, value):
        raise TypeError("Table is immutable")

    def __delattr__(self, name):
        raise TypeError("Table is immutable")

    @classmethod
    def parallelize(cls, rows, schema=None, key=None, n_partitions=None, *, partial_type=None, globals=None):
        if schema is not None and partial_type is not None:
            raise ValueError("define either schema or partial type, not both.")
        if partial_type is not None or globals is not None:
            raise NotImplementedError("partial_type and globals are not implemented in this initial slice")
        rows = list(rows)
        if any(not isinstance(row, Mapping) for row in rows):
            raise TypeError("rows must be dictionaries or Struct values")
        if schema is None:
            if not rows:
                raise ValueError("provide schema for an empty table")
            names = list(rows[0])
            if any(set(row) != set(names) for row in rows):
                raise ValueError("all rows must have the same fields")
            schema = tstruct(**{name: _infer_type([row[name] for row in rows]) for name in names})
        if isinstance(schema, builtins.str):
            schema = dtype(schema)
        if not isinstance(schema, tstruct):
            raise TypeError("schema must be a vail.tstruct")
        names = list(schema)
        for row in rows:
            if set(row) != set(names):
                raise ValueError("row fields must match schema")
            for name in names:
                _check_value(row[name], schema[name], name)
        n_partitions = 1 if n_partitions is None else n_partitions
        _validate_n(n_partitions, "n_partitions", positive=True)
        native = _native_session().parallelize(
            json.dumps([[name, type_to_wire(schema[name])] for name in names]),
            json.dumps([[encode_value(row[name]) for name in names] for row in rows], allow_nan=False),
            n_partitions,
        )
        table = cls(native, schema)
        if key is not None:
            table = table.key_by(*( [key] if isinstance(key, str) else key ))
        return table

    def __getitem__(self, item):
        if not isinstance(item, builtins.str):
            raise NotImplementedError("keyed lookup is not implemented in this initial slice")
        return _Field(self, item, self._schema[item])

    def __getattr__(self, name):
        schema = self.__dict__.get("_schema")
        if schema is not None and name in schema:
            return self[name]
        raise AttributeError(name)

    def __iter__(self):
        raise TypeError("Table is not iterable; use collect()")

    @property
    def key(self):
        return _StructExpression({name: self[name] for name in self._keys}, self)

    @property
    def row(self):
        return _StructExpression({name: self[name] for name in self._schema}, self)

    @property
    def row_value(self):
        return _StructExpression(
            {name: self[name] for name in self._schema if name not in self._keys}, self
        )

    @property
    def globals(self):
        return _StructExpression({}, self)

    def _field_name(self, field):
        if isinstance(field, Expression):
            if field.source is not self:
                raise ValueError("expression source mismatch: field belongs to a different Table")
            if field._selection_name is None:
                raise ValueError("positional selection requires a source field reference")
            return field._selection_name
        if not isinstance(field, builtins.str):
            raise NotImplementedError("only field references are supported in this initial slice")
        if field not in self._schema:
            raise KeyError(field)
        return field

    def _project(self, projections, keys=None):
        projections_json = json.dumps([[name, expr.to_ir()] for name, expr in projections])
        native = self._native.project_json(projections_json)
        schema = {
            name: field_type
            for name, field_type in (
                (field_name, type_from_wire(field_type))
                for field_name, field_type in json.loads(native.schema_json())
            )
        }
        return Table(native, tstruct(**schema), self._keys if keys is None else keys)

    def annotate(self, **named_exprs):
        if not named_exprs:
            return self
        for name in named_exprs:
            if not isinstance(name, builtins.str) or not name:
                raise ValueError("annotated field names must be non-empty strings")
            if name in self._keys:
                raise ValueError(f"cannot overwrite key field {name!r}")
        expressions = {}
        for name, value in named_exprs.items():
            expr = as_expr(value)
            if expr.source is not None and expr.source is not self:
                raise ValueError("expression source mismatch: field belongs to a different Table")
            expressions[name] = expr
        projections = []
        for name in self._schema:
            projections.append((name, expressions.pop(name, self[name])))
        projections.extend(expressions.items())
        return self._project(projections)

    def select(self, *exprs, **named_exprs):
        positional = []
        for value in exprs:
            if isinstance(value, builtins.str):
                value = self[value]
            if not isinstance(value, Expression):
                raise ValueError("positional selection requires a source field reference")
            name = self._field_name(value)
            if name in self._keys:
                raise ValueError(f"cannot overwrite key field {name!r} with select")
            positional.append((name, value))
        named = []
        for name, value in named_exprs.items():
            if not isinstance(name, builtins.str) or not name:
                raise ValueError("selected field names must be non-empty strings")
            if name in self._keys:
                raise ValueError(f"cannot overwrite key field {name!r} with select")
            expr = as_expr(value)
            if expr.source is not None and expr.source is not self:
                raise ValueError("expression source mismatch: field belongs to a different Table")
            named.append((name, expr))
        names = list(self._keys) + [name for name, _ in positional] + [name for name, _ in named]
        if len(set(names)) != len(names):
            raise ValueError("selection would produce duplicate fields")
        projections = [(name, self[name]) for name in self._keys]
        projections.extend(positional)
        projections.extend(named)
        return self._project(projections, keys=self._keys)

    def transmute(self, **named_exprs):
        if not named_exprs:
            return self
        expressions = {}
        for name, value in named_exprs.items():
            if not isinstance(name, builtins.str) or not name:
                raise ValueError("transmuted field names must be non-empty strings")
            if name in self._keys:
                raise ValueError(f"cannot overwrite key field {name!r} with transmute")
            expr = as_expr(value)
            if expr.source is not None and expr.source is not self:
                raise ValueError("expression source mismatch: field belongs to a different Table")
            expressions[name] = expr

        referenced = set()
        for expr in expressions.values():
            referenced.update(_referenced_columns(expr.to_ir()))
        drop = referenced - set(expressions) - set(self._keys)
        projections = [
            (name, expressions.get(name, self[name]))
            for name in self._schema
            if name not in drop
        ]
        present = {name for name, _ in projections}
        projections.extend((name, expr) for name, expr in expressions.items() if name not in present)
        return self._project(projections, keys=self._keys)

    def key_by(self, *keys, **named_keys):
        if named_keys:
            raise NotImplementedError("computed/named keys are not implemented in this initial slice")
        names = [self._field_name(key) for key in keys]
        if len(set(names)) != len(names):
            raise ValueError("duplicate key fields")
        native = self._native.order_by([(name, False) for name in names])
        return Table(native, self._schema, names)

    def order_by(self, *exprs):
        keys = []
        for expr in exprs:
            sort = expr if isinstance(expr, _Sort) else asc(expr)
            keys.append((self._field_name(sort.col), sort.descending))
        return Table(self._native.order_by(keys), self._schema)

    def filter(self, expr, keep=True):
        if type(keep) is not builtins.bool:
            raise TypeError("keep must be a bool")
        expr = as_expr(expr)
        if expr.source is not None and expr.source is not self:
            raise ValueError("expression source mismatch: field belongs to a different Table")
        if expr.dtype != tbool:
            raise TypeError(f"filter predicate must be bool, got {expr.dtype}")
        native = self._native.filter_json(json.dumps(expr.to_ir()), keep)
        return Table(native, self._schema, self._keys)

    def head(self, n):
        _validate_n(n, "n")
        return Table(self._native.head(n), self._schema, self._keys)

    def tail(self, n):
        _validate_n(n, "n")
        return Table(self._native.tail(n), self._schema, self._keys)

    def take(self, n, _localize=True):
        return self.head(n).collect(_localize)

    def collect(self, _localize=True):
        if not _localize:
            raise NotImplementedError("non-localized expressions are not implemented in this initial slice")
        names = list(self._schema)
        return [
            Struct(**{name: decode_value(value, self._schema[name]) for name, value in zip(names, row)})
            for row in json.loads(self._native.collect_json())
        ]

    def count(self):
        return self._native.count()

    def _force_count(self):
        return self._native.force_count()

    def n_partitions(self):
        return self._native.n_partitions()


def range_table(n, n_partitions=None):
    _validate_n(n, "n")
    if n > 2**31 - 1:
        raise ValueError("range_table supports n up to 2**31 - 1")
    n_partitions = 1 if n_partitions is None else n_partitions
    _validate_n(n_partitions, "n_partitions", positive=True)
    return Table(_native_session().range_table(n, n_partitions), tstruct(idx=tint32), ("idx",))


from . import utils  # noqa: E402


def _referenced_columns(node):
    if isinstance(node, dict):
        result = {node["name"]} if node.get("op") == "column" else set()
        for value in node.values():
            result.update(_referenced_columns(value))
        return result
    if isinstance(node, list):
        result = set()
        for value in node:
            result.update(_referenced_columns(value))
        return result
    return set()
