"""Typed, lazy scalar expressions used by the Vail Table frontend."""
from __future__ import annotations

from collections.abc import Mapping
import json

from .types import (
    Struct,
    _PrimitiveType,
    decode_value,
    encode_value,
    infer_type,
    tarray,
    tbool,
    tfloat32,
    tfloat64,
    tint32,
    tint64,
    tstr,
    tstruct,
    type_to_wire,
    unify_types,
)


def _merge_sources(left, right):
    if left is None:
        return right
    if right is None or left is right:
        return left
    raise ValueError("expression source mismatch: fields belong to different Tables")


def as_expr(value):
    if isinstance(value, Expression):
        return value
    if isinstance(value, list):
        return array(value)
    return literal(value)


class Expression:
    __slots__ = ("dtype", "source", "_ir_json", "_selection_name", "_top_level_name")

    def __init__(self, dtype, source, ir, *, selection_name=None, top_level_name=None):
        object.__setattr__(self, "dtype", dtype)
        object.__setattr__(self, "source", source)
        object.__setattr__(self, "_ir_json", json.dumps(ir, separators=(",", ":")))
        object.__setattr__(self, "_selection_name", selection_name)
        object.__setattr__(self, "_top_level_name", top_level_name)

    def __setattr__(self, name, value):
        raise TypeError("Expressions are immutable")

    @property
    def _ir(self):
        # Return a fresh tree so callers cannot mutate an expression behind its type/source.
        return json.loads(self._ir_json)

    def to_ir(self):
        return self._ir

    def __bool__(self):
        raise TypeError("Table expressions cannot be converted to Python bool")

    def _binary(self, other, operator, result_type=None):
        other = as_expr(other)
        source = _merge_sources(self.source, other.source)
        if operator in ("and", "or") and (self.dtype != tbool or other.dtype != tbool):
            raise TypeError("boolean operators require bool expressions")
        if result_type is None:
            result_type = _numeric_result_type(self.dtype, other.dtype, operator)
        return Expression(
            result_type,
            source,
            {"op": "binary", "operator": operator, "left": self.to_ir(), "right": other.to_ir()},
        )

    def _rbinary(self, other, operator, result_type=None):
        return as_expr(other)._binary(self, operator, result_type)

    def _compare(self, other, operator):
        other = as_expr(other)
        source = _merge_sources(self.source, other.source)
        _check_comparable(self.dtype, other.dtype, operator)
        return Expression(
            tbool,
            source,
            {"op": "binary", "operator": operator, "left": self.to_ir(), "right": other.to_ir()},
        )

    def __add__(self, other): return self._binary(other, "add")
    def __radd__(self, other): return self._rbinary(other, "add")
    def __sub__(self, other): return self._binary(other, "sub")
    def __rsub__(self, other): return self._rbinary(other, "sub")
    def __mul__(self, other): return self._binary(other, "mul")
    def __rmul__(self, other): return self._rbinary(other, "mul")
    def __truediv__(self, other): return self._binary(other, "div")
    def __rtruediv__(self, other): return self._rbinary(other, "div")
    def __floordiv__(self, other): return self._binary(other, "floordiv")
    def __rfloordiv__(self, other): return self._rbinary(other, "floordiv")
    def __mod__(self, other): return self._binary(other, "mod")
    def __rmod__(self, other): return self._rbinary(other, "mod")
    def __pow__(self, other): return self._binary(other, "pow")
    def __rpow__(self, other): return self._rbinary(other, "pow")

    def __eq__(self, other): return self._compare(other, "eq")
    def __ne__(self, other): return self._compare(other, "ne")
    def __lt__(self, other): return self._compare(other, "lt")
    def __le__(self, other): return self._compare(other, "le")
    def __gt__(self, other): return self._compare(other, "gt")
    def __ge__(self, other): return self._compare(other, "ge")

    def __and__(self, other): return self._binary(other, "and", tbool)
    def __rand__(self, other): return self._rbinary(other, "and", tbool)
    def __or__(self, other): return self._binary(other, "or", tbool)
    def __ror__(self, other): return self._rbinary(other, "or", tbool)

    def __neg__(self):
        _require_numeric(self.dtype, "negation")
        return Expression(self.dtype, self.source, {"op": "unary", "operator": "neg", "value": self.to_ir()})

    def __pos__(self):
        _require_numeric(self.dtype, "unary plus")
        return Expression(self.dtype, self.source, {"op": "unary", "operator": "pos", "value": self.to_ir()})

    def __invert__(self):
        if self.dtype != tbool:
            raise TypeError("logical not requires a bool expression")
        return Expression(tbool, self.source, {"op": "unary", "operator": "not", "value": self.to_ir()})

    def __getitem__(self, name):
        if isinstance(self.dtype, tarray):
            if isinstance(name, slice):
                return self._slice(name.start, name.stop, name.step)
            index = as_expr(name)
            if index.dtype != tint32:
                raise TypeError("array index must have int32 type")
            source = _merge_sources(self.source, index.source)
            return Expression(
                self.dtype.element_type,
                source,
                {"op": "array_index", "value": self.to_ir(), "index": index.to_ir()},
            )
        if not isinstance(self.dtype, tstruct):
            raise TypeError("field access requires a struct expression")
        if name not in self.dtype:
            raise KeyError(name)
        return Expression(
            self.dtype[name],
            self.source,
            {"op": "get_field", "value": self.to_ir(), "name": name},
            selection_name=name,
            top_level_name=self._top_level_name,
        )

    def _slice(self, start, stop, step):
        endpoints = []
        for endpoint in (start, stop, step):
            if endpoint is None:
                endpoints.append(None)
                continue
            expr = as_expr(endpoint)
            if expr.dtype != tint32:
                raise TypeError("array slice endpoints and step must have int32 type")
            endpoints.append(expr)
        source = self.source
        for endpoint in endpoints:
            if endpoint is not None:
                source = _merge_sources(source, endpoint.source)
        return Expression(
            self.dtype,
            source,
            {
                "op": "array_slice",
                "value": self.to_ir(),
                "start": None if endpoints[0] is None else endpoints[0].to_ir(),
                "stop": None if endpoints[1] is None else endpoints[1].to_ir(),
                "step": None if endpoints[2] is None else endpoints[2].to_ir(),
            },
        )

    def _array_length(self):
        if not isinstance(self.dtype, tarray):
            raise TypeError("length requires an array expression")
        return Expression(tint32, self.source, {"op": "array_length", "value": self.to_ir()})

    def _reduce(self, operation, filter_missing=True):
        if not isinstance(self.dtype, tarray):
            raise TypeError(f"{operation} requires an array expression")
        if self.dtype.element_type not in (tint32,tint64,tfloat32,tfloat64):
            raise TypeError(f"{operation} requires numeric array elements")
        if type(filter_missing) is not bool:
            raise TypeError("filter_missing must be bool")
        return Expression(
            self.dtype.element_type,
            self.source,
            {
                "op": "array_reduce",
                "operation": operation,
                "filter_missing": filter_missing,
                "value": self.to_ir(),
            },
        )

    def __getattr__(self, name):
        dtype = object.__getattribute__(self, "dtype")
        if isinstance(dtype, tstruct) and name in dtype:
            return self[name]
        if name == "length" and isinstance(dtype, tarray):
            return object.__getattribute__(self, "_array_length")
        raise AttributeError(name)

    def __iter__(self):
        if not isinstance(self.dtype, tstruct):
            raise TypeError("only struct expressions can be expanded as mappings")
        return iter(self.dtype)

    def __len__(self):
        if not isinstance(self.dtype, tstruct):
            raise TypeError("only struct expressions have a length")
        return len(self.dtype)

    def keys(self):
        if not isinstance(self.dtype, tstruct):
            raise TypeError("only struct expressions have keys")
        return self.dtype.keys()

    def items(self):
        if not isinstance(self.dtype, tstruct):
            raise TypeError("only struct expressions have items")
        return ((name, self[name]) for name in self.dtype)

    def collect(self):
        if self.source is None:
            raise ValueError("cannot collect a literal expression without a Table source")
        import json
        if self._ir.get("op") == "column":
            values = json.loads(self.source._native.collect_field_json(self._ir["name"]))
        else:
            projected = self.source._native.project_json(json.dumps([["__vail_collect", self.to_ir()]]))
            values = json.loads(projected.collect_field_json("__vail_collect"))
        return [decode_value(value, self.dtype) for value in values]


class Field(Expression):
    __slots__ = ("name",)

    def __init__(self, source, name, dtype):
        super().__init__(dtype, source, {"op": "column", "name": name}, selection_name=name, top_level_name=name)
        object.__setattr__(self, "name", name)


class StructExpression(Expression, Mapping):
    __slots__ = ("_fields",)

    def __init__(self, fields, source=None, *, ir=None):
        field_exprs = {name: as_expr(value) for name, value in fields.items()}
        for expr in field_exprs.values():
            source = _merge_sources(source, expr.source)
        dtype = tstruct(**{name: expr.dtype for name, expr in field_exprs.items()})
        if ir is None:
            ir = {"op": "struct", "fields": [[name, expr.to_ir()] for name, expr in field_exprs.items()]}
        super().__init__(dtype, source, ir)
        object.__setattr__(self, "_fields", field_exprs)

    def __getitem__(self, name):
        return self._fields[name]

    def __iter__(self):
        return iter(self._fields)

    def __len__(self):
        return len(self._fields)

    def __bool__(self):
        raise TypeError("Table expressions cannot be converted to Python bool")


def column(source, name, dtype):
    return Field(source, name, dtype)


def literal(value, dtype=None):
    if dtype is None:
        dtype = infer_type(value)
    elif not _valid_type(dtype):
        raise TypeError(f"literal dtype must be a Vail type, found {dtype!r}")
    return Expression(dtype, None, {"op": "literal", "dtype": type_to_wire(dtype), "value": encode_value(value)})


def struct(**fields):
    return StructExpression(fields)


def array(collection):
    if isinstance(collection, Expression):
        if not isinstance(collection.dtype, tarray):
            raise TypeError("array expression conversion requires an array expression")
        return collection
    if not isinstance(collection, list):
        raise TypeError("array expects a list or an array expression")
    exprs = [None if value is None else as_expr(value) for value in collection]
    source = None
    for expr in exprs:
        if expr is not None:
            source = _merge_sources(source, expr.source)
    typed = [expr.dtype for expr in exprs if expr is not None]
    if not typed:
        raise ValueError("cannot infer array element type from empty or all-missing values")
    element_type = typed[0]
    for other in typed[1:]:
        element_type = unify_types(element_type, other)
        if element_type is None:
            raise TypeError("array elements have incompatible types")
    normalized = []
    for expr in exprs:
        if expr is None:
            expr = literal(None, element_type)
        if expr.dtype != element_type:
            if _cast_compatible(expr.dtype, element_type):
                expr = cast(expr, element_type)
            else:
                raise TypeError(f"array element has type {expr.dtype}, expected {element_type}")
        normalized.append(expr)
    return Expression(
        tarray(element_type),
        source,
        {
            "op": "array",
            "element_type": type_to_wire(element_type),
            "elements": [element.to_ir() for element in normalized],
        },
    )


def _array_reduce(value, operation, filter_missing=True):
    return as_expr(value)._reduce(operation, filter_missing)


def array_sum(collection, filter_missing=True):
    return _array_reduce(collection, "sum", filter_missing)


def array_product(collection, filter_missing=True):
    return _array_reduce(collection, "product", filter_missing)


def array_min(*values, filter_missing=True):
    if len(values) != 1:
        raise NotImplementedError("array_min supports one array expression")
    return _array_reduce(values[0], "min", filter_missing)


def array_max(*values, filter_missing=True):
    if len(values) != 1:
        raise NotImplementedError("array_max supports one array expression")
    return _array_reduce(values[0], "max", filter_missing)


def cast(value, dtype):
    if not _valid_type(dtype):
        raise TypeError(f"cast target must be a Vail type, found {dtype!r}")
    expr = as_expr(value)
    if expr.dtype == dtype:
        return expr
    if not _cast_compatible(expr.dtype, dtype):
        raise TypeError(f"cannot cast {expr.dtype} to {dtype}")
    return Expression(dtype, expr.source, {"op": "cast", "dtype": type_to_wire(dtype), "value": expr.to_ir()})


def missing(dtype):
    return literal(None, dtype)


def if_else(condition, consequent, alternate):
    condition = as_expr(condition)
    if condition.dtype != tbool:
        raise TypeError("if_else condition must have bool type")
    consequent = as_expr(consequent)
    alternate = as_expr(alternate)
    result_type = unify_types(consequent.dtype, alternate.dtype)
    if result_type is None:
        raise TypeError(f"if_else branches have incompatible types: {consequent.dtype} and {alternate.dtype}")
    source = _merge_sources(condition.source, consequent.source)
    source = _merge_sources(source, alternate.source)
    return Expression(
        result_type,
        source,
        {
            "op": "if_else",
            "condition": condition.to_ir(),
            "consequent": consequent.to_ir(),
            "alternate": alternate.to_ir(),
        },
    )


def is_missing(value):
    expr = as_expr(value)
    return Expression(tbool, expr.source, {"op": "is_missing", "value": expr.to_ir()})


def _valid_type(dtype):
    return dtype in (tint32, tint64, tfloat32, tfloat64, tbool, tstr) or isinstance(dtype, (tstruct, tarray))


def _require_numeric(dtype, operation):
    if dtype not in (tint32, tint64, tfloat32, tfloat64):
        raise TypeError(f"{operation} requires a numeric expression")


def _numeric_result_type(left, right, operator):
    if operator == "pow":
        _require_numeric(left, "exponentiation")
        _require_numeric(right, "exponentiation")
        return tfloat64
    _require_numeric(left, "arithmetic")
    _require_numeric(right, "arithmetic")
    result = unify_types(left, right)
    if result is None:
        raise TypeError(f"incompatible arithmetic types: {left} and {right}")
    if operator == "div":
        if tfloat64 in (left, right):
            return tfloat64
        if tfloat32 in (left, right):
            return tfloat32
        return tfloat64
    return result


def _check_comparable(left, right, operator):
    if left == right:
        if isinstance(left, (tstruct, tarray)) and operator not in ("eq", "ne"):
            raise TypeError(f"{operator} does not support {left}")
        return
    if left in (tint32, tint64, tfloat32, tfloat64) and right in (tint32, tint64, tfloat32, tfloat64):
        return
    raise TypeError(f"incompatible comparison types: {left} and {right}")


def _cast_compatible(source, target):
    primitive = (tint32, tint64, tfloat32, tfloat64, tbool, tstr)
    return source in primitive and target in primitive or source == target


# Hail-shaped typed casts.
def int32(value): return cast(value, tint32)
def int64(value): return cast(value, tint64)
def float32(value): return cast(value, tfloat32)
def float64(value): return cast(value, tfloat64)
def str_(value): return cast(value, tstr)
def bool_(value): return cast(value, tbool)
