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
    tset,
    tdict,
    ttuple,
    tndarray,
    tinterval,
    tlocus,
    tcall,
    Call,
    is_type,
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


class ExpressionException(ValueError):
    """An expression references an incompatible source or index scope."""


def _merge_sources(left, right):
    if left is None:
        return right
    if right is None or left is right:
        return left
    raise ExpressionException("expression source mismatch: fields belong to different Tables")


def as_expr(value):
    if isinstance(value, Expression):
        return value
    if isinstance(value, Mapping):
        return StructExpression(value)
    if isinstance(value, list):
        return array(value)
    if isinstance(value, tuple):
        return tuple_(value)
    return literal(value)


class _IR(dict):
    """Wire IR with native singleton references retained outside its JSON payload."""
    def __init__(self, value, references, lookups=None, row_indexed=False):
        super().__init__(value)
        self.references = dict(references)
        self.lookups = dict(lookups or {})
        self.row_indexed = row_indexed


def _row_indexed(ir):
    """Retain row scope even for expressions such as an empty row struct."""
    if isinstance(ir, _IR) and ir.row_indexed:
        return True
    if isinstance(ir, dict):
        if ir.get("op") == "literal":
            return False
        return ir.get("op") == "column" or any(_row_indexed(value) for value in ir.values())
    if isinstance(ir, list):
        return any(_row_indexed(value) for value in ir)
    return False


def _references(ir):
    result = {}
    if isinstance(ir, _IR):
        result.update(ir.references)
    if isinstance(ir, dict):
        if ir.get("op") != "literal":
            for value in ir.values():
                result.update(_references(value))
    elif isinstance(ir, list):
        for value in ir:
            result.update(_references(value))
    return result


def _global_bindings(expressions):
    bindings = {}
    for expr in expressions:
        bindings.update(expr._global_refs)
    return list(bindings.items())


def _lookup_references(ir):
    result = {}
    if isinstance(ir, _IR):
        result.update(ir.lookups)
    if isinstance(ir, dict) and ir.get("op") != "literal":
        for value in ir.values():
            result.update(_lookup_references(value))
    elif isinstance(ir, list):
        for value in ir:
            result.update(_lookup_references(value))
    return result


def _external_globals(native, dtype):
    from uuid import uuid4
    reference = uuid4().hex
    return Expression(dtype, None, _IR(
        {"op": "external_global", "reference": reference, "dtype": type_to_wire(dtype)},
        {reference: native},
    ))


class Expression:
    __slots__ = ("dtype", "source", "_ir_json", "_selection_name", "_top_level_name", "_global_refs", "_lookup_refs", "_row_indexed")

    def __init__(self, dtype, source, ir, *, selection_name=None, top_level_name=None,
                 row_indexed=None):
        from types import MappingProxyType
        object.__setattr__(self, "_global_refs", MappingProxyType(_references(ir)))
        object.__setattr__(self, "_lookup_refs", MappingProxyType(_lookup_references(ir)))
        object.__setattr__(self, "_row_indexed", _row_indexed(ir) if row_indexed is None else row_indexed)
        object.__setattr__(self, "dtype", dtype)
        object.__setattr__(self, "source", source)
        object.__setattr__(self, "_ir_json", json.dumps(ir, separators=(",", ":")))
        object.__setattr__(self, "_selection_name", selection_name)
        object.__setattr__(self, "_top_level_name", top_level_name)

    # Python mappings identify expression objects; query keys use value equality.
    __hash__ = object.__hash__

    def __setattr__(self, name, value):
        raise TypeError("Expressions are immutable")

    @property
    def _ir(self):
        # Return a fresh tree so callers cannot mutate an expression behind its type/source.
        return _IR(json.loads(self._ir_json), self._global_refs, self._lookup_refs, self._row_indexed)

    def to_ir(self):
        return self._ir

    def __bool__(self):
        if isinstance(self.dtype, ttuple):
            return bool(self.dtype.types)
        raise TypeError("Table expressions cannot be converted to Python bool")

    def _binary(self, other, operator, result_type=None):
        other = as_expr(other)
        source = _merge_sources(self.source, other.source)
        if operator in ("and", "or") and (self.dtype != tbool or other.dtype != tbool):
            raise TypeError("boolean operators require bool expressions")
        if operator in ("add", "sub", "mul", "div", "floordiv", "mod", "pow") and (
            isinstance(self.dtype, tarray) or isinstance(other.dtype, tarray)
        ):
            return _array_arithmetic(self, other, operator)
        if result_type is None:
            result_type = _numeric_result_type(self.dtype, other.dtype, operator)
        return Expression(
            result_type,
            source,
            {"op": "binary", "operator": operator, "left": self.to_ir(), "right": other.to_ir()},
        )

    def _rbinary(self, other, operator, result_type=None):
        other = as_expr(other)
        if operator in ("add", "sub", "mul", "div", "floordiv", "mod", "pow") and (
            isinstance(self.dtype, tarray) or isinstance(other.dtype, tarray)
        ):
            return _array_arithmetic(other, self, operator)
        return other._binary(self, operator, result_type)

    def _compare(self, other, operator):
        other = as_expr(other)
        source = _merge_sources(self.source, other.source)
        _check_comparable(self.dtype, other.dtype, operator)
        return Expression(
            tbool,
            source,
            {"op": "binary", "operator": operator, "left": self.to_ir(), "right": other.to_ir()},
        )

    def __add__(self, other):
        if self.dtype == tstr:
            from .strings import concat
            return concat(self, other)
        return self._binary(other, "add")
    def __radd__(self, other):
        if self.dtype == tstr:
            from .strings import concat
            return concat(other, self)
        return self._rbinary(other, "add")
    def __sub__(self, other):
        if isinstance(self.dtype, tset): return self.difference(other)
        return self._binary(other, "sub")
    def __rsub__(self, other):
        if isinstance(self.dtype, tset): return as_expr(other).difference(self)
        return self._rbinary(other, "sub")
    def __mul__(self, other):
        if self.dtype == tstr:
            from .strings import repeat
            return repeat(self, other)
        if isinstance(other, str) or isinstance(other, Expression) and other.dtype == tstr:
            from .strings import repeat
            return repeat(as_expr(other), self)
        return self._binary(other, "mul")
    def __rmul__(self, other):
        if self.dtype == tstr:
            from .strings import repeat
            return repeat(self, other)
        if isinstance(other, str) or isinstance(other, Expression) and other.dtype == tstr:
            from .strings import repeat
            return repeat(as_expr(other), self)
        return self._rbinary(other, "mul")
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
    def __lt__(self, other):
        if isinstance(self.dtype, tset):
            other = as_expr(other)
            subset = self.is_subset(other) if "lt" in ("lt", "le") else other.is_subset(self)
            return subset & (self != other) if "lt" in ("lt", "gt") else subset
        return self._compare(other, "lt")
    def __le__(self, other):
        if isinstance(self.dtype, tset):
            other = as_expr(other)
            subset = self.is_subset(other) if "le" in ("lt", "le") else other.is_subset(self)
            return subset & (self != other) if "le" in ("lt", "gt") else subset
        return self._compare(other, "le")
    def __gt__(self, other):
        if isinstance(self.dtype, tset):
            other = as_expr(other)
            subset = self.is_subset(other) if "gt" in ("lt", "le") else other.is_subset(self)
            return subset & (self != other) if "gt" in ("lt", "gt") else subset
        return self._compare(other, "gt")
    def __ge__(self, other):
        if isinstance(self.dtype, tset):
            other = as_expr(other)
            subset = self.is_subset(other) if "ge" in ("lt", "le") else other.is_subset(self)
            return subset & (self != other) if "ge" in ("lt", "gt") else subset
        return self._compare(other, "ge")

    def __and__(self, other):
        if isinstance(self.dtype, tset): return self.intersection(other)
        return self._binary(other, "and", tbool)
    def __rand__(self, other):
        if isinstance(self.dtype, tset): return self.intersection(other)
        return self._rbinary(other, "and", tbool)
    def __or__(self, other):
        if isinstance(self.dtype, tset): return self.union(other)
        return self._binary(other, "or", tbool)
    def __ror__(self, other):
        if isinstance(self.dtype, tset): return self.union(other)
        return self._rbinary(other, "or", tbool)

    def __xor__(self, other):
        if not isinstance(self.dtype, tset): return NotImplemented
        other = as_expr(other)
        return self.difference(other).union(other.difference(self))

    def __rxor__(self, other): return self.__xor__(other)

    def __neg__(self):
        if isinstance(self.dtype, tarray):
            _require_numeric(self.dtype.element_type, "negation")
            return self.map(lambda value: -value)
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
        if self.dtype == tstr:
            from .strings import slice_, index
            return slice_(self, name) if isinstance(name, slice) else index(self, name)
        if isinstance(self.dtype, tdict):
            from .collections import contains, get
            key = as_expr(name)
            value = get(self, key)
            error = Expression(
                self.dtype.value_type,
                None,
                {"op": "die", "message": literal("dictionary key is missing").to_ir(),
                 "dtype": type_to_wire(self.dtype.value_type)},
            )
            return if_else(contains(self, key), value, error)
        if isinstance(self.dtype, tset) and isinstance(self.dtype.element_type, tstruct):
            return self.map(lambda item: item[name])
        if isinstance(self.dtype, tinterval) and name in ("start", "end", "includes_start", "includes_end"):
            field_type = tbool if name.startswith("includes_") else self.dtype.point_type
            return Expression(field_type, self.source, {"op": "get_field", "value": self.to_ir(), "name": name})
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
        if isinstance(self.dtype, ttuple):
            if isinstance(name, slice):
                indices = range(len(self.dtype.types))[name]
                sliced = tuple_(self[index] for index in indices)
                return if_else(is_missing(self), missing(sliced.dtype), sliced)
            if not isinstance(name, int):
                raise TypeError("tuple index must be a Python integer")
            index = name
            if not 0 <= index < len(self.dtype.types):
                raise IndexError("tuple index out of range")
            return Expression(self.dtype.types[index], self.source,
                              {"op": "get_field", "value": self.to_ir(), "name": f"__vail_tuple_{index}"})
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

    def annotate(self, **named_exprs):
        """Add or replace fields on a struct expression."""
        if not isinstance(self.dtype, tstruct):
            raise TypeError("annotate requires a struct expression")
        fields = {name: self[name] for name in self.dtype}
        for name, value in named_exprs.items():
            if not isinstance(name, str) or not name:
                raise ValueError("annotated field names must be non-empty strings")
            expr = as_expr(value)
            _merge_sources(self.source, expr.source)
            fields[name] = expr
        return StructExpression(fields, self.source,
                                row_indexed=True if self._row_indexed else None)

    def drop(self, *names):
        """Remove fields from a struct expression."""
        if not isinstance(self.dtype, tstruct):
            raise TypeError("drop requires a struct expression")
        if any(not isinstance(name, str) for name in names):
            raise TypeError("drop field names must be strings")
        missing = set(names) - set(self.dtype)
        if missing:
            raise KeyError(next(iter(missing)))
        removed = set(names)
        return StructExpression({name: self[name] for name in self.dtype if name not in removed},
                                self.source, row_indexed=self._row_indexed)

    def _to_stream(self):
        """Treat an array expression as a lazy stream expression."""
        if not isinstance(self.dtype, tarray):
            raise TypeError("_to_stream requires an array expression")
        from .streams import StreamExpression
        return StreamExpression(self)

    def grouped(self, group_size):
        """Split an array expression into consecutive fixed-size groups."""
        if not isinstance(self.dtype, tarray):
            raise TypeError("grouped requires an array expression")
        size = as_expr(group_size)
        if size.dtype != tint32:
            raise TypeError("group size must have int32 type")
        source = _merge_sources(self.source, size.source)
        return Expression(
            tarray(tarray(self.dtype.element_type)), source,
            {"op": "array_grouped", "value": self.to_ir(), "size": size.to_ir()},
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

    def _array_map(self, f):
        if not callable(f):
            raise TypeError("map requires a callable")
        import uuid
        parameter = uuid.uuid4().hex
        if isinstance(self.dtype, (tarray, tset)):
            element_type = self.dtype.element_type
        elif isinstance(self.dtype, tdict):
            element_type = tstruct(key=self.dtype.key_type, value=self.dtype.value_type)
        else:
            raise TypeError(f"map requires an array, set, or dict expression, got {self.dtype}")
        variable = Expression(element_type, self.source,
                              {"op": "lambda_variable", "name": parameter})
        body = as_expr(f(variable))
        return Expression(tarray(body.dtype), _merge_sources(self.source, body.source),
                          {"op": "array_map", "value": self.to_ir(),
                           "parameter": parameter, "body": body.to_ir()})

    def _array_flatmap(self, f):
        if not callable(f):
            raise TypeError("array.flatmap requires a callable")
        mapped = self._array_map(f)
        if not isinstance(mapped.dtype.element_type, tarray):
            raise TypeError("array.flatmap requires the callback to return an array expression")
        return Expression(mapped.dtype.element_type, mapped.source,
                          {"op": "array_flatten", "value": mapped.to_ir()})

    def _array_filter(self, f):
        if not callable(f):
            raise TypeError("array.filter requires a callable")
        import uuid
        parameter = uuid.uuid4().hex
        element_type = (tstruct(key=self.dtype.key_type, value=self.dtype.value_type)
                        if isinstance(self.dtype, tdict) else self.dtype.element_type)
        variable = Expression(element_type, self.source,
                              {"op": "lambda_variable", "name": parameter})
        predicate = as_expr(f(variable))
        if predicate.dtype != tbool:
            raise TypeError(f"array.filter predicate must be bool, got {predicate.dtype}")
        return Expression(self.dtype, _merge_sources(self.source, predicate.source),
                          {"op": "array_filter", "value": self.to_ir(),
                           "parameter": parameter, "predicate": predicate.to_ir()})

    def _array_length(self):
        if not isinstance(self.dtype, (tarray, tset, tdict)):
            raise TypeError("length requires a collection expression")
        return Expression(tint32, self.source, {"op": "array_length", "value": self.to_ir()})

    def _interval_contains(self, point):
        if not isinstance(self.dtype, tinterval):
            raise TypeError("contains requires an interval expression")
        point = as_expr(point)
        if point.dtype != self.dtype.point_type:
            raise TypeError("interval point type does not match")
        source = _merge_sources(self.source, point.source)
        return Expression(tbool, source, {"op": "interval_contains", "interval": self.to_ir(), "point": point.to_ir()})

    def _interval_overlaps(self, other):
        if not isinstance(self.dtype, tinterval):
            raise TypeError("overlaps requires an interval expression")
        other = as_expr(other)
        if other.dtype != self.dtype:
            raise TypeError("interval types do not match")
        source = _merge_sources(self.source, other.source)
        return Expression(tbool, source, {"op": "interval_overlaps", "left": self.to_ir(), "right": other.to_ir()})

    def _locus_op(self, operation, dtype):
        if not isinstance(self.dtype, tlocus):
            raise TypeError("coordinate operations require a locus expression")
        return Expression(dtype, self.source,
                          {"op": "locus_op", "operation": operation, "value": self.to_ir()})

    def _locus_sequence_context(self, *args, **kwargs):
        raise NotImplementedError("FASTA-backed sequence context is not implemented")

    def _reduce(self, operation, filter_missing=True):
        if isinstance(self.dtype, tset):
            from .collections import reduce
            return reduce(self, operation, filter_missing)
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
        if isinstance(dtype, tlocus):
            if name in ("contig", "position"):
                return self._locus_op(name, tstr if name == "contig" else tint32)
            operations = {"global_position": ("global_position", tint64),
                          "in_autosome": ("is_autosomal", tbool),
                          "in_mito": ("is_mt", tbool),
                          "in_x_par": ("in_x_par", tbool),
                          "in_y_par": ("in_y_par", tbool)}
            if name in operations:
                operation, result_type = operations[name]
                return lambda: self._locus_op(operation, result_type)
            if name == "in_x_nonpar":
                return lambda: self._locus_op("is_x", tbool) & ~self._locus_op("in_x_par", tbool)
            if name == "in_y_nonpar":
                return lambda: self._locus_op("is_y", tbool) & ~self._locus_op("in_y_par", tbool)
            if name == "in_autosome_or_par":
                return lambda: (self._locus_op("is_autosomal", tbool)
                                | self._locus_op("in_x_par", tbool)
                                | self._locus_op("in_y_par", tbool))
            if name == "sequence_context":
                return self._locus_sequence_context
        if isinstance(dtype, tinterval) and name in ("start", "end", "includes_start", "includes_end"):
            return self[name]
        if isinstance(dtype, tstruct) and name in dtype:
            return self[name]
        if dtype == tstr:
            from . import strings as ops
            if name == '_split_line':
                from .text import split_line
                return lambda *args, **kwargs: split_line(self, *args, **kwargs)
            simple = {'lower': 'lower', 'upper': 'upper'}
            if name in simple:
                return lambda: ops._call(simple[name], self)
            search = {'contains': 'contains', 'startswith': 'starts_with', 'endswith': 'ends_with'}
            if name in search:
                return lambda sub: ops._call(search[name], self, ops._text(sub))
            methods = {'length': ops.length, 'strip': ops.strip, 'reverse': ops.reverse,
                       'find': ops.find, 'matches': ops.matches, 'replace': ops.replace,
                       'split': ops.split, 'translate': ops.translate, 'join': ops.join}
            if name in methods:
                return lambda *args, **kwargs: methods[name](self, *args, **kwargs)
            if name == 'first_match_in': return ops.unsupported('Java regex capture groups')
        if isinstance(dtype, (tset, tdict)):
            from . import collections as ops
            if name in ("size", "length"):
                return lambda: ops.length(self)
            if name == "contains":
                return lambda key: ops.contains(self, key)
            if isinstance(dtype, tdict):
                methods = {"get": ops.get, "values": ops.values, "key_set": ops.key_set,
                           "map_values": ops.map_values}
                if name in methods:
                    return lambda *args, **kwargs: methods[name](self, *args, **kwargs)
            else:
                if isinstance(dtype.element_type, tstruct) and name in dtype.element_type:
                    return self.map(lambda item: item[name])
                methods = {"add": ops.add, "remove": ops.remove, "map": ops.set_map,
                           "starmap": ops.starmap, "flatmap": ops.flatmap}
                if name in methods:
                    return lambda *args: methods[name](self, *args)
                if name in ("difference", "intersection", "union", "is_subset"):
                    return lambda other: ops.set_binary(self, other, name)
                if name == "filter": return self._array_filter
                if name == "any": return lambda f: ops.predicate(self, f)
                if name == "all": return lambda f: ~ops.predicate(self, lambda item: ~as_expr(f(item)))
                if name == "find": return lambda f: ops.predicate(self, f, first=True)
                if name in ("fold", "group_by"):
                    return ops.unsupported(name)
        if name == "flatmap" and isinstance(dtype, tarray):
            return object.__getattribute__(self, "_array_flatmap")
        if name == "filter" and isinstance(dtype, tarray):
            return object.__getattribute__(self, "_array_filter")
        if name == "map" and isinstance(dtype, (tarray, tset, tdict)):
            return object.__getattribute__(self, "_array_map")
        if name == "grouped" and isinstance(dtype, tarray):
            return object.__getattribute__(self, "grouped")
        if name == "length" and isinstance(dtype, tarray):
            return object.__getattribute__(self, "_array_length")
        if name == "contains" and isinstance(dtype, tinterval):
            return object.__getattribute__(self, "_interval_contains")
        if name == "overlaps" and isinstance(dtype, tinterval):
            return object.__getattribute__(self, "_interval_overlaps")
        raise AttributeError(name)

    def __contains__(self, item):
        if isinstance(self.dtype, tstruct):
            return item in self.dtype
        raise TypeError("use the contains method on expressions")

    def __iter__(self):
        if isinstance(self.dtype, ttuple):
            return (self[index] for index in range(len(self.dtype.types)))
        if not isinstance(self.dtype, tstruct):
            raise TypeError("only struct expressions can be expanded as mappings")
        return iter(self.dtype)

    def __len__(self):
        if isinstance(self.dtype, ttuple):
            return len(self.dtype.types)
        if not isinstance(self.dtype, tstruct):
            raise TypeError("only struct expressions have a length")
        return len(self.dtype)

    def keys(self):
        if isinstance(self.dtype, tdict):
            from .collections import keys
            return keys(self)
        if not isinstance(self.dtype, tstruct):
            raise TypeError("only struct expressions have keys")
        return self.dtype.keys()

    def items(self):
        if isinstance(self.dtype, tdict):
            from .collections import to_array
            return to_array(self)
        if not isinstance(self.dtype, tstruct):
            raise TypeError("only struct expressions have items")
        return ((name, self[name]) for name in self.dtype)

    def _collect_expression(self, n=None):
        from . import agg, _ir_has_aggregate
        if _ir_has_aggregate(self.to_ir()):
            raise NotImplementedError("cannot collect an aggregated expression")
        if not self._row_indexed:
            if n == 0:
                return literal([], tarray(self.dtype))
            result = array([self])
            if self.source is not None:
                result = self.source.aggregate(result, _localize=False)
            return result
        from uuid import uuid4
        name = "__vail_collect_" + uuid4().hex
        table = self.source.select(**{name: self})
        if n is not None:
            table = table.head(n)
        return table.aggregate(agg.collect(table[name]), _localize=False)

    def collect(self, _localize=True):
        """Collect values, or return an unindexed lazy array expression."""
        if type(_localize) is not bool:
            raise TypeError("_localize must be a bool")
        from . import eval
        result = self._collect_expression()
        return eval(result) if _localize else result

    def take(self, n, _localize=True):
        """Collect the first n values, or return their lazy array expression."""
        from . import _validate_n, eval
        _validate_n(n, "n")
        if type(_localize) is not bool:
            raise TypeError("_localize must be a bool")
        result = self._collect_expression(n)
        return eval(result) if _localize else result


class Field(Expression):
    __slots__ = ("name",)

    def __init__(self, source, name, dtype, *, scope="column"):
        super().__init__(dtype, source, {"op": scope, "name": name}, selection_name=name, top_level_name=name)
        object.__setattr__(self, "name", name)


class StructExpression(Expression, Mapping):
    __slots__ = ("_fields",)

    def __init__(self, fields, source=None, *, ir=None, row_indexed=None):
        field_exprs = {name: as_expr(value) for name, value in fields.items()}
        for expr in field_exprs.values():
            source = _merge_sources(source, expr.source)
        dtype = tstruct(**{name: expr.dtype for name, expr in field_exprs.items()})
        if ir is None:
            ir = {"op": "struct", "fields": [[name, expr.to_ir()] for name, expr in field_exprs.items()]}
        super().__init__(dtype, source, ir, row_indexed=row_indexed)
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
    return Expression(dtype, None, {"op": "literal", "dtype": type_to_wire(dtype), "value": encode_value(value, dtype)})


def struct(**fields):
    return StructExpression(fields)


def interval(start, end, includes_start=True, includes_end=False):
    """Construct an interval expression with explicit inclusive/exclusive bounds."""
    if start is None and end is None:
        raise ValueError("cannot infer interval point type from two missing endpoints")
    if start is None:
        end_expr = as_expr(end)
        start_expr = missing(end_expr.dtype)
    else:
        start_expr = as_expr(start)
    if end is None:
        end_expr = missing(start_expr.dtype)
    elif start is None:
        pass
    else:
        end_expr = as_expr(end)
    if start_expr.dtype != end_expr.dtype:
        raise TypeError(f"interval endpoints must have the same type, found {start_expr.dtype} and {end_expr.dtype}")
    if type(includes_start) is bool:
        includes_start_expr = literal(includes_start, tbool)
    else:
        includes_start_expr = as_expr(includes_start)
    if type(includes_end) is bool:
        includes_end_expr = literal(includes_end, tbool)
    else:
        includes_end_expr = as_expr(includes_end)
    if includes_start_expr.dtype != tbool or includes_end_expr.dtype != tbool:
        raise TypeError("interval inclusivity flags must have bool type")
    dtype = tinterval(start_expr.dtype)
    source = None
    for expr in (start_expr, end_expr, includes_start_expr, includes_end_expr):
        source = _merge_sources(source, expr.source)
    return Expression(dtype, source, {"op": "interval", "start": start_expr.to_ir(), "end": end_expr.to_ir(),
                                     "includes_start": includes_start_expr.to_ir(), "includes_end": includes_end_expr.to_ir()})


def tuple_(iterable):
    """Construct a heterogeneous tuple without evaluating its element expressions."""
    elements = [as_expr(item) for item in iterable]
    source = None
    for element in elements:
        source = _merge_sources(source, element.source)
    return Expression(ttuple(*(element.dtype for element in elements)), source,
                      {"op": "tuple", "elements": [element.to_ir() for element in elements]})


def array(collection):
    if isinstance(collection, Expression):
        from .collections import to_array
        return to_array(collection)
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
    if not values:
        raise ValueError("min requires at least one argument")
    return _array_reduce(values[0] if len(values) == 1 else array(list(values)), "min", filter_missing)


def array_max(*values, filter_missing=True):
    if not values:
        raise ValueError("max requires at least one argument")
    return _array_reduce(values[0] if len(values) == 1 else array(list(values)), "max", filter_missing)


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


class CaseBuilder:
    """Build an ordered, lazy chain of conditional expressions."""

    def __init__(self, missing_false=False):
        if type(missing_false) is not bool:
            raise TypeError("missing_false must be a bool")
        self._missing_false = missing_false
        self._cases = []
        self._ret_type = None

    def _unify_type(self, dtype):
        result = dtype if self._ret_type is None else self._ret_type
        if self._ret_type is not None and self._ret_type != dtype:
            numeric = (tbool, tint32, tint64, tfloat32, tfloat64)
            result = next((ty for ty in reversed(numeric) if ty in (self._ret_type, dtype)), None) if self._ret_type in numeric and dtype in numeric else None
        if result is None:
            raise TypeError(f"'then' expressions must have same type, found '{self._ret_type}' and '{dtype}'")
        self._ret_type = result

    def when(self, condition, then):
        condition, then = as_expr(condition), as_expr(then)
        if condition.dtype != tbool:
            raise TypeError("case condition must have bool type")
        self._unify_type(then.dtype)
        self._cases.append((condition, then))
        return self

    def _finish(self, default):
        result = cast(default, self._ret_type)
        for condition, then in reversed(self._cases):
            if self._missing_false:
                condition = if_else(is_missing(condition), False, condition)
            result = if_else(condition, cast(then, self._ret_type), result)
        return result

    def default(self, then):
        then = as_expr(then)
        if not self._cases:
            return then
        self._unify_type(then.dtype)
        return self._finish(then)

    def or_missing(self):
        if not self._cases:
            raise ExpressionException("'or_missing' cannot be called without at least one 'when' call")
        return self._finish(missing(self._ret_type))

    def or_error(self, message):
        if not self._cases:
            raise ExpressionException("'or_error' cannot be called without at least one 'when' call")
        message = as_expr(message)
        if message.dtype != tstr:
            raise TypeError("case error message must have str type")
        error = Expression(self._ret_type, message.source,
                           {"op": "die", "message": message.to_ir(), "dtype": type_to_wire(self._ret_type)})
        return self._finish(error)


def case(missing_false=False):
    """Start an ordered conditional; missing predicates propagate by default."""
    return CaseBuilder(missing_false)


def is_missing(value):
    expr = as_expr(value)
    return Expression(tbool, expr.source, {"op": "is_missing", "value": expr.to_ir()})


def is_defined(value):
    """Return whether a value is present, including values with missing children."""
    return ~is_missing(value)


def or_missing(predicate, value):
    """Return value when predicate is true; false or missing produces missing."""
    expr = as_expr(value)
    return if_else(predicate, expr, missing(expr.dtype))


def _valid_type(dtype):
    return is_type(dtype)


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


def _array_arithmetic(left, right, operator):
    """Build Hail-style elementwise array arithmetic from ordinary array IR."""
    left_array = isinstance(left.dtype, tarray)
    right_array = isinstance(right.dtype, tarray)
    left_type = left.dtype.element_type if left_array else left.dtype
    right_type = right.dtype.element_type if right_array else right.dtype
    element_type = _numeric_result_type(left_type, right_type, operator)

    if left_array and right_array:
        from . import range as array_range
        indices = array_range(left._array_length())
        pairs = indices.map(lambda i: left[i]._binary(right[i], operator, element_type))
        matches = left._array_length() == right._array_length()
        result_type = tarray(element_type)
        error = Expression(
            result_type,
            None,
            {"op": "die",
             "message": literal("array arithmetic requires arrays of equal length").to_ir(),
             "dtype": type_to_wire(result_type)},
        )
        return if_else(matches, pairs, error)

    array_expr = left if left_array else right
    scalar_expr = right if left_array else left
    if left_array:
        return array_expr.map(lambda value: value._binary(scalar_expr, operator, element_type))
    return array_expr.map(lambda value: scalar_expr._binary(value, operator, element_type))


def _check_comparable(left, right, operator):
    if left == right:
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
def str_(value):
    """Build Hail string conversion IR, preserving string expressions unchanged."""
    expr = as_expr(value)
    if expr.dtype == tstr:
        return expr
    return Expression(tstr, expr.source, {"op": "to_string", "value": expr.to_ir()})
def bool_(value): return cast(value, tbool)


def call(*alleles, phased=False):
    if not any(isinstance(a, Expression) for a in alleles):
        return literal(Call(tuple(alleles), phased))
    if type(phased) is not bool or len(alleles) > 2:
        raise TypeError("call requires at most two alleles and a Boolean phased flag")
    alleles = [as_expr(a) for a in alleles]
    if any(a.dtype != tint32 for a in alleles):
        raise TypeError("call alleles must be int32")
    values = array(alleles)
    if len(alleles) == 2 and not phased:
        values = array([array_min(values), array_max(values)])
    packed = struct(__vail_call_alleles=values, __vail_call_phased=phased)
    result = Expression(tcall, packed.source, {"op": "cast", "dtype": "call", "value": packed.to_ir()})
    missing_allele = is_missing(alleles[0])
    for allele in alleles[1:]:
        missing_allele = missing_allele | is_missing(allele)
    return if_else(missing_allele, missing(tcall), result)


def locus(contig, pos, reference_genome="default"):
    """Construct a lazy, validated one-based coordinate in a reference."""
    from .reference_genome import resolve_reference
    reference = resolve_reference(reference_genome)
    contig, pos = as_expr(contig), as_expr(pos)
    if contig.dtype != tstr or pos.dtype != tint32:
        raise TypeError("locus requires a string contig and int32 position")
    return Expression(tlocus(reference), _merge_sources(contig.source, pos.source),
                      {"op": "locus", "reference": reference.to_wire(),
                       "contig": contig.to_ir(), "position": pos.to_ir()})


def parse_locus(value, reference_genome="default"):
    from .reference_genome import resolve_reference
    reference = resolve_reference(reference_genome)
    value = as_expr(value)
    if value.dtype != tstr:
        raise TypeError("parse_locus requires a string expression")
    return Expression(tlocus(reference), value.source,
                      {"op": "parse_locus", "reference": reference.to_wire(), "value": value.to_ir()})


def is_valid_locus(contig, position, reference_genome="default"):
    from .reference_genome import resolve_reference
    reference = resolve_reference(reference_genome)
    contig, position = as_expr(contig), as_expr(position)
    if contig.dtype != tstr or position.dtype != tint32:
        raise TypeError("is_valid_locus requires a string contig and int32 position")
    return Expression(tbool, _merge_sources(contig.source, position.source),
                      {"op": "locus_valid", "reference": reference.to_wire(),
                       "contig": contig.to_ir(), "position": position.to_ir()})


def is_valid_contig(contig, reference_genome="default"):
    return is_valid_locus(contig, 1, reference_genome)


def locus_from_global_position(global_pos, reference_genome="default"):
    from .reference_genome import resolve_reference
    reference = resolve_reference(reference_genome)
    global_pos = as_expr(global_pos)
    if global_pos.dtype not in (tint32, tint64):
        raise TypeError("global position must be an integer expression")
    if global_pos.dtype == tint32:
        global_pos = int64(global_pos)
    return Expression(tlocus(reference), global_pos.source,
                      {"op": "locus_from_global_position", "reference": reference.to_wire(),
                       "position": global_pos.to_ir()})


def get_sequence(*args, **kwargs):
    raise NotImplementedError("FASTA-backed sequence lookup is not implemented")


def liftover(*args, **kwargs):
    raise NotImplementedError("liftover is not implemented")

