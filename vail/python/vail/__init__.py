"""Experimental Hail-shaped Table frontend over Vail's native Rust execution.

This initial slice supports primitive constructors, keys, ordering and collection.
It is not yet a replacement for the full Hail frontend; unsupported operations fail
explicitly. The implementation never imports Hail, Spark or Java.
"""
from __future__ import annotations

import builtins
from collections.abc import Mapping, Sequence
import json as _json
import os
import re

from ._native import NativeSession
from .expressions import (
    Expression,
    ExpressionException,
    _external_globals,
    _global_bindings,
    Field as _Field,
    StructExpression as _StructExpression,
    array,
    tuple_ as tuple,
    array_max as max,
    array_min as min,
    array_product as product,
    array_sum as sum,
    as_expr,
    bool_ as _bool_cast,
    cast,
    case,
    float32,
    float64,
    if_else,
    int32,
    int64,
    is_missing,
    is_defined,
    or_missing,
    literal,
    locus,
    parse_locus,
    is_valid_locus,
    is_valid_contig,
    locus_from_global_position,
    get_sequence,
    liftover,
    missing,
    str_ as str,
    struct,
    call,
    interval,
)
from .types import (
    Struct,
    decode_value,
    dtype,
    encode_value,
    infer_type,
    tarray,
    tset,
    tdict,
    ttuple,
    tndarray,
    tcall,
    Call,
    Interval,
    tinterval,
    tlocus,
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
from .reference_genome import (ReferenceGenome, Locus, GRCh37, GRCh38, get_reference,
                               default_reference, reference_genomes, resolve_reference)
from . import agg
from . import scan
from .text import import_lines, import_table, read_csv, read_json
from .lookups import index as _index_table, resolve as _resolve_lookups
from .collections import set_ as set, dict_ as dict, empty_set, empty_array, empty_dict, length as len
from .strings import delimit, reverse_complement, unsupported as _unsupported_string
parse_int32 = _unsupported_string('Java int32 parsing')
parse_int64 = _unsupported_string('Java int64 parsing')
parse_float32 = _unsupported_string('Java float32 parsing')
parse_float64 = _unsupported_string('Java float64 parsing')
parse_int = parse_int32
parse_float = parse_float64
hamming = _unsupported_string('hamming')
entropy = _unsupported_string('entropy')
format = _unsupported_string('Java printf formatting')
json = _unsupported_string('Hail typed JSON serialization')
parse_json = _unsupported_string('Hail typed JSON parsing')
from .streams import StreamExpression, stream_range as _stream_range


def range(start, stop=None, step=1):
    """Return a lazy int32 array from start (inclusive) to stop (exclusive).

    A single argument specifies stop. All arguments may be int32 expressions;
    negative steps count down and missing arguments produce a missing array.
    """
    return _stream_range(start, stop, step).to_array()


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
    if value < builtins.int(positive):
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
    elif isinstance(dtype, tlocus):
        if not isinstance(value, Locus) or value.reference_genome != dtype.reference_genome:
            raise TypeError(f"field {name!r} requires a locus of reference {dtype.reference_genome}")
    elif isinstance(dtype, tinterval):
        if not isinstance(value, Interval) or value.point_type != dtype.point_type:
            raise TypeError(f"field {name!r} requires an interval with point type {dtype.point_type}")
    elif isinstance(dtype, ttuple):
        dtype._validate_value(value)
        for index, (item, item_type) in enumerate(zip(value, dtype.types)):
            _check_value(item, item_type, f"{name}[{index}]")
    elif isinstance(dtype, tdict):
        if not isinstance(value, Mapping):
            raise TypeError(f"field {name!r} requires {dtype}")
        for key, item in value.items():
            _check_value(key, dtype.key_type, f"{name}.key")
            _check_value(item, dtype.value_type, f"{name}[{key!r}]")
    elif isinstance(dtype, tarray):
        if not isinstance(value, (list, builtins.tuple)):
            raise TypeError(f"field {name!r} requires {dtype}")
        for item in value:
            _check_value(item, dtype.element_type, name)
    elif isinstance(dtype, tset):
        if not isinstance(value, (builtins.set, frozenset)):
            raise TypeError(f"field {name!r} requires {dtype}")
        for item in value:
            _check_value(item, dtype.element_type, name)
    elif isinstance(dtype, tstruct):
        if not isinstance(value, Mapping):
            raise TypeError(f"field {name!r} requires {dtype}")
        if builtins.set(value) != builtins.set(dtype):
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


def _schema_from_native(native):
    return tstruct(**{
        name: type_from_wire(field_type)
        for name, field_type in _json.loads(native.schema_json())
    })


def _keys_from_native(native):
    return builtins.tuple(native.key_names())


class Table:
    """A lazy native Table for the explicitly supported initial operation subset."""

    def __init__(self, native, schema, keys=()):
        object.__setattr__(self, "_native", native)
        object.__setattr__(self, "_schema", schema)
        object.__setattr__(self, "_keys", builtins.tuple(keys))
        object.__setattr__(self, "_global_schema", tstruct(**{
            name: type_from_wire(ty) for name, ty in _json.loads(native.global_schema_json())
        }))

    def __setattr__(self, name, value):
        raise TypeError("Table is immutable")

    def __delattr__(self, name):
        raise TypeError("Table is immutable")

    def _unlocalize_entries(self, entries_field_name, cols_field_name, col_key):
        from .matrixtable import unlocalize_entries
        return unlocalize_entries(self, entries_field_name, cols_field_name, col_key)

    def to_matrix_table(self, row_key, col_key, row_fields=(), col_fields=(), n_partitions=None):
        from .matrixtable import to_matrix_table
        return to_matrix_table(self, row_key, col_key, row_fields, col_fields, n_partitions)

    def to_matrix_table_row_major(self, columns, entry_field_name=None, col_field_name='col'):
        from .matrixtable import to_matrix_table_row_major
        return to_matrix_table_row_major(self, columns, entry_field_name, col_field_name)

    @classmethod
    def parallelize(cls, rows, schema=None, key=None, n_partitions=None, *, partial_type=None, globals=None):
        if schema is not None and partial_type is not None:
            raise ValueError("define either schema or partial type, not both.")
        if partial_type is not None:
            raise NotImplementedError("partial_type is not implemented in this initial slice")
        from .inputs import contains_expression, infer_input_type, input_expression
        if isinstance(rows, Expression):
            return cls._parallelize_expression(rows, schema, key, n_partitions, globals)
        from .arrow import as_ipc, dtype_from_arrow, arrow_type, same_arrow_layout
        try:
            import pyarrow as _pa
        except ImportError:
            _pa = None
        if _pa is not None and isinstance(rows, (_pa.Table, _pa.RecordBatch)):
            arrow_table = _pa.Table.from_batches([rows]) if isinstance(rows, _pa.RecordBatch) else rows
            if schema is None:
                schema = tstruct(**{field.name: dtype_from_arrow(field.type) for field in arrow_table.schema})
            elif isinstance(schema, builtins.str):
                schema = dtype(schema)
            if not isinstance(schema, tstruct):
                raise TypeError("schema must be a vail.tstruct")
            if list(schema) != arrow_table.column_names:
                raise ValueError("Arrow column names must match schema fields and order")
            # An explicit logical schema is authoritative. Validate physical
            # layouts rather than guessing tuples from ordinary struct names.
            for field in arrow_table.schema:
                dtype_from_arrow(field.type)  # validate any advertised metadata
                if not same_arrow_layout(field.type, arrow_type(schema[field.name])):
                    raise TypeError("Arrow field types must match the supplied schema")
            if schema:
                arrow_table = arrow_table.cast(_pa.schema([_pa.field(name, arrow_type(ty)) for name, ty in schema.items()]))
            n_partitions = 1 if n_partitions is None else n_partitions
            _validate_n(n_partitions, "n_partitions", positive=True)
            native = _native_session().parallelize_arrow(as_ipc(arrow_table), n_partitions)
            native_schema = _schema_from_native(native)
            if native_schema != schema:
                raise TypeError("Arrow field types must match the supplied schema")
            table = cls(native, native_schema)
            if globals is not None:
                table = table.annotate_globals(**globals)
            if key is not None:
                table = table.key_by(*([key] if isinstance(key, builtins.str) else key))
            return table
        rows = list(rows)
        if contains_expression(rows):
            if isinstance(schema, builtins.str):
                schema = dtype(schema)
            if schema is None:
                schema = infer_input_type(rows)
            if not isinstance(schema, tstruct):
                raise TypeError("schema must be a vail.tstruct")
            expression = input_expression(rows, tarray(schema))
            return cls._parallelize_expression(expression, schema, key, n_partitions, globals)
        if any(not isinstance(row, Mapping) for row in rows):
            raise TypeError("rows must be dictionaries or Struct values")
        if schema is None:
            if not rows:
                raise ValueError("provide schema for an empty table")
            names = list(rows[0])
            if any(builtins.set(row) != builtins.set(names) for row in rows):
                raise ValueError("all rows must have the same fields")
            schema = tstruct(**{name: _infer_type([row[name] for row in rows]) for name in names})
        if isinstance(schema, builtins.str):
            schema = dtype(schema)
        if not isinstance(schema, tstruct):
            raise TypeError("schema must be a vail.tstruct")
        names = list(schema)
        for row in rows:
            if builtins.set(row) != builtins.set(names):
                raise ValueError("row fields must match schema")
            for name in names:
                _check_value(row[name], schema[name], name)
        n_partitions = 1 if n_partitions is None else n_partitions
        _validate_n(n_partitions, "n_partitions")
        from .arrow import rows_to_ipc
        native = _native_session().parallelize_arrow(rows_to_ipc(rows, schema), n_partitions)
        table = cls(native, schema)
        if globals is not None:
            table = table.annotate_globals(**globals)
        if key is not None:
            table = table.key_by(*( [key] if isinstance(key, builtins.str) else key ))
        return table

    @classmethod
    def _parallelize_expression(cls, rows, schema, key, n_partitions, globals):
        from .inputs import bind_input
        if not isinstance(rows.dtype, tarray) or not isinstance(rows.dtype.element_type, tstruct):
            raise TypeError("parallelize expects an array expression with struct elements")
        if isinstance(schema, builtins.str):
            schema = dtype(schema)
        if schema is not None and schema != rows.dtype.element_type:
            raise TypeError("expression row type must match the supplied schema")
        rows = bind_input(rows)
        n_partitions = 1 if n_partitions is None else n_partitions
        _validate_n(n_partitions, "n_partitions", positive=True)
        native = _native_session().parallelize_expression(
            _json.dumps(rows.to_ir()), n_partitions, references=_global_bindings([rows]))
        table = cls(native, rows.dtype.element_type)
        if globals is not None:
            table = table.annotate_globals(**globals)
        if key is not None:
            table = table.key_by(*([key] if isinstance(key, builtins.str) else key))
        return table

    def __getitem__(self, item):
        if not isinstance(item, builtins.str):
            return self.index(*(item if isinstance(item, builtins.tuple) else (item,)))
        if item in self._schema:
            return _Field(self, item, self._schema[item])
        return _Field(self, item, self._global_schema[item], scope="global")

    def index(self, *expressions, all_matches=False, query_interval=False):
        """Look up one nullable non-key row using expressions from another table."""
        return _index_table(self, expressions, all_matches, query_interval)

    def __getattr__(self, name):
        schema = self.__dict__.get("_schema")
        if schema is not None and (name in schema or name in self._global_schema):
            return self[name]
        raise AttributeError(name)

    def __iter__(self):
        raise TypeError("Table is not iterable; use collect()")

    @property
    def key(self):
        return _StructExpression({name: self[name] for name in self._keys}, self, row_indexed=True)

    @property
    def row(self):
        return _StructExpression({name: self[name] for name in self._schema}, self, row_indexed=True)

    @property
    def row_value(self):
        return _StructExpression(
            {name: self[name] for name in self._schema if name not in self._keys}, self,
            row_indexed=True,
        )

    @property
    def globals(self):
        return _StructExpression({name: self._global_field(name) for name in self._global_schema}, self)

    def _global_field(self, name):
        if name not in self._global_schema:
            raise ValueError(f"global field {name!r} is not present")
        return _Field(self, name, self._global_schema[name], scope="global")

    def index_globals(self):
        """Return a lazy, unindexed reference to this table's globals."""
        return _external_globals(self._native.globals_ref(), self._global_schema)

    def _project_globals(self, fields):
        for name, expr in fields:
            if not isinstance(name, builtins.str) or not name:
                raise ValueError("global field names must be non-empty strings")
            if expr.source is not None and expr.source is not self:
                raise ExpressionException("expression source mismatch: global belongs to a different Table")
            if _referenced_columns(expr.to_ir()):
                raise ExpressionException("global expressions cannot reference table rows")
        native = self._native.project_globals_json(_json.dumps([[name, expr.to_ir()] for name, expr in fields]),
                                                   _global_bindings(expr for _, expr in fields))
        return Table(native, self._schema, self._keys)

    def annotate_globals(self, **named_exprs):
        expressions = {name: as_expr(value) for name, value in named_exprs.items()}
        fields = [(name, expressions.pop(name, self._global_field(name))) for name in self._global_schema]
        return self._project_globals(fields + list(expressions.items()))

    def select_globals(self, *exprs, **named_exprs):
        fields = []
        for value in exprs:
            expr = self._global_field(value) if isinstance(value, builtins.str) else value
            if not isinstance(expr, Expression) or expr._selection_name is None:
                raise ValueError("positional selection requires a global field reference")
            fields.append((expr._selection_name, expr))
        fields.extend((name, as_expr(value)) for name, value in named_exprs.items())
        return self._project_globals(fields)

    def transmute_globals(self, **named_exprs):
        expressions = {name: as_expr(value) for name, value in named_exprs.items()}
        referenced = builtins.set()
        for expr in expressions.values():
            referenced.update(_referenced_columns(expr.to_ir(), "global"))
        fields = [(name, expressions.pop(name, self._global_field(name))) for name in self._global_schema
                  if name not in referenced or name in expressions]
        return self._project_globals(fields + list(expressions.items()))

    def _field_name(self, field):
        if isinstance(field, Expression):
            if field.source is not self:
                raise ExpressionException("expression source mismatch: field belongs to a different Table")
            if field._selection_name is None:
                raise ValueError("positional selection requires a source field reference")
            return field._selection_name
        if not isinstance(field, builtins.str):
            raise NotImplementedError("only field references are supported in this initial slice")
        if field not in self._schema:
            raise KeyError(field)
        return field

    def write(self, output, overwrite=False, stage_locally=False, _codec_spec=None):
        """Atomically publish a local Viceberg snapshot; retain old generations on overwrite."""
        if not isinstance(overwrite, builtins.bool) or not isinstance(stage_locally, builtins.bool):
            raise TypeError("overwrite and stage_locally must be bool")
        if stage_locally or _codec_spec is not None:
            raise NotImplementedError("staging and custom codecs are not supported")
        self._native.write(os.fspath(output), overwrite)

    def export(self, output, types_file=None, header=True, parallel=None, delimiter='\t'):
        from .text import export
        return export(self, output, types_file, header, parallel, delimiter)

    def checkpoint(self, output, overwrite=False, stage_locally=False,
                   _codec_spec=None, _read_if_exists=False, _intervals=None,
                   _filter_intervals=False):
        """Write a Viceberg snapshot and return a lazy read of that snapshot."""
        for name, value in (("overwrite", overwrite), ("stage_locally", stage_locally),
                            ("_read_if_exists", _read_if_exists),
                            ("_filter_intervals", _filter_intervals)):
            if type(value) is not builtins.bool:
                raise TypeError(f"{name} must be bool")
        if (stage_locally or _codec_spec is not None or _read_if_exists
                or _intervals is not None or _filter_intervals):
            raise NotImplementedError("checkpoint currently supports only output and overwrite")
        self.write(output, overwrite=overwrite)
        return read_table(output)

    def export_csv(self, output, *, delimiter=',', quote='"', header=True):
        from .text import export_csv
        return export_csv(self, output, delimiter=delimiter, quote=quote, header=header)

    def export_json(self, output):
        from .text import export_json
        return export_json(self, output)

    def _project(self, projections, keys=None):
        projections = list(projections)
        native, expressions = _resolve_lookups(self._native, [expr for _, expr in projections])
        projections = [(name, expr) for (name, _), expr in zip(projections, expressions)]
        projections_json = _json.dumps([[name, expr.to_ir()] for name, expr in projections])
        native = native.project_json(projections_json, _global_bindings(expressions))
        schema = {
            name: field_type
            for name, field_type in (
                (field_name, type_from_wire(field_type))
                for field_name, field_type in _json.loads(native.schema_json())
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
                raise ExpressionException("expression source mismatch: field belongs to a different Table")
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
                raise ExpressionException("expression source mismatch: field belongs to a different Table")
            named.append((name, expr))
        names = list(self._keys) + [name for name, _ in positional] + [name for name, _ in named]
        if builtins.len(builtins.set(names)) != builtins.len(names):
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
                raise ExpressionException("expression source mismatch: field belongs to a different Table")
            expressions[name] = expr

        referenced = builtins.set()
        for expr in expressions.values():
            referenced.update(_referenced_columns(expr.to_ir()))
        drop = referenced - builtins.set(expressions) - builtins.set(self._keys)
        projections = [
            (name, expressions.get(name, self[name]))
            for name in self._schema
            if name not in drop
        ]
        present = {name for name, _ in projections}
        projections.extend((name, expr) for name, expr in expressions.items() if name not in present)
        return self._project(projections, keys=self._keys)

    def drop(self, *fields):
        """Return a lazy projection with the named row or global fields removed."""
        row_drop, global_drop = builtins.set(), builtins.set()
        for field in fields:
            if isinstance(field, Expression):
                ir = field.to_ir()
                if (field.source is not self or field._selection_name is None
                        or ir.get("op") not in ("column", "global")
                        or ir.get("name") != field._selection_name):
                    raise ExpressionException("drop expects top-level fields from this Table")
                name = field._selection_name
            elif isinstance(field, builtins.str):
                name = field
            else:
                raise TypeError("drop expects field names or top-level field references")
            if name in self._schema:
                if name in self._keys:
                    raise ValueError(f"cannot drop key field {name!r}")
                row_drop.add(name)
            elif name in self._global_schema:
                global_drop.add(name)
            else:
                raise IndexError(f"table has no field {name!r}")
        result = self._project([(name, self[name]) for name in self._schema if name not in row_drop])
        if global_drop:
            result = result._project_globals(
                [(name, result[name]) for name in result._global_schema if name not in global_drop]
            )
        return result

    def rename(self, mapping):
        """Rename row and global fields with lazy DataFusion projections."""
        if not isinstance(mapping, Mapping):
            raise TypeError("mapping must be a mapping")
        all_names = builtins.set(self._schema) | builtins.set(self._global_schema)
        if builtins.set(mapping) - all_names:
            raise ValueError(f"found rename rules for fields not present in table: {list(builtins.set(mapping) - all_names)}")
        if any(not isinstance(old, builtins.str) or not isinstance(new, builtins.str) or not new for old, new in mapping.items()):
            raise TypeError("rename field names must be non-empty strings")
        for scope in (self._schema, self._global_schema):
            names = [mapping.get(name, name) for name in scope]
            if builtins.len(names) != builtins.len(builtins.set(names)):
                raise ValueError("Cannot rename two fields to the same name")
            for old, new in mapping.items():
                if old in scope and new in scope and new != old and new not in mapping:
                    raise ValueError(f"Cannot rename {old!r} to {new!r}: field already exists")
        keys = builtins.tuple(mapping.get(name, name) for name in self._keys)
        if not mapping:
            return self
        renamed_row_names = {mapping.get(name, name) for name in self._schema}
        renamed_global_names = {mapping.get(name, name) for name in self._global_schema}
        if renamed_row_names & renamed_global_names:
            raise ValueError("renamed row and global fields must have distinct names")
        source = self._native.key_by([]) if self._keys else self._native
        unkeyed = Table(source, self._schema, ())
        row = unkeyed._project([(mapping.get(name, name), unkeyed[name]) for name in self._schema], keys=())
        if keys:
            row = Table(row._native.key_by(keys), row._schema, keys)
        return row._project_globals(
            [(mapping.get(name, name), row[name]) for name in row._global_schema]
        )

    def flatten(self):
        """Project nested row structs to dotted top-level names and remove keys."""
        table = Table(self._native.key_by([]), self._schema, ()) if self._keys else self
        projections = []
        def visit(prefix, expr):
            if isinstance(expr.dtype, tstruct):
                for field_name in expr.dtype:
                    visit(f"{prefix}.{field_name}" if prefix else field_name, expr[field_name])
            else:
                projections.append((prefix, expr))
        for name in table._schema:
            visit(name, table[name])
        return table._project(projections, keys=())

    def expand_types(self):
        """Expand supported composite types using lazy expression projections."""
        from .types import ttuple
        table = Table(self._native.key_by([]), self._schema, ()) if self._keys else self
        def expand(expr):
            ty = expr.dtype
            if isinstance(ty, tstruct):
                return struct(**{name: expand(expr[name]) for name in ty})
            if isinstance(ty, ttuple):
                return struct(**{f"_{i}": expand(expr[i]) for i in builtins.range(builtins.len(ty.types))})
            if isinstance(ty, tlocus):
                return struct(contig=expr.contig, position=expr.position)
            if isinstance(ty, tinterval):
                return struct(start=expr.start, end=expr.end,
                              includesStart=expr.includes_start, includesEnd=expr.includes_end)
            if isinstance(ty, tarray):
                return expr.map(expand)
            if isinstance(ty, tset):
                return array(expr).map(expand)
            if isinstance(ty, tdict):
                return expr.map(lambda entry: struct(
                    key=expand(entry.key), value=expand(entry.value)
                ))
            if ty == tcall:
                return struct(
                    alleles=Expression(tarray(tint32), expr.source,
                                       {"op": "get_field", "value": expr.to_ir(), "name": "__vail_call_alleles"}),
                    phased=Expression(tbool, expr.source,
                                      {"op": "get_field", "value": expr.to_ir(), "name": "__vail_call_phased"}),
                )
            if isinstance(ty, tndarray):
                data = Expression(tarray(ty.element_type), expr.source,
                                  {"op": "get_field", "value": expr.to_ir(), "name": "__vail_ndarray_data"})
                shape = Expression(tarray(tint64), expr.source,
                                   {"op": "get_field", "value": expr.to_ir(), "name": "__vail_ndarray_shape"})
                return struct(shape=shape, data=expand(data))
            return expr
        rows = table._project([(name, expand(table[name])) for name in table._schema], keys=())
        return rows._project_globals(
            [(name, expand(rows[name])) for name in rows._global_schema]
        )

    def key_by(self, *keys, **named_keys):
        names = []
        computed = {}
        for key in keys:
            expr = self[key] if isinstance(key, builtins.str) else as_expr(key)
            if expr.source is not self:
                raise ExpressionException("key fields must belong to this Table")
            if expr._selection_name is None or not _referenced_columns(expr.to_ir()):
                raise ExpressionException("key_by requires keyword arguments for complex expressions")
            name = expr._selection_name
            names.append(name)
            if name not in self._schema or expr.to_ir() != self[name].to_ir():
                computed[name] = expr
        for name, value in named_keys.items():
            if not isinstance(name, builtins.str) or not name:
                raise ValueError("key field names must be non-empty strings")
            expr = as_expr(value)
            if expr.source is not None and expr.source is not self:
                raise ExpressionException("expression source mismatch: field belongs to a different Table")
            names.append(name)
            computed[name] = expr
        if builtins.len(builtins.set(names)) != builtins.len(names):
            raise ValueError("duplicate key fields")
        table = self
        if computed:
            # Unkey before replacing a key field. All projections reference the
            # original row, so simultaneous assignments cannot observe each other.
            table = Table(self._native.key_by([]), self._schema)
            projections = [(name, computed[name] if name in computed else self[name])
                           for name in self._schema]
            projections.extend((name, expr) for name, expr in computed.items()
                               if name not in self._schema)
            table = table._project(projections)
        return Table(table._native.key_by(names), table._schema, names)

    def group_by(self, *keys, **named_keys):
        """Create a lazy grouped aggregation builder."""
        key_fields = []
        for key in keys:
            name = self._field_name(key)
            key_fields.append((name, self[name]))
        for name, value in named_keys.items():
            if not isinstance(name, builtins.str) or not name:
                raise ValueError("grouping field names must be non-empty strings")
            key_fields.append((name, as_expr(value)))
        names = [name for name, _ in key_fields]
        if builtins.len(builtins.set(names)) != builtins.len(names):
            raise ValueError("duplicate grouping field names")
        for _, expr in key_fields:
            if expr.source is not None and expr.source is not self:
                raise ExpressionException("expression source mismatch: field belongs to a different Table")
            ir = expr.to_ir()
            if _ir_has_aggregate(ir):
                raise ExpressionException("grouping expressions cannot contain aggregations")
        return GroupedTable(self, key_fields)

    def order_by(self, *exprs):
        keys = []
        computed = {}
        occupied = builtins.set(self._schema) | builtins.set(self._global_schema)
        for value in exprs:
            sort = value if isinstance(value, _Sort) else asc(value)
            expr = self[sort.col] if isinstance(sort.col, builtins.str) else as_expr(sort.col)
            if expr.source is not self:
                raise ValueError("expression source mismatch: sort expressions must belong to this Table")
            if not _referenced_columns(expr.to_ir()):
                raise ValueError("sort expressions must be row-indexed")
            if expr.to_ir().get("op") == "column":
                name = expr.to_ir()["name"]
            else:
                index = builtins.len(computed)
                name = f"__vail_sort_{index}"
                while name in occupied:
                    index += 1
                    name = f"__vail_sort_{index}"
                occupied.add(name)
                computed[name] = expr
            keys.append((name, sort.descending))
        table = self.annotate(**computed) if computed else self
        ordered = Table(table._native.order_by(keys), table._schema)
        if computed:
            ordered = ordered._project([(name, ordered[name]) for name in self._schema])
        return ordered

    def filter(self, expr, keep=True):
        if type(keep) is not builtins.bool:
            raise TypeError("keep must be a bool")
        expr = as_expr(expr)
        if expr.source is not None and expr.source is not self:
            raise ExpressionException("expression source mismatch: field belongs to a different Table")
        if expr.dtype != tbool:
            raise TypeError(f"filter predicate must be bool, got {expr.dtype}")
        native, expressions = _resolve_lookups(self._native, [expr])
        expression = expressions[0]
        native = native.filter_json(_json.dumps(expression.to_ir()), keep, _global_bindings(expressions))
        if expr._lookup_refs:
            native = native.project_json(_json.dumps([[name, {"op": "column", "name": name}]
                                                      for name in self._schema]))
        return Table(native, self._schema, self._keys)

    def explode(self, field, name=None):
        """Replace an array or set field with one row per element."""
        expr = self[field] if isinstance(field, builtins.str) else field
        if not isinstance(expr, Expression) or expr.source is not self:
            raise ExpressionException("explode requires a field of this Table")
        if not isinstance(expr.dtype, (tarray, tset)):
            raise ValueError(f"explode expects array or set, found {expr.dtype}")
        path = []
        node = expr.to_ir()
        while node.get("op") == "get_field":
            path.append(node["name"])
            node = node["value"]
        if node.get("op") != "column":
            raise ExpressionException("explode requires a field or nested field")
        path.append(node["name"])
        path.reverse()
        if path[0] in self._keys:
            raise ValueError("cannot explode a key field")
        if name is not None and builtins.len(path) != 1:
            raise ValueError("explode does not support name for nested fields")
        native = self._native.explode(path)
        result = Table(native, tstruct(**{
            field_name: type_from_wire(field_type)
            for field_name, field_type in _json.loads(native.schema_json())
        }), self._keys)
        return result if name is None else result.rename({path[0]: name})

    def head(self, n):
        _validate_n(n, "n")
        return Table(self._native.head(n), self._schema, self._keys)

    def tail(self, n):
        _validate_n(n, "n")
        return Table(self._native.tail(n), self._schema, self._keys)

    def union(self, *others, unify=False):
        if type(unify) is not builtins.bool:
            raise TypeError("unify must be a bool")
        result = self
        for other in others:
            if not isinstance(other, Table):
                raise TypeError("union arguments must be Tables")
            if result._keys != other._keys:
                raise ValueError("union tables must have identical key names and types")
            if any(result._schema.get(key) != other._schema.get(key) for key in result._keys):
                raise ValueError("union tables must have identical key names and types")
            fields = builtins.dict(result._schema.items())
            if unify:
                from .types import unify_types
                for name, other_type in other._schema.items():
                    if name in fields and name not in result._keys:
                        promoted = unify_types(fields[name], other_type)
                        if promoted is None:
                            raise TypeError(f"incompatible union field {name!r}: {fields[name]} and {other_type}")
                        fields[name] = promoted
                    else:
                        fields.setdefault(name, other_type)
            elif result._schema != other._schema:
                raise ValueError("union tables must have identical row schemas when unify=False")
            if unify:
                fields = {name: fields[name] for name in result._keys} | {
                    name: dtype for name, dtype in fields.items() if name not in result._keys
                }
            native = result._native.union(other._native, unify)
            result = Table(native, tstruct(**fields), result._keys)
        return result

    def join(self, right, how="inner", _mangle=None, _join_key=None):
        """Lazily join rows with matching key prefixes."""
        if not isinstance(right, Table):
            raise TypeError("right must be a Table")
        if how not in ("inner", "left", "right", "outer"):
            raise ValueError("how must be one of 'inner', 'left', 'right', or 'outer'")
        if _join_key is None:
            join_key = builtins.max(builtins.len(self._keys), builtins.len(right._keys))
        else:
            if isinstance(_join_key, builtins.bool) or not isinstance(_join_key, builtins.int):
                raise TypeError("_join_key must be an integer or None")
            if _join_key < 0:
                raise ValueError("_join_key must be non-negative")
            join_key = _join_key

        left_types = [self._schema[name] for name in self._keys[:join_key]]
        right_types = [right._schema[name] for name in right._keys[:join_key]]
        if left_types != right_types:
            raise ValueError(
                "'join': key mismatch:\n"
                f"  left:  [{', '.join(map(builtins.str, left_types))}]\n"
                f"  right: [{', '.join(map(builtins.str, right_types))}]"
            )

        # Hail exposes row and global names in one field namespace. Rename
        # colliding right fields in schema order with the usual `_1`, `_2` rule.
        occupied = builtins.set(self._schema) | builtins.set(self._global_schema)
        right_fields = list(right._global_schema)
        matched_right_keys = builtins.set(right._keys[:join_key])
        right_fields.extend(name for name in right._schema if name not in matched_right_keys)
        mangle = _mangle if _mangle is not None else (lambda name, index: f"{name}_{index}")
        if not callable(mangle):
            raise TypeError("_mangle must be callable")
        renames = {}
        for name in right_fields:
            candidate = name
            index = 0
            while candidate in occupied:
                index += 1
                if index > 100:
                    raise RecursionError(f"cannot deduplicate {name!r} after 100 attempts")
                candidate = mangle(name, index)
                if not isinstance(candidate, builtins.str) or not candidate:
                    raise TypeError("_mangle must return non-empty strings")
            occupied.add(candidate)
            if candidate != name:
                renames[name] = candidate
        if renames:
            right = right.rename(renames)

        native = self._native.join(right._native, how, join_key)
        return Table(native, _schema_from_native(native), _keys_from_native(native))

    def semi_join(self, other):
        """Keep left rows whose key prefix occurs in ``other``."""
        return self._key_filter_join(other, anti=False)

    def anti_join(self, other):
        """Keep left rows whose key prefix does not occur in ``other``."""
        return self._key_filter_join(other, anti=True)

    def _key_filter_join(self, other, *, anti):
        method = "anti_join" if anti else "semi_join"
        if not isinstance(other, Table):
            raise TypeError("other must be a Table")
        if not other._keys:
            raise ValueError(f"{method}: cannot join with a table with no key")
        right_types = [other._schema[name] for name in other._keys]
        left_types = [self._schema[name] for name in self._keys[:builtins.len(other._keys)]]
        if builtins.len(other._keys) > builtins.len(self._keys) or left_types != right_types:
            left_types = [self._schema[name] for name in self._keys]
            raise ValueError(
                f"{method}: cannot join: table must have a key of the same type(s) "
                "and be the same length or shorter:\n"
                f"   Left key: {', '.join(map(builtins.str, left_types))}\n"
                f"  Right key: {', '.join(map(builtins.str, right_types))}"
            )
        native = self._native.join(other._native, "anti" if anti else "semi", builtins.len(other._keys))
        return Table(native, _schema_from_native(native), _keys_from_native(native))

    @staticmethod
    def multi_way_zip_join(tables, data_field_name, global_field_name):
        """Lazily zip rows from tables with identical types and keys."""
        if not isinstance(tables, Sequence) or isinstance(tables, (builtins.str, bytes)):
            raise TypeError("tables must be a sequence of Tables")
        tables = list(tables)
        if not tables:
            raise ValueError("multi_way_zip_join must have at least one table as an argument")
        if any(not isinstance(table, Table) for table in tables):
            raise TypeError("all inputs to multi_way_zip_join must be Tables")
        if not isinstance(data_field_name, builtins.str) or not data_field_name:
            raise TypeError("data_field_name must be a non-empty string")
        if not isinstance(global_field_name, builtins.str) or not global_field_name:
            raise TypeError("global_field_name must be a non-empty string")
        head = tables[0]
        key_types = [(name, head._schema[name]) for name in head._keys]
        if any([(name, table._schema[name]) for name in table._keys] != key_types for table in tables[1:]):
            raise TypeError("All input tables to multi_way_zip_join must have the same key type")
        if any(table._schema != head._schema for table in tables[1:]):
            raise TypeError("All input tables to multi_way_zip_join must have the same row type")
        if any(table._global_schema != head._global_schema for table in tables[1:]):
            raise TypeError("All input tables to multi_way_zip_join must have the same global type")
        native = _native_session().multi_way_zip_join(
            [table._native for table in tables], data_field_name, global_field_name
        )
        return Table(native, _schema_from_native(native), _keys_from_native(native))

    def distinct(self):
        """Return one representative row per key."""
        return Table(self._native.distinct(), self._schema, self._keys)

    def take(self, n, _localize=True):
        return self.head(n).collect(_localize)

    def show(self, n=None, width=None, truncate=None, types=True, handler=None, n_rows=None):
        """Display a bounded prefix, with optional types and a string handler."""
        if n is not None and n_rows is not None:
            raise ValueError(f"specify one of n_rows or n, received {n_rows} and {n}")
        n = n_rows if n_rows is not None else n
        for name, value in (("n", n), ("width", width), ("truncate", truncate)):
            if value is not None and type(value) is not builtins.int:
                raise TypeError(f"{name} must be an int or None")
        if type(types) is not builtins.bool:
            raise TypeError("types must be a bool")
        if handler is not None and not callable(handler):
            raise TypeError("handler must be callable")
        from .display import TableDisplay

        text = builtins.str(TableDisplay(self, n, width, truncate, types))
        return (print if handler is None else handler)(text)

    def collect(self, _localize=True):
        if type(_localize) is not builtins.bool:
            raise TypeError("_localize must be a bool")
        if not _localize:
            return self.aggregate(agg.collect(self.row), _localize=False)
        from .arrow import decode_rows
        return decode_rows(self._native.collect_arrow(), self._schema)

    @classmethod
    def from_pandas(cls, df, key=()):
        """Build a lazy Arrow source from a pandas DataFrame, excluding its index."""
        import pandas as pd
        import pyarrow as pa
        if not isinstance(df, pd.DataFrame):
            raise TypeError("df must be a pandas.DataFrame")
        if not df.columns.is_unique or builtins.any(not isinstance(name, builtins.str) for name in df.columns):
            raise ValueError("DataFrame columns must be unique strings")
        from .arrow import arrow_type, _physical
        columns = []
        for name in df.columns:
            series = df[name]
            # NumPy floating NaNs are values; nullable pandas dtypes carry a
            # separate validity mask. Preserve that distinction in Arrow.
            if pd.api.types.is_float_dtype(series.dtype):
                numpy_type = getattr(series.dtype, "numpy_dtype", series.dtype)
                extension = isinstance(series.dtype, pd.api.extensions.ExtensionDtype)
                values = series.to_numpy(dtype=numpy_type, na_value=0) if extension else series.to_numpy()
                mask = series.isna().to_numpy() if extension else None
                columns.append(pa.array(values, mask=mask, from_pandas=False))
            elif (pd.api.types.is_integer_dtype(series.dtype)
                  and not isinstance(series.dtype, pd.api.extensions.ExtensionDtype)
                  and not pd.api.types.is_unsigned_integer_dtype(series.dtype)):
                values = series.to_numpy()
                if not builtins.len(values) or (values.min() >= -(2**31) and values.max() < 2**31):
                    columns.append(pa.array(values, type=pa.int32(), from_pandas=False))
                else:
                    columns.append(pa.array(values, from_pandas=False))
            elif pd.api.types.is_object_dtype(series.dtype):
                values = [None if value is pd.NA else value for value in series.tolist()]
                try:
                    present = [value for value in values if value is not None]
                    if (present and builtins.all(isinstance(value, Mapping) and not isinstance(value, Struct) for value in present)
                            and not (builtins.all(builtins.set(value) == builtins.set(present[0]) for value in present)
                                    and builtins.all(item is not None for value in present for item in value.values()))):
                        ty = tdict(_infer_type([key for value in present for key in value]),
                                   _infer_type([item for value in present for item in value.values()]))
                    else:
                        ty = _infer_type(values)
                except NotImplementedError as exc:
                    raise ExpressionException("cannot impute array elements with incompatible types") from exc
                columns.append(pa.array([_physical(value, ty) for value in values], type=arrow_type(ty)))
            else:
                columns.append(pa.Array.from_pandas(series))
        if columns:
            arrow_table = pa.Table.from_arrays(columns, names=list(df.columns))
        else:
            arrow_table = pa.Table.from_struct_array(pa.array([{}] * builtins.len(df), type=pa.struct([])))
        return cls.parallelize(arrow_table, key=key)

    def to_pandas(self, flatten=True, types=None):
        """Execute through Arrow and convert to Hail-compatible pandas dtypes."""
        import pandas as pd
        import pyarrow as pa
        from .arrow import read_ipc, _logical
        if type(flatten) is not builtins.bool:
            raise TypeError("flatten must be a bool")
        if types is not None and not isinstance(types, Mapping):
            raise TypeError("types must be a mapping")
        table = self.flatten() if flatten else self
        _, arrow_table = read_ipc(table._native.collect_arrow())
        nullable = {pa.int32(): pd.Int32Dtype(), pa.int64(): pd.Int64Dtype(),
                    pa.float32(): pd.Float32Dtype(), pa.float64(): pd.Float64Dtype(),
                    pa.bool_(): pd.BooleanDtype(), pa.string(): pd.StringDtype()}
        frame = arrow_table.to_pandas(types_mapper=nullable.get)
        # pandas Arrow conversion masks valid NaNs; construct its floating
        # extension array from Arrow values and validity separately.
        for name, ty in table._schema.items():
            if ty in (tfloat32, tfloat64):
                column = arrow_table[name].combine_chunks()
                values = column.fill_null(0).to_numpy(zero_copy_only=False)
                mask = column.is_null().to_numpy(zero_copy_only=False)
                frame[name] = pd.arrays.FloatingArray(values, mask)
        overrides = {} if types is None else types
        for name, ty in table._schema.items():
            if ty not in (tint32, tint64, tfloat32, tfloat64, tbool, tstr):
                frame[name] = pd.Series([_logical(value, ty) for value in arrow_table[name].to_pylist()], dtype=object)
            requested = overrides.get(name)
            if requested is None:
                requested = next((value for key, value in overrides.items() if key == ty), None)
            if requested is not None:
                frame[name] = frame[name].astype(requested)
        return frame

    def aggregate(self, expr, _localize=True):
        """Aggregate row values into one scalar expression or localized value."""
        if type(_localize) is not builtins.bool:
            raise TypeError("_localize must be a bool")
        expr = as_expr(expr)
        if expr.source is not None and expr.source is not self:
            raise ExpressionException("expression source mismatch: field belongs to a different Table")
        ir = expr.to_ir()
        _validate_aggregate_ir(ir)
        native, expressions = _resolve_lookups(self._native, [expr])
        native = native.aggregate_globals_json(
            _json.dumps(expressions[0].to_ir()), _global_bindings(expressions))
        result = _external_globals(native, tstruct(value=expr.dtype))["value"]
        if not _localize:
            return result
        return eval(result)

    def all(self, expr):
        """Evaluate whether every row satisfies a boolean expression."""
        return self.aggregate(agg.all(expr))

    def any(self, expr):
        """Evaluate whether at least one row satisfies a boolean expression."""
        return self.aggregate(agg.any(expr))

    def collect_by_key(self, name='values'):
        """Collect non-key row fields into an array for each distinct key."""
        if not isinstance(name, builtins.str):
            raise TypeError("collect_by_key name must be str")
        if not self._keys:
            raise ValueError("collect_by_key requires a keyed table")
        return self.group_by(*self._keys).aggregate(**{name: agg.collect(self.row_value)})

    def _same(self, other, tolerance=1e-6, absolute=False, reorder_fields=False):
        """Compare schemas, keys and rows using Hail's value similarity rules.

        This is an action over both lazy tables. Duplicate-key values retain
        their sequence and multiplicity; field reordering applies only at the
        top level, for both row fields and globals.
        """
        if not isinstance(other, Table):
            raise TypeError("other must be a Table")
        if tolerance is not None and not isinstance(tolerance, (builtins.int, builtins.float)):
            raise TypeError("tolerance must be numeric or None")
        if type(absolute) is not builtins.bool or type(reorder_fields) is not builtins.bool:
            raise TypeError("absolute and reorder_fields must be bool")
        same, diagnostic = self._native.same(other._native, tolerance, absolute, reorder_fields)
        if diagnostic is not None:
            print(diagnostic)
        return same

    def count(self):
        return self._native.count()

    def _force_count(self):
        return self._native.force_count()

    def n_partitions(self):
        return self._native.n_partitions()

    def _calculate_new_partitions(self, n_partitions):
        """Compute key range bounds for a later interval read (an action)."""
        _validate_n(n_partitions, "n_partitions")
        if not self._keys:
            return []
        point_type = tstruct(**{name: self._schema[name] for name in self._keys})
        values = _json.loads(self._native.calculate_new_partitions_json(n_partitions))
        return [decode_value(value, tinterval(point_type)) for value in values]

    def repartition(self, n, shuffle=True):
        """Return a lazy table with a requested number of partitions."""
        _validate_n(n, "n", positive=True)
        if type(shuffle) is not builtins.bool:
            raise TypeError("shuffle must be a bool")
        strategy = "shuffle" if shuffle else "coalesce"
        return Table(self._native.repartition(n, strategy), self._schema, self._keys)

    def naive_coalesce(self, max_partitions):
        """Lazily combine adjacent partitions, up to ``max_partitions``."""
        _validate_n(max_partitions, "max_partitions", positive=True)
        return Table(self._native.repartition(max_partitions, "naive_coalesce"),
                     self._schema, self._keys)

    def _filter_partitions(self, parts, keep=True):
        """Keep or discard source partitions by zero-based index."""
        if not isinstance(parts, Sequence) or isinstance(parts, (builtins.str, bytes, bytearray)):
            raise TypeError("parts must be a sequence of integers")
        parts = list(parts)
        if any(isinstance(part, builtins.bool) or not isinstance(part, builtins.int) for part in parts):
            raise TypeError("parts must contain integers")
        if type(keep) is not builtins.bool:
            raise TypeError("keep must be a bool")
        return Table(self._native.filter_partitions(parts, keep), self._schema, self._keys)

    def _group_within_partitions(self, name, n):
        """Group consecutive rows within each source partition."""
        if not isinstance(name, builtins.str):
            raise TypeError("name must be a string")
        if not name:
            raise ValueError("name must be a non-empty string")
        if name in self._keys:
            raise ExpressionException(f"Cannot select and assign {name!r} in the same 'select' call")
        _validate_n(n, "n", positive=True)
        native = self._native.group_within_partitions(name, n)
        schema = tstruct(**{
            field_name: type_from_wire(field_type)
            for field_name, field_type in _json.loads(native.schema_json())
        })
        return Table(native, schema, self._keys)

    def _map_partitions(self, f):
        """Build a lazy transformation over each table partition's row stream."""
        if not callable(f):
            raise TypeError("Table._map_partitions requires a callable")
        from uuid import uuid4

        rows_name = "__vail_partition_rows_" + uuid4().hex
        rows = Expression(tarray(self._schema), self, {"op": "column", "name": rows_name})._to_stream()
        result = f(rows)
        if not isinstance(result, StreamExpression):
            raise TypeError("Table._map_partitions callback must return a stream expression")
        body = result.to_array()
        if not isinstance(body.dtype, tarray) or not isinstance(body.dtype.element_type, tstruct):
            raise TypeError("Table._map_partitions callback must return a stream of structs")
        if body.source is not None and body.source is not self:
            raise ExpressionException("expression source mismatch: partition stream belongs to a different Table")
        result_schema = body.dtype.element_type
        for key in self._keys:
            if key not in result_schema:
                raise ValueError("Table._map_partitions must preserve key fields")
            if result_schema[key] != self._schema[key]:
                raise ValueError(f"Table._map_partitions must preserve key field type for {key!r}")
        native = self._native.map_partitions_json(
            rows_name, _json.dumps(body.to_ir()), _global_bindings([body])
        )
        schema = tstruct(**{
            name: type_from_wire(dtype) for name, dtype in _json.loads(native.schema_json())
        })
        return Table(native, schema, self._keys)


def range_table(n, n_partitions=None):
    _validate_n(n, "n")
    if n > 2**31 - 1:
        raise ValueError("range_table supports n up to 2**31 - 1")
    n_partitions = 1 if n_partitions is None else n_partitions
    _validate_n(n_partitions, "n_partitions", positive=True)
    return Table(_native_session().range_table(n, n_partitions), tstruct(idx=tint32), ("idx",))


from . import utils  # noqa: E402


def _referenced_columns(node, scope="column"):
    if isinstance(node, builtins.dict):
        # Literal payloads are user data, even if they contain keys named op/name.
        if node.get("op") == "literal":
            return builtins.set()
        result = {node["name"]} if node.get("op") == scope else builtins.set()
        for value in node.values():
            result.update(_referenced_columns(value, scope))
        return result
    if isinstance(node, list):
        result = builtins.set()
        for value in node:
            result.update(_referenced_columns(value, scope))
        return result
    return builtins.set()


def _validate_aggregate_ir(node):
    """Reject row references that are not inside an aggregate scope."""
    def has_aggregate(value):
        if isinstance(value, builtins.dict):
            if value.get("op") == "literal":
                return False
            if value.get("op") in ("aggregate", "agg_filter", "agg_explode", "agg_group_by", "agg_array", "aggregate_fold"):
                return True
            return any(has_aggregate(child) for child in value.values())
        if isinstance(value, list):
            return any(has_aggregate(child) for child in value)
        return False

    def visit(value, row_allowed=False):
        if isinstance(value, builtins.dict):
            op = value.get("op")
            if op == "literal":
                return
            if op == "column":
                if not row_allowed:
                    raise ExpressionException("row fields in Table.aggregate must be inside an aggregate")
                return
            if op == "aggregate_fold":
                if has_aggregate(value["seq"]) or has_aggregate(value["combine"]):
                    raise ExpressionException("nested aggregations are not supported inside fold")
                visit(value["initial"], False)
                visit(value["seq"], True)
                visit(value["combine"], False)
                return
            if op == "aggregate":
                if (value.get("options") or {}).get("__vail_scan"):
                    raise ExpressionException("scan expressions cannot be used in table aggregation")
                operand = value.get("value")
                if operand is not None and has_aggregate(operand):
                    raise ExpressionException("nested aggregations are not supported")
                if operand is not None:
                    visit(operand, True)
                return
            if op == "agg_filter":
                visit(value["predicate"], True)
                visit(value["body"], row_allowed)
                return
            if op == "agg_group_by":
                visit(value["key"], True)
                visit(value["body"], row_allowed)
                return
            if op in ("agg_explode", "agg_array"):
                visit(value["value"], True)
                visit(value["body"], row_allowed)
                return
            for child in value.values():
                visit(child, row_allowed)
        elif isinstance(value, list):
            for child in value:
                visit(child, row_allowed)

    visit(node)


def _ir_has_aggregate(node):
    if isinstance(node, builtins.dict):
        if node.get("op") == "literal":
            return False
        if node.get("op") == "aggregate":
            return not (node.get("options") or {}).get("__vail_scan", False)
        if node.get("op") == "aggregate_fold":
            return True
        if node.get("op") in ("agg_filter", "agg_explode", "agg_group_by", "agg_array"):
            def has_scan(value):
                if isinstance(value, builtins.dict):
                    if value.get("op") == "literal":
                        return False
                    if value.get("op") == "aggregate" and (value.get("options") or {}).get("__vail_scan"):
                        return True
                    return any(has_scan(child) for child in value.values())
                return isinstance(value, list) and any(has_scan(child) for child in value)
            if not has_scan(node):
                return True
        return any(_ir_has_aggregate(child) for child in node.values())
    if isinstance(node, list):
        return any(_ir_has_aggregate(child) for child in node)
    return False


class GroupedTable:
    """Deferred grouping keys; aggregation produces a native lazy Table."""

    __slots__ = ("_table", "_keys", "_npartitions")

    def __init__(self, table, keys):
        self._table = table
        self._keys = builtins.tuple(keys)
        self._npartitions = None

    def partition_hint(self, n):
        """Set the desired partition count for the grouped aggregation."""
        _validate_n(n, "n", positive=True)
        self._npartitions = n
        return self

    def aggregate(self, **fields):
        table = self._table
        keys = list(self._keys)
        names = [name for name, _ in keys]
        for name in [name for name, _ in keys] + list(fields):
            if name in table._global_schema:
                raise ExpressionException(f"grouped field {name!r} collides with globals")
        for name, value in fields.items():
            if not isinstance(name, builtins.str) or not name:
                raise ValueError("aggregate field names must be non-empty strings")
            if name in names:
                raise ValueError(f"aggregate field {name!r} collides with a grouping field")
            names.append(name)
            expr = as_expr(value)
            if expr.source is not None and expr.source is not table:
                raise ExpressionException("expression source mismatch: field belongs to a different Table")
            _validate_aggregate_ir(expr.to_ir())
        expressions = {name: as_expr(value) for name, value in fields.items()}
        native, resolved = _resolve_lookups(table._native, [expr for _, expr in keys] + list(expressions.values()))
        resolved_keys, resolved_fields = resolved[:builtins.len(keys)], resolved[builtins.len(keys):]
        key_json = _json.dumps([[name, expr.to_ir()] for (name, _), expr in zip(keys, resolved_keys)])
        fields_json = _json.dumps([[name, expr.to_ir()] for name, expr in zip(expressions, resolved_fields)])
        native = native.grouped_aggregate_json(key_json, fields_json, _global_bindings(resolved))
        if self._npartitions is not None:
            native = native.repartition(self._npartitions, "shuffle")
        schema = tstruct(**{name: type_from_wire(dtype)
                            for name, dtype in _json.loads(native.schema_json())})
        return Table(native, schema, [name for name, _ in keys])


def filter_intervals(ds, intervals, keep=True):
    """Filter keyed rows by a union of intervals, preserving key order."""
    if not isinstance(ds, Table):
        raise TypeError("filter_intervals requires a Table")
    if type(keep) is not builtins.bool:
        raise TypeError("keep must be a bool")
    if not ds._keys:
        raise ValueError("filter_intervals requires a keyed table")
    declared_point_type = None
    if isinstance(intervals, Expression):
        if not isinstance(intervals.dtype, tarray) or not isinstance(intervals.dtype.element_type, tinterval):
            raise TypeError("intervals must be an array of intervals")
        declared_point_type = intervals.dtype.element_type.point_type
        intervals = eval(intervals)
        if intervals is None:
            raise TypeError("intervals cannot be a missing array")
    elif isinstance(intervals, (list, builtins.tuple)) and any(
            isinstance(value, Expression) for value in intervals):
        intervals = eval(array(list(intervals)))
        if intervals is None:
            raise TypeError("intervals cannot be a missing array")
    if not isinstance(intervals, (list, builtins.tuple)):
        raise TypeError("intervals must be a list, tuple, or unindexed array expression")
    intervals = list(intervals)
    if any(not isinstance(value, Interval) for value in intervals):
        raise TypeError("intervals cannot contain missing or non-Interval values")
    if not intervals and declared_point_type is None:
        return ds if not keep else ds.filter(False)
    point_type = intervals[0].point_type if intervals else declared_point_type
    if any(value.point_type != point_type for value in intervals):
        raise TypeError("all intervals must have the same point type")
    first = ds._keys[0]
    if point_type == ds._schema[first]:
        point_type = tstruct(**{first: point_type})
        intervals = [Interval(Struct(**{first: value.start}), Struct(**{first: value.end}),
                              value.includes_start, value.includes_end, point_type)
                     for value in intervals]
    if not isinstance(point_type, tstruct) or builtins.len(point_type) == 0 or list(point_type.items()) != [
            (name, ds._schema[name]) for name in ds._keys[:builtins.len(point_type)]]:
        raise TypeError("interval point type must match the first key or a non-empty key prefix")
    if not intervals:
        return ds if not keep else ds.filter(False)
    if keep and ds._native.is_read_source():
        from .arrow import rows_to_ipc
        native = ds._native.filter_intervals_arrow(
            rows_to_ipc([{"interval": value} for value in intervals],
                        tstruct(interval=tinterval(point_type))), True)
        return Table(native, ds._schema, ds._keys)
    from .inputs import arrow_literal
    point = struct(**{name: ds[name] for name in point_type})
    predicate = arrow_literal(intervals[0], tinterval(point_type)).contains(point)
    for interval in intervals[1:]:
        predicate = predicate | arrow_literal(interval, tinterval(point_type)).contains(point)
    return ds.filter(predicate, keep=keep)


def read_table(path, *, _intervals=None, _filter_intervals=False, _n_partitions=None,
               _assert_type=None, _load_refs=True, _create_row_uids=False):
    """Read a local Viceberg snapshot in certified file order, without sorting."""
    if _assert_type is not None:
        raise NotImplementedError("read_table _assert_type is not yet supported")
    if type(_load_refs) is not builtins.bool:
        raise TypeError("_load_refs must be bool")
    if not _load_refs:
        raise NotImplementedError("read_table _load_refs=False is not yet supported")
    if type(_create_row_uids) is not builtins.bool:
        raise TypeError("_create_row_uids must be bool")
    # Row UIDs are private Hail IR metadata and are not part of Vail's public
    # row schema, so accepting this compatibility flag cannot change values.
    if _n_partitions:
        if _intervals is not None:
            raise ValueError("read_table does not support both _intervals and _n_partitions")
        _validate_n(_n_partitions, "_n_partitions")
    if type(_filter_intervals) is not builtins.bool:
        raise TypeError("_filter_intervals must be bool")
    if _intervals is None and _filter_intervals:
        raise ValueError("_filter_intervals requires _intervals")
    native, keys = _native_session().read_table(os.fspath(path))
    schema = tstruct(**{name: type_from_wire(dtype) for name, dtype in _json.loads(native.schema_json())})
    if _n_partitions:
        if not keys:
            raise ValueError("_n_partitions requires a keyed table")
        source = Table(native, schema, builtins.tuple(keys))
        intervals = source._calculate_new_partitions(_n_partitions)
        return read_table(path, _intervals=intervals, _filter_intervals=False,
                          _assert_type=_assert_type, _load_refs=_load_refs,
                          _create_row_uids=_create_row_uids)
    if _intervals is not None:
        if not isinstance(_intervals, (list, builtins.tuple)):
            raise TypeError("_intervals must be a list or tuple of intervals")
        if not keys:
            raise ValueError("_intervals requires a keyed table")
        intervals = list(_intervals)
        if intervals:
            if any(not isinstance(value, Interval) for value in intervals):
                raise TypeError("_intervals must contain Interval values")
            point_type = intervals[0].point_type
            if any(value.point_type != point_type for value in intervals):
                raise TypeError("all intervals must have the same point type")
            if isinstance(point_type, tstruct):
                point_fields = list(point_type.items())
                if not point_fields or builtins.len(point_fields) > builtins.len(keys):
                    raise TypeError("interval point struct must be a non-empty prefix of the table key")
                for (field_name, field_type), key_name in zip(point_fields, keys):
                    if field_name != key_name or field_type != schema[key_name]:
                        raise TypeError("interval point struct must match a prefix of the table key")
            elif builtins.len(keys) != 1 or point_type != schema[keys[0]]:
                raise TypeError("interval point type must match the table key or a key prefix struct")
        else:
            # Empty intervals still use the key type to produce a correctly typed
            # empty read. Struct keys support prefix intervals; the full key is the
            # least surprising typed empty representation.
            point_type = tstruct(**{name: schema[name] for name in keys})
        from .arrow import rows_to_ipc
        native = native.filter_intervals_arrow(
            rows_to_ipc([{"interval": value} for value in intervals],
                        tstruct(interval=tinterval(point_type))),
            _filter_intervals,
        )
    return Table(native, schema, builtins.tuple(keys))


def eval(expression):
    """Evaluate an unindexed expression, including a table's globals."""
    expr = as_expr(expression)
    if isinstance(expr.source, MatrixTable):
        from .matrixtable import _axes
        if _axes(expr.to_ir()):
            raise ValueError("eval cannot evaluate indexed matrix expressions")
        matrix = expr.source
        expr = matrix._lower(expr, matrix.globals_table())
    if _referenced_columns(expr.to_ir()):
        raise ValueError("eval cannot evaluate row-indexed expressions")
    table = expr.source if expr.source is not None else utils.range_table(0)
    from .arrow import decode_rows
    data = table._native.eval_global_arrow(_json.dumps(expr.to_ir()), _global_bindings([expr]))
    return decode_rows(data, tstruct(value=expr.dtype))[0].value

def eval_typed(expression):
    """Evaluate an unindexed expression and return its value and Vail type.

    Python values are converted to expressions using the same rules as
    :func:`eval`. Expressions may refer to Table or MatrixTable globals.
    """
    expr = as_expr(expression)
    return eval(expr), expr.dtype

from . import nd

_stream_range = _stream_range

int = int32
float = float64
bool = _bool_cast

from .collections import sorted_ as sorted
from .queries import query_table, query_matrix_table_rows

from .expressions import bind
from .matrixtable import MatrixTable, GroupedMatrixTable, range_matrix_table, read_matrix_table
utils.range_matrix_table = range_matrix_table
from .vcf import import_vcf, get_vcf_metadata
from . import expr
from .expressions import coalesce, approx_equal
from .backend import current_backend
from .vcf_export import export_vcf
from .variant import split_multi_hts
from . import genetics
