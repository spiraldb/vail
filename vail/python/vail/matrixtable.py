"""Lazy, typed MatrixTable plans lowered to native TableIR relations.

Rows and columns have independent identities; entries contain only present cells.
All transformations build native DAGs. Python never evaluates cell expressions.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import cached_property
import json
from collections.abc import Mapping
from uuid import uuid4

from . import (
    Table,
    ExpressionException,
    as_expr,
    struct,
    literal,
    missing,
    if_else,
    is_defined,
    int64,
    range_table,
    _validate_n,
    _schema_from_native,
)
from .expressions import Expression, _IR, _external_globals
from .types import tstruct, tarray, tint32, tint64, tbool

_CID = "__vail_matrix_col_id"
_ROW, _COL, _ENTRY = "__vail_matrix_row", "__vail_matrix_col", "__vail_matrix_entry"
_CELLS = "__vail_matrix_cells"
_VALUE, _PRESENT = "value", "present"
_AGG_OPS = {
    "aggregate",
    "aggregate_fold",
    "agg_filter",
    "agg_explode",
    "agg_group_by",
    "agg_array",
}


def _has_aggregate(node):
    if isinstance(node, dict):
        if node.get("op") == "literal":
            return False
        if node.get("op") == "aggregate":
            return not _is_scan(node)
        if node.get("op") == "aggregate_fold":
            return True
        return any(_has_aggregate(v) for v in node.values())
    if isinstance(node, list):
        return any(_has_aggregate(v) for v in node)
    return False


def _axes(node, *, outside_aggregates=False):
    if isinstance(node, dict):
        if node.get("op") == "literal":
            return set()
        if outside_aggregates and node.get("op") in _AGG_OPS and _has_aggregate(node):
            return set()
        if node.get("op") in ("matrix_field", "matrix_scope"):
            return (
                set()
                if node["axis"] == "global"
                else ({"row", "col"} if node["axis"] == "entry" else {node["axis"]})
            )
        return set().union(
            *(_axes(v, outside_aggregates=outside_aggregates) for v in node.values())
        )
    if isinstance(node, list):
        return set().union(
            *(_axes(v, outside_aggregates=outside_aggregates) for v in node)
        )
    return set()


def _is_scan(node):
    return node.get("op") == "aggregate" and (node.get("options") or {}).get(
        "__vail_scan", False
    )


def _expression(dtype, source, node, original=None):
    return Expression(
        dtype,
        source,
        _IR(
            node,
            {} if original is None else original._global_refs,
            {} if original is None else original._lookup_refs,
        ),
        row_indexed=bool(_axes(node)) if original is None else original._row_indexed,
    )


def _project(table, fields):
    # Native projection validates keys independently of Python metadata.
    return table.key_by()._project(fields, keys=())


def _matrix_path(value):
    import os
    from pathlib import Path
    from urllib.parse import unquote, urlsplit

    path = os.fspath(value)
    if "://" in path:
        uri = urlsplit(path)
        if uri.scheme != "file" or uri.netloc not in ("", "localhost"):
            raise NotImplementedError("matrix snapshots currently require local paths")
        path = unquote(uri.path)
    return Path(path)


_COLUMNS = "__vail_matrix_columns"


@dataclass(frozen=True, init=False)
class MatrixIR:
    """Global column metadata plus one owning row/entry relation."""

    core: Table
    row_key: tuple[str, ...]
    col_key: tuple[str, ...]

    def __init__(self, rows, cols=None, *, row_key=(), col_key=()):
        from . import agg

        if set(rows._schema) != {_ROW, _CELLS}:
            raise ValueError("matrix core must contain owning rows and entry lists")
        if tuple(rows._schema) != (_ROW, _CELLS):
            rows = _project(rows, [(_ROW, rows[_ROW]), (_CELLS, rows[_CELLS])])
        if cols is not None:
            records = cols.aggregate(
                agg.collect(struct(**{_CID: cols[_CID], _COL: cols[_COL]})),
                _localize=False,
            )
            rows = rows.annotate_globals(**{_COLUMNS: records})
        if _COLUMNS not in rows._global_schema:
            raise ValueError("matrix core lacks global column metadata")
        for name, value in dict(
            core=rows,
            row_key=tuple(row_key),
            col_key=tuple(col_key),
        ).items():
            object.__setattr__(self, name, value)

    @property
    def column_records(self):
        return _external_globals(
            self.core._native.globals_ref(), self.core._global_schema
        )[_COLUMNS]

    @cached_property
    def cols(self):
        return Table.parallelize(self.column_records)

    @property
    def row_type(self):
        return self.core._schema[_ROW]

    @property
    def col_type(self):
        return self.core._global_schema[_COLUMNS].element_type[_COL]

    @property
    def entry_type(self):
        return self.core._schema[_CELLS].element_type[_VALUE]

    @property
    def global_type(self):
        return tstruct(
            **{n: t for n, t in self.core._global_schema.items() if n != _COLUMNS}
        )


class MatrixTable:
    """An immutable lazy matrix with global, row, column and entry fields."""

    def __init__(self, mir):
        if not isinstance(mir, MatrixIR):
            raise TypeError("MatrixTable requires a MatrixIR")
        seen = set()
        for schema in (mir.global_type, mir.row_type, mir.col_type, mir.entry_type):
            overlap = seen.intersection(schema)
            if overlap:
                raise ValueError(
                    f"matrix fields must be unique across axes: {sorted(overlap)}"
                )
            seen.update(schema)
        for names, schema in ((mir.row_key, mir.row_type), (mir.col_key, mir.col_type)):
            if len(set(names)) != len(names) or any(
                name not in schema for name in names
            ):
                raise ValueError("matrix keys must be distinct fields of their axis")
        object.__setattr__(self, "_mir", mir)

    def __setattr__(self, name, value):
        raise TypeError("MatrixTables are immutable")

    def __delattr__(self, name):
        raise TypeError("MatrixTables are immutable")

    def __iter__(self):
        raise TypeError("MatrixTable is not iterable; use rows(), cols(), or entries()")

    _ROW_NAME = _ROW
    _is_scan_node = staticmethod(_is_scan)

    def _copy(self, *, restrict=False, **changes):
        old = self._mir
        rows = changes.get("rows", old.core)
        cols = changes.get("cols")
        if cols is not None:
            from . import agg, len as length, range as array_range

            ids = cols.aggregate(agg.collect(cols[_CID]), _localize=False)
            cells = rows[_CELLS]
            rows = rows.annotate(
                **{
                    _CELLS: array_range(length(ids)).map(
                        lambda j: cells.filter(lambda c: c[_CID] == ids[j])[0]
                    )
                }
            )
        return MatrixTable(
            MatrixIR(
                rows,
                cols,
                row_key=changes.get("row_key", old.row_key),
                col_key=changes.get("col_key", old.col_key),
            )
        )

    def _axis_type(self, axis):
        return getattr(self._mir, axis + "_type")

    def __getitem__(self, item):
        if not isinstance(item, str):
            if not isinstance(item, tuple) or len(item) != 2:
                raise TypeError("matrix indexing requires row and column keys")
            r, c = item
            if isinstance(r, slice) and r == slice(None):
                if isinstance(c, slice) and c == slice(None):
                    return self.index_globals()
                return self.index_cols(c)
            if isinstance(c, slice) and c == slice(None):
                return self.index_rows(r)
            return self.index_entries(r, c)
        for axis in ("global", "row", "col", "entry"):
            schema = self._axis_type(axis)
            if item in schema:
                return Expression(
                    schema[item],
                    self,
                    {"op": "matrix_field", "axis": axis, "name": item},
                    selection_name=item,
                    top_level_name=item,
                    row_indexed=axis != "global",
                )
        raise KeyError(item)

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name) from None

    def _axis_struct(self, axis, names=None):
        schema = self._axis_type(axis)
        value = struct(
            **{name: self[name] for name in (schema if names is None else names)}
        )
        return Expression(
            value.dtype,
            self,
            _IR(
                {"op": "matrix_scope", "axis": axis, "value": value.to_ir()},
                value._global_refs,
            ),
            row_indexed=axis != "global",
        )

    @property
    def row(self):
        return self._axis_struct("row")

    @property
    def col(self):
        return self._axis_struct("col")

    @property
    def entry(self):
        return self._axis_struct("entry")

    @property
    def globals(self):
        return self._axis_struct("global")

    @property
    def row_key(self):
        return self._axis_struct("row", self._mir.row_key)

    @property
    def col_key(self):
        return self._axis_struct("col", self._mir.col_key)

    @property
    def row_value(self):
        return self._axis_struct(
            "row", [n for n in self.row.dtype if n not in self._mir.row_key]
        )

    @property
    def col_value(self):
        return self._axis_struct(
            "col", [n for n in self.col.dtype if n not in self._mir.col_key]
        )

    def _validate(self, expr, axis, *, aggregate=False, allow_row_initializers=False):
        expr = as_expr(expr)
        if expr.source is not None and expr.source is not self:
            raise ExpressionException(
                "expression source mismatch: fields belong to another dataset"
            )
        allowed = {
            "global": set(),
            "row": {"row"},
            "col": {"col"},
            "entry": {"row", "col"},
        }[axis]
        actual = _axes(expr.to_ir(), outside_aggregates=aggregate)
        if not actual <= allowed:
            raise ExpressionException(
                f"{axis} expression uses incompatible axes: {sorted(actual - allowed)}"
            )
        if not aggregate and _has_aggregate(expr.to_ir()):
            raise ExpressionException(
                f"aggregation is not allowed in {axis} expression"
            )

        def check_initializers(node):
            if isinstance(node, dict) and node.get("op") != "literal":
                if node.get("op") == "aggregate":
                    for _, initializer in node.get("init", []):
                        if _axes(initializer) and not (
                            allow_row_initializers and _axes(initializer) <= {"row"}
                        ):
                            raise NotImplementedError(
                                "matrix-axis aggregate initialization requires native "
                                "Table support for group-dependent initializers"
                            )
                for value in node.values():
                    check_initializers(value)
            elif isinstance(node, list):
                for value in node:
                    check_initializers(value)

        check_initializers(expr.to_ir())
        return expr

    def _context(self, axis):
        if axis in ("row", "global"):
            return self._mir.core
        if axis == "col":
            return self._mir.cols
        source = self._mir.core.explode(_CELLS)
        source = source.filter(source[_CELLS][_PRESENT])
        e = _project(
            source,
            [
                (_ROW, source[_ROW]),
                (_CID, source[_CELLS][_CID]),
                (_ENTRY, source[_CELLS][_VALUE] if len(self.entry.dtype) else struct()),
            ],
        )
        columns = self._column_records
        col = columns.filter(lambda c: c[_CID] == e[_CID])[0][_COL]
        return e.annotate(**{_COL: col})

    @cached_property
    def _column_records(self):
        return self._mir.column_records

    def _lower(self, expr, table):
        """Bind matrix axes to private struct columns in a native Table plan."""
        glob = self.index_globals()

        def rewrite(node):
            if isinstance(node, list):
                return [rewrite(v) for v in node]
            if not isinstance(node, dict) or node.get("op") == "literal":
                return node
            if node.get("op") == "matrix_scope":
                return rewrite(node["value"])
            if node.get("op") == "matrix_field":
                axis, name = node["axis"], node["name"]
                if axis == "global":
                    return glob[name].to_ir()
                return {
                    "op": "get_field",
                    "value": {
                        "op": "column",
                        "name": {"row": _ROW, "col": _COL, "entry": _ENTRY}[axis],
                    },
                    "name": name,
                }
            return {k: rewrite(v) for k, v in node.items()}

        return Expression(
            expr.dtype,
            table,
            _IR(
                rewrite(expr.to_ir()),
                dict(expr._global_refs) | dict(glob._global_refs),
                expr._lookup_refs,
            ),
            selection_name=expr._selection_name,
            top_level_name=expr._top_level_name,
            row_indexed=bool(_axes(expr.to_ir())),
        )

    def _lower_cell(self, expr, base, cell, position=None):
        """Bind a cell expression inside its owning row's array map."""
        glob = self.index_globals()
        columns = self._column_records
        col = (columns[position] if position is not None else
               columns.filter(lambda c: c[_CID] == cell[_CID])[0])[_COL]
        axes = {
            "row": base[_ROW],
            "col": col,
            "entry": cell[_VALUE] if len(self.entry.dtype) else struct(),
            "global": glob,
        }
        refs, lookups = dict(expr._global_refs), dict(expr._lookup_refs)

        def rewrite(node):
            if isinstance(node, list):
                return [rewrite(v) for v in node]
            if not isinstance(node, dict) or node.get("op") == "literal":
                return node
            if node.get("op") == "matrix_column_position":
                if position is None:
                    raise NotImplementedError("column lookup requires the owning entry-list position")
                return position.to_ir()
            if node.get("op") == "matrix_scope":
                return rewrite(node["value"])
            if node.get("op") == "matrix_field":
                value = axes[node["axis"]][node["name"]]
                refs.update(value._global_refs)
                lookups.update(value._lookup_refs)
                return value.to_ir()
            return {k: rewrite(v) for k, v in node.items()}

        return Expression(expr.dtype, base, _IR(rewrite(expr.to_ir()), refs, lookups))

    def _row_contexts(self, base):
        from . import range as array_range, len as length

        cells = base[_CELLS]
        return (
            array_range(length(cells))
            .map(
                lambda j: struct(
                    entry=cells[j][_VALUE] if len(self.entry.dtype) else struct(),
                    present=cells[j][_PRESENT],
                    column_id=cells[j][_CID],
                    position=j,
                    bindings=struct(),
                )
            )
            .filter(lambda ctx: ctx.present)
        )

    def _axis_aggregates(self, expr, axis, base):
        if axis == "row":
            from .matrix_list_aggregates import reduce_list

            return reduce_list(self, expr, base, self._row_contexts(base))
        context = self._context("entry")
        refs, lookups = dict(expr._global_refs), dict(expr._lookup_refs)

        def rewrite(node):
            if isinstance(node, list):
                return [rewrite(v) for v in node]
            if not isinstance(node, dict) or node.get("op") == "literal":
                return node
            if node.get("op") in _AGG_OPS and _has_aggregate(node):
                raw = _expression(expr.dtype, self, node, expr)
                lowered = self._lower(raw, context)
                from .expressions import _global_bindings
                from .lookups import resolve

                native, resolved = resolve(context._native, [lowered])
                native = native.grouped_aggregate_json(
                    json.dumps([[_CID, {"op": "column", "name": _CID}]]),
                    json.dumps([["value", resolved[0].to_ir()]]),
                    _global_bindings(resolved),
                )
                grouped = Table(native, _schema_from_native(native), (_CID,))
                dtype = grouped._schema["value"]
                empty_table = Table.parallelize([], schema=context._schema)
                empty = empty_table.aggregate(
                    Expression(dtype, empty_table, lowered.to_ir()), _localize=False
                )
                lookup = grouped.index(base[_CID])
                result = if_else(is_defined(lookup), lookup.value, empty)
                refs.update(result._global_refs)
                lookups.update(result._lookup_refs)
                return result.to_ir()
            return {k: rewrite(v) for k, v in node.items()}

        return Expression(expr.dtype, self, _IR(rewrite(expr.to_ir()), refs, lookups))

    def _project_axis(self, axis, fields):
        fields = [
            (
                n,
                self._validate(
                    v,
                    axis,
                    aggregate=axis in ("row", "col"),
                    allow_row_initializers=axis == "row",
                ),
            )
            for n, v in fields
        ]
        if len({n for n, _ in fields}) != len(fields):
            raise ValueError("duplicate selected fields")
        if axis == "global" and any(n == _COLUMNS for n, _ in fields):
            raise ValueError("global field name is reserved for matrix column metadata")
        if any(not isinstance(n, str) or not n for n, _ in fields):
            raise ValueError("field names must be nonempty strings")
        if axis == "entry":
            from . import range as array_range, len as length
            base = self._mir.core
            base, prepared = self._prepare_row_lookups(base, [v for _, v in fields])
            fields = [(n, v) for (n, _), v in zip(fields, prepared)]
            cells = array_range(length(base[_CELLS])).map(
                lambda j: struct(
                    **{
                        _CID: base[_CELLS][j][_CID],
                        _PRESENT: base[_CELLS][j][_PRESENT],
                        _VALUE: struct(
                            **{n: self._lower_cell(v, base, base[_CELLS][j], j) for n, v in fields}
                        ),
                    }
                )
            )
            return self._copy(
                rows=_project(
                    base,
                    [(_ROW, base[_ROW]), (_CELLS, cells)],
                )
            )
        base = self._context(axis)
        if axis == "row":
            base, prepared = self._prepare_row_lookups(base, [v for _, v in fields])
            fields = [(n, v) for (n, _), v in zip(fields, prepared)]
        if axis == "global":
            lowered = {n: self._lower(v, base) for n, v in fields}
            return self._copy(
                rows=base.select_globals(
                    **lowered, **{_COLUMNS: self._mir.column_records}
                )
            )
        prepared = [
            (
                n,
                self._axis_aggregates(v, axis, base)
                if _has_aggregate(v.to_ir())
                else v,
            )
            for n, v in fields
        ]
        value = struct(**{n: self._lower(v, base) for n, v in prepared})
        if axis == "row":
            projected = _project(
                base,
                [(_CELLS, base[_CELLS]), (_ROW, value)],
            )
            projected = self._sort_rows(projected, self._mir.row_key)
            return self._copy(rows=projected)
        if axis == "col":
            return self._copy(cols=_project(base, [(_CID, base[_CID]), (_COL, value)]))

    @staticmethod
    def _sort_rows(rows, keys):
        if not keys:
            return rows.key_by()
        from . import asc

        return rows.order_by(*(asc(rows[_ROW][name]) for name in keys)).key_by()

    def _annotate(self, axis, named):
        schema = self._axis_type(axis)
        keys = (
            self._mir.row_key
            if axis == "row"
            else self._mir.col_key
            if axis == "col"
            else ()
        )
        if set(named).intersection(keys):
            raise ValueError(
                "cannot overwrite a key field; use key_rows_by/key_cols_by"
            )
        return self._project_axis(
            axis,
            [(n, named.get(n, self[n])) for n in schema]
            + [(n, v) for n, v in named.items() if n not in schema],
        )

    def _select(self, axis, exprs, named):
        keys = (
            self._mir.row_key
            if axis == "row"
            else self._mir.col_key
            if axis == "col"
            else ()
        )
        fields = [(n, self[n]) for n in keys]
        for expr in exprs:
            expr = self[expr] if isinstance(expr, str) else expr
            if not isinstance(expr, Expression) or expr._selection_name is None:
                raise ValueError("positional selection requires a field reference")
            self._validate(expr, axis)
            if expr._selection_name in keys:
                raise ValueError("key fields are retained automatically")
            fields.append((expr._selection_name, expr))
        if set(named).intersection(keys):
            raise ValueError("cannot overwrite key fields")
        return self._project_axis(axis, fields + list(named.items()))

    def _transmute(self, axis, named):
        keys = (
            self._mir.row_key
            if axis == "row"
            else self._mir.col_key
            if axis == "col"
            else ()
        )
        if set(named).intersection(keys):
            raise ValueError("cannot overwrite key fields")
        used = set()

        def visit(node):
            if isinstance(node, dict) and node.get("op") != "literal":
                if node.get("op") == "matrix_field" and node["axis"] == axis:
                    used.add(node["name"])
                for child in node.values():
                    visit(child)
            elif isinstance(node, list):
                for child in node:
                    visit(child)

        for value in named.values():
            visit(as_expr(value).to_ir())
        fields = [
            (n, named.get(n, self[n]))
            for n in self._axis_type(axis)
            if n not in used or n in named or n in keys
        ]
        fields += [(n, v) for n, v in named.items() if n not in dict(fields)]
        return self._project_axis(axis, fields)

    def annotate_globals(self, **named):
        return self._annotate("global", named)

    def annotate_rows(self, **named):
        return self._annotate("row", named)

    def annotate_cols(self, **named):
        return self._annotate("col", named)

    def annotate_entries(self, **named):
        return self._annotate("entry", named)

    def select_globals(self, *exprs, **named):
        return self._select("global", exprs, named)

    def select_rows(self, *exprs, **named):
        return self._select("row", exprs, named)

    def select_cols(self, *exprs, **named):
        return self._select("col", exprs, named)

    def select_entries(self, *exprs, **named):
        return self._select("entry", exprs, named)

    def transmute_globals(self, **named):
        return self._transmute("global", named)

    def transmute_rows(self, **named):
        return self._transmute("row", named)

    def transmute_cols(self, **named):
        return self._transmute("col", named)

    def transmute_entries(self, **named):
        return self._transmute("entry", named)

    def _key_by(self, axis, keys, named):
        fields = list(self._axis_type(axis))
        values = {name: self[name] for name in fields}
        names = []
        for key in keys:
            expr = self[key] if isinstance(key, str) else key
            self._validate(expr, axis)
            if expr._selection_name is None:
                raise ValueError("positional keys must be fields")
            names.append(expr._selection_name)
        names += list(named)
        values.update(named)
        # Unkey before projection to permit replacing keys with computed values.
        # Expressions still belong to this source; use this object's projector.
        result = self._project_axis(axis, list(values.items()))
        result = result._copy(**{axis + "_key": tuple(names)})
        if axis == "row":
            result = result._copy(rows=self._sort_rows(result._mir.core, names))
        return result

    def key_rows_by(self, *keys, **named):
        return self._key_by("row", keys, named)

    def key_cols_by(self, *keys, **named):
        return self._key_by("col", keys, named)

    def _filter(self, axis, expr, keep):
        if type(keep) is not bool:
            raise TypeError("keep must be bool")
        expr = self._validate(expr, axis, aggregate=axis in ("row", "col"))
        if expr.dtype != tbool:
            raise TypeError("filter predicate must be bool")
        if axis == "entry":
            from . import range as array_range, len as length

            def coalesce(value, default):
                return if_else(is_defined(value), value, default)

            base = self._mir.core
            base, prepared = self._prepare_row_lookups(base, [expr])
            expr = prepared[0]
            cells = array_range(length(base[_CELLS])).map(
                lambda j: struct(
                    **{
                        _CID: base[_CELLS][j][_CID],
                        _VALUE: base[_CELLS][j][_VALUE] if len(self.entry.dtype) else struct(),
                        _PRESENT: base[_CELLS][j][_PRESENT]
                        & coalesce(
                            self._lower_cell(expr, base, base[_CELLS][j], j)
                            if keep
                            else ~self._lower_cell(expr, base, base[_CELLS][j], j),
                            False,
                        ),
                    }
                )
            )
            return self._copy(
                rows=_project(base, [(_ROW, base[_ROW]), (_CELLS, cells)])
            )
        base = self._context(axis)
        if _has_aggregate(expr.to_ir()):
            expr = self._axis_aggregates(expr, axis, base)
        filtered = base.filter(self._lower(expr, base), keep)
        return self._copy(
            restrict=True, **{{"row": "rows", "col": "cols"}[axis]: filtered}
        )

    def filter_rows(self, expr, keep=True):
        return self._filter("row", expr, keep)

    def filter_cols(self, expr, keep=True):
        return self._filter("col", expr, keep)

    def filter_entries(self, expr, keep=True):
        return self._filter("entry", expr, keep)

    def index_globals(self):
        full = _external_globals(
            self._mir.core._native.globals_ref(), self._mir.core._global_schema
        )
        return struct(**{n: full[n] for n in self._mir.global_type})

    def _public_axis(self, axis, *, key=True):
        table = self._context(axis)
        names = self._axis_type(axis)
        value = table[_ROW if axis == "row" else _COL]
        output = _project(table, [(n, value[n]) for n in names])
        output = self._with_globals(output)
        keys = self._mir.row_key if axis == "row" else self._mir.col_key
        return output.key_by(*keys) if key else output

    def _with_globals(self, table):
        globals = self.index_globals()
        return table.select_globals(**{n: globals[n] for n in self._mir.global_type})

    def rows(self):
        return self._public_axis("row")

    def cols(self):
        return self._public_axis("col")

    def globals_table(self):
        return self._with_globals(range_table(1).key_by().select())

    def entries(self):
        context = self._context("entry")
        fields = {
            n: context[p][n]
            for p, schema in (
                (_ROW, self.row.dtype),
                (_COL, self.col.dtype),
                (_ENTRY, self.entry.dtype),
            )
            for n in schema
        }
        result = self._with_globals(_project(context, list(fields.items())))
        return result.key_by(*(self._mir.row_key + self._mir.col_key))

    def _aggregate(self, axis, expr, localize):
        expr = self._validate(expr, axis, aggregate=True)
        # Total aggregation allows axis references only under aggregation nodes.
        if _axes(expr.to_ir(), outside_aggregates=True):
            raise ExpressionException(
                "total aggregation requires indexed fields inside aggregators"
            )
        actual = _axes(expr.to_ir())
        allowed = {"row": {"row"}, "col": {"col"}, "entry": {"row", "col"}}[axis]
        if not actual <= allowed:
            raise ExpressionException("aggregation uses incompatible axes")
        table = self._context(axis)
        if axis == "col" and self._mir.col_key:
            table = table.order_by(
                *(table[_COL][n] for n in self._mir.col_key)
            ).key_by()
        return table.aggregate(self._lower(expr, table), _localize=localize)

    def aggregate_rows(self, expr, _localize=True):
        return self._aggregate("row", expr, _localize)

    def aggregate_cols(self, expr, _localize=True):
        return self._aggregate("col", expr, _localize)

    def aggregate_entries(self, expr, _localize=True):
        return self._aggregate("entry", expr, _localize)

    def _collect_expression(self, expr, n=None):
        self._validate(expr, "entry")
        axes = _axes(expr.to_ir())
        axis = (
            "entry"
            if axes == {"row", "col"}
            else "row"
            if axes == {"row"}
            else "col"
            if axes == {"col"}
            else "global"
        )
        table = self._context(axis) if axis != "global" else self.globals_table()
        lowered = self._lower(expr, table)
        output = _project(table, [("value", lowered)])
        if n is not None:
            output = output.head(n)
        from . import agg

        return output.aggregate(agg.collect(output.value), _localize=False)

    def count_rows(self, _localize=True):
        from . import agg

        if type(_localize) is not bool:
            raise TypeError("_localize must be bool")
        if _localize:
            return self._mir.core.count()
        return self._mir.core.aggregate(agg.count(), _localize=_localize)

    def count_cols(self, _localize=True):
        from . import agg

        if type(_localize) is not bool:
            raise TypeError("_localize must be bool")
        if _localize:
            return self._mir.cols.count()
        return self._mir.cols.aggregate(agg.count(), _localize=_localize)

    def count(self):
        return self.count_rows(), self.count_cols()

    def _force_count_rows(self):
        return self._mir.core._force_count()

    def _force_count_cols(self):
        return self._mir.cols._force_count()

    def n_partitions(self):
        return self._mir.core.n_partitions()

    def head(self, n_rows=None, n_cols=None, *, n=None):
        if n is not None:
            if n_rows is not None:
                raise ValueError("specify n_rows or n")
            n_rows = n
        changes = {}
        if n_rows is not None:
            _validate_n(n_rows, "n_rows")
            changes["rows"] = self._mir.core.head(n_rows)
        if n_cols is not None:
            _validate_n(n_cols, "n_cols")
            changes["cols"] = self._mir.cols.head(n_cols)
        return self._copy(restrict=True, **changes) if changes else self

    def tail(self, n_rows=None, n_cols=None, *, n=None):
        if n is not None:
            if n_rows is not None:
                raise ValueError("specify n_rows or n")
            n_rows = n
        changes = {}
        if n_rows is not None:
            _validate_n(n_rows, "n_rows")
            changes["rows"] = self._mir.core.tail(n_rows)
        if n_cols is not None:
            _validate_n(n_cols, "n_cols")
            changes["cols"] = self._mir.cols.tail(n_cols)
        return self._copy(restrict=True, **changes) if changes else self

    def add_row_index(self, name="row_idx"):
        from . import scan

        return self.annotate_rows(**{name: scan.count()})

    def add_col_index(self, name="col_idx"):
        from . import scan

        return self.annotate_cols(**{name: scan.count()})

    def repartition(self, n_partitions, shuffle=True):
        return self._copy(rows=self._mir.core.repartition(n_partitions, shuffle))

    def naive_coalesce(self, max_partitions):
        return self._copy(rows=self._mir.core.naive_coalesce(max_partitions))

    def _filter_partitions(self, parts, keep=True):
        return self._copy(
            restrict=True, rows=self._mir.core._filter_partitions(parts, keep)
        )

    def unfilter_entries(self):
        base = self._mir.core
        cells = base[_CELLS].map(
            lambda cell: struct(
                **{
                    _CID: cell[_CID],
                    _PRESENT: True,
                    _VALUE: if_else(
                        cell[_PRESENT],
                        cell[_VALUE],
                        struct(**{n: missing(t) for n, t in self.entry.dtype.items()}),
                    )
                    if len(self.entry.dtype)
                    else struct(),
                }
            )
        )
        return self._copy(rows=base.annotate(**{_CELLS: cells}))

    @classmethod
    def from_rows_table(cls, table):
        if not isinstance(table, Table):
            raise TypeError("table must be a Table")
        cells = literal(
            [], tarray(tstruct(**{_CID: tint64, _PRESENT: tbool, _VALUE: tstruct()}))
        )
        core = _project(table, [(_ROW, table.row), (_CELLS, cells)])
        cols = Table.parallelize([], schema=tstruct(**{_CID: tint64, _COL: tstruct()}))
        return cls(MatrixIR(core, cols, row_key=table._keys))

    @classmethod
    def from_parts(cls, globals=None, rows=None, cols=None, entries=None):
        if not isinstance(globals if globals is not None else {}, Mapping):
            raise TypeError("globals must be a mapping")
        parts = []
        for value in (rows, cols, entries):
            if value is not None and not isinstance(value, Mapping):
                raise TypeError("parts must be mappings")
            parts.append({n: list(v) for n, v in (value or {}).items()})
        rows, cols, entries = parts

        def length(fields):
            dims = {len(v) for v in fields.values()}
            if len(dims) > 1:
                raise ValueError("field dimensions do not match")
            return next(iter(dims)) if dims else None

        nr, nc, er = length(rows), length(cols), length(entries)
        entry_matrices = {n: [list(row) for row in vals] for n, vals in entries.items()}
        ec = {len(row) for vals in entry_matrices.values() for row in vals}
        if len(ec) > 1:
            raise ValueError(
                "entry arrays must be rectangular with matching dimensions"
            )
        inferred_c = next(iter(ec)) if ec else None
        if nr is None:
            nr = er
        if nc is None:
            nc = inferred_c
        if nr is None or nc is None:
            raise ValueError(
                "provide row and column dimensions through rows, cols, or entries"
            )
        if er is not None and er != nr or inferred_c is not None and inferred_c != nc:
            raise ValueError("matrix dimensions do not match")
        if "row_idx" in rows or "col_idx" in cols:
            raise ValueError("row_idx and col_idx are generated by from_parts")
        rs = [dict(row_idx=i, **{n: v[i] for n, v in rows.items()}) for i in range(nr)]
        cs = [dict(col_idx=j, **{n: v[j] for n, v in cols.items()}) for j in range(nc)]
        rtable = Table.parallelize(
            rs, schema=tstruct(row_idx=tint32) if not rs else None
        )
        if globals:
            rtable = rtable.annotate_globals(**globals)
        ctable = Table.parallelize(
            cs, schema=tstruct(col_idx=tint32) if not cs else None
        )
        from .types import infer_type

        if entries and (not nr or not nc):
            raise ValueError("empty entry data requires a typed localized Table")
        dtype = (
            infer_type({n: v[0][0] for n, v in entry_matrices.items()})
            if entries
            else tstruct()
        )
        cell_type = tstruct(**{_CID: tint64, _PRESENT: tbool, _VALUE: dtype})
        records = [
            {
                _ROW: rs[i],
                _CELLS: [
                    {
                        _CID: j,
                        _PRESENT: True,
                        _VALUE: {n: v[i][j] for n, v in entry_matrices.items()},
                    }
                    for j in range(nc)
                ],
            }
            for i in range(nr)
        ]
        core = Table.parallelize(
            records, schema=tstruct(**{_ROW: rtable._schema, _CELLS: tarray(cell_type)})
        )
        if globals:
            core = core.annotate_globals(**globals)
        ctable = ctable.select(**{_CID: int64(ctable.col_idx), _COL: ctable.row})
        return cls(MatrixIR(core, ctable, row_key=("row_idx",), col_key=("col_idx",)))

    def localize_entries(
        self, entries_array_field_name=None, columns_array_field_name=None
    ):
        en = entries_array_field_name or "__entries_" + uuid4().hex
        cn = columns_array_field_name or "__cols_" + uuid4().hex
        if en in self.row.dtype or cn in self.globals.dtype or en == cn:
            raise ValueError("localization field names collide")
        from . import agg

        base = self._mir.core
        entries = base[_CELLS].map(
            lambda cell: if_else(
                cell[_PRESENT],
                cell[_VALUE] if len(self.entry.dtype) else struct(),
                missing(self.entry.dtype),
            )
        )
        output = _project(
            base, [(n, base[_ROW][n]) for n in self.row.dtype] + [(en, entries)]
        ).key_by(*self._mir.row_key)
        columns = self._public_axis("col", key=False)
        output = self._with_globals(output).annotate_globals(
            **{cn: columns.aggregate(agg.collect(columns.row), _localize=False)}
        )
        if entries_array_field_name is None:
            output = output.drop(en)
        if columns_array_field_name is None:
            output = output.drop(cn)
        return output

    def _localize_entries(self, entries_field_name, cols_field_name):
        return self.localize_entries(entries_field_name, cols_field_name)

    def choose_cols(self, indices):
        if not isinstance(indices, (list, tuple)) or any(
            type(i) is not int for i in indices
        ):
            raise TypeError("indices must be a sequence of integers")
        from . import agg, len as length, range as array_range

        cols = self._mir.cols
        count = cols.count()
        if any(i < 0 or i >= count for i in indices):
            raise ValueError("column index out of bounds")
        values = cols.aggregate(agg.collect(cols[_COL]), _localize=False)
        positions = literal(list(indices), tarray(tint32))
        columns = Table.parallelize(
            array_range(length(positions)).map(
                lambda j: struct(
                    **{
                        _CID: int64(j),
                        _COL: values[positions[j]] if len(self.col.dtype) else struct(),
                    }
                )
            )
        )
        base = self._mir.core
        cells = base[_CELLS]
        selected = array_range(length(positions)).map(
            lambda j: struct(
                **{
                    _CID: int64(j),
                    _PRESENT: cells[positions[j]][_PRESENT],
                    _VALUE: cells[positions[j]][_VALUE]
                    if len(self.entry.dtype)
                    else struct(),
                }
            )
        )
        return MatrixTable(
            MatrixIR(
                base.annotate(**{_CELLS: selected}),
                columns,
                row_key=self._mir.row_key,
                col_key=self._mir.col_key,
            )
        )

    def drop(self, *fields):
        drops = set()
        for value in fields:
            if isinstance(value, str):
                self[value]
                drops.add(value)
            elif (
                isinstance(value, Expression)
                and value.source is self
                and value._top_level_name is not None
            ):
                drops.add(value._top_level_name)
            else:
                raise ValueError(
                    "drop requires field names or top-level field references"
                )
        if drops.intersection(self._mir.row_key + self._mir.col_key):
            raise ValueError("cannot drop key fields")
        result = self
        for axis in ("global", "row", "col", "entry"):
            schema = result._axis_type(axis)
            if drops.intersection(schema):
                result = result._project_axis(
                    axis, [(n, result[n]) for n in schema if n not in drops]
                )
        return result

    def rename(self, fields):
        if not isinstance(fields, Mapping):
            raise TypeError("fields must be a mapping")
        for name in fields:
            self[name]
        result = self
        for axis in ("global", "row", "col", "entry"):
            schema = result._axis_type(axis)
            if set(fields).intersection(schema):
                names = [(fields.get(n, n), result[n]) for n in schema]
                # Update key names together with the struct projection.
                if axis in ("row", "col"):
                    oldkeys = getattr(result._mir, axis + "_key")
                    unkeyed = result._copy(**{axis + "_key": ()})
                    names = [(fields.get(n, n), unkeyed[n]) for n in schema]
                    result = unkeyed._project_axis(axis, names)
                    result = result._copy(
                        **{axis + "_key": tuple(fields.get(n, n) for n in oldkeys)}
                    )
                else:
                    result = result._project_axis(axis, names)
        return result

    def _lookup_axis(self, axis, expressions, all_matches):
        table = self.rows() if axis == "row" else self.cols()
        return table.index(*expressions, all_matches=all_matches)

    def index_rows(self, *exprs, all_matches=False):
        return self._lookup_axis("row", exprs, all_matches)

    def index_cols(self, *exprs, all_matches=False):
        return self._lookup_axis("col", exprs, all_matches)

    def index_entries(self, row_exprs, col_exprs):
        def unpack(values, key):
            values = tuple(values) if isinstance(values, (list, tuple)) else (values,)
            if (
                len(values) == 1
                and isinstance(values[0], Expression)
                and isinstance(values[0].dtype, tstruct)
            ):
                values = tuple(values[0][n] for n in values[0].dtype)
            return values

        if not self._mir.row_key or not self._mir.col_key:
            raise ValueError("entry indexing requires row and column keys")
        rows = self._mir.core
        target = _project(
            rows,
            [(n, rows[_ROW][n]) for n in self._mir.row_key] + [(_CELLS, rows[_CELLS])],
        ).key_by(*self._mir.row_key)
        row = target.index(*unpack(row_exprs, self._mir.row_key))
        queries = unpack(col_exprs, self._mir.col_key)
        if len(queries) != len(self._mir.col_key):
            raise ValueError("column key lengths differ")
        from . import len as length
        from . import scan, int32
        columns = self._mir.cols
        position_name = "__matrix_col_position_" + uuid4().hex
        columns = columns.annotate(**{position_name: int32(scan.count())})
        keyed_columns = _project(
            columns,
            [(name, columns[_COL][name]) for name in self._mir.col_key]
            + [(position_name, columns[position_name])],
        ).key_by(*self._mir.col_key)
        # A column-scoped lookup can be prepared once on the caller's column
        # relation, rather than scanning the target metadata inside each cell.
        position = keyed_columns.index(*queries)[position_name]
        cell = row[_CELLS][position]
        defined = is_defined(cell) & cell[_PRESENT]
        value = cell[_VALUE] if len(self.entry.dtype) else struct()
        return if_else(defined, value, missing(self.entry.dtype))

    def _prepare_row_lookups(self, base, expressions):
        replacements = {}
        added_globals = {}
        added_lookups = {}
        for expr in expressions:

            def visit(node):
                nonlocal base
                if isinstance(node, list):
                    for child in node:
                        visit(child)
                    return
                if not isinstance(node, dict) or node.get("op") == "literal":
                    return
                if node.get("op") == "table_lookup":
                    reference = node["reference"]
                    if reference in replacements:
                        return
                    axes = set().union(*(_axes(key) for key in node["keys"]))
                    if not axes <= {"row"} and not axes <= {"col"}:
                        raise NotImplementedError(
                            "entry-dependent keyed lookup requires native correlated list-lookup support; row indices are not used"
                        )
                    native = expr._lookup_refs[reference]
                    schema = _schema_from_native(native)
                    dtype = tstruct(
                        **{
                            n: t
                            for n, t in schema.items()
                            if n not in native.key_names()
                        }
                    )
                    if node.get("all_matches"):
                        dtype = tarray(dtype)
                    raw = _expression(dtype, self, node, expr)
                    if axes == {"col"}:
                        from . import agg

                        columns = self._context("col")
                        name = "__matrix_lookup_" + uuid4().hex
                        columns = columns.annotate(**{name: self._lower(raw, columns)})
                        records = columns.aggregate(
                            agg.collect(columns[name]),
                            _localize=False,
                        )
                        position = Expression(tint32, self, {"op": "matrix_column_position"})
                        result = records[position]
                        replacements[reference] = result.to_ir()
                        added_globals.update(result._global_refs)
                        added_lookups.update(result._lookup_refs)
                        return
                    name = "__matrix_lookup_" + uuid4().hex
                    base = base.annotate(**{name: self._lower(raw, base)})
                    replacements[reference] = {"op": "column", "name": name}
                    return
                for child in node.values():
                    visit(child)

            visit(expr.to_ir())

        def rewrite(node):
            if isinstance(node, list):
                return [rewrite(v) for v in node]
            if not isinstance(node, dict) or node.get("op") == "literal":
                return node
            if node.get("op") == "table_lookup":
                return replacements[node["reference"]]
            return {k: rewrite(v) for k, v in node.items()}

        return base, [
            Expression(expr.dtype, self, _IR(
                rewrite(expr.to_ir()),
                {**expr._global_refs, **added_globals},
                {**expr._lookup_refs, **added_lookups},
            ))
            for expr in expressions
        ]

    def _key_filter(self, axis, other, anti):
        operation = f"{'anti' if anti else 'semi'}_join_{'rows' if axis == 'row' else 'cols'}"
        if not isinstance(other, Table) or not other._keys:
            raise ValueError(f"{operation}: cannot join: requires a keyed Table")
        keys = self._mir.row_key if axis == "row" else self._mir.col_key
        schema = self._axis_type(axis)
        if len(other._keys) > len(keys) or [
            schema[n] for n in keys[: len(other._keys)]
        ] != [other._schema[n] for n in other._keys]:
            raise ValueError(f"{operation}: cannot join: key types do not match")
        base = self._context(axis)
        temp = _project(
            base,
            [(n, base[_ROW if axis == "row" else _COL][n]) for n in keys]
            + [(n, base[n]) for n in base._schema],
        ).key_by(*keys)
        filtered = temp.anti_join(other) if anti else temp.semi_join(other)
        filtered = filtered.key_by()
        filtered = filtered.select(*base._schema)
        if axis == "row":
            filtered = self._sort_rows(filtered, keys)
        else:
            filtered = filtered.order_by(filtered[_CID])
        return self._copy(
            restrict=True, **{{"row": "rows", "col": "cols"}[axis]: filtered}
        )

    def semi_join_rows(self, other):
        return self._key_filter("row", other, False)

    def anti_join_rows(self, other):
        return self._key_filter("row", other, True)

    def semi_join_cols(self, other):
        return self._key_filter("col", other, False)

    def anti_join_cols(self, other):
        return self._key_filter("col", other, True)

    def compute_entry_filter_stats(
        self, row_field="entry_stats_row", col_field="entry_stats_col"
    ):
        from . import agg

        nc, nr = self.count_cols(_localize=False), self.count_rows(_localize=False)
        result = self.annotate_rows(
            **{
                row_field: struct(
                    n_filtered=nc - agg.count(),
                    n_remaining=agg.count(),
                    fraction_filtered=(nc - agg.count()) / nc,
                )
            }
        )
        return result.annotate_cols(
            **{
                col_field: struct(
                    n_filtered=nr - agg.count(),
                    n_remaining=agg.count(),
                    fraction_filtered=(nr - agg.count()) / nr,
                )
            }
        )

    def distinct_by_row(self):
        return self._distinct("row")

    def distinct_by_col(self):
        return self._distinct("col")

    def _distinct(self, axis):
        keys = self._mir.row_key if axis == "row" else self._mir.col_key
        if not keys:
            raise ValueError("distinct requires a key")
        base = self._context(axis)
        p = _ROW if axis == "row" else _COL
        t = (
            _project(
                base,
                [(n, base[p][n]) for n in keys] + [(n, base[n]) for n in base._schema],
            )
            .key_by(*keys)
            .distinct()
            .key_by()
        )
        t = t.select(*base._schema)
        if axis == "col":
            t = t.order_by(t[_CID])
        return self._copy(restrict=True, **{{"row": "rows", "col": "cols"}[axis]: t})

    def _explode(self, axis, field_expr):
        from . import agg, len as length, range as array_range, case
        from .types import tset

        field = self[field_expr] if isinstance(field_expr, str) else field_expr
        field = self._validate(field, axis)
        node, suffix = field.to_ir(), []
        while node.get("op") == "get_field":
            suffix.append(node["name"])
            node = node["value"]
        if node.get("op") != "matrix_field" or node["axis"] != axis:
            raise ValueError("explode requires a field reference on the selected axis")
        path = [node["name"]] + list(reversed(suffix))
        keys = self._mir.row_key if axis == "row" else self._mir.col_key
        if path[0] in keys:
            raise ValueError("cannot explode a key field")
        if not isinstance(field.dtype, (tarray, tset)):
            raise TypeError("explode requires an array or set")
        if axis == "row":
            base = self._mir.core
            values = self._lower(field, base)
            if isinstance(field.dtype, tset):
                values = values.to_array()
            name = "__matrix_explode_" + uuid4().hex
            expanded = base.annotate(**{name: values}).explode(name)

            def update(value, path):
                first, *rest = path
                return struct(
                    **{
                        n: (update(value[n], rest) if rest else expanded[name])
                        if n == first
                        else value[n]
                        for n in value.dtype
                    }
                )

            core = _project(
                expanded,
                [(_ROW, update(expanded[_ROW], path)), (_CELLS, expanded[_CELLS])],
            )
            return self._copy(rows=self._sort_rows(core, keys))
        base = self._mir.cols
        values = self._lower(field, base)
        if isinstance(field.dtype, tset):
            values = values.to_array()
        maximum = base.aggregate(agg.max(length(values)), _localize=False)
        width = if_else(is_defined(maximum) & (maximum > 0), int64(maximum), int64(1))
        children = array_range(length(values)).map(
            lambda j: struct(index=j, value=values[j])
        )
        expanded = base.annotate(__items=children).explode("__items")

        def updated(value, path):
            first, *rest = path
            return struct(
                **{
                    n: (updated(value[n], rest) if rest else expanded["__items"].value)
                    if n == first
                    else value[n]
                    for n in value.dtype
                }
            )

        child = (
            case()
            .when(
                expanded[_CID] <= literal(2**63 - 1, tint64) // width,
                expanded[_CID] * width + int64(expanded["__items"].index),
            )
            .or_error("matrix expansion exceeds int64 identity space")
        )
        expanded = _project(
            expanded,
            [
                ("parent", expanded[_CID]),
                (_CID, child),
                (_COL, updated(expanded[_COL], path)),
            ],
        )
        mapping = expanded.aggregate(agg.collect(expanded.row), _localize=False)
        core = self._mir.core
        cells = core[_CELLS].flatmap(
            lambda cell: mapping.filter(lambda col: col.parent == cell[_CID]).map(
                lambda col: struct(
                    **{
                        _CID: col[_CID],
                        _PRESENT: cell[_PRESENT],
                        _VALUE: cell[_VALUE] if len(self.entry.dtype) else struct(),
                    }
                )
            )
        )
        columns = _project(expanded, [(_CID, expanded[_CID]), (_COL, expanded[_COL])])
        return MatrixTable(
            MatrixIR(
                core.annotate(**{_CELLS: cells}),
                columns,
                row_key=self._mir.row_key,
                col_key=self._mir.col_key,
            )
        )

    def explode_rows(self, field_expr):
        return self._explode("row", field_expr)

    def explode_cols(self, field_expr):
        return self._explode("col", field_expr)

    def group_rows_by(self, *exprs, **named):
        return GroupedMatrixTable(self, "row", exprs, named)

    def group_cols_by(self, *exprs, **named):
        return GroupedMatrixTable(self, "col", exprs, named)

    def collect_cols_by_key(self):
        from . import agg

        if not self._mir.col_key:
            raise ValueError("collect_cols_by_key requires a column key")
        dense = self.unfilter_entries()
        return (
            dense.group_cols_by(*dense._mir.col_key)
            .aggregate_cols(**{n: agg.collect(dense[n]) for n in dense.col_value.dtype})
            .aggregate_entries(**{n: agg.collect(dense[n]) for n in dense.entry.dtype})
            .result()
        )

    def _same(self, other, tolerance=1e-6, absolute=False, reorder_fields=False):
        if (
            not isinstance(other, MatrixTable)
            or self._mir.row_key != other._mir.row_key
            or self._mir.col_key != other._mir.col_key
        ):
            return False
        entries_name = "__matrix_compare_entries_" + uuid4().hex
        columns_name = "__matrix_compare_columns_" + uuid4().hex
        return self.localize_entries(entries_name, columns_name)._same(
            other.localize_entries(entries_name, columns_name),
            tolerance,
            absolute,
            reorder_fields,
        )

    def union_rows(self, *others, _check_cols=True):
        from . import agg, range as array_range, len as length

        if type(_check_cols) is not bool:
            raise TypeError("_check_cols must be bool")
        result = self
        for other in others:
            if not isinstance(other, MatrixTable):
                raise TypeError("union arguments must be MatrixTables")
            if (
                result.row.dtype != other.row.dtype
                or result.entry.dtype != other.entry.dtype
                or result._mir.row_key != other._mir.row_key
                or result.col_key.dtype != other.col_key.dtype
            ):
                raise ValueError(
                    "union_rows requires matching row, entry, and column-key schemas"
                )
            if result.count_cols() != other.count_cols():
                raise ValueError("column counts differ")
            if _check_cols and result.col_key.collect() != other.col_key.collect():
                raise ValueError("column keys or their order differ")
            columns = result._mir.cols
            ids = columns.aggregate(agg.collect(columns[_CID]), _localize=False)
            source = other._mir.core
            cells = source[_CELLS]
            aligned = array_range(length(ids)).map(
                lambda j: struct(
                    **{
                        _CID: ids[j],
                        _PRESENT: cells[j][_PRESENT],
                        _VALUE: cells[j][_VALUE]
                        if len(result.entry.dtype)
                        else struct(),
                    }
                )
            )
            source = source.annotate(**{_CELLS: aligned})
            rows = result._mir.core.union(source)
            rows = self._sort_rows(rows, result._mir.row_key)
            result = result._copy(rows=rows)
        return result

    def union_cols(self, other, row_join_type="inner", drop_right_row_fields=True):
        from . import agg

        if not isinstance(other, MatrixTable):
            raise TypeError("other must be a MatrixTable")
        if row_join_type not in ("inner", "outer"):
            raise ValueError("row_join_type must be inner or outer")
        if type(drop_right_row_fields) is not bool:
            raise TypeError("drop_right_row_fields must be bool")
        if (
            self.entry.dtype != other.entry.dtype
            or self.col.dtype != other.col.dtype
            or self._mir.col_key != other._mir.col_key
        ):
            raise ValueError("column and entry schemas and column keys must match")
        lkeys, rkeys = self._mir.row_key, other._mir.row_key
        if not lkeys or [self.row.dtype[n] for n in lkeys] != [
            other.row.dtype[n] for n in rkeys
        ]:
            raise ValueError("row key types must match and keys must be nonempty")
        left = self._mir.core
        right = other._mir.core
        left_rows = (
            _project(
                left,
                [(n, left[_ROW][n]) for n in lkeys]
                + [("__left_row", left[_ROW]), ("__left_cells", left[_CELLS])],
            )
            .key_by(*lkeys)
            .distinct()
        )
        right_rows = (
            _project(
                right,
                [(n, right[_ROW][n]) for n in rkeys]
                + [("__right_row", right[_ROW]), ("__right_cells", right[_CELLS])],
            )
            .select_globals()
            .key_by(*rkeys)
            .distinct()
        )
        joined = left_rows.join(right_rows, row_join_type)
        rowfields = {
            n: joined[n] if n in lkeys else joined["__left_row"][n]
            for n in self.row.dtype
        }
        if not drop_right_row_fields:
            occupied = (
                set(self.row.dtype)
                | set(self.col.dtype)
                | set(self.entry.dtype)
                | set(self.globals.dtype)
            )
            for name in other.row.dtype:
                if name in rkeys:
                    continue
                output = name
                i = 0
                while output in occupied:
                    i += 1
                    output = f"{name}_{i}"
                occupied.add(output)
                rowfields[output] = joined["__right_row"][name]
        from . import array

        cols = self._mir.cols
        maximum = cols.aggregate(agg.max(cols[_CID]), _localize=False)
        offset = if_else(is_defined(maximum), maximum + 1, int64(0))
        rightcols = other._mir.cols
        rightcols = _project(
            rightcols, [(_CID, rightcols[_CID] + offset), (_COL, rightcols[_COL])]
        )

        def missing_cells(columns):
            ids = columns.aggregate(agg.collect(columns[_CID]), _localize=False)
            return ids.map(
                lambda cid: struct(
                    **{
                        _CID: cid,
                        _PRESENT: True,
                        _VALUE: struct(
                            **{n: missing(t) for n, t in self.entry.dtype.items()}
                        ),
                    }
                )
            )

        leftcells = if_else(
            is_defined(joined["__left_cells"]),
            joined["__left_cells"],
            missing_cells(cols),
        )
        shifted = joined["__right_cells"].map(
            lambda cell: struct(
                **{
                    _CID: cell[_CID] + offset,
                    _PRESENT: cell[_PRESENT],
                    _VALUE: cell[_VALUE] if len(self.entry.dtype) else struct(),
                }
            )
        )
        rightcells = if_else(
            is_defined(joined["__right_cells"]), shifted, missing_cells(rightcols)
        )
        cells = array([leftcells, rightcells]).flatmap(lambda cells: cells)
        core = _project(joined, [(_ROW, struct(**rowfields)), (_CELLS, cells)])
        core = self._sort_rows(core, lkeys)
        return MatrixTable(
            MatrixIR(
                core, cols.union(rightcols), row_key=lkeys, col_key=self._mir.col_key
            )
        )

    def make_table(self, separator="."):
        from .types import tstr

        if (
            len(self._mir.col_key) != 1
            or self.col_key.dtype[self._mir.col_key[0]] != tstr
        ):
            raise ValueError("make_table requires a single string column key")
        samples = self[self._mir.col_key[0]].collect()
        if any(s is None for s in samples) or len(set(samples)) != len(samples):
            raise ValueError("column keys must be defined and unique")
        en, cn = "__entries_" + uuid4().hex, "__cols_" + uuid4().hex
        localized = self.localize_entries(en, cn)
        fields = {n: localized[n] for n in self.row.dtype}
        for i, sample in enumerate(samples):
            for name in self.entry.dtype:
                output = sample + separator + name if name else sample
                if output in fields or output in self.globals.dtype:
                    raise ValueError(
                        "generated entry field collides with another field"
                    )
                fields[output] = localized[en][i][name]
        result = _project(localized, list(fields.items())).key_by(*self._mir.row_key)
        return self._with_globals(result)

    def show(self, n_rows=10, n_cols=10, *, handler=print, **options):
        matrix = self.head(n_rows, n_cols)
        columns = matrix.cols().collect()
        localized = matrix.localize_entries("__show_entries", "__show_cols")
        rows = localized.collect()
        headers = list(matrix.row.dtype)
        types = [str(t) for t in matrix.row.dtype.values()]
        for i, column in enumerate(columns):
            label = ".".join(str(column[k]) for k in matrix._mir.col_key) or str(i)
            for name, dtype in matrix.entry.dtype.items():
                headers.append(f"{label}.{name}")
                types.append(str(dtype))
        values = []
        for row in rows:
            cells = [row[name] for name in matrix.row.dtype]
            for entry in row["__show_entries"]:
                cells.extend(entry[name] if entry is not None else None for name in matrix.entry.dtype)
            values.append(["NA" if value is None else str(value) for value in cells])
        widths = [max(len(header), len(dtype), *(len(row[i]) for row in values))
                  for i, (header, dtype) in enumerate(zip(headers, types))]
        border = "+" + "+".join("-" * (width + 2) for width in widths) + "+\n"
        def line(fields):
            return "|" + "|".join(f" {value:>{width}} " for value, width in zip(fields, widths)) + "|\n"
        rendered = border + line(headers) + border + line(types) + border
        rendered += "".join(line(row) for row in values) + border
        return handler(rendered)

    def cache(self):
        raise NotImplementedError(
            "MatrixTable caching requires native Table result-cache support"
        )

    def persist(self, storage_level="MEMORY_AND_DISK"):
        raise NotImplementedError(
            "MatrixTable persistence levels require native Table result-cache support; use checkpoint"
        )

    def unpersist(self):
        return self

    def sample_rows(self, p, seed=None):
        raise NotImplementedError(
            "MatrixTable sampling requires stable native random expression support"
        )

    def sample_cols(self, p, seed=None):
        raise NotImplementedError(
            "MatrixTable sampling requires stable native random expression support"
        )

    def write(self, output, overwrite=False, stage_locally=False, _codec_spec=None):
        import os

        if type(overwrite) is not bool:
            raise TypeError("overwrite must be bool")
        if stage_locally or _codec_spec is not None:
            raise NotImplementedError("matrix codecs and staging are not implemented")
        root = _matrix_path(output)
        root.mkdir(parents=True, exist_ok=True)
        manifest = root / "_vail_matrix.json"
        lock = root / ".matrix.write.lock"
        try:
            fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            raise RuntimeError("another matrix writer holds the write lock") from None
        os.close(fd)
        try:
            if manifest.exists() and not overwrite:
                raise FileExistsError(output)
            generation = "generation-" + uuid4().hex
            folder = root / generation
            folder.mkdir()
            self._mir.core.write(folder / "core")
            descriptor = {
                "format": "vail-matrix",
                "version": 3,
                "generation": generation,
                "row_key": list(self._mir.row_key),
                "col_key": list(self._mir.col_key),
            }
            temp = root / (".matrix-" + uuid4().hex + ".json")
            with open(temp, "w") as stream:
                json.dump(descriptor, stream)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temp, manifest)
            directory = os.open(root, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            lock.unlink()

    def checkpoint(
        self, output, overwrite=False, stage_locally=False, _codec_spec=None, **options
    ):
        self.write(output, overwrite, stage_locally, _codec_spec)
        return read_matrix_table(output, **options)

    def describe(self, handler=print, *, widget=False):
        handler(
            "\n".join(
                f"{axis.capitalize()} fields: {self._axis_type(axis)}"
                for axis in ("global", "row", "col", "entry")
            )
            + f"\nRow key: {list(self._mir.row_key)}\nColumn key: {list(self._mir.col_key)}"
        )


def range_matrix_table(n_rows, n_cols, n_partitions=None):
    _validate_n(n_rows, "n_rows")
    _validate_n(n_cols, "n_cols")
    from . import range as array_range

    r = range_table(n_rows, n_partitions).key_by()
    c = range_table(n_cols).key_by()
    core = r.select(
        **{
            _ROW: struct(row_idx=r.idx),
            _CELLS: array_range(n_cols).map(
                lambda j: struct(**{_CID: int64(j), _PRESENT: True, _VALUE: struct()})
            ),
        }
    )
    cols = c.select(**{_CID: int64(c.idx), _COL: struct(col_idx=c.idx)})
    return MatrixTable(MatrixIR(core, cols, row_key=("row_idx",), col_key=("col_idx",)))


def read_matrix_table(
    path,
    *,
    _intervals=None,
    _filter_intervals=False,
    _n_partitions=None,
    _assert_type=None,
    _load_refs=True,
    _drop_rows=False,
    _drop_cols=False,
):
    from . import read_table

    root = _matrix_path(path)
    with open(root / "_vail_matrix.json") as stream:
        descriptor = json.load(stream)
    if descriptor.get("format") != "vail-matrix" or descriptor.get("version") != 3:
        raise ValueError("unsupported matrix descriptor")
    generation = descriptor.get("generation")
    if (
        not isinstance(generation, str)
        or not generation.startswith("generation-")
        or "/" in generation
        or "\\" in generation
        or ".." in generation
    ):
        raise ValueError("invalid matrix generation")
    folder = root / generation
    keys = tuple(descriptor["row_key"]), tuple(descriptor["col_key"])
    core = read_table(folder / "core", _load_refs=_load_refs)
    result = MatrixTable(MatrixIR(core, row_key=keys[0], col_key=keys[1]))
    if _assert_type is not None:
        raise NotImplementedError("matrix type assertions are not yet implemented")
    if _intervals is not None:
        from . import filter_intervals

        public = result.rows()
        kept = filter_intervals(public, _intervals)
        result = result.semi_join_rows(kept)
    elif _filter_intervals:
        raise ValueError("_filter_intervals requires _intervals")
    if _n_partitions is not None:
        result = result.repartition(_n_partitions)
    if _drop_rows:
        result = result.head(0)
    if _drop_cols:
        result = result.head(None, 0)
    return result


def unlocalize_entries(table, entries_field_name, cols_field_name, col_key):
    """Inverse of localization, retaining missing structs as filtered cells."""
    from . import len as length, range as array_range, case

    if (
        entries_field_name not in table._schema
        or cols_field_name not in table._global_schema
    ):
        raise ValueError("localized table lacks entry or column arrays")
    et, ct = table._schema[entries_field_name], table._global_schema[cols_field_name]
    if (
        not isinstance(et, tarray)
        or not isinstance(et.element_type, tstruct)
        or not isinstance(ct, tarray)
        or not isinstance(ct.element_type, tstruct)
    ):
        raise TypeError("localized entries and columns must be arrays of structs")
    if entries_field_name in table._keys:
        raise ValueError("entries array cannot be a row key")
    if isinstance(col_key, str):
        col_key = (col_key,)
    else:
        col_key = tuple(col_key)
    if any(n not in ct.element_type for n in col_key):
        raise ValueError("unknown column key field")
    globals = table.index_globals()
    col_values = (
        case()
        .when(is_defined(globals[cols_field_name]), globals[cols_field_name])
        .or_error("column array must be defined")
    )
    columns = Table.parallelize(
        array_range(length(col_values)).map(
            lambda j: struct(
                **{
                    _CID: int64(j),
                    _COL: col_values[j] if len(ct.element_type) else struct(),
                }
            )
        )
    )
    values = (
        case()
        .when(
            length(table[entries_field_name]) == length(col_values),
            table[entries_field_name],
        )
        .or_error("entry array length differs from column count")
    )
    # For empty entry structs, map validity before positional extraction. The
    # Table scalar extractor cannot yet reconstruct zero-field struct arrays.
    presence = values.map(lambda e: is_defined(e))
    cells = array_range(length(values)).map(
        lambda j: struct(
            **{
                _CID: int64(j),
                _PRESENT: presence[j],
                _VALUE: values[j] if len(et.element_type) else struct(),
            }
        )
    )
    core = _project(
        table,
        [
            (
                _ROW,
                struct(
                    **{n: table[n] for n in table._schema if n != entries_field_name}
                ),
            ),
            (_CELLS, cells),
        ],
    )
    core = core.select_globals(
        **{n: globals[n] for n in table._global_schema if n != cols_field_name}
    )
    return MatrixTable(MatrixIR(core, columns, row_key=table._keys, col_key=col_key))


class GroupedMatrixTable:
    """Deferred grouping on one matrix axis, including row/column annotations."""

    def __init__(self, matrix, axis, exprs, named):
        fields = []
        for value in exprs:
            expr = matrix[value] if isinstance(value, str) else value
            if not isinstance(expr, Expression) or expr._selection_name is None:
                raise ValueError("positional grouping requires field references")
            fields.append((expr._selection_name, matrix._validate(expr, axis)))
        fields += [(n, matrix._validate(v, axis)) for n, v in named.items()]
        if not fields or len({n for n, _ in fields}) != len(fields):
            raise ValueError("grouping keys must be nonempty and distinct")
        self._matrix, self._axis, self._keys = matrix, axis, fields
        self._axis_fields, self._entry_fields = {}, {}
        self._partitions = None

    def _copy(self):
        result = object.__new__(type(self))
        result.__dict__.update(self.__dict__)
        result._axis_fields, result._entry_fields = (
            dict(self._axis_fields),
            dict(self._entry_fields),
        )
        return result

    def partition_hint(self, n):
        _validate_n(n, "n", positive=True)
        result = self._copy()
        result._partitions = n
        return result

    def aggregate_rows(self, **fields):
        if self._axis != "row":
            raise ValueError("aggregate_rows requires grouped rows")
        result = self._copy()
        result._axis_fields.update(fields)
        return result

    def aggregate_cols(self, **fields):
        if self._axis != "col":
            raise ValueError("aggregate_cols requires grouped columns")
        result = self._copy()
        result._axis_fields.update(fields)
        return result

    def aggregate_entries(self, **fields):
        result = self._copy()
        result._entry_fields.update(fields)
        return result

    def aggregate(self, **fields):
        return self.aggregate_entries(**fields).result()

    def result(self):
        from . import agg

        m, axis = self._matrix, self._axis
        groupnames = tuple(n for n, _ in self._keys)
        if axis == "row":
            base = m._mir.core
            keys = {n: m._lower(expr, base) for n, expr in self._keys}
            fields = {
                n: m._lower(m._validate(value, "row", aggregate=True), base)
                for n, value in self._axis_fields.items()
            }

            def cell_result(cell):
                def prepare(expr):
                    expr = m._validate(expr, "entry", aggregate=True)

                    def fix(node, inside=False):
                        if isinstance(node, list):
                            return [fix(v, inside) for v in node]
                        if not isinstance(node, dict) or node.get("op") == "literal":
                            return node
                        if node.get("op") in _AGG_OPS and _has_aggregate(node):
                            original = {k: fix(v, True) for k, v in node.items()}
                            return {
                                "op": "agg_filter",
                                "predicate": cell[_PRESENT].to_ir(),
                                "body": original,
                            }
                        if node.get("op") == "matrix_field" and not inside:
                            if node["axis"] == "col":
                                raw = _expression(
                                    m._axis_type("col")[node["name"]], m, node, expr
                                )
                                return agg.take(raw, 1)[0].to_ir()
                            if node["axis"] not in ("global",):
                                raise ExpressionException(
                                    "grouped fields must be aggregated or belong to the fixed axis"
                                )
                        return {k: fix(v, inside) for k, v in node.items()}

                    lowered = _expression(expr.dtype, m, fix(expr.to_ir()), expr)
                    return m._lower_cell(lowered, base, cell)

                value = struct(**{n: prepare(v) for n, v in self._entry_fields.items()})
                return struct(
                    **{_CID: agg.take(cell[_CID], 1)[0], _PRESENT: True, _VALUE: value}
                )

            cells = agg.array_agg(cell_result, base[_CELLS])
            grouped = base.group_by(**keys).aggregate(**fields, **{_CELLS: cells})
            if self._partitions is not None:
                grouped = grouped.repartition(self._partitions)
            owned = grouped[_CELLS]
            aligned = m._mir.column_records.map(
                lambda col: owned.filter(lambda cell: cell[_CID] == col[_CID])[0]
            )
            core = _project(
                grouped,
                [
                    (
                        _ROW,
                        struct(**{n: grouped[n] for n in groupnames + tuple(fields)}),
                    ),
                    (_CELLS, aligned),
                ],
            )
            core = m._sort_rows(core, groupnames)
            return MatrixTable(
                MatrixIR(core, m._mir.cols, row_key=groupnames, col_key=m._mir.col_key)
            )
        from . import range as array_range, len as length
        from .matrix_list_aggregates import reduce_list

        base = m._mir.cols
        keys = {n: m._lower(expr, base) for n, expr in self._keys}
        fields = {
            n: m._lower(m._validate(v, "col", aggregate=True), base)
            for n, v in self._axis_fields.items()
        }
        grouped = base.group_by(**keys).aggregate(
            **fields, members=agg.collect(base[_CID])
        )
        grouped = grouped.order_by(*(grouped[n] for n in groupnames)).key_by()
        groups = grouped.aggregate(agg.collect(grouped.row), _localize=False)
        columns = Table.parallelize(
            array_range(length(groups)).map(
                lambda j: struct(
                    **{
                        _CID: int64(j),
                        _COL: struct(
                            **{n: groups[j][n] for n in groupnames + tuple(fields)}
                        ),
                    }
                )
            )
        )
        core = m._mir.core
        contexts = m._row_contexts(core)

        def cell(j):
            members = groups[j].members
            selected = contexts.filter(
                lambda ctx: length(members.filter(lambda cid: cid == ctx.column_id)) > 0
            )
            outside = struct(
                row=core[_ROW], col=struct(), entry=struct(), bindings=struct()
            )
            value = struct(
                **{
                    n: reduce_list(
                        m,
                        m._validate(v, "entry", aggregate=True),
                        core,
                        selected,
                        outside,
                        bound={j.to_ir()["name"]: j.dtype},
                    )
                    for n, v in self._entry_fields.items()
                }
            )
            return struct(**{_CID: int64(j), _PRESENT: True, _VALUE: value})

        return MatrixTable(
            MatrixIR(
                core.annotate(**{_CELLS: array_range(length(groups)).map(cell)}),
                columns,
                row_key=m._mir.row_key,
                col_key=groupnames,
            )
        )


def to_matrix_table(
    table, row_key, col_key, row_fields=(), col_fields=(), n_partitions=None
):
    """Group coordinate rows into matrix axes, retaining absent coordinates."""
    from . import agg

    def names(value):
        return (value,) if isinstance(value, str) else tuple(value)

    row_key, col_key, row_fields, col_fields = map(
        names, (row_key, col_key, row_fields, col_fields)
    )
    assigned = row_key + col_key + row_fields + col_fields
    if not row_key or not col_key or len(set(assigned)) != len(assigned):
        raise ValueError(
            "row and column keys must be nonempty and field assignments disjoint"
        )
    if any(n not in table._schema for n in assigned):
        raise ValueError("unknown matrix field")
    entry_names = [n for n in table._schema if n not in assigned]
    if not entry_names:
        raise ValueError("no fields remain as entry fields")
    if n_partitions is not None:
        _validate_n(n_partitions, "n_partitions", positive=True)
    rk, ck = "__row_group_" + uuid4().hex, "__col_group_" + uuid4().hex
    row_query = struct(**{n: table[n] for n in row_key})
    col_query = struct(**{n: table[n] for n in col_key})
    from . import range as array_range, len as length

    row_value = struct(**{n: table[n] for n in row_key + row_fields})
    col_value = struct(**{n: table[n] for n in col_key + col_fields})
    entry = struct(**{n: table[n] for n in entry_names})
    cols = table.group_by(**{ck: col_query}).aggregate(
        **{_COL: agg.take(col_value, 1)[0]}
    )
    cols = cols.order_by(*(cols[_COL][n] for n in col_key)).key_by()
    column_values = cols.aggregate(agg.collect(cols[_COL]), _localize=False)
    columns = Table.parallelize(
        array_range(length(column_values)).map(
            lambda j: struct(**{_CID: int64(j), _COL: column_values[j]})
        )
    )
    lookup = _project(
        columns,
        [(ck, struct(**{n: columns[_COL][n] for n in col_key})), (_CID, columns[_CID])],
    ).key_by(ck)
    # Last duplicate coordinate wins, as in Hail's dictionary construction.
    coordinates = table.group_by(**{rk: row_query, ck: col_query}).aggregate(
        **{_ROW: agg.take(row_value, 1)[0], _ENTRY: agg.collect(entry)[-1]}
    )
    coordinates = coordinates.annotate(**{_CID: lookup.index(coordinates[ck])[_CID]})
    records = coordinates.group_by(**{rk: coordinates[rk]}).aggregate(
        **{
            _ROW: agg.take(coordinates[_ROW], 1)[0],
            _CELLS: agg.collect(
                struct(**{_CID: coordinates[_CID], _VALUE: coordinates[_ENTRY]})
            ),
        }
    )
    values = records[_CELLS]

    def cell(j):
        found = values.filter(lambda c: c[_CID] == int64(j))
        return struct(
            **{
                _CID: int64(j),
                _PRESENT: length(found) > 0,
                _VALUE: if_else(
                    length(found) > 0, found[0][_VALUE], missing(entry.dtype)
                ),
            }
        )

    core = _project(
        records,
        [(_ROW, records[_ROW]), (_CELLS, array_range(length(column_values)).map(cell))],
    )
    globals = table.index_globals()
    core = core.select_globals(**{n: globals[n] for n in table._global_schema})
    core = MatrixTable._sort_rows(core, row_key)
    if n_partitions is not None:
        core = core.repartition(n_partitions)
    return MatrixTable(MatrixIR(core, columns, row_key=row_key, col_key=col_key))


def to_matrix_table_row_major(
    table, columns, entry_field_name=None, col_field_name="col"
):
    from . import array
    from .types import tstr

    if (
        not isinstance(columns, (list, tuple))
        or not columns
        or any(not isinstance(n, str) or n not in table._schema for n in columns)
    ):
        raise ValueError("columns must be nonempty field names")
    if len(set(columns)) != len(columns):
        raise ValueError("duplicate entry columns")
    types = [table._schema[n] for n in columns]
    if any(t != types[0] for t in types):
        raise ValueError("entry columns must have identical types")
    if isinstance(types[0], tstruct):
        if entry_field_name is not None:
            raise ValueError("struct entries require entry_field_name=None")
        entry_type = types[0]
        values = [table[n] for n in columns]
    else:
        if not isinstance(entry_field_name, str) or not entry_field_name:
            raise ValueError("scalar entries require an entry field name")
        entry_type = tstruct(**{entry_field_name: types[0]})
        values = [struct(**{entry_field_name: table[n]}) for n in columns]
    row_names = [n for n in table._schema if n not in columns or n in table._keys]
    occupied = set(row_names) | set(table._global_schema)
    if (
        col_field_name in occupied
        or set(entry_type).intersection(occupied)
        or col_field_name in entry_type
    ):
        raise ValueError("matrix field names collide")
    en, cn = "__matrix_entries_" + uuid4().hex, "__matrix_cols_" + uuid4().hex
    localized = _project(
        table, [(n, table[n]) for n in row_names] + [(en, array(values))]
    ).key_by(*table._keys)
    localized = localized.annotate_globals(
        **{
            cn: literal(
                [{col_field_name: n} for n in columns],
                tarray(tstruct(**{col_field_name: tstr})),
            )
        }
    )
    return unlocalize_entries(localized, en, cn, (col_field_name,))
