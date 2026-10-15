"""Table display adapted from Hail's Python ASCII formatter.

Copyright (c) 2015-2023, Hail Authors. MIT license; see licenses/HAIL.txt.
Collection and value formatting are Vail adapters, independent of the JVM.
"""
from dataclasses import dataclass
import codecs
import math
import shutil

from .types import (tarray, tset, tdict, ttuple, tstruct, tstr,
                    tinterval, tndarray, tint32, tint64, tfloat32, tfloat64)


def _text(value, budget):
    # A UTF-8 codepoint occupies at most four bytes. Copy only enough bytes to
    # supply the requested output prefix, withholding a partial final codepoint.
    data = value.as_buffer()
    prefix = data.slice(0, min(data.size, 4 * budget)).to_pybytes()
    return codecs.getincrementaldecoder('utf-8')().decode(prefix, final=len(prefix) == data.size)


def _prepare(dtype):
    """Compile ordered child formatters once; consume Arrow scalars lazily."""
    if dtype == tstr:
        escapes = {'\\': '\\\\', '"': '\\"', '\n': '\\n', '\r': '\\r', '\t': '\\t'}
        def render(value, budget):
            yield '"'
            for char in _text(value, budget):
                yield escapes.get(char, char)
            yield '"'
    elif dtype in (tfloat32, tfloat64):
        def render(value, budget):
            number = value.as_py()
            yield ('NaN' if math.isnan(number) else
                   'Infinity' if number == math.inf else
                   '-Infinity' if number == -math.inf else format(number, '.2e'))
    elif isinstance(dtype, (tarray, tset)):
        child = _prepare(dtype.element_type)
        left, right = ('[', ']') if isinstance(dtype, tarray) else ('{', '}')
        def render(value, budget):
            yield left
            for index, item in enumerate(value.values):
                if index:
                    yield ','
                yield from child(item, budget)
            yield right
    elif isinstance(dtype, tdict):
        key, item = _prepare(dtype.key_type), _prepare(dtype.value_type)
        def render(value, budget):
            yield '{'
            for index, entry in enumerate(value.values):
                if index:
                    yield ','
                yield from key(entry[0], budget)
                yield ':'
                yield from item(entry[1], budget)
            yield '}'
    elif isinstance(dtype, (ttuple, tstruct)):
        types = dtype.types if isinstance(dtype, ttuple) else tuple(ty for _, ty in dtype.items())
        children = tuple(_prepare(ty) for ty in types)
        def render(value, budget):
            yield '('
            for index, child in enumerate(children):
                if index:
                    yield ','
                yield from child(value[index], budget)
            yield ')'
    elif isinstance(dtype, tndarray):
        from .arrow import _logical_converter
        convert = _logical_converter(dtype)
        def render(value, budget):
            # Arrow numeric storage is already contiguous. NumPy's normal
            # summary formatting can inspect its edges without localizing a
            # Python list of every element.
            data = value['__vail_ndarray_data'].values
            shape = tuple(value['__vail_ndarray_shape'].values.to_pylist())
            if data.null_count and dtype.element_type in (tint32, tint64):
                yield str(convert(value.as_py()))
            else:
                yield str(data.to_numpy(zero_copy_only=False).reshape(shape))
    elif isinstance(dtype, tinterval) and dtype.point_type in (
        tint32, tint64, tfloat32, tfloat64, tstr
    ):
        point_type = dtype.point_type
        def render(value, budget):
            end_inclusive = value['__vail_interval_end_inclusive'].as_py()
            if type(end_inclusive) is not bool:
                raise TypeError('interval inclusivity flags must be bool')
            yield '(' if value['__vail_interval_start_exclusive'].as_py() else '['
            for index, name in enumerate(('__vail_interval_start', '__vail_interval_end')):
                if index:
                    yield '-'
                point = value[name]
                yield (_text(point, budget) if point_type == tstr and point.is_valid
                       else str(point.as_py()))
            yield ']' if end_inclusive else ')'
    else:
        from .arrow import _logical_converter
        convert = _logical_converter(dtype)
        def render(value, budget):
            yield str(convert(value.as_py()))

    def nullable(value, budget):
        if value is None or not value.is_valid:
            yield 'NA'
        else:
            yield from render(value, budget)
    return nullable


def _bounded(value, render, limit):
    pieces, size = [], 0
    for piece in render(value, limit + 1):
        pieces.append(piece[:limit + 1 - size])
        size += len(pieces[-1])
        if size > limit:
            return ''.join(pieces)[:limit - 3] + '...'
    return ''.join(pieces)


@dataclass(frozen=True)
class _Column:
    name: str
    dtype: object
    path: tuple
    render: object
    right_align: bool


def _columns(dtype):
    columns = []
    def visit(name, ty, path):
        if isinstance(ty, tstruct):
            for index, (field, child) in enumerate(ty.items()):
                visit(f'{name}.{field}' if name else field, child, (*path, index))
        else:
            columns.append(_Column(name, ty, path, _prepare(ty),
                                   ty in (tint32, tint64, tfloat32, tfloat64)))
    visit('', dtype, ())
    return tuple(columns)


class TableDisplay:
    def __init__(self, table, n, width, truncate, types):
        columns, lines = shutil.get_terminal_size((80, 10))
        self.n = min(max(10, lines - 20), 100) if n is None else n
        self.width = max(columns if width is None else width, 8)
        self.truncate = min(max(truncate, 4), self.width - 4) if truncate else self.width - 4
        self.types = types
        self.table = table
        self._data = None

    def __str__(self):
        return self._ascii_str()

    def data(self):
        if self._data is None:
            import pyarrow as pa
            columns = _columns(self.table._schema)
            values = (self.table.to_arrow() if self.n < 0 else
                      pa.table(self.table._native.sample_arrow(self.n + 1)))
            has_more = self.n >= 0 and values.num_rows > self.n
            if self.n >= 0:
                # The sentinel proves cardinality; none of its cells are read.
                values = values.slice(0, self.n)
            roots = tuple(values.columns)
            rows = []
            for row in range(values.num_rows):
                cells = []
                for column in columns:
                    value = roots[column.path[0]][row]
                    for index in column.path[1:]:
                        if not value.is_valid:
                            value = None
                            break
                        value = value[index]
                    cells.append(_bounded(value, column.render, self.truncate))
                rows.append(cells)
            self._data = rows, has_more, columns
        return self._data

    def _ascii_str(self):
        def trunc(text):
            return text[:self.truncate - 3] + '...' if len(text) > self.truncate else text

        rows, has_more, columns = self.data()
        fields = [trunc(column.name) for column in columns]
        types = [trunc(str(column.dtype)) for column in columns] if self.types else [''] * len(columns)
        align = [column.right_align for column in columns]
        widths = [max(len(name), len(ty), max((len(row[i]) for row in rows), default=0))
                  for i, (name, ty) in enumerate(zip(fields, types))]
        blocks, start, width = [], 0, 0
        for index, cell_width in enumerate(widths):
            added = cell_width + (4 if index == start else 3)
            if index > start and width + added > self.width:
                blocks.append((start, index))
                start, width, added = index, 0, cell_width + 4
            width += added
        blocks.append((start, len(columns)))

        def line(values, widths, align):
            if not values:
                return '||\n'
            cells = [value.rjust(width) if right else value.ljust(width)
                     for value, width, right in zip(values, widths, align)]
            return '| ' + ' | '.join(cells) + ' |\n'

        output = []
        for block_index, (start, end) in enumerate(blocks):
            if block_index:
                output.append('\n')
            block_widths, block_align = widths[start:end], align[start:end]
            rule = '+-' + '-+-'.join('-' * width for width in block_widths) + '-+\n' if block_widths else '++\n'
            output.extend((rule, line(fields[start:end], block_widths, block_align), rule))
            if self.types:
                output.extend((line(types[start:end], block_widths, block_align), rule))
            output.extend(line(row[start:end], block_widths, block_align) for row in rows)
            output.append(rule)
        if has_more:
            output.append(f"showing top {len(rows)} {'row' if len(rows) == 1 else 'rows'}\n")
        return ''.join(output)
