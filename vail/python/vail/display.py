"""Table display adapted from Hail's Python ASCII formatter.

Copyright (c) 2015-2023, Hail Authors. MIT license; see licenses/HAIL.txt.
Collection and value formatting are Vail adapters, independent of the JVM.
"""
import itertools
import math
import shutil

from .types import tarray, tset, tdict, ttuple, tstruct, tint32, tint64, tfloat32, tfloat64


def _is_numeric(dtype):
    return dtype in (tint32, tint64, tfloat32, tfloat64)


def _value(value, dtype):
    if value is None:
        return 'NA'
    if dtype in (tfloat32, tfloat64):
        if math.isnan(value):
            return 'NaN'
        if math.isinf(value):
            return 'Infinity' if value > 0 else '-Infinity'
        return format(value, '.2e')
    if isinstance(value, str):
        escaped = value.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n').replace('\r', '\\r').replace('\t', '\\t')
        return '"' + escaped + '"'
    if isinstance(dtype, (tarray, tset)):
        bounds = ('[', ']') if isinstance(dtype, tarray) else ('{', '}')
        return bounds[0] + ','.join(_value(v, dtype.element_type) for v in value) + bounds[1]
    if isinstance(dtype, tdict):
        return '{' + ','.join(_value(k, dtype.key_type) + ':' + _value(v, dtype.value_type) for k, v in value.items()) + '}'
    if isinstance(dtype, ttuple):
        return '(' + ','.join(_value(v, t) for v, t in zip(value, dtype.types)) + ')'
    if isinstance(dtype, tstruct):
        return '(' + ','.join(_value(value[k], dtype[k]) for k in dtype) + ')'
    return str(value)


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
            table = self.table.flatten()
            dtype = table.row.dtype
            if self.n < 0:
                rows = table.collect()
            else:
                from .arrow import decode_rows
                rows = decode_rows(table._native.sample_arrow(self.n + 1), dtype)
            has_more = self.n >= 0 and len(rows) > self.n
            if self.n >= 0:
                rows = rows[:self.n]
            self._data = ([{k: _value(row[k], dtype[k]) for k in dtype} for row in rows], has_more, dtype)
        return self._data

    def _ascii_str(self):
                truncate = self.truncate
                types = self.types

                def trunc(s):
                    if len(s) > truncate:
                        return s[: truncate - 3] + "..."
                    return s

                rows, has_more, dtype = self.data()
                fields = list(dtype)
                trunc_fields = [trunc(f) for f in fields]
                n_fields = len(fields)

                type_strs = [trunc(str(dtype[f])) for f in fields] if types else [''] * len(fields)
                right_align = [_is_numeric(dtype[f]) for f in fields]

                rows = [[trunc(row[f]) for f in fields] for row in rows]

                def max_value_width(i):
                    return max(itertools.chain([0], (len(row[i]) for row in rows)))

                column_width = [max(len(trunc_fields[i]), len(type_strs[i]), max_value_width(i)) for i in range(n_fields)]

                column_blocks = []
                start = 0
                i = 1
                w = column_width[0] + 4 if column_width else 0
                while i < n_fields:
                    w = w + column_width[i] + 3
                    if w > self.width:
                        column_blocks.append((start, i))
                        start = i
                        w = column_width[i] + 4
                    i = i + 1
                column_blocks.append((start, i))

                def format_hline(widths):
                    if not widths:
                        return "++\n"
                    return '+-' + '-+-'.join(['-' * w for w in widths]) + '-+\n'

                def pad(v, w, ra):
                    e = w - len(v)
                    if ra:
                        return ' ' * e + v
                    else:
                        return v + ' ' * e

                def format_line(values, widths, right_align):
                    if not values:
                        return "||\n"
                    values = map(pad, values, widths, right_align)
                    return '| ' + ' | '.join(values) + ' |\n'

                s = ''
                first = True
                for start, end in column_blocks:
                    if first:
                        first = False
                    else:
                        s += '\n'

                    block_column_width = column_width[start:end]
                    block_right_align = right_align[start:end]
                    hline = format_hline(block_column_width)

                    s += hline
                    s += format_line(trunc_fields[start:end], block_column_width, block_right_align)
                    s += hline
                    if types:
                        s += format_line(type_strs[start:end], block_column_width, block_right_align)
                        s += hline
                    for row in rows:
                        _row = row[start:end]
                        s += format_line(_row, block_column_width, block_right_align)
                    s += hline

                if has_more:
                    n_rows = len(rows)
                    s += f"showing top {n_rows} {'row' if n_rows == 1 else 'rows'}\n"

                return s
