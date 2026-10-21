"""Delimited matrix input, following Hail's import_matrix_table specification.

https://hail.is/docs/0.2/methods/impex.html#hail.methods.import_matrix_table
Only the first two records per file are collected to establish column metadata;
row values remain in the native, lazy expression plan.
"""
from __future__ import annotations


def import_matrix_table(paths, row_fields=None, row_key=(), entry_type=None,
                        missing='NA', min_partitions=None, no_header=False,
                        force_bgz=False, sep=None, delimiter=None, comment=()):
    from . import (import_lines, tint32, tint64, tfloat32, tfloat64, tstr, tarray,
                   tstruct, literal, struct, case, len as length, range as hrange,
                   is_missing, str as hstr)
    from .text import _paths, split_line, _parse_primitive, _integer_fits
    row_fields = dict(row_fields or {})
    entry_type = tint32 if entry_type is None else entry_type
    row_key = [row_key] if isinstance(row_key, str) else list(row_key)
    comments = [comment] if isinstance(comment, str) else list(comment)
    if sep is not None:
        if delimiter is not None:
            raise ValueError('expecting either sep or delimiter but received both')
        delimiter = sep
    delimiter = '\t' if delimiter is None else delimiter
    if not isinstance(delimiter, str) or len(delimiter) != 1:
        raise ValueError('delimiter or sep must be a single character')
    if missing in delimiter:
        raise ValueError(f'Missing value {missing} contains delimiter {delimiter}')
    allowed = (tint32, tint64, tfloat32, tfloat64, tstr)
    for name, ty in [*row_fields.items(), ('entry', entry_type)]:
        if ty not in allowed:
            raise ValueError(f'import_matrix_table expects primitive numeric or str types: {name!r} had type {ty}')
    add_row_id = not row_key
    if add_row_id and 'row_id' in row_fields:
        raise ValueError("import_matrix_table reserves the field name 'row_id'")
    if any(key not in row_fields for key in row_key):
        raise ValueError('row_key must be a subset of row fields')
    paths = _paths(paths)

    def filtered(table):
        keep = length(table.text) != 0
        for pattern in comments:
            match = table.text.startswith(pattern) if len(pattern) == 1 else table.text.matches(pattern, full_match=False)
            keep = keep & ~match
        return table.filter(keep)

    starts = []
    for path in paths:
        starts.append(filtered(import_lines(path, force_bgz=force_bgz)).take(2))
    nr = len(row_fields)
    if no_header:
        names = [f'f{i}' for i in range(nr)]
        columns = list(range(max(0, len(starts[0][0].text.split(delimiter)) - nr))) if starts[0] else []
        header = None
    else:
        # An empty physical header represents a zero-column matrix.
        if not starts[0]:
            names, columns, header = [], [], ''
        else:
            header = starts[0][0].text
            fields = header.split(delimiter)
            data_size = len(starts[0][1].text.split(delimiter)) if len(starts[0]) > 1 else len(fields)
            if data_size == len(fields):
                names, columns = fields[:nr], fields[nr:]
            elif data_size == len(fields) + nr:
                names, columns = [f'f{i}' for i in range(nr)], fields
            else:
                raise ValueError('expected the header line to match the number of data fields')
        for records in starts[1:]:
            other = records[0].text if records else ''
            if other != header:
                if len(other.split(delimiter)) != len(header.split(delimiter)):
                    raise ValueError('invalid header: lengths of headers differ')
                raise ValueError('invalid header: expected elements to be identical for all input paths')
    if len(names) != nr or set(names) != set(row_fields) or len(set(names)) != len(names):
        raise ValueError("row fields found in header do not match 'row_fields'")
    table = filtered(import_lines(paths, min_partitions=min_partitions, force_bgz=force_bgz))
    if header is not None:
        table = table.filter(table.text != header)
    table = table.select(data=split_line(table.text, delimiter, missing=[missing], regex=False)).add_index('row_id')
    table = table.annotate(data=case().when(length(table.data) >= nr, table.data)
                           .or_error('unexpected end of line while reading row field'))
    table = table.annotate(data=case().when(length(table.data) >= nr + len(columns), table.data)
                           .or_error('unexpected end of line while reading entries'))

    def parse(value, ty, description):
        if ty in (tint32, tint64):
            bounds = ('2147483647', '2147483648') if ty == tint32 else ('9223372036854775807', '9223372036854775808')
            value = case().when(is_missing(value) | _integer_fits(value, *bounds), value).or_error(
                f'error parsing value into {ty} at ' + description)
        return _parse_primitive(value, ty)

    # Field names come from the header, so dict insertion order cannot alter parsing.
    rows = {name: parse(table.data[i], row_fields[name], f"row field '{name}'") for i, name in enumerate(names)}
    col_type = tint32 if no_header and columns else tstr
    col_ids = literal(columns, tarray(col_type))
    entries = hrange(nr, nr + len(columns)).map(lambda i: struct(x=parse(
        table.data[i], entry_type, literal("column id '") + hstr(col_ids[i - nr]) + "'")))
    table = table.select(**rows, **({'row_id': table.row_id} if add_row_id else {}), entries=entries)
    table = table.key_by(*(['row_id'] if add_row_id else row_key))
    table = table.annotate_globals(cols=literal([{'col_id': x} for x in columns], tarray(tstruct(col_id=col_type))))
    return table._unlocalize_entries('entries', 'cols', ['col_id'], _aligned=True)


def export_expression(expr, output, delimiter='\t', missing='NA', header=True):
    """Stream a matrix entry expression as delimited rows in the native sink."""
    from uuid import uuid4
    from . import literal, str as hstr, if_else, is_missing, array, tarray, tstr
    from .matrixtable import MatrixTable, _axes
    from .text import _local_path
    if not isinstance(delimiter, str) or len(delimiter) != 1:
        raise ValueError('delimiter must be a single character')
    if not isinstance(missing, str) or type(header) is not bool:
        raise TypeError('missing must be a string and header must be bool')
    source = expr.source
    if not isinstance(source, MatrixTable) or _axes(expr.to_ir()) != {'row', 'col'}:
        raise NotImplementedError('expression export currently requires a matrix entry expression')
    en, cn, value = ['__export_' + uuid4().hex for _ in range(3)]
    matrix = source.select_entries(**{value: expr})
    keys = list(matrix.col_key.dtype)
    cols = matrix.cols().select().collect()
    ids = [str(col[keys[0]]) if keys else str(i) for i, col in enumerate(cols)]
    row_keys = list(matrix.row_key.dtype)
    localized = matrix.localize_entries(en, cn).key_by()

    def text(value):
        return if_else(is_missing(value), missing, hstr(value))

    row_values = array([text(localized[key]) for key in row_keys]) if row_keys else literal([], tarray(tstr))
    entry_values = localized[en].map(lambda entry: text(entry[value]))
    line = literal(delimiter).join(row_values.extend(entry_values))
    name = delimiter.join([*row_keys, *ids])
    table = localized.select(**{name: line})
    table._native.export_text(_local_path(output, allow_compressed=True), 'text', {
        'delimiter': ord(delimiter), 'has_header': header, 'null_value': missing,
    })
