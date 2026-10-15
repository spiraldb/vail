"""Text sources and sinks. Python builds IR; DataFusion reads and parses rows."""
import glob
import json
import os
from collections.abc import Mapping
from urllib.parse import unquote, urlparse

from .collections import _function
from .expressions import Expression, cast, literal
from .types import (tarray, tstr, tint64, tstruct, tint32, tfloat32, tfloat64,
                    tbool, type_to_wire, type_from_wire)

_COMPRESSED_SUFFIXES = ('.gz', '.bgz', '.bz2', '.xz', '.zst')


def _local_path(path):
    path = os.fspath(path)
    if path.startswith('file:'):
        parsed = urlparse(path)
        if parsed.netloc not in ('', 'localhost'):
            raise NotImplementedError('text IO currently supports local paths')
        path = unquote(parsed.path)
    if '://' in path:
        raise NotImplementedError('text IO currently supports local paths')
    if path.endswith(_COMPRESSED_SUFFIXES):
        raise NotImplementedError('compressed text IO is not implemented')
    return os.path.abspath(path)


def _paths(paths):
    paths = [paths] if isinstance(paths, (str, os.PathLike)) else list(paths)
    result = []
    for path in paths:
        path = _local_path(path)
        matches = sorted(glob.glob(path)) if glob.has_magic(path) else [path]
        if not matches:
            raise FileNotFoundError(path)
        result.extend(os.path.abspath(match) for match in matches)
    if not result:
        raise ValueError('at least one input path is required')
    if any(path.endswith(_COMPRESSED_SUFFIXES) for path in result):
        raise NotImplementedError('compressed text sources are not implemented')
    return result


def _byte(value, name):
    if not isinstance(value, str) or len(value.encode('utf-8')) != 1:
        raise ValueError(f'{name} must be a single ASCII character')
    if value in ('\r', '\n', '\x00'):
        raise ValueError(f'{name} must not be a newline or NUL')
    return value


def _schema_wire(schema):
    if not isinstance(schema, tstruct) or not len(schema):
        raise TypeError('schema must be a nonempty tstruct')
    return [(name, type_to_wire(ty)) for name, ty in schema.items()]


def _read(paths, schema, format, options):
    from . import Table, _native_session
    native = _native_session().read_text(_schema_wire(schema), paths, format, options)
    return Table(native, schema, ())


def import_lines(paths, min_partitions=None, force_bgz=False, force=False, file_per_partition=False):
    """Lazily scan UTF-8 physical lines, retaining blank lines and file names."""
    from . import _validate_n
    if force_bgz or force:
        raise NotImplementedError('BGZF and gzip text scans are not implemented')
    if type(file_per_partition) is not bool:
        raise TypeError('file_per_partition must be bool')
    paths = _paths(paths)
    if min_partitions is not None:
        _validate_n(min_partitions, 'min_partitions', positive=True)
        if file_per_partition and min_partitions > len(paths):
            raise ValueError('min_partitions exceeds the number of files with file_per_partition=True')
    table = _read(paths, tstruct(file=tstr, text=tstr), 'lines', {})
    if min_partitions is not None and min_partitions > len(paths):
        table = table.repartition(min_partitions)
    return table


def _split_line(value, delimiter, quote, missing, expected_fields=0):
    return Expression(tarray(tstr), value.source, {
        'op': 'parse_csv_line', 'value': value.to_ir(), 'delimiter': delimiter,
        'quote': quote, 'missing': missing, 'expected_fields': expected_fields,
    })


def split_line(value, delimiter, missing=None, quote=None, regex=False):
    """Build Hail's private single-line delimited parsing expression."""
    _byte(delimiter, 'delimiter')
    if regex:
        raise NotImplementedError('regex-delimited line parsing is not implemented')
    if quote is not None:
        _byte(quote, 'quote')
        if quote == delimiter:
            raise ValueError('quote and delimiter must differ')
    if isinstance(missing, Expression):
        ir = missing.to_ir()
        if missing.dtype != tarray(tstr) or ir.get('op') != 'literal':
            raise NotImplementedError('expression-valued missing markers require a literal array')
        from . import eval
        missing = eval(missing)
    markers = ['NA'] if missing is None else ([missing] if isinstance(missing, str) else list(missing))
    if any(not isinstance(marker, str) for marker in markers):
        raise TypeError('missing markers must be strings')
    return _split_line(value, delimiter, quote, markers)


def import_table(paths, key=None, min_partitions=None, impute=False, no_header=False,
                 comment=(), delimiter='\t', missing='NA', types=None, quote=None,
                 skip_blank_lines=False, force_bgz=False, filter=None, find_replace=None,
                 force=False, source_file_field=None):
    """Import single-line delimited records using a DataFusion CSV expression.

    Header discovery evaluates one row, as Hail does. The remaining table is lazy.
    Regex delimiters and type imputation are not implemented.
    """
    delimiter = _byte(delimiter, 'delimiter')
    if quote is not None:
        quote = _byte(quote, 'quote')
        if quote == delimiter:
            raise ValueError('quote and delimiter must differ')
    if impute:
        raise NotImplementedError('Hail type imputation is not implemented; supply types explicitly')
    if type(no_header) is not bool or type(skip_blank_lines) is not bool:
        raise TypeError('no_header and skip_blank_lines must be bool')
    types = {} if types is None else types
    if not isinstance(types, Mapping):
        raise TypeError('types must be a mapping')
    missing = [missing] if isinstance(missing, str) else list(missing)
    if any(not isinstance(value, str) for value in missing):
        raise TypeError('missing must contain strings')
    comments = [comment] if isinstance(comment, str) else list(comment)
    if source_file_field is not None and (not isinstance(source_file_field, str) or not source_file_field):
        raise ValueError('source_file_field must be a nonempty string')
    from . import _validate_n, eval as eval_expression
    paths = _paths(paths)
    if min_partitions is not None:
        _validate_n(min_partitions, 'min_partitions', positive=True)
    table = import_lines(paths, force_bgz=force_bgz, force=force)
    if filter is not None:
        table = table.filter(table.text.matches(filter), keep=False)
    for mark in comments:
        if not isinstance(mark, str) or not mark:
            raise ValueError('comment markers must be nonempty strings')
        table = table.filter(table.text.startswith(mark) if len(mark) == 1
                             else table.text.matches(mark, full_match=True), keep=False)
    if skip_blank_lines:
        table = table.filter(table.text != '')
    import pyarrow as pa
    first = pa.table(table._native.sample_arrow(1)).to_pylist()
    if not first:
        raise ValueError('no lines remaining after filters')
    header_text = first[0]['text']
    raw_names = eval_expression(_split_line(literal(header_text), delimiter, quote, []))
    if find_replace is not None:
        if len(find_replace) != 2:
            raise ValueError('find_replace must contain a pattern and replacement')
        table = table.annotate(text=table.text.replace(*find_replace))
    if no_header:
        names = [f'f{i}' for i in range(len(raw_names))]
    else:
        # Hail removes every row equal to the first header, including repeated
        # headers in subsequent files. Preserve that behavior deliberately.
        table = table.filter(table.text != header_text)
        names, used = [], set()
        for name in raw_names:
            candidate, suffix = name, 1
            while candidate in used:
                candidate = f'{name}_{suffix}'
                suffix += 1
            used.add(candidate)
            names.append(candidate)
    unknown = set(types) - set(names)
    if unknown:
        raise ValueError(f'types contains unknown fields: {sorted(unknown)}')
    if source_file_field in names:
        raise ValueError('source_file_field collides with an imported field')
    split_name = '__vail_text_fields'
    while split_name in table._schema or split_name in table._global_schema:
        split_name += '_'
    table = table.annotate(**{split_name: _split_line(table.text, delimiter, quote, missing, len(names))})
    split = table[split_name]
    fields = {}
    for index, name in enumerate(names):
        target = types.get(name, tstr)
        if target not in (tstr, tint32, tint64, tfloat32, tfloat64, tbool):
            raise NotImplementedError('typed text imports currently support primitive fields')
        value = _function('array_element', [split, literal(index + 1, tint64)], tstr)
        fields[name] = cast(value, target)
    if source_file_field is not None:
        fields[source_file_field] = table.file
    table = table.select(**fields)
    if min_partitions is not None and min_partitions > len(paths):
        table = table.repartition(min_partitions)
    if key is not None:
        table = table.key_by(*([key] if isinstance(key, str) else key))
    return table


def read_csv(paths, schema=None, *, delimiter=',', quote='"', header=True):
    """Read standard CSV using DataFusion's CSV file source."""
    from . import _native_session
    paths = _paths(paths)
    options = {'delimiter': ord(_byte(delimiter, 'delimiter')), 'quote': ord(_byte(quote, 'quote')),
               'has_header': header, 'newlines_in_values': True}
    if type(header) is not bool:
        raise TypeError('header must be bool')
    if schema is None:
        fields = _native_session().infer_text_schema(paths, 'csv', options)
        if len({name for name, _ in fields}) != len(fields):
            raise ValueError('duplicate CSV header names; use import_table for Hail header deduplication')
        schema = tstruct(**{name: type_from_wire(ty) for name, ty in fields})
    return _read(paths, schema, 'csv', options)


def read_json(paths, schema=None):
    """Read newline-delimited JSON objects through DataFusion's JSON source."""
    from . import _native_session
    paths = _paths(paths)
    if schema is None:
        fields = _native_session().infer_text_schema(paths, 'json', {})
        schema = tstruct(**{name: type_from_wire(ty) for name, ty in fields})
    return _read(paths, schema, 'json', {})


def export(table, output, types_file=None, header=True, parallel=None, delimiter='\t'):
    """Stream primitive fields to an unquoted text file through a DataFusion sink."""
    if types_file is not None or parallel is not None:
        raise NotImplementedError('types files and parallel text export are not implemented')
    if type(header) is not bool:
        raise TypeError('header must be bool')
    _byte(delimiter, 'delimiter')
    if any(ty not in (tstr, tint32, tint64, tfloat32, tfloat64, tbool) for ty in table._schema.values()):
        raise NotImplementedError('nested text export needs Hail-compatible JSON serialization')
    if any(ty in (tfloat32, tfloat64) for ty in table._schema.values()):
        raise NotImplementedError('Hail text export uses four-digit scientific float formatting; use export_csv for DataFusion formatting')
    table._native.export_text(_local_path(output), 'text', {
        'delimiter': ord(delimiter), 'has_header': header, 'null_value': 'NA',
    })


def export_csv(table, output, *, delimiter=',', quote='"', header=True):
    if type(header) is not bool:
        raise TypeError('header must be bool')
    table._native.export_text(_local_path(output), 'csv', {
        'delimiter': ord(_byte(delimiter, 'delimiter')), 'quote': ord(_byte(quote, 'quote')),
        'has_header': header,
    })


def export_json(table, output):
    table._native.export_text(_local_path(output), 'json', {})
