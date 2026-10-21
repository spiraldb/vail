"""Text sources and sinks. Python builds IR; DataFusion reads and parses rows."""
import glob
import json
import os
from collections.abc import Mapping
from urllib.parse import unquote, urlparse

from .collections import _function
from .expressions import Expression, as_expr, cast, literal
from .types import (dtype, tarray, tstr, tint64, tstruct, tint32, tfloat32, tfloat64,
                    tbool, type_to_wire, type_from_wire)

_COMPRESSED_SUFFIXES = ('.gz', '.bgz', '.bz2', '.xz', '.zst')


def _local_path(path, *, allow_compressed=False):
    path = os.fspath(path)
    if path.startswith('file:'):
        parsed = urlparse(path)
        if parsed.netloc not in ('', 'localhost'):
            raise NotImplementedError('text IO currently supports local paths')
        path = unquote(parsed.path)
    if '://' in path:
        raise NotImplementedError('text IO currently supports local paths')
    if path.endswith(_COMPRESSED_SUFFIXES) and not allow_compressed:
        raise NotImplementedError('compressed text IO is not implemented')
    return os.path.abspath(path)


def _paths(paths):
    paths = [paths] if isinstance(paths, (str, os.PathLike)) else list(paths)
    result = []
    for path in paths:
        path = _local_path(path, allow_compressed=True)
        matches = sorted(glob.glob(path)) if glob.has_magic(path) else [path]
        if not matches:
            raise FileNotFoundError(path)
        result.extend(os.path.abspath(match) for match in matches)
    if not result:
        raise ValueError('at least one input path is required')
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
    for name, value in [('force_bgz', force_bgz), ('force', force)]:
        if type(value) is not bool:
            raise TypeError(f'{name} must be bool')
    if type(file_per_partition) is not bool:
        raise TypeError('file_per_partition must be bool')
    paths = _paths(paths)
    if not (force or force_bgz) and any(path.endswith('.gz') for path in paths):
        raise ValueError('gzip input requires force=True or force_bgz=True')
    if any(path.endswith(('.bz2', '.xz', '.zst')) for path in paths):
        raise NotImplementedError('this compression codec is not supported for text input')
    if min_partitions is not None:
        _validate_n(min_partitions, 'min_partitions', positive=True)
        if file_per_partition and min_partitions > len(paths):
            raise ValueError('min_partitions exceeds the number of files with file_per_partition=True')
    table = _read(paths, tstruct(file=tstr, text=tstr), 'lines', {'force_bgz': force_bgz})
    if min_partitions is not None and min_partitions > len(paths):
        table = table.repartition(min_partitions)
    return table


def _split_line(value, delimiter, quote, missing, expected_fields=0):
    return Expression(tarray(tstr), value.source, {
        'op': 'parse_csv_line', 'value': value.to_ir(), 'delimiter': delimiter,
        'quote': quote, 'missing': missing, 'expected_fields': expected_fields,
    })


def split_line(value, delimiter, missing=None, quote=None, regex=False):
    """Parse one delimited record, preserving missing rows and configured markers."""
    value = as_expr(value)
    if value.dtype != tstr:
        raise TypeError('split_line requires a string')
    if not isinstance(delimiter, str) or not delimiter:
        raise ValueError('delimiter must be a nonempty string')
    if regex:
        delimiter = '(?:' + delimiter + ')'
    else:
        _byte(delimiter, 'delimiter')
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


def json(value):
    """Serialize an expression as a Hail JSON annotation."""
    return _function('__vail_json_annotation', [as_expr(value)], tstr)


def parse_json(value, dtype):
    """Parse JSON to a statically typed expression in the native engine."""
    from .types import dtype as parse_type
    value = as_expr(value)
    if value.dtype != tstr:
        raise TypeError('parse_json requires a string expression')
    target = parse_type(dtype) if isinstance(dtype, str) else dtype
    return _function('__vail_parse_json', [value], target)


def _parse_integer_array(value, target):
    from . import case, empty_array, if_else, is_missing, missing

    whitespace = r'[ \t\r\n]*'
    integer = r'(?:-?(?:0|[1-9][0-9]*)|null)'
    grammar = (whitespace + r'\[' + whitespace
               + '(?:' + integer + whitespace + '(?:,' + whitespace + integer + whitespace + ')*)?'
               + r'\]' + whitespace)
    inside = value.strip()[1:-1].strip()
    elements = inside.split(',').map(lambda element: if_else(
        element.strip() == 'null', missing(target.element_type),
        cast(element.strip(), target.element_type),
    ))
    parsed = if_else(inside == '', empty_array(target.element_type), elements)
    return (case().when(is_missing(value), missing(target))
            .when(value.matches(grammar, full_match=True), parsed)
            .or_error(f'invalid JSON integer array for {target}'))


def _integer_fits(value, positive, negative):
    from . import if_else, len as length
    digits = value.replace(r'^[+-]?0*', '')
    bound = if_else(value.startswith('-'), negative, positive)
    return value.matches(r'[+-]?[0-9]+', full_match=True) & (
        (length(digits) < len(positive)) |
        ((length(digits) == len(positive)) & (digits <= bound)))


def _parse_primitive(value, target):
    from . import case, is_missing
    if target in (tint32, tint64):
        bounds = ('2147483647', '2147483648') if target == tint32 else ('9223372036854775807', '9223372036854775808')
        return (case().when(is_missing(value) | _integer_fits(value, *bounds), cast(value, target))
                .or_error(f'cannot parse {target} from input string'))
    if target in (tfloat32, tfloat64):
        # Java's floating parser accepts an optional precision suffix.
        value = value.strip().replace(r'[fFdD]$', '')
    return cast(value, target)


def _parse_import_field(value, target):
    from .types import tcall, tlocus, tinterval
    if target in (tstr, tint32, tint64, tfloat32, tfloat64, tbool):
        return _parse_primitive(value, target)
    if target == tcall:
        return parse_json(json(value), target)
    if isinstance(target, tlocus):
        from . import parse_locus
        return parse_locus(value, target.reference_genome)
    if isinstance(target, tinterval) and isinstance(target.point_type, tlocus):
        from . import parse_locus_interval
        return parse_locus_interval(value, reference_genome=target.point_type.reference_genome)
    return parse_json(value, target)


def _impute_fields(table, split, names, types):
    """Reduce type support in the native engine; only summary booleans leave it."""
    from . import agg, struct, is_defined

    summaries = {}
    for index, name in enumerate(names):
        if types.get(name) is not None:
            continue
        value = _function('array_element', [split, literal(index + 1, tint64)], tstr)
        float_pattern = (r'[\x00-\x20]*[+-]?(?:NaN|Infinity|'
                         r'(?:(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?)[fFdD]?|'
                         r'0[xX](?:[0-9a-fA-F]+(?:\.[0-9a-fA-F]*)?|\.[0-9a-fA-F]+)[pP][+-]?[0-9]+[fFdD]?)'
                         r'[\x00-\x20]*')
        summaries[name] = struct(
            present=agg.any(is_defined(value)),
            boolean=agg.all(value.lower().matches(r'true|false', full_match=True)),
            int32=agg.all(_integer_fits(value, '2147483647', '2147483648')),
            int64=agg.all(_integer_fits(value, '9223372036854775807', '9223372036854775808')),
            float64=agg.all(value.matches(float_pattern, full_match=True)),
        )
    if not summaries:
        return {}
    observed = table.aggregate(struct(**summaries))
    inferred = {}
    for name, support in observed.items():
        inferred[name] = tstr
        if support.present:
            for category, target in [('boolean', tbool), ('int32', tint32),
                                     ('int64', tint64), ('float64', tfloat64)]:
                if support[category]:
                    inferred[name] = target
                    break
    return inferred


def import_table(paths, key=None, min_partitions=None, impute=False, no_header=False,
                 comment=(), delimiter='\t', missing='NA', types=None, quote=None,
                 skip_blank_lines=False, force_bgz=False, filter=None, find_replace=None,
                 force=False, source_file_field=None):
    """Import single-line delimited records using a DataFusion CSV expression.

    Header discovery evaluates one row, as Hail does. The remaining table is lazy.
    Raw headers are removed before find_replace transforms data records.
    """
    if not isinstance(delimiter, str) or not delimiter:
        raise ValueError('import_table: empty delimiter is not supported')
    if quote is not None:
        quote = _byte(quote, 'quote')
        if quote == delimiter:
            raise ValueError('quote and delimiter must differ')
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
        raise ValueError(f"Invalid file: no lines remaining after filters\n Files provided: {', '.join(paths)}")
    header_text = first[0]['text']
    raw_names = eval_expression(_split_line(literal(header_text), delimiter, quote, []))
    if no_header:
        names = [f'f{i}' for i in range(len(raw_names))]
    else:
        # Hail removes every row equal to the first header, including repeated
        # headers in subsequent files. Preserve that behavior deliberately.
        table = table.filter(table.text != header_text)
        names, used, next_suffix = [], set(), {}
        for name in raw_names:
            candidate, suffix = name, next_suffix.get(name, 1)
            while candidate in used:
                candidate = f'{name}_{suffix}'
                suffix += 1
            next_suffix[name] = suffix
            used.add(candidate)
            names.append(candidate)
    if find_replace is not None:
        if len(find_replace) != 2:
            raise ValueError('find_replace must contain a pattern and replacement')
        table = table.annotate(text=table.text.replace(*find_replace))
    unknown = set(types) - set(names)
    if unknown:
        raise ValueError(f'types contains unknown fields: {sorted(unknown)}')
    if source_file_field in names:
        raise ValueError('source_file_field collides with an imported field')
    split_name = '__vail_text_fields'
    while split_name in table._schema or split_name in table._global_schema:
        split_name += '_'
    # Imputation must validate record widths while scanning all values. For
    # explicit types defer width validation to the projected field expression,
    # allowing an earlier row's value conversion to report its own error.
    table = table.annotate(**{split_name: _split_line(table.text, delimiter, quote, missing, len(names) if impute else 0)})
    split = table[split_name]
    if impute:
        types = dict(types)
        types.update(_impute_fields(table, split, names, types))
    fields = {}
    for index, name in enumerate(names):
        target = types.get(name, tstr)
        if isinstance(target, str):
            target = dtype(target)
        integer_array = isinstance(target, tarray) and target.element_type in (tint32, tint64)
        value = _function('array_element', [split, literal(index + 1, tint64)], tstr)
        fields[name] = (_parse_integer_array(value, target) if integer_array else
                        _parse_import_field(value, target))
        if not impute:
            from . import case, len as length, str as stringify
            fields[name] = (case().when(length(split) == len(names), fields[name])
                .or_error(f'Expected {len(names)} fields, found ' + stringify(length(split)) + ' field(s)'))
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
    from .types import tinterval, tlocus
    from .expressions import if_else, str_, or_missing, is_defined
    interval_fields = {name: ty for name, ty in table._schema.items()
                       if isinstance(ty, tinterval) and isinstance(ty.point_type, tlocus)}
    if interval_fields:
        table = table.key_by()
        fields = {}
        for name, ty in table._schema.items():
            value = table[name]
            if name in interval_fields:
                value = or_missing(is_defined(value),
                    if_else(value.includes_start, '[', '(')
                    + value.start.contig + ':' + str_(value.start.position) + '-'
                    + if_else(value.start.contig == value.end.contig, '', value.end.contig + ':') + str_(value.end.position)
                    + if_else(value.includes_end, ']', ')'))
            fields[name] = value
        table = table.select(**fields)
    if any(ty not in (tstr, tint32, tint64, tbool) for ty in table._schema.values()):
        table = table.key_by()
        table = table.select(**{name: table[name] if ty in (tstr, tint32, tint64, tbool)
                                else _function('__vail_export_annotation', [table[name]], tstr) for name, ty in table._schema.items()})
    table._native.export_text(_local_path(output, allow_compressed=True), 'text', {
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
