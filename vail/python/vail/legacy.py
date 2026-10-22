"""Read Hail 0.2 native storage without a JVM.

The wire definitions live in Hail's compatibility/LegacyEncodedTypeParser.scala,
types/encoded, io/InputBuffers.scala, and rvd/AbstractRVDSpec.scala. This is an
eager migration reader: it scans parts in metadata order and materializes values
before constructing a Vail plan. Indexes are deliberately unnecessary.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from functools import lru_cache
import gzip
import json
import math
import struct
from pathlib import Path

from . import types as t


@dataclass(frozen=True)
class _Encoding:
    kind: str
    required: bool = False
    children: tuple = ()


class _Parser(t._TypeParser):
    def fields(self, parse):
        self.expect('{')
        fields = []
        seen = set()
        if not self.accept('}'):
            while True:
                name = self.identifier()
                self.expect(':')
                if name in seen:
                    self.error(f'duplicate field {name!r}')
                seen.add(name)
                fields.append((name, parse()))
                if not self.accept(','):
                    break
            self.expect('}')
        return tuple(fields)

    def keys(self):
        self.expect('[')
        keys = []
        if not self.accept(']'):
            while True:
                self.skip_space()
                if self.text[self.position:self.position + 1] == '[':
                    keys.extend(self.keys())
                else:
                    keys.append(self.identifier())
                if not self.accept(','):
                    break
            self.expect(']')
        return keys

    def encoded(self):
        required = self.accept('+')
        kind = self.identifier()
        children = ()
        if kind == 'EBaseStruct':
            children = self.fields(self.encoded)
        elif kind in ('EArray', 'EArray2', 'EArrayLegacyFullWidthIntegerLength'):
            self.expect('[')
            children = (self.encoded(),)
            self.expect(']')
        elif kind == 'ENDArrayColumnMajor':
            self.expect('[')
            element = self.encoded()
            self.expect(',')
            rank = int(self.identifier())
            self.expect(']')
            children = (element, rank)
        elif kind not in ('EBoolean', 'EInt32', 'EInt64', 'EFloat32', 'EFloat64',
                          'EVarint', 'EBinary', 'EBinary2', 'EBinaryLegacyFullWidthIntegerLength'):
            raise NotImplementedError(f'unsupported Hail encoding {kind}')
        return _Encoding(kind, required, children)

    def virtual(self):
        """Return a Vail type and its pre-TypedCodecSpec packed encoding."""
        required = self.accept('+')
        kind = self.identifier()
        primitives = {'Boolean': t.tbool, 'Int': t.tint32, 'Int32': t.tint32,
                      'Int64': t.tint64, 'Float32': t.tfloat32, 'Float64': t.tfloat64,
                      'String': t.tstr, 'Call': t.tcall}
        if kind in primitives:
            ek = {'Int': 'EInt32', 'String': 'EBinary', 'Call': 'EInt32'}.get(kind, 'E' + kind)
            return primitives[kind], _Encoding(ek, required)
        if kind == 'Struct':
            fields = self.fields(self.virtual)
            return (t.tstruct(**{n: v for n, (v, _) in fields}),
                    _Encoding('EBaseStruct', required, tuple((n, e) for n, (_, e) in fields)))
        if kind == 'Locus':
            self.expect('(')
            reference = self.identifier()
            self.expect(')')
            return t.tlocus(reference), _Encoding('EBaseStruct', required, (
                ('contig', _Encoding('EBinary', True)), ('position', _Encoding('EInt32', True))))
        if kind in ('Array', 'Set', 'Dict', 'Interval', 'Tuple', 'NDArray'):
            self.expect('[')
            args = []
            if not self.accept(']'):
                while True:
                    if kind == 'NDArray' and args:
                        args.append(int(self.identifier()))
                    else:
                        args.append(self.virtual())
                    if not self.accept(','):
                        break
                self.expect(']')
            if kind == 'Tuple':
                return t.ttuple(*(v for v, _ in args)), _Encoding('EBaseStruct', required,
                    tuple((str(i), e) for i, (_, e) in enumerate(args)))
            v, e = args[0]
            if kind == 'NDArray':
                return t.tndarray(v, args[1]), _Encoding('ENDArrayColumnMajor', required, (e, args[1]))
            if kind == 'Dict':
                v2, e2 = args[1]
                return t.tdict(v, v2), _Encoding('EArray', required, (
                    _Encoding('EBaseStruct', True, (('key', e), ('value', e2))),))
            if kind == 'Interval':
                return t.tinterval(v), _Encoding('EBaseStruct', required, (
                    ('start', e), ('end', e), ('includesStart', _Encoding('EBoolean', True)),
                    ('includesEnd', _Encoding('EBoolean', True))))
            return {'Array': t.tarray, 'Set': t.tset}[kind](v), _Encoding('EArray', required, (e,))
        raise NotImplementedError(f'unsupported Hail type {kind}')

    def relation(self):
        kind = self.identifier()
        if kind not in ('Table', 'Matrix', 'RVDType', 'OrderedRVDType'):
            self.error('expected Hail relational type')
        fields = {}
        self.expect('{')
        while not self.accept('}'):
            name = self.identifier()
            self.expect(':')
            fields[name] = self.keys() if name in ('key', 'row_key', 'col_key') else self.virtual()
            if not self.accept(','):
                self.expect('}')
                break
        return fields

    def finish(self, parse):
        value = parse()
        self.skip_space()
        if self.position != len(self.text):
            self.error('unexpected trailing input')
        return value


def _parse(text, method):
    parser = _Parser(text)
    return parser.finish(getattr(parser, method))


class _Input:
    def __init__(self, data, leb=False):
        self.data = memoryview(data)
        self.pos = 0
        self.leb = leb

    def view(self, n):
        if n < 0 or n > len(self.data) - self.pos:
            raise ValueError('truncated Hail data or invalid length')
        result = self.data[self.pos:self.pos + n]
        self.pos += n
        return result

    def read(self, n):
        return self.view(n).tobytes()

    def number(self, fmt):
        n = struct.calcsize(fmt)
        if n > len(self.data) - self.pos:
            raise ValueError('truncated Hail primitive')
        value = struct.unpack_from('<' + fmt, self.data, self.pos)[0]
        self.pos += n
        return value

    def varint(self, bits=32):
        value = 0
        for shift in range(0, bits, 7):
            b = self.number('B')
            value |= (b & 127) << shift
            if not b & 128:
                if value >= 1 << bits:
                    raise ValueError('overflow in Hail variable length integer')
                return value
        raise ValueError('invalid Hail variable length integer')

    def integer(self, bits=32):
        if self.leb:
            value = self.varint(bits)
            return value - (1 << bits) if value & (1 << (bits - 1)) else value
        return self.number('i' if bits == 32 else 'q')

    def length(self, kind):
        value = self.varint() if kind.endswith('2') else self.integer()
        if value < 0:
            raise ValueError('negative Hail collection length')
        return value


def _block(data, spec):
    kind = spec['name']
    if kind in ('StreamBlockBufferSpec', 'StreamBlockBufferSpec2'):
        while data.pos < len(data.data):
            n = data.varint() if kind.endswith('2') else data.number('i')
            yield data.read(n)
        return
    for block in _block(data, spec['child']):
        source = _Input(block)
        if kind == 'LZ4SizeBasedBlockBufferSpec':
            flag = source.number('i')
            if flag == 0:
                yield source.read(len(source.data) - source.pos)
                continue
            if flag != 1:
                raise ValueError('invalid Hail LZ4 compression flag')
        elif kind not in ('LZ4BlockBufferSpec', 'LZ4HCBlockBufferSpec', 'LZ4FastBlockBufferSpec',
                          'ZstdBlockBufferSpec', 'ZstdSizedBasedBlockBufferSpec'):
            raise NotImplementedError(f'unsupported Hail block buffer {kind}')
        size = source.number('i')
        if kind == 'ZstdSizedBasedBlockBufferSpec':
            if not size & 1:
                yield source.read(len(source.data) - source.pos)
                continue
            size >>= 1
        if not 0 <= size <= spec['blockSize']:
            raise ValueError('invalid Hail decompressed block length')
        import pyarrow as pa
        codec = 'zstd' if kind.startswith('Zstd') else 'lz4_raw'
        yield pa.decompress(source.read(len(source.data) - source.pos), size,
                            codec=codec, asbytes=True)


def _buffer(data, spec):
    leb = spec['name'] == 'LEB128BufferSpec'
    if leb:
        spec = spec['child']
    if spec['name'] == 'BlockingBufferSpec':
        data = b''.join(_block(_Input(data), spec['child']))
    elif spec['name'] != 'StreamBufferSpec':
        raise NotImplementedError(f"unsupported Hail buffer {spec['name']}")
    return _Input(data, leb)


def _decode(source, encoding):
    kind = encoding.kind
    if kind == 'EBoolean':
        value = source.number('B')
        if value > 1:
            raise ValueError('invalid Hail Boolean')
        return bool(value)
    if kind in ('EInt32', 'EInt64'):
        return source.integer(32 if kind == 'EInt32' else 64)
    if kind == 'EVarint':
        return source.varint(64)
    if kind in ('EFloat32', 'EFloat64'):
        return source.number('f' if kind == 'EFloat32' else 'd')
    if kind.startswith('EBinary'):
        return source.read(source.length(kind))
    if kind == 'EBaseStruct':
        optional = sum(not e.required for _, e in encoding.children)
        missing = source.view((optional + 7) // 8)
        result = {}
        i = 0
        for name, child in encoding.children:
            absent = not child.required and missing[i >> 3] & (1 << (i & 7))
            i += not child.required
            result[name] = None if absent else _decode(source, child)
        return result
    if kind.startswith('EArray'):
        n = source.length(kind)
        child, = encoding.children
        if child.required:
            return [_decode(source, child) for _ in range(n)]
        missing = source.view((n + 7) // 8)
        return [None if missing[i >> 3] & (1 << (i & 7)) else _decode(source, child) for i in range(n)]
    if kind == 'ENDArrayColumnMajor':
        import numpy as np
        child, rank = encoding.children
        shape = tuple(source.integer(64) for _ in range(rank))
        if any(n < 0 for n in shape):
            raise ValueError('negative Hail ndarray dimension')
        return np.array([_decode(source, child) for _ in range(math.prod(shape))], dtype=object).reshape(shape, order='F')
    raise NotImplementedError(f'unsupported Hail encoding {kind}')


def _call(value):
    value &= 0xffffffff
    ploidy, phased, allele = (value >> 1) & 3, bool(value & 1), value >> 3
    if ploidy == 0:
        alleles = ()
    elif ploidy == 1:
        alleles = (allele,)
    elif ploidy == 2:
        k = (math.isqrt(8 * allele + 1) - 1) // 2
        j = allele - k * (k + 1) // 2
        alleles = (j, k - j if phased else k)
    else:
        raise ValueError('invalid Hail call ploidy')
    return t.Call(alleles, phased)


@lru_cache(maxsize=256)
def _localizer(dtype, *, key=False):
    """Compile type dispatch once; returned converters accept missing values.

    In key context nested containers produce hashable Python representations.
    Native type introspection stays outside the per-record decoding loop.
    """
    convert = lambda value: value
    if dtype == t.tstr:
        convert = lambda value: value.decode('utf-8')
    elif dtype in (t.tint32, t.tint64):
        bits = 32 if dtype == t.tint32 else 64
        convert = lambda value: value - (1 << bits) if value >= 1 << (bits - 1) else value
    elif dtype == t.tcall:
        convert = _call
    elif isinstance(dtype, t.tlocus):
        from .reference_genome import Locus
        reference = dtype.reference_genome
        convert = lambda value: Locus(value['contig'].decode('utf-8'), value['position'], reference)
    elif isinstance(dtype, t.tstruct):
        fields = tuple((n, _localizer(ty, key=key)) for n, ty in dtype.items())
        convert = lambda value: t.Struct(**{n: child(value[n]) for n, child in fields})
    elif isinstance(dtype, t.ttuple):
        fields = tuple((str(i), _localizer(ty, key=key)) for i, ty in enumerate(dtype.types))
        convert = lambda value: tuple(child(value[n]) for n, child in fields)
    elif isinstance(dtype, (t.tarray, t.tset)):
        is_set = isinstance(dtype, t.tset)
        child = _localizer(dtype.element_type, key=key or is_set)
        container = frozenset if is_set else tuple if key else list
        convert = lambda value: container(child(v) for v in value)
    elif isinstance(dtype, t.tdict):
        convert_key = _localizer(dtype.key_type, key=True)
        convert_value = _localizer(dtype.value_type, key=key)
        container = frozenset if key else dict
        convert = lambda value: container((convert_key(v['key']), convert_value(v['value'])) for v in value)
    elif isinstance(dtype, t.tndarray):
        import numpy as np
        child = _localizer(dtype.element_type)
        numpy_type = str(dtype.element_type)
        convert = lambda value: np.asarray([child(v) for v in value.flat], dtype=numpy_type).reshape(value.shape)
    elif isinstance(dtype, t.tinterval):
        point_type = dtype.point_type
        child = _localizer(point_type, key=key)
        convert = lambda value: t.Interval(child(value['start']), child(value['end']),
                                          value['includesStart'], value['includesEnd'], point_type)
    return lambda value: None if value is None else convert(value)


def _metadata(path):
    with gzip.open(path / 'metadata.json.gz', 'rt', encoding='utf-8') as stream:
        return json.load(stream)


def _check_encoding(encoding, dtype):
    """Establish that decoded wire values have the declared logical types.

    After this recursive check, the localizer and Arrow converter can process
    records without repeating native type introspection for every cell.
    Requiredness is carried by the wire encoding's bitmaps, not virtual types.
    """
    kind = encoding.kind
    primitive = {
        'EBoolean': (t.tbool,), 'EInt32': (t.tint32, t.tcall),
        'EInt64': (t.tint64,), 'EVarint': (t.tint32, t.tint64, t.tcall),
        'EFloat32': (t.tfloat32,), 'EFloat64': (t.tfloat64,),
        'EBinary': (t.tstr,), 'EBinary2': (t.tstr,),
        'EBinaryLegacyFullWidthIntegerLength': (t.tstr,),
    }
    if kind in primitive and dtype in primitive[kind]:
        return
    if kind == 'EBaseStruct':
        if isinstance(dtype, t.tstruct):
            fields = dict(dtype.items())
        elif isinstance(dtype, t.ttuple):
            fields = {str(i): ty for i, ty in enumerate(dtype.types)}
        elif isinstance(dtype, t.tlocus):
            fields = dict(contig=t.tstr, position=t.tint32)
        elif isinstance(dtype, t.tinterval):
            fields = dict(start=dtype.point_type, end=dtype.point_type,
                          includesStart=t.tbool, includesEnd=t.tbool)
        else:
            fields = {}
        if set(n for n, _ in encoding.children) == set(fields) and (
            fields or isinstance(dtype, (t.tstruct, t.ttuple))
        ):
            for name, child in encoding.children:
                _check_encoding(child, fields[name])
            return
    elif kind.startswith('EArray'):
        if isinstance(dtype, (t.tarray, t.tset)):
            _check_encoding(encoding.children[0], dtype.element_type)
            return
        if isinstance(dtype, t.tdict):
            _check_encoding(encoding.children[0], t.tstruct(key=dtype.key_type, value=dtype.value_type))
            return
    elif kind == 'ENDArrayColumnMajor' and isinstance(dtype, t.tndarray):
        child, rank = encoding.children
        if rank == dtype.ndim:
            _check_encoding(child, dtype.element_type)
            return
    raise ValueError(f'Hail encoding {kind} is incompatible with {dtype}')


def _rvd(path):
    spec = _metadata(path)
    kind = spec['name']
    if kind in ('OrderedRVDSpec2', 'IndexedRVDSpec2'):
        codec = spec['_codecSpec']
        if codec['name'] != 'TypedCodecSpec':
            raise NotImplementedError(f"unsupported Hail codec {codec['name']}")
        dtype, _ = _parse(codec['_vType'], 'virtual')
        encoding = _parse(codec['_eType'], 'encoded')
        buffer = codec['_bufferSpec']
        parts = spec['_partFiles']
    elif kind in ('OrderedRVDSpec', 'IndexedRVDSpec', 'UnpartitionedRVDSpec'):
        if spec['codecSpec']['name'] != 'PackCodecSpec':
            raise NotImplementedError(f"unsupported Hail codec {spec['codecSpec']['name']}")
        if kind == 'UnpartitionedRVDSpec':
            dtype, encoding = _parse(spec['rowType'], 'virtual')
        else:
            relation = _parse(spec.get('rvdType', spec.get('orvdType')), 'relation')
            dtype, encoding = relation['row']
        encoding = replace(encoding, required=True)
        buffer, parts = spec['codecSpec']['child'], spec['partFiles']
    else:
        raise NotImplementedError(f'unsupported Hail RVD {kind}')
    _check_encoding(encoding, dtype)
    localize = _localizer(dtype)
    rows = []
    counts = []
    for part in parts:
        part_path = path / 'parts' / part
        try:
            source = _buffer(part_path.read_bytes(), buffer)
            start = len(rows)
            while True:
                marker = source.number('B')
                if marker == 0:
                    break
                if marker != 1:
                    raise ValueError('invalid Hail row marker')
                rows.append(localize(_decode(source, encoding)))
            if source.pos != len(source.data):
                raise ValueError('trailing bytes after Hail partition terminator')
            counts.append(len(rows) - start)
        except (ValueError, OSError) as error:
            raise ValueError(f'cannot read Hail partition {part_path}: {error}') from error
    return dtype, rows, counts


def _open(path, expected):
    from .vcf import _local_path
    root = Path(_local_path(path))
    spec = _metadata(root)
    if spec.get('name') != expected:
        raise ValueError(f"expected {expected}, found {spec.get('name')!r}")
    version = spec.get('file_version')
    if not isinstance(version, int) or not 65536 <= version <= 67584:
        raise ValueError(f'unsupported Hail native file version {version!r}; expected formats 1.0 through 1.8')
    from .reference_genome import ReferenceGenome
    references = root / spec.get('references_rel_path', 'references')
    for reference in sorted(references.glob('*.json*')):
        opener = gzip.open if reference.suffix == '.gz' else open
        with opener(reference, 'rt', encoding='utf-8') as stream:
            config = json.load(stream)
        if 'lengths' in config:
            ReferenceGenome.from_wire(config)
            continue
        par = []
        for interval in config.get('par', []):
            if interval['start']['contig'] != interval['end']['contig']:
                raise ValueError('PAR interval endpoints must be on the same contig')
            par.append((interval['start']['contig'], interval['start']['position'],
                        interval['end']['position']))
        ReferenceGenome(config['name'], [c['name'] for c in config['contigs']],
                        {c['name']: c['length'] for c in config['contigs']},
                        config.get('xContigs', ()), config.get('yContigs', ()),
                        config.get('mtContigs', ()), par)
    return root, spec


def _component(root, spec, name):
    component = spec['components'][name]
    if component.get('name') != 'RVDComponentSpec':
        raise ValueError(f'invalid Hail {name} component')
    return _rvd(root / component['rel_path'])


def _globals(root, spec, expected):
    dtype, rows, _ = _component(root, spec, 'globals')
    if dtype != expected:
        raise ValueError('Hail global schema differs from dataset metadata')
    if len(rows) != 1:
        raise ValueError('Hail globals must contain exactly one record')
    from .expressions import literal
    return {n: literal(rows[0][n], ty) for n, ty in dtype.items()}


def _check_counts(spec, counts):
    expected = spec['components'].get('partition_counts', {}).get('counts')
    if expected is not None and expected != counts:
        raise ValueError(f'Hail partition counts differ: expected {expected}, found {counts}')


def read_table_legacy(path):
    from . import Table
    from .arrow import rows_to_arrow
    root, spec = _open(path, 'TableSpec')
    relation = _parse(spec['table_type'], 'relation')
    dtype, rows, counts = _component(root, spec, 'rows')
    if dtype != relation['row'][0]:
        raise ValueError('Hail row schema differs from table metadata')
    _check_counts(spec, counts)
    return Table.parallelize(rows_to_arrow(rows, dtype), schema=dtype, key=relation['key'],
                             globals=_globals(root, spec, relation['global'][0]))


def read_matrix_legacy(path):
    from . import Table
    from .arrow import rows_to_arrow
    from .matrixtable import MatrixTable, MatrixIR, _ROW, _CELLS, _CID, _VALUE, _COL, _COLUMNS
    root, spec = _open(path, 'MatrixTableSpec')
    relation = _parse(spec['matrix_type'], 'relation')
    row_type, rows, counts = _component(root, spec, 'rows')
    col_type, cols, _ = _component(root, spec, 'cols')
    entry_type, entries, entry_counts = _component(root, spec, 'entries')
    if row_type != relation['row'][0] or col_type != relation['col'][0]:
        raise ValueError('Hail axis schema differs from matrix metadata')
    if len(entry_type) != 1:
        raise ValueError('Hail entries component must have one array field')
    entry_field = next(iter(entry_type))
    if entry_type[entry_field] != t.tarray(relation['entry'][0]):
        raise ValueError('Hail entry schema differs from matrix metadata')
    if counts != entry_counts:
        raise ValueError('Hail row and entry partition counts differ')
    _check_counts(spec, counts)
    globals = _globals(root, spec, relation['global'][0])
    if _COLUMNS in globals:
        raise NotImplementedError(f'Hail global field {_COLUMNS!r} is reserved by Vail matrices')
    for row_index, (row, entry) in enumerate(zip(rows, entries)):
        cells = entry[entry_field]
        if cells is None or len(cells) != len(cols):
            raise ValueError('Hail entry array length differs from column count')
        rows[row_index] = {_ROW: row, _CELLS: [
            {_CID: i, _VALUE: cell} for i, cell in enumerate(cells)
        ]}
        # Release each source entry array as its slots acquire their owning
        # core record. Do not retain both complete layouts during Arrow input.
        entries[row_index] = None
    del entries
    from .expressions import literal
    globals[_COLUMNS] = literal(
        [{_CID: i, _COL: col} for i, col in enumerate(cols)],
        t.tarray(t.tstruct(**{_CID: t.tint64, _COL: col_type})),
    )
    schema = t.tstruct(**{
        _ROW: row_type,
        _CELLS: t.tarray(t.tstruct(**{_CID: t.tint64, _VALUE: relation['entry'][0]})),
    })
    table = Table.parallelize(rows_to_arrow(rows, schema), schema=schema, globals=globals)
    # The two physical partition streams have been checked above. Slot IDs
    # preserve column occurrences, including duplicate keys and filtered cells.
    return MatrixTable(MatrixIR(table, row_key=relation['row_key'],
                               col_key=relation['col_key'], _aligned=True))
