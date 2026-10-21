"""BGEN 1.2 import: bounded native variant decoding and index metadata planning.

Format: https://biobank.ctsu.ox.ac.uk/ukb/ukb/docs/bgen12formats.pdf
The native reader supports Hail's unphased, diploid, biallelic 8-bit subset.
"""
from __future__ import annotations
import gzip
import json
import os
import struct
from pathlib import Path
from .vcf import _paths, _local_path
from .utils import FatalError


def _bgen_paths(path):
    paths = _paths(path)
    expanded = []
    for filename in paths:
        if os.path.isdir(filename):
            manifest = os.path.join(filename, 'shard-manifest.txt')
            if os.path.exists(manifest):
                with open(manifest) as f:
                    expanded.extend(os.path.join(filename, line.strip()) for line in f if line.strip())
            else:
                expanded.extend(str(p) for p in sorted(Path(filename).iterdir())
                                if p.is_file() and (p.suffix == '.bgen' or (p.name.startswith('part-') and not p.suffix)))
        else:
            expanded.append(filename)
    if not expanded:
        raise ValueError('no BGEN files found')
    return expanded


def _uint(f, size=4):
    data = f.read(size)
    if len(data) != size:
        raise ValueError('truncated BGEN file')
    return int.from_bytes(data, 'little')


def _string(f, size=2):
    length = _uint(f, size)
    if length > 16 * 1024 * 1024:
        raise ValueError('BGEN identifier exceeds 16 MB')
    data = f.read(length)
    if len(data) != length:
        raise ValueError('truncated BGEN identifier')
    return data.decode('utf-8')


def _header(path):
    with open(path, 'rb') as f:
        offset, length, variants, n = (_uint(f) for _ in range(4))
        magic = f.read(4)
        if length < 20 or length > offset or magic not in (b'bgen', b'\0' * 4):
            raise ValueError('invalid BGEN header')
        f.seek(length)
        flags = _uint(f)
        if flags >> 2 & 15 != 2 or flags & 3 > 2:
            raise ValueError('BGEN requires layout 2 and zlib, zstd or uncompressed blocks')
        samples = None
        if flags >> 31:
            sample_length, sample_n = _uint(f), _uint(f)
            if sample_n != n or length + sample_length > offset:
                raise ValueError('BGEN sample identifier block mismatch')
            samples = [_string(f) for _ in range(n)]
        return offset + 4, variants, n, samples


def _metadata(path):
    if os.path.isdir(path):
        with gzip.open(os.path.join(path, 'metadata.json.gz'), 'rt') as f:
            return json.load(f)
    # Hail idx2 1.3 embeds JSON metadata followed by its 64-bit byte offset.
    with open(path, 'rb') as f:
        if f.read(1) == b'{':
            f.seek(0)
            return json.load(f)
        f.seek(-8, 2)
        offset = _uint(f, 8)
        end = f.tell() - 8
        if offset > end or end - offset > 16 * 1024 * 1024:
            raise ValueError('invalid BGEN index metadata offset')
        f.seek(offset)
        return json.loads(f.read(end - offset))


def _samples(path, n):
    with open(_local_path(path)) as f:
        header = f.readline().split()
        types = f.readline().split()
        if not header or len(types) != len(header):
            raise ValueError('invalid Oxford SAMPLE header')
        samples = [line.split()[0] for line in f if line.strip()]
    if len(samples) != n:
        raise ValueError('number of samples in SAMPLE and BGEN files differs')
    return samples


def index_bgen(path, index_file_map=None, reference_genome='default', contig_recoding=None,
               skip_invalid_loci=False, _buffer_size=16000000):
    from .reference_genome import resolve_reference
    reference = None if reference_genome is None else resolve_reference(reference_genome)
    recoding = contig_recoding or {}
    lengths = None if reference is None else reference.lengths
    paths = _bgen_paths(path)
    index_file_map = {_local_path(k): _local_path(v) for k,v in (index_file_map or {}).items()}
    for filename in paths:
        index = index_file_map.get(filename, filename + '.idx2')
        if not index.endswith('.idx2'):
            raise FatalError('BGEN index file is missing a .idx2 file extension')
        if index == filename:
            raise ValueError('BGEN index and source paths must differ')
        offset, count, n, _ = _header(filename)
        offsets = []
        with open(filename, 'rb') as f:
            f.seek(offset)
            file_length = os.fstat(f.fileno()).st_size
            for _ in range(count):
                start = f.tell()
                _string(f); _string(f)
                contig = _string(f)
                contig = recoding.get(contig, contig)
                position = _uint(f)
                k = _uint(f, 2)
                if k != 2:
                    raise FatalError('BGEN requires biallelic variants')
                alleles = [_string(f, 4) for _ in range(k)]
                size = _uint(f)
                if f.tell() + size > file_length:
                    raise ValueError('truncated BGEN variant block')
                valid = lengths is None or (contig in lengths and 0 < position <= lengths[contig])
                if not valid and not skip_invalid_loci:
                    raise FatalError(f'Invalid locus {contig}:{position}')
                if valid:
                    offsets.append([contig, position, alleles, start])
                f.seek(size, 1)
        metadata = {'fileVersion': 1, 'nKeys': len(offsets), 'attributes': {
            'reference_genome': None if reference is None else reference.name,
            'contig_recoding': recoding, 'skip_invalid_loci': skip_invalid_loci},
            'vail_variant_offsets': offsets,
            'vail_variant_bounds': _offset_bounds(offsets, reference)}
        # A Vail index is a single metadata file, as modern Hail idx2 files are.
        # The variant offsets permit future indexed byte-range scans.
        with open(index, 'w') as f:
            json.dump(metadata, f)


def _offset_bounds(offsets, reference):
    if not offsets:
        return None
    keys = [(reference.contig_index(c) if reference is not None else c, p, alleles)
            for c,p,alleles,_ in offsets]
    return [min(keys), max(keys)]


def _variant_bounds(filename, header, attributes, reference):
    """Read identifying metadata only to enforce Hail's multi-file span rule.

    Probability blocks are skipped with seeks. Only two scalar keys are retained.
    Newly written Vail indexes already contain these offsets; legacy Hail indexes
    require this metadata walk because their JSON footer omits the end keys.
    """
    recoding = attributes['contig_recoding']
    lengths = None if reference is None else reference.lengths
    lower = upper = None
    with open(filename, 'rb') as f:
        f.seek(header[0])
        file_length = os.fstat(f.fileno()).st_size
        for _ in range(header[1]):
            _string(f); _string(f)
            contig = _string(f)
            contig = recoding.get(contig, contig)
            position = _uint(f)
            k = _uint(f, 2)
            if k != 2:
                raise FatalError('BGEN requires biallelic variants')
            alleles = tuple(_string(f, 4) for _ in range(k))
            size = _uint(f)
            if f.tell() + size > file_length:
                raise FatalError('truncated BGEN variant block')
            f.seek(size, 1)
            if reference is not None:
                if contig not in lengths or not 0 < position <= lengths[contig]:
                    if attributes['skip_invalid_loci']:
                        continue
                    raise FatalError(f'Invalid locus {contig}:{position}')
                contig_key = reference.contig_index(contig)
            else:
                contig_key = contig
            key = (contig_key, position, alleles)
            lower = key if lower is None or key < lower else lower
            upper = key if upper is None or key > upper else upper
    return None if lower is None else (lower, upper)


def import_bgen(path, entry_fields, sample_file=None, n_partitions=None, block_size=None,
                index_file_map=None, variants=None, _row_fields=('varid', 'rsid')):
    from . import Table, _validate_n
    from .types import tstruct, tarray, tstr, tint32, tint64, tfloat64, tcall, tlocus
    from .reference_genome import resolve_reference
    from .matrixtable import MatrixTable, MatrixIR, _CID, _COL, _ROW, _CELLS
    from .text import _read
    for name, value in [('n_partitions', n_partitions), ('block_size', block_size)]:
        if value is not None:
            _validate_n(value, name, positive=True)
    entry_fields = [entry_fields] if isinstance(entry_fields, str) else list(entry_fields)
    if any(f not in ('GT', 'GP', 'dosage') for f in entry_fields):
        raise ValueError('entry_fields must contain only GT, GP, dosage')
    if any(f not in ('varid', 'rsid') for f in _row_fields):
        raise ValueError('_row_fields must contain only varid and rsid')
    paths = _bgen_paths(path)
    mapping = {_local_path(k): _local_path(v) for k,v in (index_file_map or {}).items()}
    metadata = []
    headers = []
    for filename in paths:
        index = mapping.get(filename, filename + '.idx2')
        if not os.path.exists(index):
            raise FatalError(f'BGEN files have no .idx2 index file: {filename}')
        metadata.append(_metadata(index))
        headers.append(_header(filename))
    references = {m['attributes']['reference_genome'] for m in metadata}
    if len(references) != 1:
        raise FatalError('Found multiple reference genomes were specified in the BGEN index files')
    reference_name = next(iter(references))
    reference = None if reference_name is None else resolve_reference(reference_name)
    if len(paths) > 1:
        bounds = [((meta['vail_variant_bounds'][0][0], meta['vail_variant_bounds'][0][1], tuple(meta['vail_variant_bounds'][0][2])),
                   (meta['vail_variant_bounds'][1][0], meta['vail_variant_bounds'][1][1], tuple(meta['vail_variant_bounds'][1][2])))
                  if meta.get('vail_variant_bounds') is not None
                  else _variant_bounds(filename, header, meta['attributes'], reference)
                  for filename, header, meta in zip(paths, headers, metadata)]
        bounds = sorted(bound for bound in bounds if bound is not None)
        if any(left[1] >= right[0] for left, right in zip(bounds, bounds[1:])):
            raise FatalError('Each BGEN file must contain a region of the genome disjoint from other files')
    n = headers[0][2]
    if any(h[2] != n for h in headers):
        raise ValueError('BGEN files have different numbers of samples')
    samples = _samples(sample_file, n) if sample_file is not None else headers[0][3]
    if samples is None:
        samples = [f'_{i}' for i in range(n)]
    locus_type = tstruct(contig=tstr, position=tint32) if reference is None else tlocus(reference.name)
    row = tstruct(locus=locus_type, alleles=tarray(tstr), **{f:tstr for f in ('rsid','varid') if f in _row_fields})
    entry = tstruct(**{f: {'GT':tcall, 'GP':tarray(tfloat64), 'dosage':tfloat64}[f] for f in entry_fields})
    schema = tstruct(**{_ROW:row, _CELLS:tarray(tstruct(**{_CID:tint64, 'value':entry}))})
    cores = []
    for filename, meta in zip(paths, metadata):
        options = dict(meta['attributes'], lengths=None if reference is None else reference.lengths,
                       contigs=None if reference is None else list(reference.contigs),
                       entry_fields=entry_fields, row_fields=list(_row_fields))
        cores.append(_read([filename], schema, 'bgen', options))
    core = cores[0].union(*cores[1:]) if len(cores)>1 else cores[0]
    cols = Table.parallelize([{_CID:i, _COL:{'s':s}} for i,s in enumerate(samples)],
                            schema=tstruct(**{_CID:tint64, _COL:tstruct(s=tstr)})).key_by()
    mt = MatrixTable(MatrixIR(core, cols, row_key=('locus','alleles'), col_key=('s',), _aligned=True))
    mt = mt._copy(rows=mt._sort_rows(mt._mir.core, mt._mir.row_key))
    if variants is not None:
        from .expressions import Expression
        if isinstance(variants, Expression):
            expression = variants
            fields = ({name: expression[name] for name in expression.dtype}
                      if isinstance(expression.dtype, tstruct) else {'locus': expression})
            keys = [name for name in ('locus', 'alleles') if name in fields]
            if isinstance(expression.source, MatrixTable):
                from .matrixtable import _project
                source = expression.source
                base = source._context('row')
                variants = _project(base, [(name, source._lower(value, base)) for name,value in fields.items()]).key_by(*keys)
            elif isinstance(expression.source, Table):
                variants = expression.source._project(list(fields.items()), keys=()).key_by(*keys)
            else:
                raise ValueError('variants expression must be row-indexed')
        if not isinstance(variants, Table):
            if not variants:
                return mt.filter_rows(False)
            first = variants[0]
            if hasattr(first, 'contig'):
                variants = [dict(locus=v) for v in variants]
            variants = Table.parallelize(variants)
            variants = variants.key_by(*[f for f in ('locus','alleles') if f in variants._schema])
        mt = mt.semi_join_rows(variants.select())
    if n_partitions is not None and block_size is None:
        mt = mt.repartition(min(n_partitions, max(1, sum(m['nKeys'] for m in metadata))))
    return mt
