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
    if length > os.fstat(f.fileno()).st_size - f.tell():
        raise ValueError('truncated BGEN identifier')
    data = f.read(length)
    if len(data) != length:
        raise ValueError('truncated BGEN identifier')
    return data.decode('utf-8')


def _header(path):
    with open(path, 'rb') as f:
        offset, length, variants, n = (_uint(f) for _ in range(4))
        magic = f.read(4)
        if length < 20 or length > offset or n > 2**31-1 or magic not in (b'bgen', b'\0' * 4):
            raise ValueError('invalid BGEN header')
        f.seek(length)
        flags = _uint(f)
        if flags >> 2 & 15 != 2 or flags & 3 > 2:
            raise ValueError('BGEN requires layout 2 and zlib, zstd or uncompressed blocks')
        samples = None
        if flags >> 31:
            sample_length, sample_n = _uint(f), _uint(f)
            if sample_n != n or sample_length < 8 + 2*n or length + sample_length > offset:
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
                if position > 2**31 - 1:
                    raise FatalError('BGEN position exceeds signed 32-bit locus range')
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


def _indexed_variants(filename, header, metadata, reference):
    """Read legacy identifying metadata once; own indexes provide exact offsets.

    Legacy Hail idx2 does not expose endpoints/offsets in its JSON footer. This
    compatibility walk never reads probability blocks and is replaced entirely
    by stored offsets for indexes generated by index_bgen.
    """
    if 'vail_variant_offsets' in metadata:
        return metadata['vail_variant_offsets']
    attributes = metadata['attributes']
    recoding = attributes['contig_recoding']
    lengths = None if reference is None else reference.lengths
    variants = []
    with open(filename, 'rb') as f:
        f.seek(header[0])
        file_length = os.fstat(f.fileno()).st_size
        for _ in range(header[1]):
            offset = f.tell()
            _string(f); _string(f)
            contig = _string(f)
            contig = recoding.get(contig, contig)
            position = _uint(f)
            if position > 2**31 - 1:
                raise FatalError('BGEN position exceeds signed 32-bit locus range')
            k = _uint(f, 2)
            if k != 2:
                raise FatalError('BGEN requires biallelic variants')
            alleles = [_string(f, 4) for _ in range(k)]
            size = _uint(f)
            if f.tell() + size > file_length:
                raise FatalError('truncated BGEN variant block')
            f.seek(size, 1)
            if lengths is not None and (contig not in lengths or not 0 < position <= lengths[contig]):
                if attributes['skip_invalid_loci']:
                    continue
                raise FatalError(f'Invalid locus {contig}:{position}')
            variants.append([contig,position,alleles,offset])
    return variants


def _literal_variants(variants):
    """Normalize supported literal key shapes without evaluating expressions."""
    keys = set()
    for value in variants:
        locus = value if hasattr(value, 'contig') else value['locus']
        contig = locus.contig if hasattr(locus, 'contig') else locus['contig']
        position = locus.position if hasattr(locus, 'position') else locus['position']
        alleles = None if hasattr(value, 'contig') or 'alleles' not in value else tuple(value['alleles'])
        keys.add((contig, position, alleles))
    return keys


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
    indexes = [_indexed_variants(filename, header, meta, reference)
               for filename, header, meta in zip(paths, headers, metadata)]
    if len(paths) > 1:
        bounds = sorted(bound for index in indexes if (bound := _offset_bounds(index, reference)) is not None)
        if any(left[1] >= right[0] for left, right in zip(bounds, bounds[1:])):
            raise FatalError('Each BGEN file must contain a region of the genome disjoint from other files')
    from .expressions import Expression
    literal_variants = variants is not None and not isinstance(variants, (Table, Expression))
    if literal_variants:
        desired = _literal_variants(variants)
        indexes = [[v for v in index if (v[0],v[1],None) in desired or (v[0],v[1],tuple(v[2])) in desired]
                   for index in indexes]
    n = headers[0][2]
    if any(h[2] != n for h in headers):
        raise ValueError('BGEN files have different numbers of samples')
    samples = _samples(sample_file, n) if sample_file is not None else headers[0][3]
    locus_type = tstruct(contig=tstr, position=tint32) if reference is None else tlocus(reference.name)
    row = tstruct(locus=locus_type, alleles=tarray(tstr), **{f:tstr for f in ('rsid','varid') if f in _row_fields})
    entry = tstruct(**{f: {'GT':tcall, 'GP':tarray(tfloat64), 'dosage':tfloat64}[f] for f in entry_fields})
    schema = tstruct(**{_ROW:row, _CELLS:tarray(tstruct(**{'value':entry}))})
    selector = None
    if variants is not None and not literal_variants:
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
        selector = variants.select().select_globals()
    # Partition the indexed records themselves. Each native source seeks only
    # its selected variant offsets, before reading any probability blocks.
    key = lambda v: (reference.contig_index(v[0]) if reference is not None else v[0], v[1], tuple(v[2]))
    for index in indexes:
        index.sort(key=key)
    total = sum(len(index) for index in indexes)
    target = min(n_partitions, max(total, 1)) if n_partitions is not None and block_size is None else None
    chunks = []
    ordered = sorted(zip(paths, metadata, indexes), key=lambda item: key(item[2][0]) if item[2] else (-1, 0, ()) if reference is not None else ('',0,()))
    if target is not None:
        # Allocate source partitions proportionally, with at least one for every
        # nonempty file. Empty inputs retain a single empty source partition.
        allocations = [max(1, round(target*len(index)/max(total,1))) for _,_,index in ordered]
        while sum(allocations) < target:
            possible = [i for i,(_,_,index) in enumerate(ordered) if allocations[i]<len(index)]
            if not possible: break
            allocations[max(possible,key=lambda i:len(ordered[i][2])/allocations[i])] += 1
        while sum(allocations) > max(target,len(ordered)):
            possible = [i for i,npart in enumerate(allocations) if npart>1]
            if not possible: break
            allocations[max(possible,key=lambda i:allocations[i])] -= 1
        for (filename,meta,index),npart in zip(ordered,allocations):
            for i in range(npart):
                selected = index[len(index)*i//npart:len(index)*(i+1)//npart]
                chunks.append((filename,meta,selected))
    else:
        limit = (block_size if block_size is not None else 128)*1024*1024
        for filename,meta,index in ordered:
            start = 0
            for end in range(1,len(index)):
                if index[end][3] - index[start][3] >= limit:
                    chunks.append((filename,meta,index[start:end])); start=end
            chunks.append((filename,meta,index[start:]))
    partition_options = []
    for filename,meta,index in chunks:
        options = dict(meta['attributes'], lengths=None if reference is None else reference.lengths,
                       contigs=None if reference is None else list(reference.contigs),
                       entry_fields=entry_fields, row_fields=list(_row_fields),
                       variant_offsets=[v[3] for v in index],
                       variant_keys=[[v[0],v[1],v[2]] for v in index])
        partition_options.append(options)
    core = _read([filename for filename,_,_ in chunks], schema, 'bgen',
                 {'partition_options': partition_options})
    if selector is not None:
        core = Table(core._native.bgen_select(selector._native), schema, ())
    if samples is None:
        from . import range_table, int64, str as text, struct as record
        columns = range_table(n).key_by()
        cols = columns.select(**{_CID:int64(columns.idx), _COL:record(s='_' + text(columns.idx))})
    else:
        cols = Table.parallelize([{_CID:i, _COL:{'s':s}} for i,s in enumerate(samples)],
                                 schema=tstruct(**{_CID:tint64, _COL:tstruct(s=tstr)})).key_by()
    mt = MatrixTable(MatrixIR(core, cols, row_key=('locus','alleles'), col_key=('s',), _aligned=True))
    # The index keys and source partitions above are in canonical genomic
    # order. Keep that established order without a global sort that would
    # collapse indexed source partitions into one physical partition.
    return mt


def export_bgen(mt, output, gp=None, varid=None, rsid=None, parallel=None, compression_codec='zlib'):
    """Write BGEN 1.2 with 8-bit probabilities using a native Arrow stream.

    Column identifiers alone are localized for the SAMPLE/header metadata.
    Genotype arrays remain lazy and are serialized natively one variant at a time.
    """
    from . import (MatrixTable, array, delimit, str as text, coalesce, literal)
    from .types import tarray, tfloat64, tstr, tlocus
    from .expressions import Expression, as_expr
    if not isinstance(mt, MatrixTable):
        raise TypeError('export_bgen requires a MatrixTable')
    if tuple(mt.row_key.dtype) != ('locus', 'alleles') or not isinstance(mt.locus.dtype, tlocus) or mt.alleles.dtype != tarray(tstr):
        raise ValueError('export_bgen requires row key locus and alleles')
    if len(mt.col_key.dtype) != 1 or next(iter(mt.col_key.dtype.values())) != tstr:
        raise ValueError('export_bgen requires one string column key')
    if parallel not in (None, 'header_per_shard', 'separate_header'):
        raise ValueError('parallel must be None, header_per_shard, or separate_header')
    if compression_codec not in ('zlib', 'zstd'):
        raise ValueError('compression_codec must be zlib or zstd')
    if gp is None:
        if mt.entry.dtype.get('GP') != tarray(tfloat64):
            raise ValueError('exporting to BGEN requires a GP array<float64> field in the entry')
        gp = mt.GP
    gp = as_expr(gp)
    if gp.dtype != tarray(tfloat64):
        raise TypeError('gp must have type array<float64>')
    if varid is None and mt.row.dtype.get('varid') == tstr:
        varid = mt.varid
    if rsid is None and mt.row.dtype.get('rsid') == tstr:
        rsid = mt.rsid
    default_id = delimit(array([mt.locus.contig, text(mt.locus.position), mt.alleles[0], mt.alleles[1]]), ':')
    varid = default_id if varid is None else coalesce(as_expr(varid), default_id)
    rsid = literal('.') if rsid is None else coalesce(as_expr(rsid), '.')
    if varid.dtype != tstr or rsid.dtype != tstr:
        raise TypeError('varid and rsid must be string expressions')
    for expression,axis in [(gp,'entry'), (varid,'row'), (rsid,'row')]:
        mt._validate(expression,axis)
    columns = mt._public_axis('col', key=False)
    key = next(iter(mt.col_key.dtype))
    samples = columns.select(id=columns[key]).id.collect()
    if any(s is None or not s or any(c.isspace() for c in s) for s in samples):
        raise ValueError('BGEN sample identifiers must be nonempty and contain no whitespace')
    matrix = mt.annotate_rows(__bgen_varid=varid, __bgen_rsid=rsid)
    gp = Expression(gp.dtype, matrix if gp.source is not None else None, gp.to_ir())
    matrix = matrix.select_entries(__bgen_gp=gp)
    rows = matrix.localize_entries('__bgen_entries', '__bgen_cols').key_by()
    rows = rows.select(contig=rows.locus.contig, position=rows.locus.position,
                       alleles=rows.alleles, varid=rows.__bgen_varid, rsid=rows.__bgen_rsid,
                       gp=rows.__bgen_entries.map(lambda entry: entry.__bgen_gp))
    output = _local_path(output)
    rows._native.export_bgen(output + '.bgen', samples, parallel, compression_codec)
    with open(output + '.sample', 'w') as stream:
        stream.write('ID_1 ID_2 missing\n0 0 0\n')
        for sample in samples:
            stream.write(f'{sample} {sample} 0\n')
