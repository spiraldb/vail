"""Independent Hail writer/oracle and public Vail reader validation.

Run `hail ROOT`, `vail ROOT`, or `measure ROOT CASE` in separate environments.
ROOT contains generated data, never implementation-derived expected results.
"""
import argparse
from collections.abc import Mapping
import hashlib
import json
import math
from pathlib import Path
import resource
import time


def canonical(value):
    if value is None:
        return None
    kind = type(value).__name__
    if kind == 'Locus':
        return {'locus': [value.contig, value.position, value.reference_genome.name]}
    if kind == 'Call':
        return {'call': [list(value.alleles), value.phased]}
    if kind == 'Interval':
        return {'interval': [canonical(value.start), canonical(value.end), value.includes_start, value.includes_end]}
    if kind == 'ndarray':
        return {'ndarray': [list(value.shape), str(value.dtype), canonical(value.tolist())]}
    if isinstance(value, Mapping):
        if all(isinstance(k, str) for k in value):
            return {k: canonical(v) for k, v in sorted(value.items())}
        return {'dictionary': sorted([[canonical(k), canonical(v)] for k, v in value.items()], key=lambda x: json.dumps(x[0], sort_keys=True))}
    if isinstance(value, (set, frozenset)):
        return {'set': sorted([canonical(v) for v in value], key=lambda x: json.dumps(x, sort_keys=True))}
    if isinstance(value, (tuple, list)) or kind == 'frozenlist':
        return [canonical(v) for v in value]
    if isinstance(value, float) and not math.isfinite(value):
        return {'float': repr(value)}
    return value


def snapshot(hl, dataset, matrix=False):
    def describe(dtype):
        kind = type(dtype).__name__
        if kind == 'tstruct':
            return {'struct': [[n, describe(ty)] for n, ty in dtype.items()]}
        if kind == 'ttuple':
            return {'tuple': [describe(ty) for ty in dtype.types]}
        if kind == 'tlocus':
            return {'locus': dtype.reference_genome.name}
        if kind == 'tinterval':
            return {'interval': describe(dtype.point_type)}
        if kind == 'tdict':
            return {'dict': [describe(dtype.key_type), describe(dtype.value_type)]}
        if kind == 'tndarray':
            return {'ndarray': [describe(dtype.element_type), dtype.ndim]}
        if kind in ('tarray', 'tset'):
            return {kind: describe(dtype.element_type)}
        return str(dtype)
    if matrix:
        table = dataset.localize_entries('cells', 'columns')
        return dict(rows=canonical(table.collect()), globals=canonical(hl.eval(table.index_globals())),
                    row_key=list(dataset.row_key), col_key=list(dataset.col_key),
                    schemas={axis: describe(getattr(dataset, axis).dtype) for axis in ('row', 'col', 'entry', 'globals')})
    return dict(rows=canonical(dataset.collect()), globals=canonical(hl.eval(dataset.index_globals())),
                key=list(dataset.key), schemas={axis: describe(getattr(dataset, axis).dtype) for axis in ('row', 'globals')})


def generate(root):
    import hail as hl
    hl.init(master='local[2]', quiet=True, log=str(root / 'hail.log'))
    table = hl.utils.range_table(23, n_partitions=5)
    i = table.idx
    table = table.annotate(
        i64=hl.if_else(i % 3 == 0, hl.int64(-9223372036854775808), hl.int64(9223372036854775807)),
        f64=hl.if_else(i % 4 == 0, float('nan'), hl.if_else(i % 4 == 1, float('inf'), -1.25 * i)),
        text=hl.if_else(i % 5 == 0, hl.missing(hl.tstr), hl.str(i) + '😀é' * 20000),
        array=hl.range(i % 9).map(lambda x: hl.or_missing(x % 3 != 1, x - 4)),
        nested=hl.or_missing(i % 4 != 0, hl.struct(x=hl.or_missing(i % 2 == 0, i), flag=i % 3 == 0)),
        call=hl.or_missing(i % 6 != 0, hl.call(i % 3, (i + 1) % 4, phased=True)),
        interval=hl.interval(i, i + 2, includes_start=False, includes_end=True),
        nd=hl.nd.array([[i, -i, 7], [0, i + 1, -8]]),
        dictionary=hl.dict([(hl.array(['x', hl.missing(hl.tstr)]), hl.float64(i))]),
        aset=hl.set([hl.missing(hl.tstr), 'x', hl.str(i)]),
        **{'escaped field ` : 😀': hl.tuple([i, hl.missing(hl.tbool)])},
    ).annotate_globals(g=hl.missing(hl.tarray(hl.tint64)), description='live Hail oracle')
    matrix = hl.utils.range_matrix_table(9, 7, n_partitions=4)
    matrix = matrix.annotate_rows(locus=hl.locus('1', matrix.row_idx // 2 + 1), label='row-' + hl.str(matrix.row_idx))
    matrix = matrix.key_rows_by('locus')
    matrix = matrix.annotate_cols(s=hl.str(6 - matrix.col_idx // 2))
    matrix = matrix.key_cols_by('s')
    matrix = matrix.annotate_entries(
        GT=hl.or_missing(matrix.row_idx % 3 != 0, hl.call(matrix.row_idx % 3, matrix.col_idx % 4, phased=True)),
        value=hl.int64(matrix.row_idx) * 100 - matrix.col_idx,
        array=hl.array([matrix.row_idx, hl.missing(hl.tint32), matrix.col_idx]),
    )
    matrix = matrix.filter_entries((matrix.row_idx * 7 + matrix.col_idx) % 4 != 0)
    matrix = matrix.annotate_globals(note='duplicate keys and filtered cells', missing=hl.missing(hl.tint32))
    datasets = {'varied.ht': (table, False), 'varied.mt': (matrix, True)}
    expected = {'hail_version': hl.__version__, 'datasets': {}}
    for name, (dataset, is_matrix) in datasets.items():
        dataset.write(str(root / name), overwrite=True)
        reread = hl.read_matrix_table(str(root / name)) if is_matrix else hl.read_table(str(root / name))
        expected['datasets'][name] = snapshot(hl, reread, is_matrix)
    # Nullable arrays stress bitmap width; a larger matrix measures wrapper and
    # Arrow peak memory. Expected reductions are independent closed forms.
    for n in (100000, 400000):
        arr = hl.utils.range_table(1).annotate(a=hl.range(n).map(lambda j: hl.or_missing(j % 3 != 1, j)))
        arr.write(str(root / f'array-{n}.ht'), overwrite=True)
    large = hl.utils.range_matrix_table(5000, 100, n_partitions=8)
    large = large.annotate_entries(x=large.row_idx * 100 + large.col_idx, flag=large.col_idx % 2 == 0)
    large = large.filter_entries((large.row_idx + large.col_idx) % 7 != 0)
    large.write(str(root / 'scale.mt'), overwrite=True)
    (root / 'expected.json').write_text(json.dumps(expected, sort_keys=True, ensure_ascii=False, allow_nan=False))
    hl.stop()
    print(json.dumps({'status': 'generated', 'hail_version': expected['hail_version']}, sort_keys=True))


def check(root):
    import vail as hl
    expected = json.loads((root / 'expected.json').read_text())
    result = {'hail_version': expected['hail_version'], 'checks': []}
    for name, want in expected['datasets'].items():
        started = time.perf_counter()
        is_matrix = name.endswith('.mt')
        dataset = (hl.MatrixTable if is_matrix else hl.Table).read_legacy(root / name)
        actual = snapshot(hl, dataset, is_matrix)
        assert actual == want, name
        result['checks'].append({'dataset': name, 'status': 'passed', 'wall_seconds': time.perf_counter() - started,
                                  'canonical_sha256': hashlib.sha256(json.dumps(actual, sort_keys=True, ensure_ascii=False).encode()).hexdigest()})
    print(json.dumps(result, sort_keys=True))


def oracle_snapshot(root):
    import hail as hl
    hl.init(master='local[2]', quiet=True, log=str(root / 'hail-snapshot.log'))
    result = {'hail_version': hl.__version__, 'datasets': {}}
    for name in ('varied.ht', 'varied.mt'):
        matrix = name.endswith('.mt')
        obj = hl.read_matrix_table(str(root / name)) if matrix else hl.read_table(str(root / name))
        result['datasets'][name] = snapshot(hl, obj, matrix)
    (root / 'expected.json').write_text(json.dumps(result, sort_keys=True, ensure_ascii=False, allow_nan=False))
    hl.stop()


def measure(root, name):
    import vail as hl
    started = time.perf_counter()
    cpu_start = time.process_time()
    matrix = name.endswith('.mt')
    dataset = (hl.MatrixTable if matrix else hl.Table).read_legacy(root / name)
    import_wall = time.perf_counter() - started
    if matrix:
        assert dataset.count() == (5000, 100)
        entries = dataset.entries().count()
        assert entries == sum((i + j) % 7 != 0 for i in range(5000) for j in range(100))
    else:
        row = dataset.collect()[0]
        n = int(name.split('-')[1].split('.')[0])
        assert len(row.a) == n
        assert row.a == [None if j % 3 == 1 else j for j in range(n)]
    usage = resource.getrusage(resource.RUSAGE_SELF)
    print(json.dumps({'case': name, 'status': 'passed', 'import_wall_seconds': import_wall,
                      'wall_seconds': time.perf_counter() - started, 'cpu_seconds': time.process_time() - cpu_start,
                      'peak_rss_bytes_macos': usage.ru_maxrss}, sort_keys=True))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=('hail', 'oracle-snapshot', 'vail', 'measure'))
    parser.add_argument('root', type=Path)
    parser.add_argument('case', nargs='?')
    args = parser.parse_args()
    args.root.mkdir(parents=True, exist_ok=True)
    if args.mode == 'hail':
        generate(args.root)
    elif args.mode == 'oracle-snapshot':
        oracle_snapshot(args.root)
    elif args.mode == 'vail':
        check(args.root)
    else:
        measure(args.root, args.case)
