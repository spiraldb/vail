"""Public native-table pipelines; preparation is outside query timing."""
import argparse
import hashlib
import json
import math
import pathlib
import resource
import time

import vail as hl
import vail._native as native

parser = argparse.ArgumentParser()
parser.add_argument('path')
parser.add_argument('--prepare', action='store_true')
parser.add_argument('--rows', type=int, default=8000)
parser.add_argument('--cols', type=int, default=1000)
parser.add_argument('--case', choices=['fractions4', 'fractions12', 'filter_features', 'sum'], default='fractions4')
parser.add_argument('--repeat', type=int, default=4)
a = parser.parse_args()
if a.prepare:
    m = hl.utils.range_matrix_table(a.rows, a.cols, n_partitions=8)
    m = m.annotate_entries(value=hl.or_missing(m.col_idx % 13 != 0, (m.row_idx + m.col_idx) % 101))
    m = m.filter_entries(m.col_idx % 11 != 0)
    m.write(a.path)
    raise SystemExit
thresholds = [-5, 20, 50, 95] if a.case == 'fractions4' else list(range(0, 120, 10))
present = [c for c in range(a.cols) if c % 11 != 0]
expected = sum((a.rows // 101 + (r < a.rows % 101)) * sum(
    sum(c % 13 != 0 and (r + c) % 101 > threshold for c in present) / len(present)
    for threshold in thresholds) for r in range(101))
if a.case == 'filter_features':
    expected = sum((a.rows // 101 + (r < a.rows % 101)) * sum(
        (r + c) % 101 + 1 for c in present if c % 13 != 0 and (r + c) % 101 > 50) for r in range(101))
if a.case == 'sum':
    expected = sum((a.rows // 101 + (r < a.rows % 101)) * sum(
        (r + c) % 101 for c in present if c % 13 != 0) for r in range(101))
print(json.dumps({'case': a.case, 'native': native.__file__, 'sha256': hashlib.sha256(pathlib.Path(native.__file__).read_bytes()).hexdigest(), 'expected': expected}), flush=True)
for iteration in range(a.repeat):
    before = resource.getrusage(resource.RUSAGE_SELF)
    start = time.perf_counter()
    m = hl.read_matrix_table(a.path)
    if a.case == 'filter_features':
        r = m.localize_entries('entries', 'cols')
        r = r.select(
            kept=r.entries.filter(lambda e: hl.is_defined(e.value) & (e.value > 50)),
            n=hl.len(r.entries.filter(lambda item: hl.is_defined(item.value) & (item.value > 50))))
        value = r.aggregate(hl.agg.sum(r.n) + hl.agg.sum(hl.sum(r.kept.map(lambda e: e.value))))
    elif a.case == 'sum':
        value = m.aggregate_entries(hl.agg.sum(m.value))
    else:
        r = m.annotate_rows(**{f'f{i}': hl.agg.fraction(m.value > t) for i, t in enumerate(thresholds)}).rows()
        value = r.aggregate(hl.agg.sum(sum(r[f'f{i}'] for i in range(len(thresholds)))))
    after = resource.getrusage(resource.RUSAGE_SELF)
    assert math.isclose(value, expected, rel_tol=1e-10, abs_tol=1e-10), (value, expected)
    print(json.dumps({'iteration': iteration, 'warmup': iteration == 0, 'wall': time.perf_counter() - start, 'cpu': after.ru_utime + after.ru_stime - before.ru_utime - before.ru_stime, 'maxrss': after.ru_maxrss, 'result': value}), flush=True)
