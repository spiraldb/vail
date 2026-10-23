"""Paired reads of identical Hail-written legacy files; no format conversion."""
import argparse
import json
import statistics
import time
from pathlib import Path

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('engine', choices=['hail', 'vail'])
p.add_argument('--output', required=True)
p.add_argument('--warmup', type=int, default=2)
p.add_argument('--runs', type=int, default=5)
p.add_argument('--cases', nargs='+', default=['table_force_count', 'table_scalar_sum', 'matrix_decode_count', 'matrix_decode_count_gt', 'matrix_entries_count', 'matrix_sum_dp'])
p.add_argument('--matrix', default='/Users/danielking/projects/vail/vail/conformance/runs/20261016-hail-paired/data/profile25.mt')
p.add_argument('--table', default='/private/tmp/vail-legacy-native-benchmark.ht')
a = p.parse_args()
if a.engine == 'hail':
    import hail as hl
    hl.init(master='local[14]', quiet=True, log=a.output + '.hail.log')
    read_table, read_matrix = hl.read_table, hl.read_matrix_table
else:
    import vail as hl
    read_table, read_matrix = hl.Table.read_legacy, hl.MatrixTable.read_legacy
matrix, table = a.matrix, a.table
def run(case):
    if case.startswith('table'):
        t = read_table(table)
        return t._force_count() if case == 'table_force_count' else t.aggregate(hl.agg.sum(t.x))
    m = read_matrix(matrix)
    if case == 'matrix_decode_count_gt':
        return m.select_entries('GT')._force_count_rows()
    if case == 'matrix_decode_count':
        return m._force_count_rows()
    if case == 'matrix_entries_count':
        return m.entries()._force_count()
    return m.aggregate_entries(hl.agg.sum(m.DP))
results = {}
for case in a.cases:
    times, values = [], []
    try:
        for i in range(a.warmup + a.runs):
            start = time.perf_counter()
            value = run(case)
            elapsed = time.perf_counter() - start
            values.append(value)
            assert all(v == value for v in values)
            if i >= a.warmup:
                times.append(elapsed)
            print(case, i, elapsed, value, flush=True)
        results[case] = dict(seconds=times, median=statistics.median(times), value=value)
    except Exception as e:
        results[case] = dict(error=repr(e))
        print(case, repr(e), flush=True)
    Path(a.output).write_text(json.dumps(dict(engine=a.engine, warmup=a.warmup, runs=a.runs, matrix=matrix, table=table, results=results), indent=2)+'\n')
if a.engine == 'hail':
    hl.stop()
