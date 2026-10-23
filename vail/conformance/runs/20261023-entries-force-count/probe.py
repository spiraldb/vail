"""Public workload probe; execute preparation separately from measurements."""
import argparse
import json
import os
import resource
import time
from pathlib import Path
import vail as hl

p = argparse.ArgumentParser()
p.add_argument('path', type=Path)
p.add_argument('--prepare', action='store_true')
p.add_argument('--rows', type=int, default=10000)
p.add_argument('--cols', type=int, default=2500)
p.add_argument('--iterations', type=int, default=3)
p.add_argument('--case', choices=['rows', 'entries', 'collect'], default='entries')
a = p.parse_args()
if a.prepare:
    m = hl.range_matrix_table(a.rows, a.cols, 16)
    m = m.annotate_entries(DP=(m.row_idx + m.col_idx) % 51,
                           GT=hl.call(m.row_idx % 2, m.col_idx % 2))
    m.write(a.path)
    print(json.dumps({'prepared': str(a.path), 'rows': a.rows, 'cols': a.cols}))
else:
    m = hl.read_matrix_table(a.path)
    print(json.dumps({'pid': os.getpid(), 'python': hl.__file__,
                      'row_key_unique': m._mir.row_key_unique,
                      'column_ordering': m._mir.column_ordering}), flush=True)
    print(m.entries()._native.physical_plan(), flush=True)
    for rep in range(a.iterations + 1):
        before = resource.getrusage(resource.RUSAGE_SELF)
        start = time.monotonic()
        if a.case == 'entries': value = m.entries()._force_count()
        elif a.case == 'rows': value = m._force_count_rows()
        else: value = len(m.entries().collect())
        after = resource.getrusage(resource.RUSAGE_SELF)
        print(json.dumps({'iteration': rep, 'warmup': rep == 0, 'case': a.case,
                          'value': value, 'wall_seconds': time.monotonic() - start,
                          'cpu_seconds': after.ru_utime + after.ru_stime - before.ru_utime - before.ru_stime,
                          'peak_rss_platform_units': after.ru_maxrss}), flush=True)
