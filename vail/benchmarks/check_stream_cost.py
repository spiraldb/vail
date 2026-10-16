"""Bounded public stream-scan and keyed streaming workloads."""
import hashlib
import json
from pathlib import Path
import resource
import time

import vail as hl
import vail._native as native


def report(workload, start, **details):
    print(json.dumps(dict(workload=workload, seconds=time.perf_counter() - start,
                          max_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                          **details)), flush=True)


print(json.dumps({'native_sha256': hashlib.sha256(Path(native.__file__).read_bytes()).hexdigest()}))
hl.eval(1)
for n in [1024, 4096, 16384, 65536]:
    start = time.perf_counter()
    result = hl.eval(hl.stream_range(n).aggregate_scan(lambda x: hl.scan.sum(x)).to_array())
    assert result[-1] == (n - 2) * (n - 1) // 2
    report('exclusive_sum', start, elements=n)

for transform in [lambda rows: rows, lambda rows: rows.map(lambda row: row.annotate(x=row.idx + 1)),
                  lambda rows: rows.filter(lambda row: row.idx >= 0)]:
    start = time.perf_counter()
    table = hl.range_table(2**31 - 1, n_partitions=3).map_partitions(transform)
    assert table.key.dtype == hl.tstruct(idx=hl.tint32)
    assert table.head(1).collect()[0].idx == 0
    report('keyed_partition_head', start, source_rows=2**31 - 1, source_partitions=3)

for width in [64, 256, 1024]:
    start = time.perf_counter()
    table = hl.range_table(1).map_partitions(
        lambda rows: rows.map(lambda row: hl.struct(idx=row.idx, **{f'x{i}': row.idx + i for i in range(width)})))
    result = table.collect()
    assert result[0][f'x{width-1}'] == width - 1
    report('wide_partition_map', start, fields=width + 1)
