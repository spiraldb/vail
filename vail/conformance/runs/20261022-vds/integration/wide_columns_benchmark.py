"""Fresh-process public-API RSS/time comparison for unused column payloads.

Use identical arguments with the frozen baseline and candidate PYTHONPATHs.
Reported results include native hashes; timing covers the action, RSS covers
this complete process. No private counters or implementation probes are used.
Defaults bound even a suspected whole-column-list-per-entry intermediate to
about 512 MiB of string bytes. The intended single-payload-per-entry expansion
is about 16 MiB; these estimates exclude Arrow buffers, other fields and copies.
The former 128x128/16KiB case overflowed an Arrow offset in current main and is
not a valid throughput comparison; retain that failure separately.
"""
import argparse
import hashlib
import json
import platform
from pathlib import Path
import resource
import time

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--label', required=True)
p.add_argument('--rows', type=int, default=128)
p.add_argument('--cols', type=int, default=32)
p.add_argument('--payload-bytes', type=int, default=4096)
a = p.parse_args()
import vail as hl
import vail._native as native

start = time.perf_counter()
m = hl.range_matrix_table(a.rows, a.cols, 4)
m = m.annotate_cols(payload=hl.struct(
    text=hl.str(m.col_idx) + 'x' * a.payload_bytes,
    nested=hl.struct(values=hl.range(32), marker=m.col_idx),
))
m = m.annotate_entries(value=m.row_idx * 3 + 1)
construction = time.perf_counter() - start
wall = time.perf_counter()
cpu = time.process_time()
result = m.aggregate_entries(hl.struct(n=hl.agg.count(), checksum=hl.agg.sum(m.value)))
wall = time.perf_counter() - wall
cpu = time.process_time() - cpu
assert result.n == a.rows * a.cols
assert result.checksum == a.cols * (3 * a.rows * (a.rows - 1) // 2 + a.rows)
rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
path = Path(native.__file__).resolve()
print(json.dumps(dict(
    label=a.label, rows=a.rows, cols=a.cols, payload_bytes=a.payload_bytes,
    native_path=str(path), native_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
    construction_seconds=construction, wall_seconds=wall, cpu_seconds=cpu,
    input_text_bytes=sum(a.payload_bytes + len(str(c)) for c in range(a.cols)),
    one_copy_per_entry_text_bytes=a.rows * sum(a.payload_bytes + len(str(c)) for c in range(a.cols)),
    whole_column_list_per_entry_text_bytes=a.rows * a.cols * sum(a.payload_bytes + len(str(c)) for c in range(a.cols)),
    peak_rss_bytes=rss if platform.system() == 'Darwin' else rss * 1024,
    result=dict(n=result.n, checksum=result.checksum), correctness='pass',
)), flush=True)
