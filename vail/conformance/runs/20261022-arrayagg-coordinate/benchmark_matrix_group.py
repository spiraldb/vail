import json
import os
import resource
import statistics
import time

import vail as hl

rows = int(os.environ.get("VAIL_BENCH_ROWS", "100000"))
cols = int(os.environ.get("VAIL_BENCH_COLS", "128"))
partitions = int(os.environ.get("VAIL_BENCH_PARTITIONS", "16"))
repeats = int(os.environ.get("VAIL_BENCH_REPEATS", "3"))

matrix = hl.range_matrix_table(rows, cols, n_partitions=partitions)
matrix = matrix.annotate_entries(x=matrix.row_idx + matrix.col_idx)
grouped = matrix.group_rows_by(new_idx=matrix.row_idx % 3).aggregate(
    x=hl.agg.mean(matrix.x)
)

# Warm once outside the reported trials; each timed action still executes the
# complete lazy query and forces the grouped row result.
assert grouped._force_count_rows() == 3
trials = []
for trial in range(repeats):
    wall0 = time.perf_counter()
    cpu0 = time.process_time()
    count = grouped._force_count_rows()
    cpu = time.process_time() - cpu0
    wall = time.perf_counter() - wall0
    assert count == 3
    trials.append({"trial": trial + 1, "wall_s": wall, "process_cpu_s": cpu})

# Verify the entry aggregate is materialized (and correct), rather than relying
# only on a row-count action that might prune entry outputs.
entries = grouped.entries().collect()
assert len(entries) == 3 * cols
expected_row_mean = {0: (rows - 1) / 2, 1: (rows - 1) / 2 - 0.5, 2: (rows - 1) / 2 + 0.5}
for entry in entries:
    expected = expected_row_mean[entry.new_idx] + entry.col_idx
    assert abs(entry.x - expected) < 1e-9, (entry.new_idx, entry.col_idx, entry.x, expected)

# macOS ru_maxrss is bytes.
rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
print(json.dumps({
    "rows": rows,
    "cols": cols,
    "partitions": partitions,
    "repeats": repeats,
    "query": "range_matrix_table; annotate_entries(x=row_idx+col_idx); group_rows_by(row_idx%3).aggregate(x=mean(x)); _force_count_rows",
    "warmup_excluded": True,
    "peak_rss_bytes": rss,
    "trials": trials,
    "median_wall_s": statistics.median(t["wall_s"] for t in trials),
    "median_process_cpu_s": statistics.median(t["process_cpu_s"] for t in trials),
}, sort_keys=True))
