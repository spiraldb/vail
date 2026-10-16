"""Bounded public execution with a large shared scalar string argument."""
import json
import resource
import time
import vail as hl

for rows in [1000, 10000]:
    table = hl.range_table(rows)
    start = time.perf_counter()
    out = table.select(
        h=hl.hamming('😀' * 8192, hl.str(table.idx)),
        g=hl.str(table.idx).first_match_in('(x)'),
    ).collect()
    assert all(row.h is None and row.g is None for row in out)
    print(json.dumps({'rows': rows, 'shared_scalar_codepoints': 8192,
                      'seconds': time.perf_counter() - start,
                      'max_rss_bytes': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}))
