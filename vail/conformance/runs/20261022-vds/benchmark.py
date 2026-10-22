"""Exploratory end-to-end densification timing, after engine initialization.

Run separately for Hail and Vail, providing equivalent native fixture paths.
This is a smoke comparison on a shared host, not a throughput benchmark.
"""

import argparse
import json
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("engine", choices=["hail", "vail"])
parser.add_argument("--vds-path", required=True)
args = parser.parse_args()
if args.engine == "hail":
    import hail as hl

    hl.init(master="local[2]", quiet=True)
else:
    import vail as hl
vds = hl.vds.read_vds(args.vds_path)
try:
    for i in range(2):
        start = time.monotonic()
        matrix = hl.vds.to_dense_mt(vds)
        planning = time.monotonic() - start
        start = time.monotonic()
        rows = matrix.localize_entries("entries", "cols").collect()
        execution = time.monotonic() - start
        print(
            json.dumps(
                dict(
                    engine=args.engine,
                    iteration=i,
                    plan_seconds=planning,
                    execute_seconds=execution,
                    rows=len(rows),
                )
            ),
            flush=True,
        )
finally:
    if args.engine == "hail":
        hl.stop()
