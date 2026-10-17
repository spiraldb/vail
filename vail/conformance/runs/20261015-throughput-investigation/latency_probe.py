"""Warm public total-action latency; query results stay inside the worker."""
import argparse
import hashlib
import json
from pathlib import Path
import statistics
import sys
import time

parser = argparse.ArgumentParser()
parser.add_argument("--package", type=Path, required=True)
parser.add_argument("--output", type=Path, required=True)
parser.add_argument("--repetitions", type=int, default=40)
args = parser.parse_args()
package = args.package.resolve()
native = package / "vail/_native.abi3.so"
if not native.is_file():
    raise FileNotFoundError(native)
sys.path.insert(0, str(package))
import vail as hl
assert Path(hl.__file__).resolve().parent == package / "vail"

cases = []
for size in (0, 1, 32, 1024):
    table = hl.range_table(size, n_partitions=1)
    expression = hl.agg.sum(table.idx)
    expected = size * (size - 1) // 2
    for _ in range(5):
        assert table.aggregate(expression) == expected
    elapsed = []
    cpu = []
    for _ in range(args.repetitions):
        cpu_started = time.process_time_ns()
        started = time.perf_counter_ns()
        actual = table.aggregate(expression)
        elapsed.append((time.perf_counter_ns() - started) / 1e6)
        cpu.append((time.process_time_ns() - cpu_started) / 1e6)
        assert actual == expected
    cases.append({
        "rows": size,
        "wall_ms": elapsed,
        "cpu_ms": cpu,
        "median_wall_ms": statistics.median(elapsed),
        "p95_wall_ms": statistics.quantiles(elapsed, n=20, method="inclusive")[18],
        "median_cpu_ms": statistics.median(cpu),
    })
args.output.write_text(json.dumps({
    "native_sha256": hashlib.sha256(native.read_bytes()).hexdigest(),
    "package": str(package),
    "warmups": 5,
    "repetitions": args.repetitions,
    "cases": cases,
}, indent=2) + "\n")
