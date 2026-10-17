"""Bounded public consumer for large constant array generators."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

parser = argparse.ArgumentParser()
parser.add_argument("--package", type=Path, required=True)
parser.add_argument("--output", type=Path, required=True)
parser.add_argument("--rows", type=int, required=True)
parser.add_argument("--length", type=int, required=True)
parser.add_argument("--kind", choices=("zeros", "range"), required=True)
args = parser.parse_args()
if args.rows < 0 or args.length < 0:
    parser.error("--rows and --length must be nonnegative")

package = args.package.resolve()
output = args.output.resolve()
output.mkdir(parents=True, exist_ok=True)
vail_package = package / "vail"
native = vail_package / "_native.abi3.so"
if not (vail_package / "__init__.py").is_file() or not native.is_file():
    raise FileNotFoundError(f"package is missing expected vail files: {package}")
sys.path.insert(0, str(package))
import vail as hl

if Path(hl.__file__).resolve() != (vail_package / "__init__.py").resolve():
    raise RuntimeError(f"imported vail from {hl.__file__}, expected {vail_package}")
native_sha256 = hashlib.sha256(native.read_bytes()).hexdigest()

repo_root = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(repo_root / "benchmarks"))
from benchmark_telemetry import Measurement

os.environ["VAIL_BENCHMARK_MAX_RSS_GIB"] = "6"
measurement = Measurement(output / "telemetry").start()
guard_dir = output / "guard"
guard_dir.mkdir(parents=True, exist_ok=True)
run_case = Path(__file__).with_name("run_case.py")
memory_guard = subprocess.Popen(
    [sys.executable, str(run_case), "--memory-guard", str(os.getpid()), str(guard_dir)],
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
)
deadline_code = """import os, signal, sys, time
worker = int(sys.argv[1])
output = sys.argv[2]
time.sleep(60)
if os.getppid() == worker:
    with open(os.path.join(output, "deadline.txt"), "w") as stream:
        stream.write("60-second worker guard reached; process killed\\n")
    os.kill(worker, signal.SIGKILL)
"""
deadline_guard = subprocess.Popen(
    [sys.executable, "-c", deadline_code, str(os.getpid()), str(guard_dir)],
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
)

construct_seconds = None
action_seconds = None
actual = None
status = "failed"
started = time.perf_counter()
try:
    construct_started = time.perf_counter()
    table = hl.utils.range_table(args.rows, n_partitions=1)
    values = hl.zeros(args.length) if args.kind == "zeros" else hl.range(args.length)
    table = table.annotate(n=hl.len(values))
    construct_seconds = time.perf_counter() - construct_started

    action_started = time.perf_counter()
    actual = int(table.aggregate(hl.agg.sum(table.n)))
    action_seconds = time.perf_counter() - action_started
    expected = args.rows * args.length
    if actual != expected:
        raise AssertionError(f"aggregate result {actual} did not match {expected}")
    status = "completed"
finally:
    for guard in (deadline_guard, memory_guard):
        if guard.poll() is None:
            guard.terminate()
        try:
            guard.wait(timeout=5)
        except subprocess.TimeoutExpired:
            guard.kill()
            guard.wait()
    telemetry = measurement.stop()

summary = {
    "status": status,
    "kind": args.kind,
    "rows": args.rows,
    "length": args.length,
    "actual_sum": actual,
    "expected_sum": args.rows * args.length,
    "construct_seconds": construct_seconds,
    "action_seconds": action_seconds,
    "wall_seconds": time.perf_counter() - started,
    "native_sha256": native_sha256,
    "telemetry": telemetry,
}
(output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
print(json.dumps(summary), flush=True)
