"""Run the warm latency probe in sequential ABBA order and compare packages."""
import argparse
import json
from pathlib import Path
import statistics
import subprocess
import sys

PROBE = Path(__file__).with_name("latency_probe.py")
ACCEPTANCE_OVERHEAD_MS = 1.0


def run_phase(label, package, index, repetitions, output_dir):
    raw_path = output_dir / f"{label}-{index}.json"
    subprocess.run(
        [sys.executable, str(PROBE), "--package", str(package), "--output", str(raw_path),
         "--repetitions", str(repetitions)],
        check=True,
        timeout=60,
    )
    return json.loads(raw_path.read_text())


def percentile95(values):
    return statistics.quantiles(values, n=20, method="inclusive")[18]


parser = argparse.ArgumentParser()
parser.add_argument("--before-package", type=Path, required=True)
parser.add_argument("--after-package", type=Path)
parser.add_argument("--repetitions", type=int, default=40)
parser.add_argument("--output-dir", type=Path, default=Path(__file__).with_name("latency-comparison"))
args = parser.parse_args()
if args.repetitions < 2:
    parser.error("--repetitions must be at least 2")
args.output_dir.mkdir(parents=True, exist_ok=True)

# ABBA reduces bias from slow drift while keeping all probes sequential.
labels = (["before", "after", "after", "before"] if args.after_package else ["before"] * 2)
packages = {"before": args.before_package.resolve()}
if args.after_package:
    packages["after"] = args.after_package.resolve()
phases = {label: [] for label in packages}
for index, label in enumerate(labels, 1):
    phases[label].append(run_phase(label, packages[label], index, args.repetitions, args.output_dir))

for label, results in phases.items():
    reference = results[0]
    for result in results[1:]:
        if result["native_sha256"] != reference["native_sha256"]:
            raise RuntimeError(f"{label} probe phases used different native binaries")
        if [case["rows"] for case in result["cases"]] != [case["rows"] for case in reference["cases"]]:
            raise RuntimeError(f"{label} probe phases returned different cases")
if args.after_package:
    before_rows = [case["rows"] for case in phases["before"][0]["cases"]]
    after_rows = [case["rows"] for case in phases["after"][0]["cases"]]
    if before_rows != after_rows:
        raise RuntimeError("before and after probes returned different cases")

summary = {}
for label, results in phases.items():
    samples_by_rows = {}
    for result in results:
        for case in result["cases"]:
            samples_by_rows.setdefault(case["rows"], []).extend(case["wall_ms"])
    by_rows = {
        str(rows): {"median_wall_ms": statistics.median(walls), "p95_wall_ms": percentile95(walls)}
        for rows, walls in samples_by_rows.items()
    }
    summary[label] = {"package": str(packages[label]), "native_sha256": results[0]["native_sha256"], "cases": by_rows}

comparison = {"phases": summary, "acceptance_overhead_ms": ACCEPTANCE_OVERHEAD_MS}
if args.after_package:
    cases = {}
    for rows in summary["before"]["cases"]:
        before = summary["before"]["cases"][rows]["median_wall_ms"]
        after = summary["after"]["cases"][rows]["median_wall_ms"]
        overhead = after - before
        cases[rows] = {"before_median_wall_ms": before, "after_median_wall_ms": after,
                       "absolute_overhead_ms": overhead,
                       "ratio": after / before if before else None,
                       "passed": overhead <= ACCEPTANCE_OVERHEAD_MS}
    comparison["comparison"] = cases
comparison_path = args.output_dir / "comparison.json"
comparison_path.write_text(json.dumps(comparison, indent=2) + "\n")
print(comparison_path)
