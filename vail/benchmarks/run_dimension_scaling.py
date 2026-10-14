"""Run serialized, instrumented sample/variant scaling comparisons."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import time

import psutil

from profile_samples import CASES


CONFIGS = (
    ("baseline", 10, 100_000, 50_000),
    ("samples-2x", 20, 100_000, 50_000),
    ("samples-4x", 40, 100_000, 50_000),
    ("variants-2x", 10, 200_000, 100_000),
    ("variants-4x", 10, 400_000, 200_000),
)


def build_processes():
    result = []
    for process in psutil.process_iter(["pid", "name"]):
        if process.info["name"] in {"rustc", "clang", "clang++", "cc1", "ld"}:
            result.append(process.info)
    return result


def wait_for_builds():
    announced = False
    while builds := build_processes():
        if not announced:
            print(f"WAIT for {len(builds)} compiler/linker processes: {builds}", flush=True)
            announced = True
        time.sleep(5)


def run(args):
    args.output.mkdir(parents=True, exist_ok=True)
    scripts = Path(__file__).resolve().parent
    report = dict(commit=subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
                  platform=platform.platform(), host_logical_cpus=os.cpu_count(),
                  measured_seconds_per_capture=args.seconds, warmups=3,
                  configs=CONFIGS, cases=CASES, runs=[],
                  collector_sha256=hashlib.sha256((scripts / "profile_samples.py").read_bytes()).hexdigest())
    report_path = args.output / "provenance.json"
    started = time.perf_counter()
    for config_index, (tag, samples, matrix_rows, vcf_rows) in enumerate(CONFIGS):
        for case_index, case in enumerate(CASES):
            # Alternate engine order; never overlap benchmark processes.
            engines = ("vail", "hail") if (config_index + case_index) % 2 == 0 else ("hail", "vail")
            for engine in engines:
                wait_for_builds()
                folder = args.output / tag / f"{case}-{engine}"
                print(f"START {tag} {case} {engine}", flush=True)
                capture_started = time.perf_counter()
                command = [args.monitor_python, str(scripts / "profile_samples.py"),
                           "--engine", engine, "--case", case, "--samples", str(samples),
                           "--matrix-rows", str(matrix_rows), "--vcf-rows", str(vcf_rows),
                           "--seconds", str(args.seconds), "--python", getattr(args, f"{engine}_python"),
                           "--output", str(folder)]
                with (args.output / "captures.log").open("a") as log:
                    completed = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT)
                record = dict(config=tag, case=case, engine=engine, returncode=completed.returncode,
                              wall_seconds=time.perf_counter() - capture_started,
                              host_load_average_after=os.getloadavg(),
                              compiler_processes_after=build_processes())
                if completed.returncode == 0:
                    summary = json.loads((folder / "summary.json").read_text())
                    record.update(median_seconds=summary["median_seconds"],
                                  mean_cpu_cores=summary["mean_cpu_cores"],
                                  repetitions=summary["repetitions"])
                report["runs"].append(record)
                report["wall_seconds_so_far"] = time.perf_counter() - started
                report_path.write_text(json.dumps(report, indent=2) + "\n")
                print(f"DONE {record}", flush=True)
                if completed.returncode:
                    raise RuntimeError(f"Capture failed: {folder}; see captures.log and run.log")
    report["wall_seconds"] = time.perf_counter() - started
    report_path.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--monitor-python", required=True)
    parser.add_argument("--vail-python", required=True)
    parser.add_argument("--hail-python", required=True)
    parser.add_argument("--seconds", type=float, default=10)
    run(parser.parse_args())
