"""Full baseline suite and separate sample/variant scaling on an idle Linux VM."""
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
from profile_cloud import CASES
from run_dimension_scaling import CONFIGS, build_processes


def run(args):
    args.output.mkdir(parents=True, exist_ok=args.resume)
    scripts = Path(__file__).resolve().parent
    if build_processes():
        raise RuntimeError("Compilers still running; finish setup before benchmarking")
    report = dict(commit=args.commit, platform=platform.platform(),
        logical_cpus=os.cpu_count(), memory_bytes=psutil.virtual_memory().total,
        cores=32, warmups=3, measured_seconds_per_capture=args.seconds,
        configs=CONFIGS, baseline_cases=CASES, scaling_cases=CASES[11:], runs=[],
        script_hashes={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in scripts.glob("*.py")},
        lscpu=subprocess.check_output(["lscpu"], text=True),
        perf_version=subprocess.check_output(["perf", "--version"], text=True),
        java_version=subprocess.run(["java", "-version"], capture_output=True,text=True).stderr)
    if args.resume:
        previous = json.loads((args.output / "provenance.json").read_text())
        if previous["commit"] != args.commit or previous["measured_seconds_per_capture"] != args.seconds:
            raise ValueError("Resume configuration differs from the original run")
        previous.setdefault("attempts", list(previous["runs"]))
        previous["runs"] = [r for r in previous["runs"] if r["returncode"] == 0]
        previous["script_hashes_on_resume"] = report["script_hashes"]
        report = previous
    completed = {(r["config"], r["case"], r["engine"]) for r in report["runs"] if r["returncode"] == 0}
    started = time.perf_counter() - report.get("wall_seconds", 0)
    for config_index, (tag, samples, matrix_rows, vcf_rows) in enumerate(CONFIGS):
        selected = CASES if tag == "baseline" else CASES[11:]
        for case_index, case in enumerate(selected):
            for engine in (("vail", "hail") if (config_index+case_index)%2==0 else ("hail", "vail")):
                if (tag, case, engine) in completed:
                    continue
                if build_processes():
                    raise RuntimeError("Compilation started during the benchmark suite")
                print(f"START {tag} {case} {engine}", flush=True)
                capture_started = time.perf_counter()
                folder = args.output / tag / f"{case}-{engine}"
                if args.resume and folder.exists():
                    folder.rename(folder.with_name(folder.name + f"-failed-{time.time_ns()}"))
                command = [args.monitor_python, str(scripts / "profile_cloud.py"),
                    "--engine", engine, "--case", case, "--output", str(folder),
                    "--python", getattr(args, f"{engine}_python"), "--cores", "32",
                    "--samples", str(samples), "--matrix-rows", str(matrix_rows),
                    "--vcf-rows", str(vcf_rows), "--seconds", str(args.seconds)]
                with (args.output / "captures.log").open("a") as log:
                    result = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT)
                record = dict(config=tag, case=case, engine=engine, returncode=result.returncode,
                    wall_seconds=time.perf_counter()-capture_started, host_load_after=os.getloadavg(),
                    collector_sha256=hashlib.sha256((scripts / "profile_cloud.py").read_bytes()).hexdigest())
                if result.returncode == 0:
                    summary = json.loads((folder / "summary.json").read_text())
                    record.update(median_seconds=summary["median_seconds"],
                        mean_cpu_cores=summary["mean_cpu_cores"], repetitions=summary["repetitions"])
                report["runs"].append(record)
                report.setdefault("attempts", []).append(record)
                report["wall_seconds"] = time.perf_counter()-started
                (args.output / "provenance.json").write_text(json.dumps(report,indent=2)+"\n")
                print(f"DONE {record}", flush=True)
                if result.returncode:
                    raise RuntimeError(f"Capture failed; see {folder}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--monitor-python", required=True)
    parser.add_argument("--vail-python", required=True)
    parser.add_argument("--hail-python", required=True)
    parser.add_argument("--seconds", type=float, default=10)
    parser.add_argument("--resume", action="store_true")
    run(parser.parse_args())
