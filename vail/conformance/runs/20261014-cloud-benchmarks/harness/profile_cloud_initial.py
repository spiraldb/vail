"""Linux process-tree telemetry and guest-wide perf counters at 10 Hz."""
from __future__ import annotations

import argparse
import csv
import hashlib
import importlib
import importlib.metadata
import json
import os
from pathlib import Path
import signal
import statistics
import subprocess
import time
from types import SimpleNamespace

import psutil
from compare_hail import make_cases


CASES = (
    "table_range_force_count", "table_key_by_shuffle", "table_group_sorted",
    "table_group_unsorted", "table_annotate_100_fields", "table_stats",
    "table_inner_join", "table_foreign_key_lookup", "table_scan_sum",
    "table_write", "table_read", "matrix_force_count_rows", "matrix_entry_sum",
    "matrix_filter_rows_columns", "vcf_import_entry_sum",
)


def worker(args):
    hl = importlib.import_module(args.engine)
    settings = dict(version=importlib.metadata.version(args.engine), package=hl.__file__)
    if args.engine == "hail":
        hl.init(master=f"local[{args.cores}]", spark_conf={
            "spark.driver.memory": "64g", "spark.executor.memory": "64g",
            "spark.driver.maxResultSize": "8g", "spark.ui.enabled": "false",
            "spark.sql.shuffle.partitions": str(args.cores),
        }, quiet=True, log=str(args.output / "hail.log"))
        from pyspark import SparkContext
        sc = SparkContext._active_spark_context
        settings.update(master=sc.master, jvm_max_heap=sc._jvm.java.lang.Runtime.getRuntime().maxMemory())
    else:
        native = next(Path(hl.__file__).parent.glob("_native*.so"))
        settings["native_sha256"] = hashlib.sha256(native.read_bytes()).hexdigest()
    config = SimpleNamespace(cores=args.cores, scale=1, matrix_rows=args.matrix_rows,
        matrix_columns=args.samples, vcf_samples=args.samples, vcf_rows=args.vcf_rows,
        case=[args.case], engine=args.engine, workdir=args.output)
    cases = make_cases(hl, config)
    query, parameters = cases[args.case]
    if args.case == "table_read":
        cases["table_write"][0]()
    for _ in range(3):
        query()
    (args.output / "ready").write_text(str(os.getpid()))
    while not (args.output / "go").exists():
        time.sleep(.01)
    events = []
    start = time.perf_counter_ns()
    while time.perf_counter_ns() - start < args.seconds * 1e9:
        before = time.perf_counter_ns()
        query()
        events.append(dict(start_ns=before, end_ns=time.perf_counter_ns()))
    report = dict(engine=args.engine, case=args.case, parameters=parameters,
        start_ns=start, end_ns=time.perf_counter_ns(), iterations=events, settings=settings,
        median_seconds=statistics.median((e["end_ns"]-e["start_ns"])/1e9 for e in events))
    (args.output / "worker.json").write_text(json.dumps(report, indent=2) + "\n")
    time.sleep(.3)
    if args.engine == "hail":
        hl.stop()


def monitor(args):
    args.output.mkdir(parents=True, exist_ok=False)
    env = os.environ.copy()
    env.pop("PYTHONPATH", None)
    env["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
    command = [args.python, str(Path(__file__).resolve()), "--worker", "--engine", args.engine,
        "--case", args.case, "--output", str(args.output), "--samples", str(args.samples),
        "--matrix-rows", str(args.matrix_rows), "--vcf-rows", str(args.vcf_rows),
        "--cores", str(args.cores), "--seconds", str(args.seconds)]
    snapshots, tracked = [], {}
    perf = None
    with (args.output / "run.log").open("w") as log:
        process = subprocess.Popen(command, env=env, stdout=log, stderr=subprocess.STDOUT,
                                   start_new_session=True)
        parent = psutil.Process(process.pid)
        try:
            while not (args.output / "ready").exists():
                if process.poll() is not None:
                    raise RuntimeError(f"Worker failed: {args.output}")
                time.sleep(.05)

            def snapshot():
                begin = time.perf_counter_ns()
                rss = 0
                live = [parent] + parent.children(recursive=True)
                for p in live:
                    try:
                        cpu, memory, io = p.cpu_times(), p.memory_info(), p.io_counters()
                        stat = Path(f"/proc/{p.pid}/stat").read_text().rsplit(")", 1)[1].split()
                        tracked[p.pid, p.create_time()] = dict(cpu_seconds=cpu.user+cpu.system,
                            read_bytes=io.read_bytes, write_bytes=io.write_bytes,
                            minor_faults=int(stat[7]), major_faults=int(stat[9]))
                        rss += memory.rss
                    except (psutil.NoSuchProcess, FileNotFoundError):
                        continue
                disks = psutil.disk_io_counters(perdisk=True)
                # Whole NVMe block devices only, excluding their partitions.
                disks = {name: value for name, value in disks.items()
                         if name.startswith("nvme") and name.endswith("n1")}
                now = time.perf_counter_ns()
                return dict(monotonic_ns=now, collector_seconds=(now-begin)/1e9,
                    rss_bytes=rss, process_count=len(live),
                    **{key: sum(value[key] for value in tracked.values()) for key in
                       ("cpu_seconds", "read_bytes", "write_bytes", "minor_faults", "major_faults")},
                    ebs_read_bytes=sum(d.read_bytes for d in disks.values()),
                    ebs_write_bytes=sum(d.write_bytes for d in disks.values()))

            snapshots.append(snapshot())
            perf_start = time.perf_counter_ns()
            perf = subprocess.Popen(["perf", "stat", "--all-cpus", "--interval-print", "100",
                "--no-big-num", "-x", ",", "-e", "instructions,cycles,cache-misses,context-switches,page-faults",
                "-o", str(args.output / "perf.csv"), "--", "sleep", "7200"],
                stdout=log, stderr=log, start_new_session=True)
            time.sleep(.15)
            if perf.poll() is not None:
                raise RuntimeError("perf failed; see run.log")
            (args.output / "go").write_text("go")
            deadline = time.perf_counter()
            while not (args.output / "worker.json").exists():
                deadline += .1
                time.sleep(max(0, deadline-time.perf_counter()))
                snapshots.append(snapshot())
                if process.poll() is not None:
                    raise RuntimeError(f"Worker failed: {args.output}")
            snapshots.append(snapshot())
            if process.wait():
                raise RuntimeError("Worker shutdown failed")
        finally:
            if perf is not None and perf.poll() is None:
                os.killpg(perf.pid, signal.SIGINT)
                perf.wait(timeout=10)
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
                process.wait(timeout=15)
    measured = json.loads((args.output / "worker.json").read_text())
    rows = []
    for before, after in zip(snapshots, snapshots[1:]):
        dt = (after["monotonic_ns"]-before["monotonic_ns"])/1e9
        delta = lambda key: after[key]-before[key]
        rows.append(dict(time_seconds=(after["monotonic_ns"]-measured["start_ns"])/1e9,
            interval_seconds=dt,
            measured=before["monotonic_ns"]>=measured["start_ns"] and after["monotonic_ns"]<=measured["end_ns"],
            cpu_cores=delta("cpu_seconds")/dt, rss_mib=after["rss_bytes"]/2**20,
            process_read_mib_s=delta("read_bytes")/dt/2**20,
            process_write_mib_s=delta("write_bytes")/dt/2**20,
            ssd_read_mib_s=delta("ebs_read_bytes")/dt/2**20,
            ssd_write_mib_s=delta("ebs_write_bytes")/dt/2**20,
            minor_faults_per_second=delta("minor_faults")/dt,
            major_faults_per_second=delta("major_faults")/dt,
            collector_seconds=after["collector_seconds"]))
    with (args.output / "metrics.csv").open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader(); writer.writerows(rows)
    active = [row for row in rows if row["measured"]]
    duration = sum(r["interval_seconds"] for r in active)
    weighted = lambda key: sum(r[key]*r["interval_seconds"] for r in active)/duration
    summary = dict(engine=args.engine, case=args.case, median_seconds=measured["median_seconds"],
        repetitions=len(measured["iterations"]), measured_samples=len(active), observed_seconds=duration,
        mean_cpu_cores=weighted("cpu_cores"), peak_rss_mib=max(r["rss_mib"] for r in active),
        median_interval_seconds=statistics.median(r["interval_seconds"] for r in active),
        max_collector_seconds=max(r["collector_seconds"] for r in active),
        perf_start_ns=perf_start, hardware_counter_scope="entire otherwise idle guest VM",
        process_scope="worker plus descendants; RSS may count shared pages twice")
    (args.output / "counters.json").write_text(json.dumps(snapshots, indent=2)+"\n")
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2)+"\n")
    print(json.dumps(summary), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--engine", choices=("hail", "vail"), required=True)
    parser.add_argument("--case", choices=CASES, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--python")
    parser.add_argument("--cores", type=int, default=32)
    parser.add_argument("--samples", type=int, default=10)
    parser.add_argument("--matrix-rows", type=int, default=100_000)
    parser.add_argument("--vcf-rows", type=int, default=50_000)
    parser.add_argument("--seconds", type=float, default=10)
    args = parser.parse_args()
    if min(args.cores, args.samples, args.matrix_rows, args.vcf_rows, args.seconds) <= 0:
        parser.error("Workload dimensions and measurement duration must be positive")
    worker(args) if args.worker else monitor(args)
