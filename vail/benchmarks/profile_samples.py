"""Compare macOS process-tree and SSD telemetry for sample-dependent workloads.

Uses the existing proc_pid_rusage collector, including the Hail JVM. SSD
statistics are device-wide IORegistry counters, not attributed to the worker.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import importlib
import importlib.metadata
import json
import os
from pathlib import Path
import plistlib
import statistics
import subprocess
import time

from profile_annotate import counters


CASES = ("matrix_force_count_rows", "matrix_entry_sum",
         "matrix_filter_rows_columns", "vcf_import_entry_sum")


def worker(args):
    hl = importlib.import_module(args.engine)
    settings = {"package": hl.__file__, "version": importlib.metadata.version(args.engine)}
    if args.engine == "hail":
        hl.init(master="local[14]", spark_conf={
            "spark.driver.memory": "48g", "spark.executor.memory": "48g",
            "spark.driver.maxResultSize": "8g", "spark.ui.enabled": "false",
            "spark.sql.shuffle.partitions": "14",
        }, quiet=True, log=str(args.output / "hail.log"))
        from pyspark import SparkContext
        sc = SparkContext._active_spark_context
        settings.update(master=sc.master, jvm_max_heap=sc._jvm.java.lang.Runtime.getRuntime().maxMemory())
    else:
        native = Path(hl.__file__).parent / "_native.abi3.so"
        settings["native_sha256"] = hashlib.sha256(native.read_bytes()).hexdigest()
    samples, rows = args.samples, args.matrix_rows
    vcf = args.output / "input.vcf"
    if args.case == "vcf_import_entry_sum":
        with vcf.open("w") as out:
            out.write("##fileformat=VCFv4.2\n##contig=<ID=1,length=249250621>\n")
            out.write('##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">\n')
            out.write('##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Depth">\n')
            out.write("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\t" + "\t".join(f"s{i}" for i in range(samples)) + "\n")
            entries = "\t".join(["0/1:20"] * samples)
            for pos in range(1, args.vcf_rows + 1):
                out.write(f"1\t{pos}\t.\tA\tC\t50\tPASS\t.\tGT:DP\t{entries}\n")

    def query():
        if args.case == "vcf_import_entry_sum":
            m = hl.import_vcf(str(vcf), reference_genome="GRCh37")
            assert m.aggregate_entries(hl.agg.sum(m.DP)) == args.vcf_rows * samples * 20
            return
        m = hl.utils.range_matrix_table(rows, samples, 14)
        if args.case == "matrix_force_count_rows":
            assert m._force_count_rows() == rows
        elif args.case == "matrix_entry_sum":
            m = m.annotate_entries(value=m.row_idx + m.col_idx)
            assert m.aggregate_entries(hl.agg.sum(hl.int64(m.value))) == samples * rows * (rows - 1) // 2 + rows * samples * (samples - 1) // 2
        else:
            m = m.filter_rows(m.row_idx % 2 == 0)
            m = m.filter_cols(m.col_idx % 2 == 0)
            assert m._force_count_rows() == (rows + 1) // 2
            assert m.count_cols() == (samples + 1) // 2

    for _ in range(3):
        query()
    (args.output / "ready").write_text(str(os.getpid()))
    while not (args.output / "go").exists():
        time.sleep(0.01)
    events = []
    start = time.perf_counter_ns()
    while time.perf_counter_ns() - start < args.seconds * 1e9:
        before = time.perf_counter_ns()
        query()
        events.append({"start_ns": before, "end_ns": time.perf_counter_ns()})
    end = time.perf_counter_ns()
    report = dict(engine=args.engine, case=args.case, samples=samples,
                  rows=args.vcf_rows if args.case == "vcf_import_entry_sum" else rows,
                  start_ns=start, end_ns=end, iterations=events,
                  median_seconds=statistics.median((e["end_ns"]-e["start_ns"])/1e9 for e in events),
                  settings=settings)
    (args.output / "worker.json").write_text(json.dumps(report, indent=2) + "\n")
    time.sleep(0.3)
    if args.engine == "hail":
        hl.stop()


def ssd_usage():
    data = plistlib.loads(subprocess.check_output([
        "/usr/sbin/ioreg", "-r", "-c", "IOBlockStorageDriver", "-d", "2", "-a"]))
    devices = []
    for entry in data:
        media = entry.get("IORegistryEntryChildren", [])
        names = [m.get("IORegistryEntryName", "") for m in media]
        if not any("SSD" in name for name in names):
            continue
        stats = entry["Statistics"]
        devices.append({"name": names, "id": entry["IORegistryEntryID"],
                        "read": stats["Bytes (Read)"], "write": stats["Bytes (Write)"],
                        "read_ops": stats["Operations (Read)"], "write_ops": stats["Operations (Write)"]})
    if not devices:
        raise RuntimeError("No SSD IORegistry counters found")
    return devices


def monitor(args):
    import psutil
    args.output.mkdir(parents=True, exist_ok=True)
    for marker in ("ready", "go", "worker.json"):
        if (args.output / marker).exists():
            raise RuntimeError(f"Refusing to reuse run directory: {args.output}")
    read, tick_ns = counters()
    cumulative = ("user_time", "system_time", "instructions", "cycles",
                  "diskio_bytesread", "diskio_byteswritten", "pageins")
    tracked, snapshots = {}, []
    command = [args.python, str(Path(__file__).resolve()), "--worker", "--engine", args.engine,
               "--case", args.case, "--samples", str(args.samples), "--seconds", str(args.seconds),
               "--matrix-rows", str(args.matrix_rows), "--vcf-rows", str(args.vcf_rows),
               "--output", str(args.output)]
    env = os.environ.copy()
    env.pop("PYTHONPATH", None)
    env["JAVA_HOME"] = "/Library/Java/JavaVirtualMachines/temurin-11.jre/Contents/Home"
    with (args.output / "run.log").open("w") as log:
        process = subprocess.Popen(command, cwd="/private/tmp", env=env, stdout=log, stderr=subprocess.STDOUT)
        parent = psutil.Process(process.pid)
        while not (args.output / "ready").exists():
            if process.poll() is not None:
                raise RuntimeError(f"worker failed: {process.returncode}; see {args.output / 'run.log'}")
            time.sleep(0.05)

        def snapshot():
            begin = time.perf_counter_ns()
            live = [parent] + parent.children(recursive=True)
            rss = footprint = 0
            for p in live:
                try:
                    key = (p.pid, p.create_time())
                    value = read(p.pid)
                except (OSError, psutil.NoSuchProcess):
                    continue
                tracked[key] = value
                rss += value["resident_size"]
                footprint += value["phys_footprint"]
            now = time.perf_counter_ns()
            disk = ssd_usage()
            disk_time = time.perf_counter_ns()
            totals = {name: sum(v[name] for v in tracked.values()) for name in cumulative}
            return dict(monotonic_ns=now, ssd_monotonic_ns=disk_time,
                        collector_seconds=(disk_time-begin)/1e9,
                        rss_bytes=rss, footprint_bytes=footprint, process_count=len(live),
                        **totals, ssd_read_bytes=sum(d["read"] for d in disk),
                        ssd_write_bytes=sum(d["write"] for d in disk))

        snapshots.append(snapshot())
        devices = ssd_usage()
        (args.output / "go").write_text("go")
        deadline = time.perf_counter()
        while not (args.output / "worker.json").exists():
            deadline += 0.1
            time.sleep(max(0, deadline-time.perf_counter()))
            snapshots.append(snapshot())
            if process.poll() is not None:
                raise RuntimeError(f"worker failed: {process.returncode}; see run.log")
        snapshots.append(snapshot())
        if process.wait():
            raise RuntimeError("worker shutdown failed; see run.log")

    measured = json.loads((args.output / "worker.json").read_text())
    (args.output / "counters.json").write_text(json.dumps(snapshots, indent=2)+"\n")
    rows = []
    for before, after in zip(snapshots, snapshots[1:]):
        dt = (after["monotonic_ns"]-before["monotonic_ns"])/1e9
        disk_dt = (after["ssd_monotonic_ns"]-before["ssd_monotonic_ns"])/1e9
        if dt <= 0:
            continue
        d = {k: after[k]-before[k] for k in cumulative}
        cpu = (d["user_time"]+d["system_time"])*tick_ns/1e9
        rows.append(dict(time_seconds=(after["monotonic_ns"]-measured["start_ns"])/1e9,
                         interval_seconds=dt, ssd_interval_seconds=disk_dt,
                         measured=before["monotonic_ns"]>=measured["start_ns"] and after["ssd_monotonic_ns"]<=measured["end_ns"],
                         cpu_cores=cpu/dt, rss_mib=after["rss_bytes"]/2**20,
                         footprint_mib=after["footprint_bytes"]/2**20,
                         instructions_per_second=d["instructions"]/dt, cycles_per_second=d["cycles"]/dt,
                         ipc=d["instructions"]/d["cycles"] if d["cycles"] else 0,
                         process_read_mib_s=d["diskio_bytesread"]/dt/2**20,
                         process_write_mib_s=d["diskio_byteswritten"]/dt/2**20,
                         ssd_read_mib_s=(after["ssd_read_bytes"]-before["ssd_read_bytes"])/disk_dt/2**20,
                         ssd_write_mib_s=(after["ssd_write_bytes"]-before["ssd_write_bytes"])/disk_dt/2**20,
                         collector_seconds=after["collector_seconds"]))
    with (args.output / "metrics.csv").open("w", newline="") as out:
        writer = csv.DictWriter(out, fieldnames=list(rows[0]), lineterminator="\n"); writer.writeheader(); writer.writerows(rows)
    active = [r for r in rows if r["measured"]]
    duration = sum(r["interval_seconds"] for r in active)
    weighted = lambda name: sum(r[name]*r["interval_seconds"] for r in active)/duration
    summary = dict(engine=args.engine, case=args.case, median_seconds=measured["median_seconds"],
                   repetitions=len(measured["iterations"]), measured_samples=len(active),
                   observed_seconds=duration, mean_cpu_cores=weighted("cpu_cores"),
                   peak_rss_mib=max(r["rss_mib"] for r in active),
                   peak_footprint_mib=max(r["footprint_mib"] for r in active),
                   instructions=weighted("instructions_per_second")*duration,
                   cycles=weighted("cycles_per_second")*duration,
                   process_read_mib=weighted("process_read_mib_s")*duration,
                   process_write_mib=weighted("process_write_mib_s")*duration,
                   ssd_read_mib=sum(r["ssd_read_mib_s"]*r["ssd_interval_seconds"] for r in active),
                   ssd_write_mib=sum(r["ssd_write_mib_s"]*r["ssd_interval_seconds"] for r in active),
                   median_interval_seconds=statistics.median(r["interval_seconds"] for r in active),
                   max_collector_seconds=max(r["collector_seconds"] for r in active),
                   tracked_pids=[pid for pid, _ in tracked], ssd_devices=devices,
                   cache_misses="not exposed by proc_pid_rusage V4")
    summary["ipc"] = summary["instructions"]/summary["cycles"] if summary["cycles"] else None
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2)+"\n")
    print(json.dumps(summary), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--engine", choices=("hail", "vail"), required=True)
    parser.add_argument("--case", choices=CASES, required=True)
    parser.add_argument("--samples", type=int, default=20)
    parser.add_argument("--matrix-rows", type=int, default=100_000)
    parser.add_argument("--vcf-rows", type=int, default=50_000)
    parser.add_argument("--seconds", type=float, default=10)
    parser.add_argument("--python")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if min(args.samples, args.matrix_rows, args.vcf_rows) < 1 or args.seconds <= 0 or (not args.worker and not args.python):
        parser.error("positive samples/seconds and a worker Python are required")
    worker(args) if args.worker else monitor(args)
