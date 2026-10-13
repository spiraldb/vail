"""Render measured 10 Hz Hail/Vail samples and resource comparisons."""
import argparse
import csv
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from profile_samples import CASES


def render(root):
    labels = ("Matrix row count", "Matrix entry sum", "Matrix filtering", "VCF import + sum")
    metrics = ("cpu_cores", "rss_mib", "process_read_mib_s", "ssd_read_mib_s",
               "instructions_per_second", "cycles_per_second", "ipc")
    units = ("CPU cores", "RSS (MiB)", "Process I/O (MiB/s)", "SSD device I/O (MiB/s)",
             "Instructions (billions/s)", "Cycles (billions/s)", "Instructions/cycle")
    fig, axes = plt.subplots(7, 4, figsize=(18, 17), sharex="col")
    rows = []
    for col, (case, title) in enumerate(zip(CASES, labels)):
        axes[0, col].set_title(title)
        for engine, color in (("vail", "#2372b5"), ("hail", "#d16c18")):
            folder = root / f"{case}-{engine}"
            summary = json.loads((folder / "summary.json").read_text())
            worker = json.loads((folder / "worker.json").read_text())
            with (folder / "metrics.csv").open() as source:
                samples = [r for r in csv.DictReader(source) if r["measured"] == "True"]
            times = [float(s["time_seconds"]) for s in samples]
            for row, metric in enumerate(metrics):
                divisor = 1e9 if metric in ("instructions_per_second", "cycles_per_second") else 1
                axes[row, col].plot(times, [float(s[metric])/divisor for s in samples],
                                    label=engine.capitalize(), color=color, linewidth=1.2)
                if metric in ("process_read_mib_s", "ssd_read_mib_s"):
                    write = metric.replace("read", "write")
                    axes[row, col].plot(times, [float(s[write]) for s in samples],
                                        color=color, linestyle="--", linewidth=1.2)
            seconds = (worker["end_ns"]-worker["start_ns"])/1e9
            mean_query_seconds = seconds / summary["repetitions"]
            instr_query = summary["instructions"]/summary["observed_seconds"]*mean_query_seconds
            rows.append(dict(case=case, engine=engine, median_seconds=summary["median_seconds"],
                             mean_cpu_cores=summary["mean_cpu_cores"], peak_rss_mib=summary["peak_rss_mib"],
                             peak_footprint_mib=summary["peak_footprint_mib"],
                             cpu_seconds_per_query=summary["mean_cpu_cores"]*mean_query_seconds,
                             instructions_per_query=instr_query,
                             cycles_per_query=summary["cycles"]/summary["observed_seconds"]*mean_query_seconds,
                             ipc=summary["ipc"], process_read_mib=summary["process_read_mib"],
                             process_write_mib=summary["process_write_mib"],
                             ssd_read_mib=summary["ssd_read_mib"], ssd_write_mib=summary["ssd_write_mib"],
                             repetitions=summary["repetitions"], samples=summary["measured_samples"]))
        for row in range(7):
            axes[row, col].grid(alpha=0.2)
            axes[row, col].set_ylim(bottom=0)
            if col == 0:
                axes[row, col].set_ylabel(units[row])
        axes[-1, col].set_xlabel("Seconds since measurement began")
    axes[0, 0].legend(loc="upper right")
    fig.suptitle("20 samples · 10 Hz · blue: Vail, orange: Hail\n"
                 "CPU/RAM/counters include worker + descendants (Hail JVM); I/O solid: read, dashed: write\n"
                 "SSD device I/O includes all host activity; process I/O is benchmark-attributed", fontsize=13)
    fig.tight_layout(rect=(0, 0, 1, 0.94))
    fig.savefig(root / "comparison.png", dpi=130)
    fig.savefig(root / "comparison.svg")
    plt.close(fig)
    with (root / "comparison.csv").open("w", newline="") as out:
        writer = csv.DictWriter(out, fieldnames=list(rows[0]), lineterminator="\n"); writer.writeheader(); writer.writerows(rows)
    lines = ["# 20-sample Hail/Vail resource telemetry", "",
             "Eight serialized captures: 100,000 Matrix rows × 20 samples, or 50,000 VCF variants × 20 samples. Each starts a fresh engine, performs three warmups, then repeats fresh lazy queries for at least ten seconds. Every query checks the same result as the benchmark harness. No forced garbage collection occurs between queries. These sustained-run medians can differ from the earlier five-iteration run.", "",
             "The independent monitor samples at a target 10 Hz. CPU, RAM, disk I/O, instructions and cycles sum the Python worker and descendants, including Hail's JVM. RAM is summed RSS (shared pages can be counted in multiple processes); physical footprint is also retained in the CSV. One CPU core is one fully occupied core. Hail uses Spark local[14] and a 48 GiB max heap; Vail uses the established release wheel.", "",
             "SSD activity is read from the APPLE SSD IOBlockStorageDriver device statistics. It includes other applications and the monitor's small coordination writes; it cannot be attributed to the benchmark. Process disk I/O is listed separately. Warm cached VCF reads may generate no physical I/O. Cache-miss counters are not exposed by proc_pid_rusage V4.", "",
             "Only complete sample intervals inside the measured phase contribute to summaries. CPU mean is time-weighted, RSS is the largest sampled value, and instructions/query and CPU seconds/query are estimates from measured rates divided by query throughput. Raw timestamps, instructions/cycles, sampling intervals and collector overhead are retained. The traces show rates during equal-duration windows; faster engines complete more queries.", "",
             "| Workload | Engine | Median/query (s) | Mean CPU cores | Peak RSS (MiB) | CPU s/query | Billion instructions/query | IPC | Process read/write (MiB) | SSD read/write (MiB) |",
             "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for case, label in zip(CASES, labels):
        for r in (r for r in rows if r["case"] == case):
            lines.append(f'| {label} | {r["engine"]} | {r["median_seconds"]:.4f} | {r["mean_cpu_cores"]:.2f} | {r["peak_rss_mib"]:.0f} | {r["cpu_seconds_per_query"]:.4f} | {r["instructions_per_query"]/1e9:.2f} | {r["ipc"]:.2f} | {r["process_read_mib"]:.2f}/{r["process_write_mib"]:.2f} | {r["ssd_read_mib"]:.2f}/{r["ssd_write_mib"]:.2f} |')
    lines += ["", "![10 Hz comparison](comparison.png)", "",
              "The comparison CSV includes cycles/query, physical footprint and repetition/sample counts. Each case directory contains worker.json with query boundaries/provenance, metrics.csv with interval traces, and summary.json with process IDs, SSD counters and sampling-quality statistics. run.log contains engine diagnostics."]
    (root / "SUMMARY.md").write_text("\n".join(lines)+"\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    render(parser.parse_args().root)
