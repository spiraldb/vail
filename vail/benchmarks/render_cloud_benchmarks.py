"""Render complete Linux benchmark results and 10 Hz scaling telemetry."""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from profile_cloud import CASES
from run_dimension_scaling import CONFIGS


LABELS = dict(zip(CASES, (
    "Range count · 100M rows", "Rekey · 1M rows", "Sorted grouping · 10M rows",
    "Unsorted grouping · 10M rows", "Annotate 100 fields · 1M rows", "Statistics · 10M rows",
    "Inner join · 1M rows", "Keyed lookup · 1M rows", "Prefix scan · 1M rows",
    "Table write · 1M rows", "Table read · 1M rows", "Matrix row count",
    "Matrix entry sum", "Matrix filtering", "VCF import + entry sum",
)))
COLORS = ("#526ab5", "#c0632a", "#3e8a54", "#9b4d9f")


def read_perf(folder, summary, worker):
    events = {}
    with (folder / "perf.csv").open() as source:
        for row in csv.reader(source):
            if len(row) < 4 or row[0].startswith("#"):
                continue
            try:
                elapsed, count = float(row[0]), float(row[1])
            except ValueError:
                continue
            events.setdefault(elapsed, {})[row[3]] = count
    selected = []
    previous = 0
    for elapsed, values in sorted(events.items()):
        before = summary["perf_start_ns"] + previous*1e9
        after = summary["perf_start_ns"] + elapsed*1e9
        dt = elapsed-previous
        previous = elapsed
        if before < worker["start_ns"] or after > worker["end_ns"]:
            continue
        selected.append(dict(time_seconds=(after-worker["start_ns"])/1e9,
            interval_seconds=dt,
            **{name: count/dt for name, count in values.items()}))
    if not selected or any("instructions" not in r or "cycles" not in r for r in selected):
        raise ValueError(f"Incomplete hardware counter data: {folder}")
    return selected


def render(root, baseline_only=False):
    results, rows, cache_counts = {}, [], []
    provenance = json.loads((root / "provenance.json").read_text())
    configs = provenance.get("configs", CONFIGS)
    series = {
        dimension: [("baseline", 1)] + [(tag, int(tag.split("-")[1][:-1]))
            for tag, *_ in configs if tag.startswith(dimension + "-")]
        for dimension in ("samples", "variants")
    }
    for tag, _, _, _ in (configs[:1] if baseline_only else configs):
        selected_cases = CASES if tag == "baseline" else CASES[11:]
        groups = [selected_cases[i:i+5] for i in range(0, len(selected_cases), 5)]
        for page, cases in enumerate(groups, 1):
            fig, axes = plt.subplots(7, len(cases), figsize=(4.3*len(cases), 15), sharex="col", squeeze=False)
            units = ("CPU cores", "RSS (MiB)", "Process I/O (MiB/s)", "EBS I/O (MiB/s)",
                     "Instructions (billions/s)", "Cycles (billions/s)", "Instructions/cycle")
            for column, case in enumerate(cases):
                axes[0, column].set_title(LABELS[case], fontsize=10)
                for engine, color in (("vail", "#2372b5"), ("hail", "#d16c18")):
                    folder = root / tag / f"{case}-{engine}"
                    summary = json.loads((folder / "summary.json").read_text())
                    worker = json.loads((folder / "worker.json").read_text())
                    with (folder / "metrics.csv").open() as source:
                        readings = [r for r in csv.DictReader(source) if r["measured"] == "True"]
                    perf = read_perf(folder, summary, worker)
                    results[tag, case, engine] = summary["median_seconds"]
                    times = [float(r["time_seconds"]) for r in readings]
                    for row, metric in enumerate(("cpu_cores", "rss_mib", "process_read_mib_s", "ssd_read_mib_s")):
                        axes[row, column].plot(times, [float(r[metric]) for r in readings], color=color, linewidth=1, label=engine.capitalize())
                        if "read_mib_s" in metric:
                            axes[row, column].plot(times, [float(r[metric.replace("read", "write")]) for r in readings], color=color, linewidth=1, linestyle="--")
                    perf_times = [r["time_seconds"] for r in perf]
                    for row, event in ((4, "instructions"), (5, "cycles")):
                        axes[row, column].plot(perf_times, [r[event]/1e9 for r in perf], color=color, linewidth=1)
                    axes[6, column].plot(perf_times, [r["instructions"]/r["cycles"] if r["cycles"] else 0 for r in perf], color=color, linewidth=1)
                    duration = sum(r["interval_seconds"] for r in perf)
                    total = lambda event: sum(r.get(event, 0)*r["interval_seconds"] for r in perf)
                    mean_query = (worker["end_ns"]-worker["start_ns"])/1e9/summary["repetitions"]
                    rows.append(dict(config=tag, case=case, engine=engine, parameters=json.dumps(worker["parameters"],sort_keys=True),
                        median_seconds=summary["median_seconds"], mean_query_seconds=mean_query,
                        mean_cpu_cores=summary["mean_cpu_cores"], peak_rss_mib=summary["peak_rss_mib"],
                        instructions_per_query=total("instructions")/duration*mean_query,
                        cycles_per_query=total("cycles")/duration*mean_query,
                        ipc=total("instructions")/total("cycles"), cache_misses_reported=total("cache-misses"),
                        repetitions=summary["repetitions"], measured_samples=summary["measured_samples"],
                        median_sampling_interval_seconds=summary["median_interval_seconds"]))
                    cache_counts.append(total("cache-misses"))
                for row, unit in enumerate(units):
                    axes[row, column].grid(alpha=.2)
                    axes[row, column].set_ylim(bottom=0)
                    if column == 0:
                        axes[row, column].set_ylabel(unit)
                axes[-1, column].set_xlabel("Seconds since measurement began")
            axes[0, 0].legend()
            fig.suptitle(f"32-vCPU VM · {tag} · 10 Hz · blue: Vail; orange: Hail\n"
                "CPU/RAM/process I/O: worker + descendants. Hardware counters and EBS I/O: entire idle VM.\n"
                "I/O solid: read; dashed: write. Hardware timeline alignment is approximate to process-launch latency.", fontsize=12)
            fig.tight_layout(rect=(0, 0, 1, .93))
            filename = f"telemetry-{tag}-{page}"
            fig.savefig(root / f"{filename}.png", dpi=125)
            fig.savefig(root / f"{filename}.svg")
            plt.close(fig)

    totals = []
    baseline_totals = {engine:sum(results["baseline", case, engine] for case in CASES) for engine in ("vail", "hail")}
    if baseline_only:
        (root / "baseline-totals.json").write_text(json.dumps(baseline_totals, indent=2)+"\n")
        return
    for dimension in ("samples", "variants"):
        tags, scales = zip(*series[dimension])
        fig, ax = plt.subplots(figsize=(13, 7))
        for case, color in zip(CASES[11:], COLORS):
            for engine, style in (("vail", "-"), ("hail", "--")):
                ax.plot(scales, [results[tag, case, engine] for tag in tags], color=color,
                    linestyle=style, marker="o", label=f"{LABELS[case]} · {engine.capitalize()}")
        ax.set_xticks(scales, [f"{scale}×" for scale in scales])
        ax.set_yscale("log")
        ax.set_xlabel(f"{dimension.capitalize()} multiplier")
        ax.set_ylabel("Median query time (seconds, log scale)")
        ax.set_title(f"{dimension.capitalize()} scaling · 32-vCPU VM · solid: Vail; dashed: Hail")
        ax.grid(alpha=.2, which="both")
        ax.legend(ncol=2, fontsize=9)
        fig.tight_layout()
        fig.savefig(root / f"scaling-{dimension}.png", dpi=150)
        fig.savefig(root / f"scaling-{dimension}.svg")
        plt.close(fig)
        for scale, tag in zip(scales, tags):
            vail = sum(results[tag, case, "vail"] for case in CASES[11:])
            hail = sum(results[tag, case, "hail"] for case in CASES[11:])
            totals.append(dict(dimension=dimension, scale=scale, vail_seconds=vail,
                               hail_seconds=hail, hail_over_vail=hail/vail))
    with (root / "results.csv").open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]),lineterminator="\n")
        writer.writeheader(); writer.writerows(rows)
    unique_totals = {engine: baseline_totals[engine] + sum(results[tag, case, engine]
                    for tag, *_ in configs[1:] for case in CASES[11:])
                    for engine in ("vail", "hail")}
    unique_count = len(CASES) + len(CASES[11:]) * (len(configs)-1)
    (root / "totals.json").write_text(json.dumps(dict(baseline=baseline_totals, scaling=totals,
        **{f"all_{unique_count}_unique_workload_configurations": unique_totals}), indent=2)+"\n")
    lines = ["# Cloud Hail/Vail benchmarks", "",
        "AWS m7i.8xlarge, Ubuntu 22.04, 32 vCPUs (16 cores × 2 SMT threads), 128 GiB RAM, "
        "200 GiB gp3 EBS. Both engines use Python 3.11.17. Synthetic sources request 32 partitions; "
        "Vail has 32 runtime workers and Hail uses Spark local[32] with a 64 GiB heap. Stock Hail is installed from PyPI; "
        "Vail is built with --release --locked and target-cpu=native, using Rust 1.98.0.", "",
        "All runs execute serially on the otherwise idle VM after compilation. Each has three warmups "
        "and at least ten measured seconds of fresh lazy queries. Result-bearing workloads assert their "
        "results every iteration; writing is also exercised by the separately checked read workload. "
        "Totals sum per-case medians (one invocation of each case), excluding startup/warmups. "
        "Raw iterations, 10 Hz traces, perf output, script hashes, configuration, and elapsed wall time are retained.", "",
        "## Complete 15-case baseline", "", "| Benchmark | Vail (s) | Hail (s) | Hail/Vail |",
        "|---|---:|---:|---:|"]
    for case in CASES:
        v, h = (results["baseline", case, engine] for engine in ("vail", "hail"))
        lines.append(f"| {LABELS[case]} | {v:.6f} | {h:.6f} | {h/v:.2f}× |")
    v, h = baseline_totals["vail"], baseline_totals["hail"]
    lines.append(f"| **Total** | **{v:.6f}** | **{h:.6f}** | **{h/v:.2f}×** |")
    lines += ["", f"Summing all {unique_count} distinct workload configurations (the baseline is counted once): "
        f"Vail **{unique_totals['vail']:.6f} s**, Hail **{unique_totals['hail']:.6f} s**."]
    lines += ["", "## Scaling totals", "",
        "The four Matrix/VCF cases have sample/variant dimensions. 1× is 10 samples, "
        "100,000 Matrix rows and 50,000 VCF variants; one dimension changes at a time. "
        "The two series share their 1× baseline. Table-only workloads are measured in the full baseline above.", "",
        "| Dimension | Scale | Vail total (s) | Hail total (s) | Hail/Vail |", "|---|---:|---:|---:|---:|"]
    for row in totals:
        lines.append(f'| {row["dimension"]} | {row["scale"]}× | {row["vail_seconds"]:.6f} | {row["hail_seconds"]:.6f} | {row["hail_over_vail"]:.2f}× |')
    for dimension in ("samples", "variants"):
        tags, scales = zip(*series[dimension])
        lines += ["", f"## {dimension.capitalize()} medians", "",
                  "| Benchmark | " + " | ".join(f"{engine} {scale}×"
                    for scale in scales for engine in ("Vail", "Hail")) + " |",
                  "|---|" + "---:|" * (2*len(scales))]
        for case in CASES[11:]:
            values = [results[tag, case, engine] for tag in tags for engine in ("vail", "hail")]
            lines.append("| " + LABELS[case] + " | " + " | ".join(f"{value:.6f}" for value in values) + " |")
        lines += ["", f"![{dimension} scaling](scaling-{dimension}.png)"]
    lines += ["", "## Telemetry", "",
        "CPU/RAM/process I/O include the worker and its descendants, including the Hail JVM. "
        "RSS may double-count shared pages. Hardware counters include the entire guest and the monitor; "
        "EBS rates are for the VM's virtual block device, not physical SSD hardware. "
        "Only full intervals within measurement contribute to plots and summaries. "
        "Hardware interval alignment includes small process-launch latency."]
    lines += ["", "The local attempt was discarded after unrelated compilation caused CPU contention. "
        "The cloud write capture was retried after the monitor encountered a procfs permission race "
        "on a short-lived helper. Completed captures were retained; failed captures do not contribute "
        "to totals. Subsequent captures record unreadable descendant PIDs explicitly in summary.json. "
        "The primary worker is never silently skipped. Process-tree snapshots can miss short-lived "
        "helpers, so Hail write's process CPU/RSS/I/O measurements are approximate; the guest-wide "
        "hardware counters include those helpers."]
    if not any(cache_counts):
        lines += ["", "The virtual PMU reported zero cache misses in every capture. "
                  "Do not interpret that as evidence of no cache misses; the raw counts are retained, "
                  "but this counter is not used for performance conclusions."]
    for tag, *_ in configs:
        pages = 3 if tag == "baseline" else 1
        for page in range(1, pages+1):
            lines += ["", f"![{tag} 10 Hz telemetry, page {page}](telemetry-{tag}-{page}.png)"]
    (root / "SUMMARY.md").write_text("\n".join(lines)+"\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("--baseline-only", action="store_true")
    args = parser.parse_args()
    render(args.root, args.baseline_only)
