"""Render sample/variant scaling lines, 10 Hz traces, and complete totals."""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from profile_samples import CASES
from run_dimension_scaling import CONFIGS


LABELS = ("Matrix row count", "Matrix entry sum", "Matrix filtering", "VCF import + entry sum")
COLORS = ("#526ab5", "#c0632a", "#3e8a54", "#9b4d9f")
ENGINES = ("vail", "hail")


def render(root):
    results = {}
    resource_rows = []
    for tag, samples, matrix_rows, vcf_rows in CONFIGS:
        fig, axes = plt.subplots(7, 4, figsize=(18, 15), sharex="col")
        metrics = (
            ("cpu_cores", "CPU cores", 1),
            ("rss_mib", "RSS (MiB)", 1),
            ("process_read_mib_s", "Process I/O (MiB/s)", 1),
            ("ssd_read_mib_s", "SSD I/O (MiB/s)", 1),
            ("instructions_per_second", "Instructions (billions/s)", 1e9),
            ("cycles_per_second", "Cycles (billions/s)", 1e9),
            ("ipc", "Instructions/cycle", 1),
        )
        for column, (case, label) in enumerate(zip(CASES, LABELS)):
            axes[0, column].set_title(label)
            for engine, color in (("vail", "#2372b5"), ("hail", "#d16c18")):
                folder = root / tag / f"{case}-{engine}"
                summary = json.loads((folder / "summary.json").read_text())
                worker = json.loads((folder / "worker.json").read_text())
                results[tag, case, engine] = summary["median_seconds"]
                with (folder / "metrics.csv").open() as source:
                    readings = [r for r in csv.DictReader(source) if r["measured"] == "True"]
                if not readings:
                    raise ValueError(f"No measured samples: {folder}")
                times = [float(r["time_seconds"]) for r in readings]
                for row, (metric, unit, divisor) in enumerate(metrics):
                    axes[row, column].plot(times, [float(r[metric])/divisor for r in readings],
                                           color=color, label=engine.capitalize(), linewidth=1)
                    if "read_mib_s" in metric:
                        axes[row, column].plot(times, [float(r[metric.replace("read", "write")]) for r in readings],
                                               color=color, linestyle="--", linewidth=1)
                    axes[row, column].set_ylim(bottom=0)
                    axes[row, column].grid(alpha=.2)
                    if column == 0:
                        axes[row, column].set_ylabel(unit)
                mean_seconds = (worker["end_ns"] - worker["start_ns"])/1e9/summary["repetitions"]
                resource_rows.append(dict(config=tag, case=case, engine=engine, samples=samples,
                    rows=worker["rows"], median_seconds=summary["median_seconds"],
                    mean_seconds=mean_seconds, repetitions=summary["repetitions"],
                    mean_cpu_cores=summary["mean_cpu_cores"], peak_rss_mib=summary["peak_rss_mib"],
                    cpu_seconds_per_query=summary["mean_cpu_cores"]*mean_seconds,
                    instructions_per_query=summary["instructions"]/summary["observed_seconds"]*mean_seconds,
                    cycles_per_query=summary["cycles"]/summary["observed_seconds"]*mean_seconds,
                    ipc=summary["ipc"], process_read_mib=summary["process_read_mib"],
                    process_write_mib=summary["process_write_mib"],
                    ssd_read_mib=summary["ssd_read_mib"], ssd_write_mib=summary["ssd_write_mib"],
                    measured_samples=summary["measured_samples"],
                    median_sampling_interval_seconds=summary["median_interval_seconds"]))
            axes[-1, column].set_xlabel("Seconds since measurement began")
        axes[0, 0].legend()
        fig.suptitle(f"{tag}: {samples} samples · {matrix_rows:,} Matrix rows · {vcf_rows:,} VCF variants · 10 Hz\n"
                     "Blue: Vail; orange: Hail. I/O solid: read; dashed: write.\n"
                     "CPU/RAM/counters include worker + descendants; SSD counters are device-wide.", fontsize=13)
        fig.tight_layout(rect=(0, 0, 1, .93))
        fig.savefig(root / f"telemetry-{tag}.png", dpi=125)
        fig.savefig(root / f"telemetry-{tag}.svg")
        plt.close(fig)

    for dimension in ("samples", "variants"):
        tags = ("baseline", f"{dimension}-2x", f"{dimension}-4x")
        fig, ax = plt.subplots(figsize=(13, 7))
        for case, label, color in zip(CASES, LABELS, COLORS):
            for engine in ENGINES:
                ax.plot([1, 2, 4], [results[tag, case, engine] for tag in tags],
                        color=color, linestyle="-" if engine == "vail" else "--", marker="o",
                        label=f"{label} · {engine.capitalize()}")
        ax.set_xticks([1, 2, 4], ["1×", "2×", "4×"])
        ax.set_yscale("log")
        ax.set_xlabel(f"{dimension.capitalize()} multiplier")
        ax.set_ylabel("Median query time (seconds, log scale)")
        ax.set_title(f"{dimension.capitalize()} scaling · solid: Vail, dashed: Hail\n"
                     "Fresh queries; three warmups; ≥10 seconds measured per workload")
        ax.grid(alpha=.2, which="both")
        ax.legend(ncol=2, fontsize=9)
        fig.tight_layout()
        fig.savefig(root / f"scaling-{dimension}.png", dpi=150)
        fig.savefig(root / f"scaling-{dimension}.svg")
        plt.close(fig)

    with (root / "results.csv").open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=list(resource_rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(resource_rows)
    totals = []
    lines = ["# Sample and variant scaling", "",
        "Runtime commit: `" + json.loads((root / "provenance.json").read_text())["commit"] + "`.", "",
        "Four Matrix/VCF benchmarks, compared using stock PyPI Hail and the optimized Vail release wheel. "
        "Hail uses Spark local[14] with a 48 GiB heap. Vail has 32 runtime workers; synthetic Matrix "
        "inputs request 14 source partitions in both engines. Each capture runs independently and serially, "
        "with three warmups and at least ten seconds of measured fresh queries. Every iteration checks its result.", "",
        "1× = 10 samples, 100,000 Matrix rows and 50,000 VCF variants. Samples and variants vary separately; "
        "Matrix row count represents the variant dimension for the synthetic cases. The 1× capture is shared "
        "between the two series. Table-only cases have no sample/variant dimension and are outside this run.", "",
        "Totals below sum the four per-case medians: the expected time to run each case once. "
        "They exclude engine startup, warmups and monitoring. Capture wall times and every iteration "
        "remain in provenance.json and worker.json.", "",
        "| Dimension | Scale | Vail total (s) | Hail total (s) | Hail/Vail |",
        "|---|---:|---:|---:|---:|"]
    for dimension in ("samples", "variants"):
        for scale, tag in ((1, "baseline"), (2, f"{dimension}-2x"), (4, f"{dimension}-4x")):
            vail = sum(results[tag, case, "vail"] for case in CASES)
            hail = sum(results[tag, case, "hail"] for case in CASES)
            totals.append(dict(dimension=dimension, scale=scale, vail_seconds=vail, hail_seconds=hail,
                               hail_over_vail=hail/vail))
            lines.append(f"| {dimension} | {scale}× | {vail:.6f} | {hail:.6f} | {hail/vail:.2f}× |")
    for dimension in ("samples", "variants"):
        lines += ["", f"## {dimension.capitalize()} per-case medians (seconds)", "",
                  "| Benchmark | Vail 1× | Hail 1× | Vail 2× | Hail 2× | Vail 4× | Hail 4× |",
                  "|---|---:|---:|---:|---:|---:|---:|"]
        for case, label in zip(CASES, LABELS):
            values = [results[tag, case, engine] for tag in ("baseline", f"{dimension}-2x", f"{dimension}-4x") for engine in ENGINES]
            lines.append("| " + label + " | " + " | ".join(f"{v:.6f}" for v in values) + " |")
        lines += ["", f"![{dimension} scaling](scaling-{dimension}.png)"]
    lines += ["", "## 10 Hz metrics", "",
        "CPU and performance counters include the Python worker and descendants (including Hail's JVM). "
        "RSS sums process RSS and may double-count shared pages. One CPU core means one fully utilized core. "
        "SSD I/O is host-wide; process I/O is shown separately. Cache misses are unavailable through this counter API. "
        "Only intervals completely within measurement are plotted. Faster engines complete more queries "
        "in the equal-duration windows; raw iteration boundaries and counters are retained."]
    for tag, *_ in CONFIGS:
        lines += ["", f"![{tag} 10 Hz metrics](telemetry-{tag}.png)"]
    (root / "totals.json").write_text(json.dumps(totals, indent=2) + "\n")
    (root / "SUMMARY.md").write_text("\n".join(lines) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    render(parser.parse_args().root)
