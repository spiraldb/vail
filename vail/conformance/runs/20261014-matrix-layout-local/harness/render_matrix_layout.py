"""Render local before/after Matrix diagnostics; raw captures use profile_samples."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

CASES = {
    "matrix_entry_sum": "Entry sum",
    "matrix_filter_rows_columns": "Row + column filter (row count)",
}
CONFIGS = [(f"samples-{n}", n, 100_000) for n in (10, 20, 40, 80)] + [
    ("rows-200k", 10, 200_000),
    ("rows-400k", 10, 400_000),
]
COLORS = {"before": "#b45927", "after": "#16789d"}


def render(root):
    rows = []
    for tag, samples, matrix_rows in CONFIGS:
        for case in CASES:
            for version in COLORS:
                folder = root / tag / f"{case}-{version}"
                summary = json.loads((folder / "summary.json").read_text())
                worker = json.loads((folder / "worker.json").read_text())
                rows.append(
                    dict(
                        config=tag,
                        samples=samples,
                        rows=matrix_rows,
                        case=case,
                        version=version,
                        median_ms=1000 * summary["median_seconds"],
                        peak_rss_mib=summary["peak_rss_mib"],
                        mean_cpu_cores=summary["mean_cpu_cores"],
                        estimated_instructions_per_query=summary["instructions"]
                        / summary["observed_seconds"]
                        * summary["median_seconds"],
                        repetitions=summary["repetitions"],
                        native_sha256=worker["settings"]["native_sha256"],
                    )
                )
    with (root / "comparison.csv").open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)

    def save(figure, name):
        figure.savefig(root / f"{name}.png", dpi=160)
        plt.close(figure)

    for dimension in ("samples", "rows"):
        configs = CONFIGS[:4] if dimension == "samples" else [CONFIGS[0], *CONFIGS[4:]]
        figure, axes = plt.subplots(1, 2, figsize=(11, 4.5), constrained_layout=True)
        for case_index, case in enumerate(CASES):
            for version, color in COLORS.items():
                selected = [
                    next(
                        r
                        for r in rows
                        if r["config"] == tag
                        and r["case"] == case
                        and r["version"] == version
                    )
                    for tag, _, _ in configs
                ]
                for axis, metric, label in zip(
                    axes,
                    ("median_ms", "peak_rss_mib"),
                    ("Median query time (ms)", "Peak RSS (MiB)"),
                ):
                    axis.plot(
                        [r[dimension] for r in selected],
                        [r[metric] for r in selected],
                        marker="o" if case_index == 0 else "s",
                        color=color,
                        linestyle="-" if case_index == 0 else "--",
                        label=f"{CASES[case]} — {version}",
                    )
                    axis.set_xlabel(
                        "Samples (100k rows)"
                        if dimension == "samples"
                        else "Rows (10 samples)"
                    )
                    axis.set_ylabel(label)
                    axis.set_yscale("log")
                    axis.grid(alpha=0.2)
        axes[0].legend(fontsize=7)
        figure.suptitle(
            "Local Matrix diagnostics: before vs after (14 source partitions)"
        )
        save(figure, f"{dimension}-scaling")

    metrics = [
        ("cpu_cores", "CPU cores", 1),
        ("rss_mib", "RSS (MiB)", 1),
        ("instructions_per_second", "Instructions/s (billions)", 1e-9),
        ("cycles_per_second", "Cycles/s (billions)", 1e-9),
        ("ssd_read_mib_s", "System SSD read (MiB/s)", 1),
        ("ssd_write_mib_s", "System SSD write (MiB/s)", 1),
    ]
    for case in CASES:
        figure, axes = plt.subplots(
            len(metrics), 4, figsize=(14, 13), constrained_layout=True
        )
        for column, (tag, samples, _) in enumerate(CONFIGS[:4]):
            for version, color in COLORS.items():
                path = root / tag / f"{case}-{version}" / "metrics.csv"
                with path.open() as source:
                    captured = [
                        r for r in csv.DictReader(source) if r["measured"] == "True"
                    ]
                for row, (metric, label, factor) in enumerate(metrics):
                    axis = axes[row, column]
                    axis.plot(
                        [float(r["time_seconds"]) for r in captured],
                        [float(r[metric]) * factor for r in captured],
                        color=color,
                        label=version,
                        linewidth=1,
                    )
                    axis.grid(alpha=0.2)
                    if column == 0:
                        axis.set_ylabel(label)
                    if row == 0:
                        axis.set_title(f"{samples} samples")
                    if row == len(metrics) - 1:
                        axis.set_xlabel("Measured seconds")
        axes[0, 0].legend(fontsize=8)
        figure.suptitle(
            f"{CASES[case]} — local before/after, 10 Hz; SSD is system-wide"
        )
        save(figure, f"{case}-10hz")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    render(parser.parse_args().root)
