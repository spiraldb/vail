"""Render the two measured local runs; no benchmark execution."""
import csv
from pathlib import Path

import plotly.graph_objects as go
from plotly.subplots import make_subplots

ROOT = Path(__file__).parent
CASE = "vail/benchmark_table--test_table_aggregate_linreg/metrics.csv"
fig = make_subplots(rows=5, cols=2, shared_xaxes="columns", shared_yaxes=True,
                    subplot_titles=["Columnar predictors · 29.78 s", "Bounded parallel input · 5.97 s"])
for column, (label, title) in enumerate([
    ("after-columnar", "Columnar predictors · 29.78 s"),
    ("after-bounded-parallel", "Bounded parallel input · 5.97 s"),
]):
    with (ROOT / (label + "-telemetry") / CASE).open() as source:
        rows = list(csv.DictReader(source))
    start = int(rows[0]["monotonic_ns"])
    times = [(int(row["monotonic_ns"]) - start) / 1e9 for row in rows]
    def rates(field, divisor):
        return [
            (int(rows[i][field]) - int(rows[i - 1][field]))
            / ((int(rows[i]["monotonic_ns"]) - int(rows[i - 1]["monotonic_ns"])) / 1e9)
            / divisor
            for i in range(1, len(rows))
        ]
    series = [
        (times, [float(row["cpu_percent"]) / 100 for row in rows], "CPU cores"),
        (times, [int(row["rss_bytes"]) / 2**30 for row in rows], "RSS (GiB)"),
        (times[1:], rates("instructions", 1e9), "Instructions (billions/s)"),
        (times[1:], rates("cycles", 1e9), "Cycles (billions/s)"),
        (times[1:], rates("disk_read_bytes", 2**20), "Process reads (MiB/s)"),
    ]
    for index, (x, y, ylabel) in enumerate(series):
        fig.add_trace(go.Scatter(x=x, y=y, mode="lines", name=title,
                                legendgroup=label, showlegend=index == 0,
                                line={"color": "#527ab8" if column == 0 else "#dc7835"}),
                      row=index + 1, col=column + 1)
        fig.update_yaxes(title_text=ylabel if column == 0 else None,
                         rangemode="tozero", row=index + 1, col=column + 1)
    fig.update_xaxes(title_text="Seconds since measurement began", row=5, col=column + 1)
fig.update_layout(height=1250, template="plotly_white",
                  title="75 regressions · published 7.5M-row input · same laptop<br>"
                        "10 Hz telemetry; separate elapsed-time axes; Samply enabled in both runs.",
                  legend={"orientation": "h", "y": 1.05}, margin={"t": 150})
fig.write_html(ROOT / "bounded-parallel-telemetry.html", include_plotlyjs=True)
