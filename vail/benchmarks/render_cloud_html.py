#!/usr/bin/env python3
"""Build a standalone HTML comparison from curated and optional upstream runs."""
from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import html
import json
from pathlib import Path


LABELS = {
    "table_range_force_count": "Range count · 100M rows",
    "table_key_by_shuffle": "Rekey · 1M rows",
    "table_group_sorted": "Sorted grouping · 10M rows",
    "table_group_unsorted": "Unsorted grouping · 10M rows",
    "table_annotate_100_fields": "Annotate 100 fields · 1M rows",
    "table_stats": "Numeric statistics · 10M rows",
    "table_inner_join": "Inner join · 1M rows",
    "table_foreign_key_lookup": "Keyed lookup · 1M rows",
    "table_scan_sum": "Prefix scan · 1M rows",
    "table_write": "Table write · 1M rows",
    "table_read": "Table read · 1M rows",
    "matrix_force_count_rows": "Matrix row count",
    "matrix_entry_sum": "Matrix entry sum",
    "matrix_filter_rows_columns": "Matrix filtering",
    "vcf_import_entry_sum": "VCF import + entry sum",
}

STYLE = """
:root{color-scheme:light;--ink:#182536;--muted:#637084;--line:#dce3ec;--blue:#2458be;--bg:#f5f7fb}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:15px/1.55 system-ui,-apple-system,sans-serif}
main{max-width:1400px;margin:auto;padding:38px 22px 72px}h1{font-size:38px;line-height:1.2;margin:5px 0 12px}h2{font-size:24px;margin:36px 0 10px}
p{max-width:1050px}.muted,small{color:var(--muted)}.eyebrow{text-transform:uppercase;letter-spacing:1.8px;font-size:12px;color:var(--muted)}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:14px;margin:22px 0}.card,details,figure,.table-wrap,.note{background:white;border:1px solid var(--line);border-radius:10px}
.card{padding:18px}.label{font-size:13px;color:var(--muted)}.big{font-size:30px;font-weight:700;line-height:1.25}.note{padding:16px 20px;border-left:4px solid #d8a44c;background:#fffbf0}
.toolbar{display:flex;gap:14px;align-items:center;flex-wrap:wrap;margin:16px 0}select,input[type=search]{padding:8px 11px;border:1px solid var(--line);border-radius:7px;background:white;font:inherit}
.table-wrap{overflow:auto}table{width:100%;border-collapse:collapse;font-size:13px;white-space:nowrap}th{background:#eaf0f8;color:#465368;text-align:left;font-size:11px;text-transform:uppercase;letter-spacing:.4px}
th,td{padding:10px 12px;border-bottom:1px solid var(--line);vertical-align:top}tbody tr:last-child td{border:0}td:not(:first-child){font-variant-numeric:tabular-nums}small{display:block;white-space:normal;max-width:380px;font-size:11px;overflow-wrap:anywhere}
.positive{color:#167543;font-weight:650}.negative{color:#a54029;font-weight:650}.status{font-weight:650}.passed{color:#167543}.unsupported,.failed,.timed_out,.fixture_timeout{color:#a54029}.upstream_skipped{color:#637084}
figure{margin:15px 0;overflow:hidden}figcaption{padding:12px 16px;font-weight:650}img{display:block;width:100%;height:auto}details{padding:12px 17px;margin:10px 0}summary{cursor:pointer;font-weight:650}code{font-size:12px;overflow-wrap:anywhere}footer{margin-top:32px;border-top:1px solid var(--line);padding-top:18px;color:var(--muted);font-size:12px}
@media(max-width:720px){main{padding:22px 12px}h1{font-size:31px}}
"""


def esc(value) -> str:
    return html.escape(str(value), quote=True)


def seconds(value) -> str:
    if value in (None, ""):
        return "—"
    return f"{float(value) * 1000:.2f}"


def number(value) -> str:
    if value in (None, ""):
        return "—"
    return f"{float(value):,.1f}"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _read_upstream(path: Path | None) -> tuple[dict[str, dict], dict]:
    if path is None:
        return {}, {}
    report = json.loads(path.read_text())
    cases = {item["id"]: item for item in report.get("cases", []) if "id" in item}
    return cases, report


def render(root: Path, output: Path, upstream_vail: Path | None = None,
           upstream_hail: Path | None = None) -> None:
    results_path = root / "results.csv"
    provenance_path = root / "provenance.json"
    totals_path = root / "totals.json"
    if not all(path.is_file() for path in (results_path, provenance_path, totals_path)):
        raise FileNotFoundError(f"curated result root must contain results.csv, provenance.json and totals.json: {root}")

    provenance = json.loads(provenance_path.read_text())
    totals = json.loads(totals_path.read_text())
    with results_path.open(newline="") as source:
        source_rows = list(csv.DictReader(source))
    curated = {}
    configs = []
    case_order = []
    for row in source_rows:
        config, case, engine = row["config"], row["case"], row["engine"]
        configs.append(config)
        if case not in case_order:
            case_order.append(case)
        curated.setdefault((config, case), {})[engine] = row
    configs = list(dict.fromkeys(configs))

    vail_cases, vail_report = _read_upstream(upstream_vail)
    hail_cases, hail_report = _read_upstream(upstream_hail)
    upstream_ids = sorted(set(vail_cases) | set(hail_cases))
    upstream_statuses = {}
    for engine_cases in (vail_cases, hail_cases):
        for item in engine_cases.values():
            status = item.get("status", "missing status")
            upstream_statuses[status] = upstream_statuses.get(status, 0) + 1

    baseline = totals.get("baseline", {})
    unique_key = next((key for key in totals if key.startswith("all_") and key.endswith("_unique_workload_configurations")), None)
    unique = totals.get(unique_key, {}) if unique_key else {}
    baseline_ratio = baseline.get("hail", 0) / baseline["vail"] if baseline.get("vail") else None
    unique_ratio = unique.get("hail", 0) / unique["vail"] if unique.get("vail") else None

    def metric_card(title: str, ratio, vail, hail):
        headline = f"{ratio:.2f}×" if ratio is not None else "—"
        return (f'<div class="card"><div class="label">{esc(title)} · Hail / Vail</div>'
                f'<div class="big">{headline}</div><div>Vail <b>{float(vail):.3f} s</b> · '
                f'Hail {float(hail):.3f} s</div></div>')

    parts = ["<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">",
             "<title>Vail and Hail benchmark report</title><style>", STYLE, "</style></head><body><main>"]
    parts += [f'<div class="eyebrow">Cloud benchmark report · source {esc(provenance.get("commit", "unknown SHA"))}</div>',
              "<h1>Vail and Hail benchmark comparison</h1>",
              '<p class="muted">Curated workloads use repeated warm runs; upstream coverage attempts invoke each original case once. These timing methodologies are separate and should not be directly compared.</p>',
              '<div class="cards">',
              metric_card("Curated 15-case baseline median sum", baseline_ratio, baseline.get("vail", 0), baseline.get("hail", 0)),
              metric_card(f"Curated {len(curated)} workload configurations median sum", unique_ratio, unique.get("vail", 0), unique.get("hail", 0)),
              f'<div class="card"><div class="label">Upstream case results</div><div class="big">{len(upstream_ids)} cases</div><div>{esc(" · ".join(f"{k}: {v}" for k, v in sorted(upstream_statuses.items())) or "No upstream reports supplied")}</div></div>',
              "</div>"]

    warmups = provenance.get("warmups", "unknown")
    measured = provenance.get("measured_seconds_per_capture", "unknown")
    source_sha = provenance.get("commit", "unknown")
    provenance_sha = sha256(provenance_path)
    parts.append(
        f'<p class="note">Curated timings are medians from {esc(warmups)} warmups and '
        f'{esc(measured)} seconds of measured execution per capture. Curated provenance commit SHA: '
        f'<code>{esc(source_sha)}</code>; provenance file SHA-256: <code>{provenance_sha}</code>. '
        'Upstream durations below are one function-body invocation; they exclude fixture generation, '
        'IPC, and engine shutdown. Their resource columns are unavailable in the upstream runner. '
        'Failure, timeout, skipped, and unsupported statuses remain visible.</p>')

    parts += ["<h2 id=\"curated\">Curated benchmark timings</h2>",
              '<p class="muted">Time is the median query duration in milliseconds. CPU is mean process-tree cores. Ratio is Hail / Vail, so values above 1 favor Vail.</p>',
              '<div class="toolbar"><label>Configuration <select id="config"><option value="all">All configurations</option>']
    for config in configs:
        parts.append(f'<option value="{esc(config)}">{esc(config)}</option>')
    parts += ['</select></label><input type="search" id="search" placeholder="Find a benchmark or case" aria-label="Find benchmark"><label><input id="slower" type="checkbox"> Hail faster only</label><span id="count" class="muted"></span></div>',
              '<div class="table-wrap"><table id="curated-table"><thead><tr><th>Benchmark</th><th>Configuration</th><th>Vail · ms</th><th>Hail · ms</th><th>Hail / Vail</th><th>CPU · cores Vail / Hail</th><th>RAM · MiB Vail / Hail</th><th>Parameters</th></tr></thead><tbody>']
    config_order = configs
    for config in config_order:
        for case in case_order:
            pair = curated.get((config, case))
            if not pair:
                continue
            v, h = pair.get("vail"), pair.get("hail")
            if not v or not h:
                continue
            ratio = float(h["median_seconds"]) / float(v["median_seconds"]) if float(v["median_seconds"]) else 0
            cls = "positive" if ratio >= 1 else "negative"
            params = v.get("parameters", "")
            parts.append(
                f'<tr data-config="{esc(config)}" data-slower="{int(ratio < 1)}">'
                f'<td>{esc(LABELS.get(case, case.replace("_", " ")))}<small>{esc(case)}</small></td>'
                f'<td>{esc(config)}</td><td>{seconds(v.get("median_seconds"))}</td>'
                f'<td>{seconds(h.get("median_seconds"))}</td><td class="{cls}">{ratio:.2f}×</td>'
                f'<td>{number(v.get("mean_cpu_cores"))} / {number(h.get("mean_cpu_cores"))}</td>'
                f'<td>{number(v.get("peak_rss_mib"))} / {number(h.get("peak_rss_mib"))}</td>'
                f'<td><small>{esc(params)}</small></td></tr>')
    parts.append('</tbody></table></div>')

    parts += ['<h2 id="upstream">Upstream benchmark coverage</h2>',
              '<p class="muted">Original upstream function cases, attempted once per engine. A missing engine report is shown as “not supplied”; statuses are not silently omitted.</p>',
              '<div class="table-wrap"><table id="upstream-table"><thead><tr><th>Upstream case</th><th>Vail status</th><th>Vail · ms</th><th>Hail status</th><th>Hail · ms</th><th>Hail / Vail</th><th>Details</th></tr></thead><tbody>']
    if not upstream_ids:
        parts.append('<tr><td colspan="7">No upstream coverage JSON was supplied. Pass --upstream-vail and/or --upstream-hail to include reports.</td></tr>')
    for case_id in upstream_ids:
        v, h = vail_cases.get(case_id), hail_cases.get(case_id)
        v_status = v.get("status", "unknown") if v else "not supplied"
        h_status = h.get("status", "unknown") if h else "not supplied"
        vt = v.get("benchmark_seconds") if v else None
        ht = h.get("benchmark_seconds") if h else None
        ratio = (float(ht) / float(vt)
                 if v_status == h_status == "passed" and vt not in (None, 0) and ht is not None
                 else None)
        details = []
        for engine, item in (("Vail", v), ("Hail", h)):
            if item and item.get("reason"):
                details.append(f"{engine}: {item['reason']}")
            if item and item.get("error"):
                details.append(f"{engine}: {item['error']}")
            for field in ("timeout_seconds", "fixture_timeout_seconds"):
                if item and item.get(field) is not None:
                    details.append(f"{engine}: {field.replace('_', ' ')} {item[field]}s")
        cls = "positive" if ratio is not None and ratio >= 1 else "negative"
        speedup = f'<span class="{cls}">{ratio:.2f}×</span>' if ratio is not None else "—"
        parts.append(
            f'<tr data-config="upstream" data-slower="{int(ratio is not None and ratio < 1)}">'
            f'<td>{esc(case_id)}</td><td class="status {esc(v_status)}">{esc(v_status)}</td>'
            f'<td>{seconds(vt)}</td><td class="status {esc(h_status)}">{esc(h_status)}</td>'
            f'<td>{seconds(ht)}</td><td>{speedup}</td><td><small>{esc("; ".join(details))}</small></td></tr>')
    parts.append('</tbody></table></div>')

    for heading, patterns in (("Scaling charts", ("scaling-*.png",)), ("10 Hz telemetry charts", ("telemetry-*.png",))):
        charts = sorted({path for pattern in patterns for path in root.glob(pattern)})
        if not charts:
            continue
        parts.append(f'<h2>{esc(heading)}</h2>')
        for chart in charts:
            encoded = base64.b64encode(chart.read_bytes()).decode("ascii")
            parts.append(f'<figure><figcaption>{esc(chart.stem.replace("-", " "))}</figcaption>'
                         f'<img alt="{esc(chart.name)}" src="data:image/png;base64,{encoded}"></figure>')

    parts += ["<h2>Run provenance</h2>", "<ul>",
              f'<li>Curated implementation SHA: <code>{esc(source_sha)}</code></li>',
              f'<li>Curated provenance.json SHA-256: <code>{provenance_sha}</code></li>',
              f'<li>Curated captures: {esc(len(source_rows))} engine rows across {esc(len(curated))} paired workload configurations.</li>',
              f'<li>Curated warmups: {esc(warmups)}; measured seconds per capture: {esc(measured)}.</li>']
    for name, path, report in (("Vail upstream", upstream_vail, vail_report), ("Hail upstream", upstream_hail, hail_report)):
        if path:
            parts.append(f'<li>{name} report SHA-256: <code>{sha256(path)}</code>; engine: {esc(report.get("engine", "unknown"))}; smoke fixtures: {esc(report.get("smoke_fixtures", "unknown"))}.</li>')
    parts += ["</ul>", '<footer>This page is self-contained: charts are embedded as PNG data URLs, and tables are rendered into the HTML. Generated by benchmarks/render_cloud_html.py.</footer>',
              "<script>",
              "const config=document.getElementById('config'),search=document.getElementById('search'),slower=document.getElementById('slower');",
              "function filter(){let n=0;document.querySelectorAll('tbody tr').forEach(r=>{const isUpstream=r.closest('#upstream-table')!==null;const matchConfig=isUpstream||config.value==='all'||r.dataset.config===config.value;const matchSlower=!slower.checked||r.dataset.slower==='1';const matchText=r.textContent.toLowerCase().includes(search.value.toLowerCase());const visible=matchConfig&&matchSlower&&matchText;r.hidden=!visible;if(visible)n++});document.getElementById('count').textContent=n+' rows'}",
              "[config,search,slower].forEach(x=>x.addEventListener('input',filter));filter();",
              "</script></main></body></html>"]
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(parts), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("curated_root", type=Path, help="Folder containing results.csv, provenance.json and totals.json")
    parser.add_argument("--upstream-vail", type=Path, help="JSON report from upstream_benchmark_coverage.py --engine vail")
    parser.add_argument("--upstream-hail", type=Path, help="JSON report from upstream_benchmark_coverage.py --engine hail")
    parser.add_argument("--output", type=Path, help="Output standalone HTML (default: <curated_root>/report.html)")
    args = parser.parse_args()
    render(args.curated_root, args.output or (args.curated_root / "report.html"), args.upstream_vail, args.upstream_hail)


if __name__ == "__main__":
    main()
