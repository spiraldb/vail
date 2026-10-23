#!/usr/bin/env python3
"""Aggregate Vail-first paired upstream benchmark repetitions."""
import csv
import json
import html
import math
import statistics
import sys
from pathlib import Path


def geomean(values):
    values = [x for x in values if isinstance(x, (int, float)) and x > 0 and math.isfinite(x)]
    return math.exp(sum(math.log(x) for x in values) / len(values)) if values else None


def median_or_none(values):
    values=list(values)
    return statistics.median(values) if values else None

def main(root: Path):
    reps = sorted(p for p in root.glob('rep-*') if p.is_dir() and (p / 'selection.json').is_file())
    if not reps:
        raise SystemExit(f'no rep-* output directories under {root}')
    attempts = []
    rep_summaries = []
    per_case = {}
    nonpasses = []
    for rep in reps:
        selection = json.loads((rep / 'selection.json').read_text())
        by_engine = {}
        for engine in ('vail', 'hail'):
            report_path = rep / f'{engine}.json'
            report = json.loads(report_path.read_text()) if report_path.exists() else {'cases': []}
            normalized = {}
            for original in report['cases']:
                case_result = dict(original)
                if case_result.get('worker_exitcode') == 86:
                    case_result['status'] = 'memory_limited'
                normalized[case_result['id']] = case_result
                if case_result.get('status')!='passed':
                    nonpasses.append(dict(repetition=rep.name,engine=engine,**case_result))
            by_engine[engine] = normalized
        pairs = []
        for case in selection:
            ident = case['id']
            v = by_engine['vail'].get(ident, {'status': 'missing'})
            h = by_engine['hail'].get(ident, {'status': 'missing'})
            row = {'repetition': rep.name, 'case_id': ident,
                   'vail_status': v.get('status'), 'hail_status': h.get('status')}
            for engine, result in (('vail', v), ('hail', h)):
                row[f'{engine}_wall_seconds'] = result.get('benchmark_seconds')
                row[f'{engine}_cpu_seconds'] = (result.get('telemetry') or {}).get('cpu_seconds')
            if v.get('status') == 'passed' and h.get('status') == 'passed':
                row['hail_over_vail_wall_ratio'] = row['hail_wall_seconds'] / row['vail_wall_seconds']
                row['hail_over_vail_cpu_ratio'] = (row['hail_cpu_seconds'] / row['vail_cpu_seconds']
                                                    if row['hail_cpu_seconds'] and row['vail_cpu_seconds'] else None)
                pairs.append(row)
                per_case.setdefault(ident, []).append(row)
            else:
                row['hail_over_vail_wall_ratio'] = None
                row['hail_over_vail_cpu_ratio'] = None
            attempts.append(row)
        rep_summaries.append({
            'repetition': rep.name,
            'selected': len(selection),
            'vail_attempts': len(by_engine['vail']),
            'hail_attempts_or_gated_entries': len(by_engine['hail']),
            'common_passes': len(pairs),
            'vail_wall_sum_seconds': sum(r['vail_wall_seconds'] for r in pairs),
            'hail_wall_sum_seconds': sum(r['hail_wall_seconds'] for r in pairs),
            'vail_cpu_sum_seconds': sum(r['vail_cpu_seconds'] for r in pairs if r['vail_cpu_seconds'] is not None),
            'hail_cpu_sum_seconds': sum(r['hail_cpu_seconds'] for r in pairs if r['hail_cpu_seconds'] is not None),
            'vail_status_counts': {status: sum(c.get('status') == status for c in by_engine['vail'].values()) for status in sorted({c.get('status') for c in by_engine['vail'].values()})},
            'hail_status_counts': {status: sum(c.get('status') == status for c in by_engine['hail'].values()) for status in sorted({c.get('status') for c in by_engine['hail'].values()})},
            'wall_ratio_geomean': geomean([r['hail_over_vail_wall_ratio'] for r in pairs]),
            'cpu_ratio_geomean': geomean([r['hail_over_vail_cpu_ratio'] for r in pairs]),
            'wall_sum_ratio': (sum(r['hail_wall_seconds'] for r in pairs) / sum(r['vail_wall_seconds'] for r in pairs)) if pairs else None,
            'cpu_sum_ratio': (sum(r['hail_cpu_seconds'] for r in pairs) / sum(r['vail_cpu_seconds'] for r in pairs)) if pairs else None,
        })
    with (root / 'attempts.csv').open('w', newline='') as f:
        fields = ['repetition', 'case_id', 'vail_status', 'hail_status',
                  'vail_wall_seconds', 'hail_wall_seconds', 'vail_cpu_seconds', 'hail_cpu_seconds',
                  'hail_over_vail_wall_ratio', 'hail_over_vail_cpu_ratio']
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(attempts)

    (root/'nonpasses.json').write_text(json.dumps(nonpasses,indent=2)+'\n')
    medians = []
    for ident in sorted({r['case_id'] for r in attempts}):
        vrows = [r for r in attempts if r['case_id'] == ident and r['vail_status'] == 'passed']
        hrows = [r for r in attempts if r['case_id'] == ident and r['hail_status'] == 'passed']
        if not vrows or not hrows:
            continue
        row = {'case_id': ident, 'paired_repetitions': len(per_case.get(ident, [])),
               'vail_repetitions': len(vrows), 'hail_repetitions': len(hrows)}
        for engine, rows in [('vail', vrows), ('hail', hrows)]:
            for metric in ['wall', 'cpu']:
                values = [r[f'{engine}_{metric}_seconds'] for r in rows if r[f'{engine}_{metric}_seconds'] is not None]
                for kind, fn in [('median', statistics.median), ('min', min), ('max', max)]:
                    row[f'{engine}_{metric}_{kind}_seconds'] = fn(values) if values else None
        for metric in ['wall', 'cpu']:
            v, h = row[f'vail_{metric}_median_seconds'], row[f'hail_{metric}_median_seconds']
            row[f'hail_over_vail_{metric}_ratio'] = h/v if v and h else None
        medians.append(row)
    with (root / 'case-medians.csv').open('w', newline='') as f:
        fields = list(medians[0]) if medians else ['case_id']
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(medians)
    summary = {
        'repetitions': rep_summaries,
        'paired_cases_with_at_least_one_successful_pair': len(medians),
        'all_rep_paired_cases': sum(x['vail_repetitions'] == len(reps) and x['hail_repetitions'] == len(reps) for x in medians),
        'common_paired_case_count': sum(x['vail_repetitions'] == len(reps) and x['hail_repetitions'] == len(reps) for x in medians),
        'variable_paired_case_count': sum(x['vail_repetitions'] < len(reps) or x['hail_repetitions'] < len(reps) for x in medians),
        'paired_case_outcomes': {
            'wall_vail_faster_gt_10pct': sum(r['hail_over_vail_wall_ratio'] > 1.1 for r in medians),
            'wall_within_10pct': sum(0.9 <= r['hail_over_vail_wall_ratio'] <= 1.1 for r in medians),
            'wall_hail_faster_gt_10pct': sum(r['hail_over_vail_wall_ratio'] < 0.9 for r in medians),
            'cpu_vail_faster_gt_10pct': sum(r['hail_over_vail_cpu_ratio'] > 1.1 for r in medians if r['hail_over_vail_cpu_ratio'] is not None),
            'cpu_within_10pct': sum(0.9 <= r['hail_over_vail_cpu_ratio'] <= 1.1 for r in medians if r['hail_over_vail_cpu_ratio'] is not None),
            'cpu_hail_faster_gt_10pct': sum(r['hail_over_vail_cpu_ratio'] < 0.9 for r in medians if r['hail_over_vail_cpu_ratio'] is not None),
        },
        'case_medians': {
            'wall_ratio_geomean': geomean([r['hail_over_vail_wall_ratio'] for r in medians]),
            'cpu_ratio_geomean': geomean([r['hail_over_vail_cpu_ratio'] for r in medians]),
            'vail_wall_sum_seconds': sum(r['vail_wall_median_seconds'] for r in medians),
            'hail_wall_sum_seconds': sum(r['hail_wall_median_seconds'] for r in medians),
            'vail_cpu_sum_seconds': sum(r['vail_cpu_median_seconds'] for r in medians if r['vail_cpu_median_seconds'] is not None),
            'hail_cpu_sum_seconds': sum(r['hail_cpu_median_seconds'] for r in medians if r['hail_cpu_median_seconds'] is not None),
            'wall_sum_ratio': sum(r['hail_wall_median_seconds'] for r in medians) / sum(r['vail_wall_median_seconds'] for r in medians) if medians else None,
            'cpu_sum_ratio': (sum(r['hail_cpu_median_seconds'] for r in medians if r['hail_cpu_median_seconds'] and r['vail_cpu_median_seconds']) / sum(r['vail_cpu_median_seconds'] for r in medians if r['hail_cpu_median_seconds'] and r['vail_cpu_median_seconds'])) if any(r['hail_cpu_median_seconds'] and r['vail_cpu_median_seconds'] for r in medians) else None,
            'median_repetition_wall_geomean': median_or_none(x['wall_ratio_geomean'] for x in rep_summaries if x['wall_ratio_geomean'] is not None),
            'median_repetition_cpu_geomean': median_or_none(x['cpu_ratio_geomean'] for x in rep_summaries if x['cpu_ratio_geomean'] is not None),
            'median_repetition_wall_sum_ratio': median_or_none(x['wall_sum_ratio'] for x in rep_summaries if x['wall_sum_ratio'] is not None),
            'median_repetition_cpu_sum_ratio': median_or_none(x['cpu_sum_ratio'] for x in rep_summaries if x['cpu_sum_ratio'] is not None),
        },
        'metric_definitions': {
            'wall_seconds': 'benchmark_seconds, per fresh worker/query; excludes startup and fixture preparation',
            'cpu_seconds': 'sum of user+system process-tree counters; exited descendants retained by PID+creation time; 10 Hz sample lower-bound for very short children',
            'ratios': 'Hail / Vail; geomean over per-case ratios; sum ratio is sum of per-case times divided by sum of per-case times',
            'case_medians': 'independent per-engine successful samples: three fresh repetitions per engine; observed minimum and maximum are descriptive ranges, not confidence intervals; ratios computed from medians',
        }
    }
    (root / 'aggregate-summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    summary_rows = []
    for label, value in (
        ('Full inventory cases', len(json.loads((root/'rep-01/selection.json').read_text()))),
        ('Paired cases with at least one successful pair', summary['paired_cases_with_at_least_one_successful_pair']),
        ('Cases with every selected repetition successful on both engines', summary['all_rep_paired_cases']),
        ('Cases with incomplete successful repetitions', summary['variable_paired_case_count']),
        ('Vail faster by >10% (wall / CPU)', f"{summary['paired_case_outcomes']['wall_vail_faster_gt_10pct']} / {summary['paired_case_outcomes']['cpu_vail_faster_gt_10pct']}"),
        ('Within 10% (wall / CPU)', f"{summary['paired_case_outcomes']['wall_within_10pct']} / {summary['paired_case_outcomes']['cpu_within_10pct']}"),
        ('Hail faster by >10% (wall / CPU)', f"{summary['paired_case_outcomes']['wall_hail_faster_gt_10pct']} / {summary['paired_case_outcomes']['cpu_hail_faster_gt_10pct']}"),
        ('Median-case Hail/Vail wall geomean', summary['case_medians']['wall_ratio_geomean']),
        ('Median-case Hail/Vail CPU geomean', summary['case_medians']['cpu_ratio_geomean']),
        ('Median-case Hail/Vail total wall ratio', summary['case_medians']['wall_sum_ratio']),
        ('Median-case Hail/Vail total CPU ratio', summary['case_medians']['cpu_sum_ratio']),
        ('Median repetition Hail/Vail wall geomean', summary['case_medians']['median_repetition_wall_geomean']),
        ('Median repetition Hail/Vail CPU geomean', summary['case_medians']['median_repetition_cpu_geomean']),
    ):
        summary_rows.append(f'<tr><th>{html.escape(str(label))}</th><td>{html.escape(str(value))}</td></tr>')
    case_rows = []
    for row in medians:
        def range_text(median_key, min_key, max_key):
            if row[median_key] is None:
                return ''
            return f"{row[median_key]} ({row[min_key]}–{row[max_key]})"
        values = (row['case_id'], f"{row['vail_repetitions']} / {row['hail_repetitions']}",
                  range_text('vail_wall_median_seconds', 'vail_wall_min_seconds', 'vail_wall_max_seconds'),
                  range_text('hail_wall_median_seconds', 'hail_wall_min_seconds', 'hail_wall_max_seconds'),
                  row['hail_over_vail_wall_ratio'],
                  range_text('vail_cpu_median_seconds', 'vail_cpu_min_seconds', 'vail_cpu_max_seconds'),
                  range_text('hail_cpu_median_seconds', 'hail_cpu_min_seconds', 'hail_cpu_max_seconds'),
                  row['hail_over_vail_cpu_ratio'])
        case_rows.append('<tr>' + ''.join(f'<td>{html.escape(str(value))}</td>' for value in values) + '</tr>')
    rep_rows = []
    for row in rep_summaries:
        rep_rows.append('<tr>' + ''.join(f'<td>{html.escape(str(row.get(k, "")))}</td>' for k in (
            'repetition', 'selected', 'common_passes', 'wall_ratio_geomean', 'cpu_ratio_geomean',
            'wall_sum_ratio', 'cpu_sum_ratio')) + '</tr>')
    report_links = ''.join(f'<li><a href="{html.escape(rep.name)}/report.html">{html.escape(rep.name)} interactive CPU/RSS/disk telemetry</a></li>' for rep in reps)
    page = '''<!doctype html><meta charset="utf-8"><title>Full upstream benchmark aggregate</title>
<style>body{font:14px system-ui,sans-serif;margin:2rem;color:#222}table{border-collapse:collapse;margin:1rem 0 2rem}th,td{border:1px solid #ccc;padding:.35rem .55rem;text-align:right}th:first-child,td:first-child{text-align:left}thead{background:#eee}code{font-size:.9em}</style>
<h1>Full upstream benchmark aggregate</h1><p>Policy: three fresh repetitions per engine for the full suite; a validation-only selection may use fewer and is identified by provenance. Ranges describe observed samples, not confidence intervals.</p>
<p>Wall times are per fresh query worker and exclude startup/fixture preparation. CPU seconds sum user+system counters across the worker process tree, including Spark JVM descendants. Process CPU is sampled at 10 Hz and is a lower bound for very short-lived children. Hail/Vail ratios are greater than 1 when Hail uses more time or CPU. Geomeans are over per-case ratios; total ratios divide summed times. Cases without both engines passing are excluded from paired ratios and retained in <code>attempts.csv</code> and <code>nonpasses.json</code>.</p>
<h2>Overall</h2><table><tbody>SUMMARY</tbody></table>
<h2>Per repetition</h2><table><thead><tr><th>Repetition</th><th>Selected</th><th>Common passes</th><th>Wall geomean</th><th>CPU geomean</th><th>Total wall ratio</th><th>Total CPU ratio</th></tr></thead><tbody>REPS</tbody></table>
<h2>Interactive process telemetry</h2><ul>REPORT_LINKS</ul>
<h2>Per-case median and observed range over independent successful engine samples</h2><table><thead><tr><th>Case</th><th>Vail / Hail samples</th><th>Vail wall median (min–max) s</th><th>Hail wall median (min–max) s</th><th>Hail/Vail wall</th><th>Vail CPU median (min–max) s</th><th>Hail CPU median (min–max) s</th><th>Hail/Vail CPU</th></tr></thead><tbody>CASES</tbody></table>
<p>See <code>attempts.csv</code> for every engine/case/repetition status and raw timing/CPU values; see each <code>rep-*/report.html</code> for detailed per-run results.</p>'''.replace('SUMMARY', ''.join(summary_rows)).replace('REPS', ''.join(rep_rows)).replace('CASES', ''.join(case_rows)).replace('REPORT_LINKS', report_links)
    (root / 'aggregate-report.html').write_text(page)
    print(json.dumps(summary, indent=2))

if __name__ == '__main__':
    main(Path(sys.argv[1]))
