"""Derive complete three-sample comparisons; retain all scheduled cases."""
import argparse
import json
import math
import statistics as stats
from pathlib import Path

p = argparse.ArgumentParser()
p.add_argument('--evidence', type=Path, required=True)
p.add_argument('--out', type=Path, required=True)
a = p.parse_args()
protocol = Path(__file__).parent / 'protocol-attempt2'
tiers = json.loads((protocol / 'evidence-tiers.json').read_text())['cases']
schedule = json.loads((protocol / 'schedule.json').read_text())['jobs']
assert len(tiers) == 112 and len({t['case_id'] for t in tiers}) == 112
phases = {name: json.loads((a.evidence / 'results' / name / 'results.json').read_text())
          for name in ['preflight', 'rep1', 'rep2', 'rep3']}
def key(r):
    return (r['case_id'], r['version'], r['mode'], r['repetition'])
for name, observations in phases.items():
    expected = [j for j in schedule if j['phase'] == 'preflight'] if name == 'preflight' else [
        j for j in schedule if j['phase'] == 'timed' and j['repetition'] == int(name[-1])]
    assert len(observations) == len(expected)
    assert len({key(r) for r in observations}) == len(observations)
    assert {key(r) for r in observations} == {key(r) for r in expected}, name
def positive(x):
    return isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x) and x > 0
rows = []
for tier in tiers:
    case_id = tier['case_id']
    row = {'case_id': case_id, 'tier': tier['evidence_tier'],
           'format_applicability': tier['input']['format_applicability'],
           'versions': {}, 'ratios': None}
    pre = [r for r in phases['preflight'] if r['case_id'] == case_id]
    assert len(pre) == 1, (case_id, 'preflight cardinality')
    row['preflight'] = pre[0]['status']
    for version in ['current', 'hail']:
        observations = []
        for repetition in range(1, 4):
            found = [r for r in phases[f'rep{repetition}']
                     if r['case_id'] == case_id and r['version'] == version]
            assert len(found) == 1, (case_id, version, repetition, len(found))
            observations.append(found[0])
        eligible = (tier['evidence_tier'] != 'planning' and all(
            r.get('sample_eligible_for_timed_ratio') is True and r.get('actual_sample') is True
            and r.get('status') == 'passed' and r.get('phase') == 'timed'
            and not r.get('reused_nonpass_status', False)
            and positive(r.get('benchmark_seconds'))
            and positive((r.get('telemetry') or {}).get('cpu_seconds')) for r in observations))
        values = {'statuses': [r['status'] for r in observations],
                  'eligible': eligible, 'reused': [r.get('reused_nonpass_status', False) for r in observations]}
        if tier['evidence_tier'] == 'planning':
            values['planning_observations'] = [
                {'wall_seconds': r.get('benchmark_seconds'),
                 'cpu_seconds': (r.get('telemetry') or {}).get('cpu_seconds'),
                 'status': r['status'], 'planning_sample': r.get('planning_sample', False)}
                for r in observations]
        if eligible:
            wall = [r['benchmark_seconds'] for r in observations]
            cpu = [r['telemetry']['cpu_seconds'] for r in observations]
            assert all(positive(x) for x in wall + cpu)
            values.update(wall=wall, cpu=cpu, wall_median=stats.median(wall),
                          cpu_median=stats.median(cpu), wall_min=min(wall), wall_max=max(wall),
                          cpu_min=min(cpu), cpu_max=max(cpu))
        row['versions'][version] = values
    v, h = row['versions']['current'], row['versions']['hail']
    if v['eligible'] and h['eligible']:
        row['ratios'] = {'wall': h['wall_median'] / v['wall_median'],
                         'cpu': h['cpu_median'] / v['cpu_median']}
    rows.append(row)
paired = [r for r in rows if r['ratios'] is not None]
assert len(rows) == 112
def describe(rs):
    ratios = [r['ratios'] for r in rs]
    if not ratios:
        return {'pairs': 0, 'median_wall_speedup': None, 'median_cpu_speedup': None,
                'geomean_wall_speedup': None, 'geomean_cpu_speedup': None,
                'vail_wall_slower': 0, 'vail_cpu_higher': 0,
                'vail_wall_at_least_5x': 0, 'vail_wall_at_least_10x': 0,
                'sum_case_median_wall': {'current': 0, 'hail': 0},
                'sum_case_median_cpu': {'current': 0, 'hail': 0}}
    return {'pairs': len(rs),
            'median_wall_speedup': stats.median(r['wall'] for r in ratios),
            'median_cpu_speedup': stats.median(r['cpu'] for r in ratios),
            'geomean_wall_speedup': math.exp(stats.mean(math.log(r['wall']) for r in ratios)),
            'geomean_cpu_speedup': math.exp(stats.mean(math.log(r['cpu']) for r in ratios)),
            'vail_wall_slower': sum(r['wall'] < 1 for r in ratios),
            'vail_cpu_higher': sum(r['cpu'] < 1 for r in ratios),
            'vail_wall_at_least_5x': sum(r['wall'] >= 5 for r in ratios),
            'vail_wall_at_least_10x': sum(r['wall'] >= 10 for r in ratios),
            'sum_case_median_wall': {v: sum(r['versions'][v]['wall_median'] for r in rs)
                                     for v in ['current', 'hail']},
            'sum_case_median_cpu': {v: sum(r['versions'][v]['cpu_median'] for r in rs)
                                    for v in ['current', 'hail']}}
summary = {'scheduled_cases': len(rows), 'eligible_pairs': len(paired),
           'all': describe(paired),
           'storage': describe([r for r in paired if r['format_applicability'] != 'format-independent']),
           'format_independent': describe([r for r in paired if r['format_applicability'] == 'format-independent']),
           'unpaired': [r['case_id'] for r in rows if r['ratios'] is None],
           'rows': rows}
a.out.write_text(json.dumps(summary, indent=2) + '\n')
print(json.dumps({k: v for k, v in summary.items() if k != 'rows'}, indent=2))
