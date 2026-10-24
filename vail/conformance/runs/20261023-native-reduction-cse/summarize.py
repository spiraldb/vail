import json
import pathlib
import statistics

root = pathlib.Path(__file__).parent
summary = {}
for campaign, directory in [('80m', root / 'evidence'), ('500m', root / 'evidence' / '500m'), ('confirmation', root / 'evidence' / 'confirmation'), ('final500m', root / 'evidence' / 'final500m')]:
    metrics = {}
    for path in sorted(directory.glob('*.jsonl')):
        records = [json.loads(line) for line in path.read_text().splitlines()]
        samples = [r for r in records if r.get('warmup') is False]
        if not samples:
            continue
        assert len(samples) == 3
        metrics[path.stem] = {
            'native_sha256': records[0]['sha256'], 'samples': len(samples),
            'wall_median': statistics.median(r['wall'] for r in samples),
            'wall_range': [min(r['wall'] for r in samples), max(r['wall'] for r in samples)],
            'cpu_median': statistics.median(r['cpu'] for r in samples),
            'peak_rss_gib': max(r['maxrss'] for r in samples) / 2**30,
        }
    cases = {}
    for key in metrics:
        if key.startswith('baseline-'):
            case = key.removeprefix('baseline-')
            b, c = metrics[key], metrics['candidate-' + case]
            cases[case] = {'baseline': b, 'candidate': c,
                          'wall_speedup': b['wall_median'] / c['wall_median'],
                          'cpu_speedup': b['cpu_median'] / c['cpu_median']}
    summary[campaign] = cases
(root / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
for campaign, cases in summary.items():
    for case, row in cases.items():
        b, c = row['baseline'], row['candidate']
        print(campaign, case, f"{b['wall_median']:.4f} -> {c['wall_median']:.4f}s", f"{b['cpu_median']:.3f} -> {c['cpu_median']:.3f}CPU", f"{row['wall_speedup']:.3f}x")
