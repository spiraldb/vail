"""One frozen eight-process comparison, after a successful Linux artifact gate."""
import argparse
import json
from pathlib import Path
import statistics
from common import EXPECTED, HERE, digest, environment, guarded, protocol_fingerprints

p = argparse.ArgumentParser()
p.add_argument('--gate', required=True)
p.add_argument('--out', required=True)
a = p.parse_args()
gate = Path(a.gate).resolve()
out = Path(a.out).resolve()
out.mkdir(parents=True, exist_ok=False)
lock_path = gate / 'artifact-lock.json'
lock = json.loads(lock_path.read_text())
if (lock.get('rust_cases'), lock.get('public_cases')) != (7, 446):
    raise RuntimeError('Required Linux correctness gate absent')
if lock.get('protocol_files') != protocol_fingerprints():
    raise RuntimeError('Executable protocol changed after artifact gate')
if lock['expected_protocol_sha256'] != digest(HERE / 'expected.json'):
    raise RuntimeError('Protocol changed after artifact gate')
if lock['candidate']['source'] != EXPECTED['candidate_source'] or lock['baseline']['source'] != EXPECTED['baseline_source']:
    raise RuntimeError('Source lock differs')
def compiler_idle():
    active = []
    for directory in Path('/proc').iterdir():
        if not directory.name.isdigit():
            continue
        try:
            name = (directory / 'comm').read_text().strip()
        except FileNotFoundError:
            continue
        if name in {'cargo', 'rustc', 'maturin', 'cc1', 'cc1plus', 'ld', 'lld'}:
            active.append((directory.name, name))
    if active:
        raise RuntimeError(f'Compiler work remains: {active}')
compiler_idle()
records = []
for index, version in enumerate(EXPECTED['process_order']):
    compiler_idle()
    command = [lock[version]['python'], str(HERE / 'worker.py'), version, '--lock', str(lock_path),'--reference',str(gate/'independent-reference.json')]
    stage = guarded(command, out / f'{index:02d}-{version}', out, environment(), 60, query=True)
    events = [json.loads(line) for line in (stage / 'stdout').read_text().splitlines()]
    actions = [event for event in events if event['phase'] == 'action']
    if len(actions) != 15 or sum(event['measured'] for event in actions) != 12:
        raise RuntimeError('Incomplete fixed action sequence')
    watchdog = json.loads((stage / 'result.json').read_text())
    per_query = {}
    for query in ('projected', 'whole_full', 'scalar'):
        measured = [event for event in actions if event['query'] == query and event['measured']]
        if len(measured) != 4:
            raise RuntimeError('Expected four repeated observations per process/query')
        per_query[query] = {metric: statistics.median(event[metric] for event in measured)
                            for metric in ('wall_seconds', 'core_seconds')}
    records.append(dict(process=index, version=version, medians=per_query,
                        process_peak_rss_bytes=max(watchdog['monitor_maxrss_bytes'],
                            max(event['cumulative_process_maxrss_bytes'] for event in actions))))
    (out / 'completed-processes.json').write_text(json.dumps(records, indent=2)+'\n')
pairs = []
for start in range(0, 8, 2):
    pair = {record['version']: record for record in records[start:start+2]}
    ratios = {query: {metric: pair['baseline']['medians'][query][metric] / pair['candidate']['medians'][query][metric]
                       for metric in ('wall_seconds', 'core_seconds')} for query in ('projected', 'whole_full', 'scalar')}
    pairs.append(dict(process_indices=[start, start+1], baseline_over_candidate=ratios))
summary = dict(independent_processes_per_version=4, repeated_timed_observations_per_process_per_query=4,
               process_order=EXPECTED['process_order'], records=records, adjacent_balanced_pairs=pairs,
               expected_origin='independent baseline row reducers plus Python key arithmetic/Unicode length fold; fingerprints frozen before timing',
               memory_scope='whole process including construction, all three query warmups and all three query workloads',
               latency_scope='consumed action only; construction/warmup excluded; watchdog overhead not subtracted',
               interpretation='four independent processes/version; no claim of16 independent trials or asymptotic scaling')
(out / 'summary.json').write_text(json.dumps(summary, indent=2)+'\n')
compiler_idle()
print(json.dumps(summary, indent=2))
