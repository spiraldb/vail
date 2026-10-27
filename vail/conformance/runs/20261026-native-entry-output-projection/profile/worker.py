"""One current public materialized-entry projection; no production modifications."""
import argparse
import json
import os
from pathlib import Path
import resource
import time
from common import EXPECTED, digest
p = argparse.ArgumentParser()
p.add_argument('mode', choices=['confirm', 'sample'])
p.add_argument('--stop-launch', required=True)
a = p.parse_args()
import vail as h
import vail._native as native
if os.environ.get('PYTHONPATH') or os.cpu_count() != 32:
    raise RuntimeError('Import environment or CPU count differs')
for name, value in EXPECTED['threads'].items():
    if os.environ.get(name) != value: raise RuntimeError(f'Thread setting differs: {name}')
if digest(native.__file__) != EXPECTED['native_sha256']:
    raise RuntimeError('Prevalidated current native differs; stop without build')
frontend = Path(h.__file__).parent.resolve()
if frontend != Path(EXPECTED['python']).parent.parent / 'lib/python3.12/site-packages/vail':
    raise RuntimeError('Frontend escaped accepted normal environment')
for name, sha256 in EXPECTED['frontend'].items():
    if digest(frontend / name) != sha256:
        raise RuntimeError(f'Frontend differs: {name}')
fixture = Path(EXPECTED['fixture'])
for name, sha256 in EXPECTED['metadata_sha256'].items():
    if digest(fixture / name) != sha256:
        raise RuntimeError(f'Original fixture metadata differs: {name}')
def emit(**event):
    print(json.dumps(event, allow_nan=False), flush=True)
emit(phase='provenance', main=EXPECTED['main'], production=EXPECTED['compiled_production'],
     native_sha256=digest(native.__file__), metadata_dimensions=EXPECTED['dimensions'], threads=EXPECTED['threads'], pid=os.getpid())
m = h.read_matrix_table(str(fixture))
if 'GQ' not in m.entry.dtype or m.entry.dtype['GQ'] != h.tint32:
    raise RuntimeError('Original GQ Int32 absent/different; no substitute')
# Deliberately project AFTER entries: no manual upstream select_entries/rows.
t = m.entries().select('GQ')
q = h.struct(total=h.agg.sum(t.GQ), present=h.agg.count_where(h.is_defined(t.GQ)),
             high=h.agg.count_where(t.GQ >= 20))
if a.mode == 'confirm':
    plan = t._native.aggregate_physical_plan(q._native_expr)
    if len(plan) > 2_000_000:
        raise RuntimeError('Diagnostic plan bound exceeded')
    emit(phase='diagnostic_plan', plan=plan, scope='diagnostic only, not a test oracle',
         upstream_matrix_take_observed='vail_matrix_take' in plan)
    if EXPECTED['target_diagnostic_name'] not in plan:
        raise RuntimeError('Public pipeline does not reach selected custom path; stop')
first_completed = None
for action in range(1 if a.mode == 'confirm' else 12):
    if first_completed is not None and (Path(a.stop_launch).exists() or time.monotonic()-first_completed >= 6):
        break
    emit(phase='action_start', id=action, monotonic=time.monotonic())
    wall = time.perf_counter()
    cpu = time.process_time()
    result = dict(t.aggregate(q))
    elapsed = time.perf_counter()-wall
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024
    if result != EXPECTED['result']:
        raise RuntimeError(f'Full public result differs: {result!r}')
    completed = time.monotonic()
    emit(phase='action', id=action, result=result, wall_seconds=elapsed,
         core_seconds=time.process_time()-cpu, maxrss_bytes=peak, completed_monotonic=completed)
    if elapsed > 10 or peak > 6*1024**3:
        raise RuntimeError('Action resource budget exceeded')
    if first_completed is None:
        first_completed = completed
