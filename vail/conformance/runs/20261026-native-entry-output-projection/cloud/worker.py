"""Fixed consumed public queries; four repeated actions are not independent trials."""
import argparse
import json
import os
from pathlib import Path
import resource
import time
from common import EXPECTED, digest

parser = argparse.ArgumentParser()
parser.add_argument('version', choices=['baseline', 'candidate'])
parser.add_argument('--lock', required=True)
args = parser.parse_args()
lock = json.loads(Path(args.lock).read_text())
if os.environ.get('PYTHONPATH') or os.cpu_count() != 32:
    raise RuntimeError('Unexpected PYTHONPATH or CPU count')
for name, value in EXPECTED['threads'].items():
    if os.environ.get(name) != value:
        raise RuntimeError(f'thread environment differs: {name}')
import vail as h
import vail._native as native
frontend = Path(h.__file__).parent.resolve()
if str(frontend) != lock[args.version]['frontend']:
    raise RuntimeError('Frontend escaped enrolled normal environment')
if digest(native.__file__) != lock[args.version]['native_sha256']:
    raise RuntimeError('Native fingerprint differs')
for relative, sha256 in EXPECTED[args.version+'_frontend'].items():
    if digest(frontend / relative) != sha256:
        raise RuntimeError(f'Frontend fingerprint differs: {relative}')
fixture = Path(EXPECTED['fixture'])
for relative, sha256 in EXPECTED['metadata_sha256'].items():
    if digest(fixture / relative) != sha256:
        raise RuntimeError(f'Input metadata differs: {relative}')
def emit(**event):
    print(json.dumps(event, allow_nan=False), flush=True)
emit(phase='provenance', version=args.version, source=lock[args.version]['source'],
     native_sha256=digest(native.__file__), frontend=str(frontend), pid=os.getpid(),
     metadata_dimensions=EXPECTED['dimensions'], threads=EXPECTED['threads'])
m = h.read_matrix_table(str(fixture))
if 'GQ' not in m.entry.dtype or m.entry.dtype['GQ'] != h.tint32:
    raise RuntimeError('Required original GQ Int32 absent/different; no substitute')
t = m.entries()
narrow = t.select('GQ')
# Explicit all-user-field selection remains the unchanged full-output route.
full = t.select(*[name for name in t.row.dtype.fields if name not in t.key.dtype.fields])
def consumed(table):
    return table.aggregate(h.struct(total=h.agg.sum(table.GQ),
        present=h.agg.count_where(h.is_defined(table.GQ)),
        high=h.agg.count_where(table.GQ >= 20)))
queries = {'projected': lambda: dict(consumed(narrow)), 'full': lambda: dict(consumed(full))}
# Identical warmup and measured order on every independent process/version.
order = [('projected', False), ('full', False)]
for iteration in range(4):
    order.extend((name, True) for name in (('projected', 'full') if iteration % 2 == 0 else ('full', 'projected')))
for action_id, (name, measured) in enumerate(order):
    emit(phase='action_start', id=action_id, query=name, measured=measured, monotonic=time.monotonic())
    wall = time.perf_counter()
    cpu = time.process_time()
    result = queries[name]()
    elapsed = time.perf_counter()-wall
    core_seconds = time.process_time()-cpu
    if result != EXPECTED['gq_recorded_baseline_result']:
        raise RuntimeError(f'Full public GQ result differs: {result!r}')
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024
    emit(phase='action', id=action_id, query=name, measured=measured,
         wall_seconds=elapsed, core_seconds=core_seconds, result=result,
         cumulative_process_maxrss_bytes=peak)
    if elapsed > 10 or peak > 6*1024**3:
        raise RuntimeError('Completed action exceeded resource budget')
