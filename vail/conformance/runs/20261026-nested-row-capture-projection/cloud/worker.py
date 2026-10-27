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
parser.add_argument('--reference', required=True)
parser.add_argument('--profile-mode', choices=['confirm','sample'])
parser.add_argument('--stop-launch')
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
from queries import construct
reference_path=Path(args.reference)
if digest(reference_path)!=lock['oracle_sha256']:raise RuntimeError('Independent reference fingerprint differs')
reference=json.loads(reference_path.read_text())
cases=('projected',) if args.profile_mode else ('projected','whole_full','scalar')
queries={name:construct(m,name) for name in cases}
if args.profile_mode:
    order=[('projected',False)]*(1 if args.profile_mode=='confirm' else 12)
else:
    order=[(name,False) for name in cases]
    for iteration in range(4):
        order.extend((name,True) for name in (cases if iteration%2==0 else tuple(reversed(cases))))
first_completed=None
for action_id, (name, measured) in enumerate(order):
    if args.profile_mode and first_completed is not None and (Path(args.stop_launch).exists() or time.monotonic()-first_completed>=6):break
    emit(phase='action_start', id=action_id, query=name, measured=measured, monotonic=time.monotonic())
    wall = time.perf_counter()
    cpu = time.process_time()
    result = queries[name]()
    elapsed = time.perf_counter()-wall
    core_seconds = time.process_time()-cpu
    if result != reference[name]:
        raise RuntimeError(f'Full public independent result differs: {result!r}')
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024
    emit(phase='action', id=action_id, query=name, measured=measured,
         wall_seconds=elapsed, core_seconds=core_seconds, result=result,
         cumulative_process_maxrss_bytes=peak,maxrss_bytes=peak,completed_monotonic=time.monotonic())
    if first_completed is None:first_completed=time.monotonic()
    if elapsed > 10 or peak > 6*1024**3:
        raise RuntimeError('Completed action exceeded resource budget')
