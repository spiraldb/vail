"""Fixed Number=R public expansion of transparently replicated real LAD/LA."""
import argparse,json,resource,time
from pathlib import Path
from common import EXPECTED,digest
from preflight import verify
from query import construct
p=argparse.ArgumentParser();p.add_argument('mode',choices=['confirm','sample']);p.add_argument('--stop-launch',required=True);p.add_argument('--reference');a=p.parse_args()
h=verify();lock=json.loads(Path(EXPECTED['generated_lock']).read_text());fixture=Path(EXPECTED['generated_fixture'])
if lock['original_sha256']!=EXPECTED['source_sha256'] or lock['replications']!=4096:raise RuntimeError('Fixture provenance differs')
for relative,record in lock['generated_files'].items():
    path=fixture/relative
    if path.stat().st_size!=record['bytes'] or digest(path)!=record['sha256']:raise RuntimeError(f'Generated input differs:{relative}')
def emit(**e):print(json.dumps(e,allow_nan=False),flush=True)
emit(phase='provenance',main=EXPECTED['main'],production=EXPECTED['compiled_production'],native_sha256=EXPECTED['native_sha256'],reference_sha256=digest(EXPECTED['generated_lock']),metadata_dimensions=EXPECTED['dimensions'])
m=h.read_matrix_table(str(fixture))
if str(m.row.dtype)!=lock['generated_schema']['row'] or str(m.entry.dtype)!=lock['generated_schema']['entry']:raise RuntimeError('Generated schema differs')
action=construct(h,m);first=None
confirmation=None if a.reference is None else json.loads(Path(a.reference).read_text())
for i in range(1 if a.mode=='confirm' else 12):
    if first is not None and (Path(a.stop_launch).exists() or time.monotonic()-first>=6):break
    emit(phase='action_start',id=i,monotonic=time.monotonic());wall=time.perf_counter();cpu=time.process_time()
    result=action();elapsed=time.perf_counter()-wall;peak=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024;completed=time.monotonic()
    if result!=lock['expected'] or (confirmation is not None and result!=confirmation):raise RuntimeError('Full independent aggregate reference differs')
    emit(phase='action',id=i,result=result,wall_seconds=elapsed,core_seconds=time.process_time()-cpu,maxrss_bytes=peak,completed_monotonic=completed)
    if elapsed>10 or peak>6*1024**3:raise RuntimeError('Completed action budget')
    if first is None:first=completed
