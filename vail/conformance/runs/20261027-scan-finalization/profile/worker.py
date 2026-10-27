"""One public full coordinate scan, preserving original DP missingness."""
import argparse,json,time,resource
from pathlib import Path
from common import EXPECTED,digest
from preflight import verify
from values import encode,compatible
p=argparse.ArgumentParser();p.add_argument('mode',choices=['confirm','sample']);p.add_argument('--stop-launch',required=True);p.add_argument('--reference');a=p.parse_args()
h=verify();fixture=Path(EXPECTED['generated_fixture']);lock=json.loads(Path(EXPECTED['generated_lock']).read_text())
for name,x in lock['files'].items():
    path=fixture/name
    if path.stat().st_size!=x['bytes'] or digest(path)!=x['sha256']:raise RuntimeError('Persisted input differs')
t=h.read_table(str(fixture))
if str(t.row.dtype)!=lock['schema']:raise RuntimeError('Schema differs')
t=t.annotate(prefix=h.scan.array_agg(lambda x:h.scan.mean(x),t['values'])).select('key','prefix')
reference=None if a.reference is None else json.loads(Path(a.reference).read_text())
expected={json.dumps(e['key'],sort_keys=True):e for e in lock['expected']}
def emit(**e):print(json.dumps(e,allow_nan=False),flush=True)
first=None;i=0
while True:
    if a.mode=='confirm' and i:break
    if first is not None and (Path(a.stop_launch).exists() or time.monotonic()>=first+5):break
    emit(phase='action_start',id=i,monotonic=time.monotonic());wall=time.perf_counter();cpu=time.process_time()
    rows=t.collect();observed={}
    if len(rows)!=256:raise RuntimeError('Incomplete output domain')
    for row in rows:
        key=encode(row['key']);token=json.dumps(key,sort_keys=True)
        if token in observed or token not in expected:raise RuntimeError('Duplicate/unexpected variant key')
        value=dict(key=key,prefix=encode(row.prefix))
        if not compatible(value,expected[token]):raise RuntimeError('Full independent prefix differs')
        observed[token]=value
    result=[observed[k] for k in sorted(observed)]
    if reference is not None and not compatible(result,reference):raise RuntimeError('Confirmation differs')
    elapsed=time.perf_counter()-wall;peak=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024;done=time.monotonic()
    emit(phase='action',id=i,result=result,wall_seconds=elapsed,core_seconds=time.process_time()-cpu,maxrss_bytes=peak,completed_monotonic=done)
    if elapsed>10 or peak>6*1024**3:raise RuntimeError('Completed action budget')
    if first is None:first=done
    i+=1
