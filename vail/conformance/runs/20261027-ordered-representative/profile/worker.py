"""Fully consume duplicate-coordinate last values; no optimizer forcing hooks."""
import argparse,json,resource,time
from pathlib import Path
from common import EXPECTED,digest
from preflight import verify
p=argparse.ArgumentParser();p.add_argument('mode',choices=['confirm','sample']);p.add_argument('--stop-launch',required=True);p.add_argument('--reference');a=p.parse_args()
h=verify();fixture=Path(EXPECTED['generated_fixture']);lock=json.loads(Path(EXPECTED['generated_lock']).read_text())
for relative,x in lock['files'].items():
    path=fixture/relative
    if path.stat().st_size!=x['bytes'] or digest(path)!=x['sha256']:raise RuntimeError('Input fingerprint differs')
t=h.read_table(str(fixture))
if str(t.row.dtype)!=lock['source_schema']:raise RuntimeError('Source schema differs')
t=t.order_by(t.source_index).key_by().select('r','c','value','payload')
m=t.to_matrix_table(['r'],['c'])
localized=m.localize_entries('entries','columns')
localized=localized.annotate(__h091_columns=localized.columns)
def emit(**e):print(json.dumps(e,allow_nan=False),flush=True)
reference=None if a.reference is None else json.loads(Path(a.reference).read_text())
first=None;action_id=0
while True:
    if a.mode=='confirm' and action_id:break
    if first is not None and (Path(a.stop_launch).exists() or time.monotonic()>=first+5):break
    emit(phase='action_start',id=action_id,monotonic=time.monotonic());wall=time.perf_counter();cpu=time.process_time()
    rows=localized.collect()
    if len(rows)!=4096 or sorted(row.r for row in rows)!=list(range(4096)):raise RuntimeError('Incomplete row domain')
    result=[]
    for row in sorted(rows,key=lambda r:r.r):
        # Column singleton array is fully consumed and checked in every row.
        if [c.c for c in row['__h091_columns']]!=[0,1,2,3]:raise RuntimeError('Column domain differs')
        if len(row.entries)!=4:raise RuntimeError('Incomplete entry domain')
        values=[]
        for c,entry in enumerate(row.entries):
            expected=row.r+4096*c+15*16384
            if entry is None or entry.value!=expected or entry.payload!=str(expected)+':'+'x'*64:raise RuntimeError('Complete last representative differs')
            values.append(dict(value=entry.value,payload=entry.payload))
        result.append(dict(r=row.r,entries=values))
    if reference is not None and result!=reference:raise RuntimeError('Complete output differs from confirmation')
    elapsed=time.perf_counter()-wall;peak=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024;completed=time.monotonic()
    emit(phase='action',id=action_id,result=result,wall_seconds=elapsed,core_seconds=time.process_time()-cpu,maxrss_bytes=peak,completed_monotonic=completed)
    if elapsed>10 or peak>6*1024**3:raise RuntimeError('Action budget')
    if first is None:first=completed
    action_id+=1
