import argparse,hashlib,json,pathlib,resource,time,os
import vail as h
import vail._native as native
p=argparse.ArgumentParser();p.add_argument('--query',choices=['bare','sums_count'],required=True);a=p.parse_args()
m=h.read_matrix_table('/private/tmp/entries-profile25.mt')
query=h.agg.sum(m.DP) if a.query=='bare' else h.struct(dp=h.agg.sum(m.DP),gq=h.agg.sum(m.GQ),n=h.agg.count())
print(json.dumps({'phase':'provenance','native':native.__file__,'sha256':hashlib.sha256(pathlib.Path(native.__file__).read_bytes()).hexdigest(),'query':a.query,'fixture':'/private/tmp/entries-profile25.mt','rchar_bytes':None,'reason':'Linux proc io counters unavailable on macOS'}),flush=True)
def run():
 r=m.aggregate_entries(query)
 return r if a.query=='bare' else dict(r)
reference=run()
print(json.dumps({'phase':'warmup','reference':reference}),flush=True)
for i in range(7):
 b=resource.getrusage(resource.RUSAGE_SELF);start=time.perf_counter();r=run();e=resource.getrusage(resource.RUSAGE_SELF)
 assert r==reference
 print(json.dumps({'phase':'query','iteration':i,'wall':time.perf_counter()-start,'cpu':e.ru_utime+e.ru_stime-b.ru_utime-b.ru_stime,'result':r}),flush=True)
