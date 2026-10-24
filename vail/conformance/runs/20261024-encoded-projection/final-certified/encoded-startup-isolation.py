import argparse,hashlib,json,pathlib,resource,time
import vail as h
import vail._native as native
p=argparse.ArgumentParser();p.add_argument('--files',type=int,required=True);a=p.parse_args()
w=h.utils.range_table(1);assert w.aggregate(h.agg.sum(w.idx))==0
b=time.perf_counter();t=h.read_table(f'/private/tmp/encoded-dense-small-{a.files}.ht');read=time.perf_counter()-b
expected=sum((r+c)%101 for r in range(a.files*4) for c in range(32))
print(json.dumps({'phase':'provenance','native':native.__file__,'sha256':hashlib.sha256(pathlib.Path(native.__file__).read_bytes()).hexdigest(),'files':a.files,'native_read_setup':read,'expected':expected}),flush=True)
for i in range(8):
 b=resource.getrusage(resource.RUSAGE_SELF);start=time.perf_counter();value=t.aggregate(h.agg.sum(h.sum(t.items.map(lambda e:h.int64(e.DP)))));e=resource.getrusage(resource.RUSAGE_SELF)
 assert value==expected
 print(json.dumps({'phase':'first_query' if i==0 else 'same_source_warm_query','wall':time.perf_counter()-start,'cpu':e.ru_utime+e.ru_stime-b.ru_utime-b.ru_stime,'result':value}),flush=True)
