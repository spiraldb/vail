import argparse,hashlib,json,pathlib,resource,time
import vail as h
import vail._native as native
p=argparse.ArgumentParser();p.add_argument('--fixture',required=True);p.add_argument('--expected',type=int,required=True);p.add_argument('--repeat',type=int,default=12);p.add_argument('--fresh',action='store_true');a=p.parse_args()
w=h.utils.range_table(1);assert w.aggregate(h.agg.sum(w.idx))==0
t=h.read_table(a.fixture);assert t.aggregate(h.agg.sum(h.len(t.items)))==a.expected
print(json.dumps({'source':native.__file__,'sha256':hashlib.sha256(pathlib.Path(native.__file__).read_bytes()).hexdigest(),'fixture':a.fixture,'fresh_source':a.fresh,'expected':a.expected}),flush=True)
for i in range(a.repeat):
 if a.fresh:t=h.read_table(a.fixture)
 b=resource.getrusage(resource.RUSAGE_SELF);start=time.perf_counter();v=t.aggregate(h.agg.sum(h.len(t.items)));end=time.perf_counter();e=resource.getrusage(resource.RUSAGE_SELF);assert v==a.expected
 print(json.dumps({'iteration':i,'wall':end-start,'cpu':e.ru_utime+e.ru_stime-b.ru_utime-b.ru_stime,'maxrss':e.ru_maxrss,'result':v}),flush=True)
