import argparse,hashlib,json,pathlib,resource,time
import vail as h
import vail._native as native
p=argparse.ArgumentParser();p.add_argument('--duration',type=float,default=0);p.add_argument('--repeat',type=int,default=100);a=p.parse_args()
w=h.utils.range_table(1);assert w.aggregate(h.agg.sum(w.idx))==0
t=h.read_table('/private/tmp/encoded-dense-small-128.ht')
print(json.dumps({'native':native.__file__,'sha256':hashlib.sha256(pathlib.Path(native.__file__).read_bytes()).hexdigest()}),flush=True)
start=time.monotonic();i=0
while i<a.repeat or time.monotonic()-start<a.duration:
 b=resource.getrusage(resource.RUSAGE_SELF);s=time.perf_counter();v=t.aggregate(h.agg.sum(h.sum(t.items.map(lambda e:h.int64(e.DP)))));e=resource.getrusage(resource.RUSAGE_SELF);assert v==812144
 if i%10==0:print(json.dumps({'iteration':i,'wall':time.perf_counter()-s,'cpu':e.ru_utime+e.ru_stime-b.ru_utime-b.ru_stime,'maxrss':e.ru_maxrss}),flush=True)
 i+=1
