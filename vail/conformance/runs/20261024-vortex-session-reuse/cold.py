import argparse,hashlib,json,pathlib,resource,time
import vail as h
import vail._native as native
p=argparse.ArgumentParser();p.add_argument('--files',type=int,required=True);p.add_argument('--repeat',type=int,default=12);a=p.parse_args()
w=h.utils.range_table(1);assert w.aggregate(h.agg.sum(w.idx))==0
expected=sum((r+c)%101 for r in range(a.files*4)for c in range(32))
print(json.dumps({'sha256':hashlib.sha256(pathlib.Path(native.__file__).read_bytes()).hexdigest(),'files':a.files}),flush=True)
for i in range(a.repeat):
 start=time.perf_counter();t=h.read_table(f'/private/tmp/encoded-dense-small-{a.files}.ht');setup=time.perf_counter()-start
 for phase in ['fresh_source','same_source']:
  b=resource.getrusage(resource.RUSAGE_SELF);start=time.perf_counter();v=t.aggregate(h.agg.sum(h.sum(t.items.map(lambda e:h.int64(e.DP)))));e=resource.getrusage(resource.RUSAGE_SELF);assert v==expected
  print(json.dumps({'i':i,'phase':phase,'setup':setup,'wall':time.perf_counter()-start,'cpu':e.ru_utime+e.ru_stime-b.ru_utime-b.ru_stime,'result':v,'maxrss':e.ru_maxrss}),flush=True)
