import argparse,hashlib,json,pathlib,resource,time,os
import vail as hl
import vail._native as native
p=argparse.ArgumentParser();p.add_argument('--prepare',action='store_true');p.add_argument('--files',type=int,default=1);p.add_argument('--repeat',type=int,default=8);a=p.parse_args()
path=f'/private/tmp/encoded-dense-small-{a.files}.ht'
if a.prepare:
 t=hl.utils.range_table(a.files*4,n_partitions=a.files)
 t=t.annotate(items=hl.range(32).map(lambda c:hl.struct(DP=hl.int32((t.idx+c)%101))))
 t.write(path,overwrite=True)
 raise SystemExit
expected=sum((r+c)%101 for r in range(a.files*4) for c in range(32))
def run():
 t=hl.read_table(path)
 return t.aggregate(hl.agg.sum(hl.sum(t.items.map(lambda e:hl.int64(e.DP)))))
print(json.dumps({'phase':'provenance','native':native.__file__,'sha256':hashlib.sha256(pathlib.Path(native.__file__).read_bytes()).hexdigest(),'files':a.files,'rows':a.files*4,'expected':expected}),flush=True)
for i in range(a.repeat):
 b=resource.getrusage(resource.RUSAGE_SELF);start=time.perf_counter();v=run();end=resource.getrusage(resource.RUSAGE_SELF)
 assert v==expected,(v,expected)
 print(json.dumps({'phase':'fresh_read_query','iteration':i,'wall':time.perf_counter()-start,'cpu':end.ru_utime+end.ru_stime-b.ru_utime-b.ru_stime,'sum':v}),flush=True)
