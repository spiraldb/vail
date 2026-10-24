import argparse,hashlib,json,pathlib,resource,time
import pyarrow as pa
import numpy as np
import vail as hl
import vail._native as native
p=argparse.ArgumentParser();p.add_argument('--prepare',action='store_true');p.add_argument('--owner',type=int,default=10000);p.add_argument('--repeat',type=int,default=8);a=p.parse_args()
path=f'/private/tmp/encoded-orphan-{a.owner}.ht'
# Native public writer preserves logical null-list semantics independently of
# the unobservable payload beneath the middle missing list.
if a.prepare:
 values=np.arange(a.owner,dtype=np.int32)%101
 entries=pa.StructArray.from_arrays([pa.array(values)],names=['n'])
 lists=pa.ListArray.from_arrays(pa.array([0,64,a.owner-64,a.owner],type=pa.int32()),entries,mask=pa.array([False,True,False]))
 hl.Table.parallelize(pa.Table.from_arrays([lists],names=['items'])).write(path,overwrite=True)
 raise SystemExit
expected=sum(i%101 for i in list(range(64))+list(range(a.owner-64,a.owner)))
t=hl.read_table(path)
def query():return t.aggregate(hl.agg.sum(hl.sum(t.items.map(lambda e:hl.int64(e.n)))))
print(json.dumps({'phase':'provenance','native':native.__file__,'sha256':hashlib.sha256(pathlib.Path(native.__file__).read_bytes()).hexdigest(),'owner':a.owner,'visible':128,'expected':expected}),flush=True)
for i in range(a.repeat):
 b=resource.getrusage(resource.RUSAGE_SELF);start=time.perf_counter();v=query();e=resource.getrusage(resource.RUSAGE_SELF)
 assert v==expected,(v,expected)
 print(json.dumps({'phase':'query','iteration':i,'wall':time.perf_counter()-start,'cpu':e.ru_utime+e.ru_stime-b.ru_utime-b.ru_stime,'result':v}),flush=True)
