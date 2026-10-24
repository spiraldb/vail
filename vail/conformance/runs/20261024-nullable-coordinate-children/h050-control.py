"""Public Arrow-input coordinate workload controls with explicit physical gaps."""
import argparse,json,resource,time
import numpy as np
import pyarrow as pa
import vail as h
p=argparse.ArgumentParser();p.add_argument('--mode',choices=['allvalid','zero','fragmented','runs','huge'],required=True);p.add_argument('--repeat',type=int,default=5);p.add_argument('--rows',type=int,default=524288);p.add_argument('--width',type=int,default=16);a=p.parse_args()
n=64 if a.mode=='huge' else a.rows;w=a.width
valid=np.ones(n,dtype=np.bool_) if a.mode=='allvalid' else np.arange(n)%2==0
if a.mode=='runs':valid=np.arange(n)//4096%4!=0
lengths=np.where(valid,w,0 if a.mode=='zero' else w+1).astype(np.int32)
if a.mode=='huge':lengths[1]=8388609
offsets=np.empty(n+1,dtype=np.int32);offsets[0]=0;np.cumsum(lengths,out=offsets[1:])
values=np.ones(int(offsets[-1]),dtype=np.int64)
xs=pa.ListArray.from_arrays(pa.array(offsets),pa.array(values),mask=pa.array(~valid))
t=h.Table.parallelize(pa.table({'xs':xs}),n_partitions=16);q=h.agg.array_agg(lambda x:h.agg.sum(x),t.xs)
expected=[int(valid.sum())]*w
print(json.dumps({'phase':'ready','mode':a.mode,'rows':n,'width':w,'selected_children':int(valid.sum())*w,'physical_children':int(offsets[-1])}),flush=True)
for i in range(a.repeat):
 before=resource.getrusage(resource.RUSAGE_SELF);start=time.perf_counter();cpu=time.process_time();result=t.aggregate(q)
 elapsed=time.perf_counter()-start;used=time.process_time()-cpu;after=resource.getrusage(resource.RUSAGE_SELF)
 assert result==expected,(result,expected)
 print(json.dumps({'iteration':i,'seconds':elapsed,'cpu_seconds':used,'maxrss_kib':after.ru_maxrss,'minor_faults':after.ru_minflt-before.ru_minflt,'result':result}),flush=True)
