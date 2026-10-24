"""Public Arrow producer + Table expression: fixed visible values, growing hidden payload.

Run each artifact/size in its own process. Input construction is outside measured
query windows; retain producer peak RSS separately. No private helper or hook.
"""
import hashlib,json,os,resource,sys,time
import numpy as np
import pyarrow as pa
import vail as h
import vail._native as native
hidden=int(sys.argv[1]);iterations=int(sys.argv[2])
values=np.full(hidden+4,7,dtype=np.int32);values[-2:]=9
input=pa.ListArray.from_arrays(pa.array([0,2,hidden+2,hidden+4],type=pa.int32()),pa.array(values),mask=pa.array([False,True,False]))
arrow=pa.table({'items':input})
t=h.Table.parallelize(arrow,n_partitions=1)
t=t.select(result=t.items.aggregate(lambda x:h.agg.collect_as_set(x)))
print(json.dumps({'phase':'ready','pid':os.getpid(),'hidden':hidden,'visible_values':4,'native_sha256':hashlib.sha256(open(native.__file__,'rb').read()).hexdigest(),'producer_offsets':input.offsets.to_pylist(),'producer_peakrss':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}),flush=True)
for i in range(iterations):
 b=resource.getrusage(resource.RUSAGE_SELF);start=time.perf_counter();cpu=time.process_time();out=t.collect();after=resource.getrusage(resource.RUSAGE_SELF)
 assert [row.result for row in out]==[{7},None,{9}],out
 print(json.dumps({'iteration':i,'wall_seconds':time.perf_counter()-start,'cpu_seconds':time.process_time()-cpu,'maxrss_bytes':after.ru_maxrss,'minor_faults':after.ru_minflt-b.ru_minflt,'major_faults':after.ru_majflt-b.ru_majflt,'result':[[7],None,[9]]}),flush=True)
