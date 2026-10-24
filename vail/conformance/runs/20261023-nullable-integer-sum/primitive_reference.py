"""In-memory Arrow C++ reference, not a stored/segmented Vail pipeline."""
import concurrent.futures, json, resource, statistics, time
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
n=100_000_000
idx=np.arange(n,dtype=np.int32)
col=idx%2500
valid=(col%11!=0)&(col%13!=0)
values=idx%101
expected=int(np.sum(values,where=valid,dtype=np.int64))
del idx,col
for width in [32,64]:
 v=values if width==32 else values.astype(np.int64)
 a=pa.array(v,mask=~valid)
 for workers in [1,14]:
  arrays=[a.slice(i*n//workers,(i+1)*n//workers-i*n//workers) for i in range(workers)]
  with concurrent.futures.ThreadPoolExecutor(workers) as pool:
   for i in range(8):
    b=time.process_time();s=time.perf_counter();answer=sum(pool.map(lambda x:pc.sum(x).as_py(),arrays));wall=time.perf_counter()-s;cpu=time.process_time()-b
    assert answer==expected,(answer,expected)
    print(json.dumps({'width':width,'workers':workers,'iteration':i,'warmup':i==0,'wall':wall,'cpu':cpu,'logical_bytes':n*(width//8)+n//8,'result':answer}),flush=True)
