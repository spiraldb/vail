import argparse, gc, json, resource, time
import numpy as np
import pyarrow as pa
import vail as hl
p=argparse.ArgumentParser();p.add_argument('--backing',type=int,default=50_000_000);p.add_argument('--repeat',type=int,default=60);p.add_argument('--visible',type=int,default=128);a=p.parse_args()
width=a.visible
n=(a.backing//width)*width
idx=np.arange(n,dtype=np.int32)
depth=pa.array(idx%101,mask=(idx%7==0));payload=pa.StructArray.from_arrays([depth],names=['depth'])
cells=pa.StructArray.from_arrays([payload],names=['payload'],mask=pa.array(idx%11==0))
lists=pa.ListArray.from_arrays(pa.array(np.arange(n//width+1,dtype=np.int32)*width),cells)
row=n//(width*2);start=row*width
selected=lists.slice(row,1)
t=hl.Table.parallelize(pa.Table.from_arrays([selected],names=['items']))
r=t.select(total=hl.sum(t.items.map(lambda x:hl.int64(x.payload.depth))))
expected=sum(i%101 for i in range(start,start+width) if i%7 and i%11)
del idx;gc.collect()
for i in range(a.repeat):
 b=resource.getrusage(resource.RUSAGE_SELF);s=time.perf_counter();value=r.collect()[0].total;e=resource.getrusage(resource.RUSAGE_SELF)
 assert value==expected,(value,expected)
 print(json.dumps({'backing':n,'visible':width,'iteration':i,'warmup':i<5,'wall':time.perf_counter()-s,'cpu':e.ru_utime+e.ru_stime-b.ru_utime-b.ru_stime,'rss':e.ru_maxrss,'result':value}),flush=True)
