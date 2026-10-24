import argparse,json,time,resource
import vail as hl
p=argparse.ArgumentParser();p.add_argument('kind',choices=['stats','float']);p.add_argument('--path',required=True);a=p.parse_args()
t=hl.read_table(a.path)
for i in range(4):
 w=time.perf_counter();c=time.process_time()
 if a.kind=='stats':
  r=t.aggregate(hl.agg.stats(t.x)); result=dict(r)
 else: result=t.aggregate(hl.agg.stats(hl.float64(t.x) + 0.25).stdev)
 print(json.dumps({'iteration':i,'seconds':time.perf_counter()-w,'cpu_seconds':time.process_time()-c,'result':result,'maxrss':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}),flush=True)
