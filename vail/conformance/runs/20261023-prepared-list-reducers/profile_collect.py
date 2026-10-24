import json, os, time, resource
import vail as hl
m=hl.utils.range_matrix_table(2000000,int(os.environ.get('COLLECT_COLUMNS','64')),8)
m=m.annotate_entries(x=hl.or_missing(m.col_idx%5!=0,m.row_idx+m.col_idx))
m=m.filter_entries(m.col_idx%7!=0)
r=m.annotate_rows(xs=hl.agg.collect(hl.struct(x=m.x, parity=m.col_idx%2))).rows()
start=time.perf_counter(); cpu=time.process_time()
print(json.dumps({'pid':os.getpid(),'phase':'ready'}),flush=True)
for i in range(int(os.environ.get('COLLECT_ITERATIONS','4'))):
 t=time.perf_counter();c=time.process_time()
 result=r.aggregate(hl.agg.sum(hl.len(r.xs)))
 print(json.dumps({'iteration':i,'seconds':time.perf_counter()-t,'cpu_seconds':time.process_time()-c,'result':result,'maxrss':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}),flush=True)
