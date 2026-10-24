import argparse,json,os,resource,time
import vail as h
p=argparse.ArgumentParser();p.add_argument('--query',choices=['ad_any','pl_bundle'],default='ad_any');p.add_argument('--repeat',type=int,default=4);p.add_argument('--path',default='/home/danielking/cloud-scan-20261023/data/vail-from-hail/profile25_mt.mt');a=p.parse_args()
m=h.read_matrix_table(a.path)
if a.query=='ad_any':
 value=m.AD.aggregate(lambda x:h.agg.any(x>0))
 result_expr=h.agg.sum(value)
else:
 score=m.PL.aggregate(lambda x:h.struct(some=h.agg.any(x<50),every=h.agg.all(x<50),n=h.agg.count_where(x<50),fraction=h.agg.fraction(x<50)))
 result_expr=h.struct(some=h.agg.sum(score.some),every=h.agg.sum(score.every),n=h.agg.sum(score.n),fraction=h.agg.sum(score.fraction))
print(json.dumps({'pid':os.getpid(),'phase':'ready','query':a.query,'entry_schema':str(m.entry.dtype)}),flush=True)
for i in range(a.repeat):
 usage=resource.getrusage(resource.RUSAGE_SELF);wall=time.perf_counter();cpu=time.process_time()
 result=m.aggregate_entries(result_expr)
 elapsed=time.perf_counter()-wall;used=time.process_time()-cpu;after=resource.getrusage(resource.RUSAGE_SELF)
 if a.query=='pl_bundle':result={name:result[name] for name in ['some','every','n','fraction']}
 print(json.dumps({'iteration':i,'query':a.query,'result':result,'seconds':elapsed,'cpu_seconds':used,'maxrss_kib':after.ru_maxrss,'minor_faults':after.ru_minflt-usage.ru_minflt,'major_faults':after.ru_majflt-usage.ru_majflt}),flush=True)
