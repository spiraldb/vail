import argparse,json,os,resource,time
import vail as h
p=argparse.ArgumentParser();p.add_argument('--query',choices=['ad_sum','ad_bundle','pl_count'],default='ad_sum');p.add_argument('--repeat',type=int,default=6);p.add_argument('--path',default='/home/danielking/cloud-scan-20261023/data/vail-from-hail/profile25_mt.mt');a=p.parse_args()
m=h.read_matrix_table(a.path)
if a.query=='ad_sum':
 value=m.AD.aggregate(lambda x:h.agg.filter(x>0,h.agg.sum(x)))
elif a.query=='ad_bundle':
 score=m.AD.aggregate(lambda x:h.agg.filter(x>3,h.struct(total=h.agg.sum(x),n=h.agg.count())))
 value=score.total+score.n
else:
 value=m.PL.aggregate(lambda x:h.agg.filter(x<50,h.agg.count()))
print(json.dumps({'pid':os.getpid(),'phase':'ready','query':a.query,'entry_schema':str(m.entry.dtype)}),flush=True)
for i in range(a.repeat):
 usage=resource.getrusage(resource.RUSAGE_SELF);wall=time.perf_counter();cpu=time.process_time()
 result=m.aggregate_entries(h.agg.sum(value))
 elapsed=time.perf_counter()-wall;used=time.process_time()-cpu;after=resource.getrusage(resource.RUSAGE_SELF)
 print(json.dumps({'iteration':i,'query':a.query,'result':result,'seconds':elapsed,'cpu_seconds':used,'maxrss_kib':after.ru_maxrss,'minor_faults':after.ru_minflt-usage.ru_minflt,'major_faults':after.ru_majflt-usage.ru_majflt}),flush=True)
