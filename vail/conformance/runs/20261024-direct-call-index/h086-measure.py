import argparse, json, os, resource, time
import vail as h
p=argparse.ArgumentParser();p.add_argument('--query', choices=['first','last','dynamic','row'], default='first');p.add_argument('--repeat',type=int,default=6);p.add_argument('--path',default='/home/danielking/parallel-vcf-export/data/profile-native.mt');a=p.parse_args()
m=h.read_matrix_table(a.path)
if a.query=='first': value=m.GT[0]
elif a.query=='last': value=m.GT[-1]
elif a.query=='dynamic': value=m.GT[m.GT.n_alt_alleles() % 2]
else:
 r=m.annotate_rows(h086_sum=h.agg.sum(m.GT[0])).rows()
print(json.dumps({'pid':os.getpid(),'phase':'ready','query':a.query,'entry_schema':str(m.entry.dtype)}),flush=True)
for i in range(a.repeat):
 wall=time.perf_counter();cpu=time.process_time()
 result=r.aggregate(h.agg.sum(r.h086_sum)) if a.query=='row' else m.aggregate_entries(h.agg.sum(value))
 print(json.dumps({'iteration':i,'query':a.query,'result':result,'seconds':time.perf_counter()-wall,'cpu_seconds':time.process_time()-cpu,'maxrss_kib':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}),flush=True)
