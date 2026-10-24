"""Public native-matrix coordinate reductions over actual AD entry vectors."""
import argparse,json,os,resource,time,pathlib
import vail as h
p=argparse.ArgumentParser();p.add_argument('--query',choices=['single','bundle','missing','row_single','row_bundle'],default='single');p.add_argument('--repeat',type=int,default=4)
p.add_argument('--path',default='/home/danielking/cloud-scan-20261023/data/vail-from-hail/profile25_mt.mt');p.add_argument('--biallelic',action='store_true');a=p.parse_args()
m=h.read_matrix_table(a.path)
if a.biallelic:m=m.filter_rows(m.alleles.length()==2)
if a.query=='row_single':q=h.agg.array_agg(lambda x:h.agg.sum(x),m.AD)
elif a.query=='row_bundle':q=h.agg.array_agg(lambda x:h.struct(s=h.agg.sum(x),n=h.agg.count_where(x>10),lo=h.agg.min(x)),m.AD)
elif a.query=='missing':q=h.agg.count_where(h.is_missing(m.AD))
elif a.query=='single':q=h.agg.array_agg(lambda x:h.agg.sum(x),m.AD)
else:q=h.agg.array_agg(lambda x:h.struct(s=h.agg.sum(x),n=h.agg.count_where(x>10),lo=h.agg.min(x)),m.AD)
print(json.dumps({'phase':'ready','pid':os.getpid(),'dtype':str(m.AD.dtype),'query':a.query}),flush=True)
for i in range(a.repeat):
 before=resource.getrusage(resource.RUSAGE_SELF);w=time.perf_counter();c=time.process_time()
 if a.query.startswith('row_'):
  rows=m.annotate_rows(summary=q)
  if a.query=='row_single':result=rows.aggregate_rows(h.agg.sum(h.sum(rows.summary)))
  else:result=rows.aggregate_rows(h.struct(s=h.agg.sum(h.sum(rows.summary.map(lambda x:x.s))),n=h.agg.sum(h.sum(rows.summary.map(lambda x:x.n))),lo=h.agg.sum(h.sum(rows.summary.map(lambda x:x.lo)))))
 else:result=m.aggregate_entries(q)
 after=resource.getrusage(resource.RUSAGE_SELF)
 print(json.dumps({'iteration':i,'seconds':time.perf_counter()-w,'cpu_seconds':time.process_time()-c,'maxrss_kib':after.ru_maxrss,'minor_faults':after.ru_minflt-before.ru_minflt,'result':dict(result) if a.query=='row_bundle' else [dict(x) for x in result] if a.query=='bundle' else result}),flush=True)
