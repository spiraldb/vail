"""Representative total-aggregation classes, shared by both normal runtimes."""
import argparse,json,os,resource,time,pathlib
import vail as h
p=argparse.ArgumentParser()
p.add_argument('--query',choices=['bare','two_sums','sums_count','computed'],default='sums_count')
p.add_argument('--repeat',type=int,default=6)
p.add_argument('--path',default='/home/danielking/cloud-scan-20261023/data/vail-from-hail/profile25_mt.mt')
a=p.parse_args();m=h.read_matrix_table(a.path)
if a.query=='bare':query=h.agg.sum(m.DP)
elif a.query=='two_sums':query=h.struct(dp=h.agg.sum(m.DP),gq=h.agg.sum(m.GQ))
elif a.query=='sums_count':query=h.struct(dp=h.agg.sum(m.DP),gq=h.agg.sum(m.GQ),n=h.agg.count())
else:query=h.struct(score=h.agg.sum(m.DP+m.GQ),dp=h.agg.sum(m.DP),n=h.agg.count())
print(json.dumps({'pid':os.getpid(),'query':a.query,'phase':'ready'}),flush=True)
def io_counts():
 return {key:int(value) for key,value in (line.split(':') for line in pathlib.Path('/proc/self/io').read_text().splitlines())}
for i in range(a.repeat):
 io_before=io_counts()
 before=resource.getrusage(resource.RUSAGE_SELF);w=time.perf_counter();c=time.process_time()
 result=m.aggregate_entries(query)
 elapsed=time.perf_counter()-w;used=time.process_time()-c
 after=resource.getrusage(resource.RUSAGE_SELF);io_after=io_counts()
 print(json.dumps({'iteration':i,'query':a.query,'result':result if a.query=='bare' else dict(result),'seconds':elapsed,'cpu_seconds':used,'maxrss_kib':after.ru_maxrss,'minor_faults':after.ru_minflt-before.ru_minflt,'major_faults':after.ru_majflt-before.ru_majflt,'rchar_bytes':io_after['rchar']-io_before['rchar'],'disk_read_bytes':io_after['read_bytes']-io_before['read_bytes']}),flush=True)
