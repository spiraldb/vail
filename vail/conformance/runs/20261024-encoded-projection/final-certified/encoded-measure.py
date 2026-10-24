import argparse,hashlib,json,pathlib,resource,time,os
import vail as hl
import vail._native as native
p=argparse.ArgumentParser();p.add_argument('--repeat',type=int,default=8);p.add_argument('--pause',type=float,default=0);p.add_argument('--duration',type=float,default=0);p.add_argument('--fixture',default='/private/tmp/vail-laptop-dp-profile/dp500m.mt');a=p.parse_args()
rows,cols=200000,2500
expected=sum((rows//101+(r<rows%101))*sum((r+c)%101 for c in range(cols) if c%11!=0 and c%13!=0) for r in range(101))
m=hl.read_matrix_table(a.fixture)
print(json.dumps({'phase':'provenance','pid':os.getpid(),'native':native.__file__,'sha256':hashlib.sha256(pathlib.Path(native.__file__).read_bytes()).hexdigest(),'schema':str(m.entry.dtype),'rows':rows,'cols':cols,'expected':expected,'fixture':a.fixture}),flush=True)
def query(): return m.aggregate_entries(hl.agg.sum(m.DP))
assert query()==expected
print(json.dumps({'phase':'warmup_complete','time_ns':time.monotonic_ns()}),flush=True)
time.sleep(a.pause)
begin=time.perf_counter();i=0
while i<a.repeat or time.perf_counter()-begin<a.duration:
 b=resource.getrusage(resource.RUSAGE_SELF);t=time.perf_counter();value=query();e=resource.getrusage(resource.RUSAGE_SELF)
 assert value==expected,(value,expected)
 print(json.dumps({'phase':'query','iteration':i,'start_monotonic':t,'wall':time.perf_counter()-t,'cpu':e.ru_utime+e.ru_stime-b.ru_utime-b.ru_stime,'maxrss':e.ru_maxrss,'minor_faults':e.ru_minflt-b.ru_minflt,'major_faults':e.ru_majflt-b.ru_majflt,'result':value}),flush=True);i+=1
