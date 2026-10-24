import hashlib,json,os,resource,sys,time
import vail as h
import vail._native as native
m=h.read_matrix_table('/private/tmp/entries-profile25.mt')
expr=h.agg.corr(m.DP,m.GQ)
print(json.dumps({'phase':'ready','pid':os.getpid(),'native_path':native.__file__,'native_sha256':hashlib.sha256(open(native.__file__,'rb').read()).hexdigest(),'schema':str(m.entry.dtype)}),flush=True)
for i in range(int(sys.argv[1]) if len(sys.argv)>1 else 8):
 before=resource.getrusage(resource.RUSAGE_SELF);wall=time.perf_counter();cpu=time.process_time();value=m.aggregate_entries(expr);elapsed=time.perf_counter()-wall;used=time.process_time()-cpu;after=resource.getrusage(resource.RUSAGE_SELF)
 print(json.dumps({'iteration':i,'result':value,'seconds':elapsed,'cpu_seconds':used,'maxrss_bytes':after.ru_maxrss,'minor_faults':after.ru_minflt-before.ru_minflt,'major_faults':after.ru_majflt-before.ru_majflt}),flush=True)
