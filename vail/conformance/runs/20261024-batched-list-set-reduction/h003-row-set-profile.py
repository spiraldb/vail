import hashlib,json,os,resource,sys,time
import vail as h
import vail._native as native
m=h.read_matrix_table('/private/tmp/entries-profile25.mt')
mode=sys.argv[2] if len(sys.argv)>2 else "native"
value=h.agg.collect_as_set(m.DP) if mode=="native" else h.set(h.agg.collect(m.DP))
m=m.annotate_rows(distinct=value)
rows=m.rows()
expr=h.agg.sum(h.len(rows["distinct"]))
print(json.dumps({'phase':'ready','pid':os.getpid(),'native_sha256':hashlib.sha256(open(native.__file__,'rb').read()).hexdigest(),'query':'native_matrix_DP_row_set_then_sum_set_lengths','mode':mode,'dtype':str(rows["distinct"].dtype)}),flush=True)
for i in range(int(sys.argv[1]) if len(sys.argv)>1 else 4):
 before=resource.getrusage(resource.RUSAGE_SELF);t=time.perf_counter();c=time.process_time();result=rows.aggregate(expr);after=resource.getrusage(resource.RUSAGE_SELF)
 print(json.dumps({'iteration':i,'result':result,'wall_seconds':time.perf_counter()-t,'cpu_seconds':time.process_time()-c,'maxrss_bytes':after.ru_maxrss,'minor_faults':after.ru_minflt-before.ru_minflt,'major_faults':after.ru_majflt-before.ru_majflt}),flush=True)
