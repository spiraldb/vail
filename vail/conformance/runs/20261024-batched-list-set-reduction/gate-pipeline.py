import hashlib,json,os,resource,sys,time
import vail as h
import vail._native as native
mode=sys.argv[1];iterations=int(sys.argv[2])
if mode=='wide_struct':
 m=h.read_matrix_table('/private/tmp/native-reduction-cse-500m.mt').head(4096)
 value=h.struct(value=m.value, column=m.col_idx)
else:
 m=h.read_matrix_table('/private/tmp/entries-profile25.mt')
 value=h.float64(m.DP) if mode=='float_control' else m.DP
m=m.annotate_rows(row_distinct_values=h.agg.collect_as_set(value));rows=m.rows();expr=h.agg.sum(h.len(rows["row_distinct_values"]))
print(json.dumps({'phase':'ready','pid':os.getpid(),'mode':mode,'native_sha256':hashlib.sha256(open(native.__file__,'rb').read()).hexdigest(),'set_dtype':str(rows["row_distinct_values"].dtype)}),flush=True)
for i in range(iterations):
 b=resource.getrusage(resource.RUSAGE_SELF);t=time.perf_counter();c=time.process_time();result=rows.aggregate(expr);a=resource.getrusage(resource.RUSAGE_SELF)
 print(json.dumps({'iteration':i,'result':result,'wall_seconds':time.perf_counter()-t,'cpu_seconds':time.process_time()-c,'maxrss_bytes':a.ru_maxrss,'minor_faults':a.ru_minflt-b.ru_minflt,'major_faults':a.ru_majflt-b.ru_majflt}),flush=True)
