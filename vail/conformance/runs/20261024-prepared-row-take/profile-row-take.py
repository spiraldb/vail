"""Actual public row-entry take pipeline; source/profile hypothesis only.

Usage: python profile-row-take.py [unordered|ordered|collect] iterations
Profile runs and unprofiled resource runs must be separate. No build is needed.
"""
import hashlib,json,os,resource,sys,time
import vail as h
import vail._native as native
mode=sys.argv[1];iterations=int(sys.argv[2])
m=h.read_matrix_table('/private/tmp/entries-profile25.mt')
if mode=='collect': expression=h.agg.collect(m.DP)
elif mode=='ordered': expression=h.agg.take(m.DP,8,ordering=m.DP)
elif mode=='unordered': expression=h.agg.take(m.DP,8)
else: raise ValueError(mode)
m=m.annotate_rows(sampled=expression);rows=m.rows();total=h.agg.sum(h.sum(rows['sampled']))
print(json.dumps({'phase':'ready','pid':os.getpid(),'mode':mode,'native_sha256':hashlib.sha256(open(native.__file__,'rb').read()).hexdigest(),'dtype':str(rows['sampled'].dtype)}),flush=True)
results=[]
for i in range(iterations):
 before=resource.getrusage(resource.RUSAGE_SELF);t=time.perf_counter();cpu=time.process_time();value=rows.aggregate(total);after=resource.getrusage(resource.RUSAGE_SELF)
 results.append(value);assert all(x==value for x in results)
 print(json.dumps({'iteration':i,'checksum':value,'wall_seconds':time.perf_counter()-t,'cpu_seconds':time.process_time()-cpu,'peakrss_bytes':after.ru_maxrss,'minor_faults':after.ru_minflt-before.ru_minflt,'major_faults':after.ru_majflt-before.ru_majflt}),flush=True)
