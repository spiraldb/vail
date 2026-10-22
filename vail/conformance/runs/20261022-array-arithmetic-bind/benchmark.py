import json, resource, sys, time
import vail as hl
m=hl.read_matrix_table('/tmp/entries-profile25.mt')
m=m.filter_rows(m.alleles.length()==2)
m=m.select_entries(dosage=hl.pl_dosage(m.PL)).select_rows()
# Warm only source readers and code, outside query interval.
m._force_count_rows()
a=resource.getrusage(resource.RUSAGE_SELF); start=time.perf_counter()
n=m._force_count_rows()
b=resource.getrusage(resource.RUSAGE_SELF)
print(json.dumps({'wall_seconds':time.perf_counter()-start,'core_seconds':b.ru_utime+b.ru_stime-a.ru_utime-a.ru_stime,'process_peak_rss_bytes':b.ru_maxrss,'rows':n,'python_module':hl.__file__}),flush=True)
