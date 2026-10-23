import vail as hl,time,resource,json,sys,os
path='/tmp/grouped-collect-followup-20k.mt'
if not os.path.exists(path):
 m=hl.range_matrix_table(n_rows=20000,n_cols=1000,n_partitions=32)
 m=m.annotate_entries(x=(m.row_idx+m.col_idx)%91)
 m.write(path)
t=hl.read_matrix_table(path).localize_entries('e','c')
q=t.group_by(*t.key).aggregate(value=hl.agg.collect(t.row_value))
print(q._native.physical_plan(),flush=True)
a=resource.getrusage(resource.RUSAGE_SELF); st=time.perf_counter()
print(q._force_count(),flush=True)
b=resource.getrusage(resource.RUSAGE_SELF)
print(json.dumps(dict(wall=time.perf_counter()-st,cpu=b.ru_utime+b.ru_stime-a.ru_utime-a.ru_stime,rss=b.ru_maxrss)),flush=True)
