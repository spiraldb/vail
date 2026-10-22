import vail as hl,sys,time,resource,json
m=hl.range_matrix_table(20000,1000,32)
m=m.annotate_entries(x=(m.row_idx+m.col_idx)%91)
m=m.annotate_rows(c=hl.agg.counter(hl.min(100,m.x)))
a=resource.getrusage(resource.RUSAGE_SELF);st=time.perf_counter();m.rows()._force_count();b=resource.getrusage(resource.RUSAGE_SELF)
print(json.dumps({'wall':time.perf_counter()-st,'cpu':b.ru_utime+b.ru_stime-a.ru_utime-a.ru_stime,'rss':b.ru_maxrss}),flush=True)
