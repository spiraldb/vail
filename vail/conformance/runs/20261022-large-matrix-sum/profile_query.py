import resource,time,json
import vail as hl
m=hl.range_matrix_table(n_cols=500000,n_rows=10000,n_partitions=2500)
m=m.annotate_entries(x=m.col_idx+m.row_idx)
q=m.annotate_cols(foo=hl.agg.sum(m.x))
a=resource.getrusage(resource.RUSAGE_SELF);t=time.perf_counter()
print(q._force_count_cols(),flush=True)
b=resource.getrusage(resource.RUSAGE_SELF)
print(json.dumps({'wall':time.perf_counter()-t,'cpu':b.ru_utime+b.ru_stime-a.ru_utime-a.ru_stime,'rss':b.ru_maxrss}),flush=True)
