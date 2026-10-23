import sys,resource,time,json
import vail as hl
rows,cols,partitions=map(int,sys.argv[1:])
m=hl.range_matrix_table(n_rows=rows,n_cols=cols,n_partitions=partitions)
m=m.annotate_entries(x=m.col_idx+m.row_idx)
q=m.annotate_cols(foo=hl.agg.sum(m.x))
a=resource.getrusage(resource.RUSAGE_SELF);t=time.perf_counter()
n=q._force_count_cols()
b=resource.getrusage(resource.RUSAGE_SELF)
print(json.dumps(dict(rows=rows,cols=cols,partitions=partitions,result=n,wall=time.perf_counter()-t,core=b.ru_utime+b.ru_stime-a.ru_utime-a.ru_stime,rss=b.ru_maxrss)),flush=True)
