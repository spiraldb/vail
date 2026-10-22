import vail as hl, time, json, resource
m=hl.range_matrix_table(20000,1000,32)
m=m.annotate_entries(x=(m.row_idx+m.col_idx)%91)
m=m.annotate_rows(mean=hl.agg.mean(m.x),count_array=hl.rbind(hl.agg.counter(hl.min(100,m.x)),lambda c:hl.range(0,100).map(lambda i:c.get(i,0))))
t=m.rows()
t=t.annotate(median=hl.rbind(hl.sum(t.count_array)/2,lambda s:hl.find(lambda x:x>s,hl.array_scan(lambda i,j:i+j,0,t.count_array))), **{f'above_{x}':hl.sum(t.count_array[x:]) for x in [1,5,10,15,20,25,30,50,100]})
a=resource.getrusage(resource.RUSAGE_SELF); st=time.perf_counter(); print(t._force_count(),flush=True); b=resource.getrusage(resource.RUSAGE_SELF); print(json.dumps({'wall':time.perf_counter()-st,'cpu':b.ru_utime+b.ru_stime-a.ru_utime-a.ru_stime,'rss':b.ru_maxrss}),flush=True)
