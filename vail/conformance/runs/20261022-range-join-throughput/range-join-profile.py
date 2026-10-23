import resource,time,json
import vail as hl
n=1000000000
q=hl.utils.range_table(n).join(hl.utils.range_table(n),'inner')
a=resource.getrusage(resource.RUSAGE_SELF); t=time.perf_counter()
print(q.count(),flush=True)
b=resource.getrusage(resource.RUSAGE_SELF)
print(json.dumps(dict(wall=time.perf_counter()-t,cpu=b.ru_utime+b.ru_stime-a.ru_utime-a.ru_stime,rss=b.ru_maxrss)),flush=True)
