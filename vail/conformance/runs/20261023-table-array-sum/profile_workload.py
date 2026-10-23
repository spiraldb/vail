import vail as hl
import time,resource,json
for rep in range(3):
 t=hl.utils.range_table(10_000_000)
 b=resource.getrusage(resource.RUSAGE_SELF);s=time.perf_counter()
 x=t.aggregate(hl.agg.array_sum(hl.range(0,100)))
 a=resource.getrusage(resource.RUSAGE_SELF)
 assert list(x)==[10_000_000*i for i in range(100)]
 print(json.dumps(dict(rep=rep,wall=time.perf_counter()-s,core=a.ru_utime+a.ru_stime-b.ru_utime-b.ru_stime)),flush=True)
