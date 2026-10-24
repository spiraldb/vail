import json, resource, time, math
import vail as h
for mode in ['direct','struct']:
 for size,path in [('small','/private/tmp/native-reduction-cse-input.mt'),('large','/private/tmp/native-reduction-cse-large.mt'),('500m','/private/tmp/native-reduction-cse-500m.mt')]:
  for rep in range(3):
   m=h.read_matrix_table(path)
   fields={f'f{i}':h.agg.fraction(m.value>t) for i,t in enumerate([-5,20,50,95])}
   start=time.perf_counter(); r0=resource.getrusage(resource.RUSAGE_SELF)
   if mode=='direct':
    r=m.annotate_rows(**fields).rows(); result=r.aggregate(h.agg.sum(sum(r[f'f{i}'] for i in range(4))))
   else:
    r=m.annotate_rows(stats=h.struct(**fields)).rows(); result=r.aggregate(h.agg.sum(sum(r.stats[f'f{i}'] for i in range(4))))
   r1=resource.getrusage(resource.RUSAGE_SELF)
   print(json.dumps(dict(mode=mode,size=size,rep=rep,wall=time.perf_counter()-start,cpu=r1.ru_utime+r1.ru_stime-r0.ru_utime-r0.ru_stime,result=result)),flush=True)
for mode in ['direct','struct']:
 m=h.utils.range_matrix_table(2,0)
 fields=dict(f=h.agg.fraction(m.col_idx>0), n=h.agg.count(), mean=h.agg.mean(h.float64(m.col_idx)))
 r=m.annotate_rows(**fields) if mode=='direct' else m.annotate_rows(stats=h.struct(**fields))
 print(mode, r.rows().collect(),flush=True)
