"""Public mixed numeric row reducers over a previously prepared sparse matrix."""
import argparse, hashlib, json, math, pathlib, resource, time
import vail as h
import vail._native as native
p=argparse.ArgumentParser(); p.add_argument('path'); p.add_argument('--rows',type=int,default=200000); p.add_argument('--cols',type=int,default=2500); p.add_argument('--numeric-only',action='store_true'); a=p.parse_args()
columns=[c for c in range(a.cols) if c%11!=0]
expected=0.0
for r in range(101):
 values=[(r+c)%101 for c in columns if c%13!=0]
 contribution=sum(values)+sum(values)/len(values)+len(columns)
 if not a.numeric_only:
  contribution+=sum(v>20 for v in values)/len(columns)+sum(v>50 for v in values)/len(columns)
 expected+=(a.rows//101+(r<a.rows%101))*contribution
print(json.dumps(dict(native=native.__file__,sha256=hashlib.sha256(pathlib.Path(native.__file__).read_bytes()).hexdigest(),expected=expected)),flush=True)
for i in range(4):
 before=resource.getrusage(resource.RUSAGE_SELF); start=time.perf_counter()
 m=h.read_matrix_table(a.path)
 fields=dict(n=h.agg.count(),s=h.agg.sum(m.value),mean=h.agg.mean(m.value))
 if not a.numeric_only:
  fields.update(f=h.agg.fraction(m.value>20),g=h.agg.fraction(m.value>50))
 r=m.annotate_rows(**fields).rows()
 output=r.n+r.s+r.mean
 if not a.numeric_only:
  output=output+r.f+r.g
 result=r.aggregate(h.agg.sum(output))
 after=resource.getrusage(resource.RUSAGE_SELF)
 assert math.isclose(result,expected,rel_tol=1e-10),(result,expected)
 print(json.dumps(dict(iteration=i,warmup=i==0,wall=time.perf_counter()-start,cpu=after.ru_utime+after.ru_stime-before.ru_utime-before.ru_stime,result=result)),flush=True)
