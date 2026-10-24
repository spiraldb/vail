"""Stored native integer sums across deterministic mixed logical validities."""
import argparse, pathlib, json, resource, time
import vail as hl
p=argparse.ArgumentParser();p.add_argument('--prepare',action='store_true');p.add_argument('--width',type=int,default=32);p.add_argument('--missing',type=int,default=10);p.add_argument('--rows',type=int,default=100000);p.add_argument('--repeat',type=int,default=6);a=p.parse_args()
root=pathlib.Path('/private/tmp/vail-nullable-sum-input');root.mkdir(exist_ok=True)
path=str(root/f'int{a.width}-null{a.missing}.mt');rows,cols=a.rows,2500
if a.prepare:
 m=hl.utils.range_matrix_table(rows,cols,n_partitions=16)
 k=(m.row_idx*17+m.col_idx*37)%1009
 value=k-504
 if a.width==64:value=hl.int64(value)*2_000_000_000
 m=m.annotate_entries(DP=hl.or_missing((k*43)%100>=a.missing,value))
 k=(m.row_idx*17+m.col_idx*37)%1009
 m=m.filter_entries((k*71)%100>=a.missing//2)
 m.write(path);raise SystemExit
# The data and validity depend only on row/column residues modulo1009.
expected=0
for r in range(1009):
 rc=rows//1009+(r<rows%1009)
 for c in range(1009):
  cc=cols//1009+(c<cols%1009);k=(r*17+c*37)%1009
  if (k*43)%100>=a.missing and (k*71)%100>=a.missing//2:expected+=rc*cc*(k-504)*(2_000_000_000 if a.width==64 else 1)
m=hl.read_matrix_table(path)
for i in range(a.repeat):
 b=resource.getrusage(resource.RUSAGE_SELF);t=time.perf_counter();result=m.aggregate_entries(hl.agg.sum(m.DP));e=resource.getrusage(resource.RUSAGE_SELF)
 assert result==expected,(result,expected)
 print(json.dumps({'width':a.width,'missing':a.missing,'iteration':i,'warmup':i==0,'wall':time.perf_counter()-t,'cpu':e.ru_utime+e.ru_stime-b.ru_utime-b.ru_stime,'rss':e.ru_maxrss,'result':result,'rows':rows,'cols':cols}),flush=True)
