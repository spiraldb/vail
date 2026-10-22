"""Public-API matched-native comparison; isolated Python routing prototypes."""
import argparse, hashlib, json, platform, resource, time
from pathlib import Path
p=argparse.ArgumentParser()
p.add_argument('--label',required=True);p.add_argument('--case',required=True)
p.add_argument('--rows',type=int,required=True);p.add_argument('--cols',type=int,required=True)
p.add_argument('--payload',type=int,default=0);p.add_argument('--prepare',action='store_true')
a=p.parse_args()
import vail as hl
import vail._native as native
import vail.matrixtable as module
fixture=Path(f'/tmp/vail-aligned-generalization-{a.rows}-{a.cols}-{a.payload}.mt')
if a.prepare:
 if not fixture.exists():
  m=hl.range_matrix_table(a.rows,a.cols,4)
  m=m.annotate_rows(metadata=hl.struct(offset=m.row_idx*3,label='row:'+hl.str(m.row_idx),**{f'f{i}':m.row_idx+i for i in range(14)}))
  m=m.annotate_cols(coeff=m.col_idx%7,payload=hl.str(m.col_idx)+'x'*a.payload)
  m=m.annotate_globals(shift=17)
  m=m.annotate_entries(x=hl.int64(m.row_idx)*a.cols+m.col_idx)
  m=m.filter_entries((m.row_idx+m.col_idx)%11!=0)
  m.write(str(fixture))
 print(json.dumps(dict(fixture=str(fixture),prepared=True)));raise SystemExit
m=hl.read_matrix_table(str(fixture))
start=time.perf_counter()
case=a.case
if case=='annotate':out=m.annotate_entries(y=m.x*3+7)
elif case=='select':out=m.select_entries(y=m.x*3+7)
elif case=='transmute':out=m.transmute_entries(y=m.x*3+7)
elif case=='strings':out=m.select_entries(y=m.x+1,tag='v:'+hl.str(m.x))
elif case=='nested':out=m.select_entries(y=hl.range(4).map(lambda i:m.x+i))
elif case=='filter_entry':out=m.filter_entries(m.x%3==0)
elif case=='filter_row':out=m.filter_entries((m.x+m.metadata.offset+m.shift)%5==0)
elif case=='filter_row_only':out=m.filter_entries((m.x+m.metadata.offset)%5==0)
elif case=='filter_global':out=m.filter_entries((m.x+m.shift)%5==0)
elif case=='filter_col':out=m.filter_entries((m.x+m.coeff)%3==0)
elif case=='filter_missing':out=m.filter_entries(hl.or_missing(m.x%5!=0,m.x%3==0),keep=False)
elif case=='control_row':out=m.select_entries(y=m.x+m.metadata.offset+m.shift)
else:raise ValueError(case)
if case.startswith('filter'): value=out.x
elif case=='nested':value=hl.sum(out.y)
else:value=out.y
expression=hl.struct(n=hl.agg.count(),checksum=hl.agg.sum(value))
if case=='strings':expression=expression.annotate(tag_length=hl.agg.sum(hl.len(out.tag)))
plan=time.perf_counter()-start
expected=dict(n=0,checksum=0)
if case=='strings':expected['tag_length']=0
for r in range(a.rows):
 for c in range(a.cols):
  x=r*a.cols+c
  if (r+c)%11==0:continue
  if case=='filter_entry' and x%3!=0:continue
  if case=='filter_row' and (x+r*3+17)%5!=0:continue
  if case=='filter_row_only' and (x+r*3)%5!=0:continue
  if case=='filter_global' and (x+17)%5!=0:continue
  if case=='filter_col' and (x+c%7)%3!=0:continue
  if case=='filter_missing' and (x%5==0 or x%3==0):continue
  expected['n']+=1
  expected['checksum']+=x if case.startswith('filter') else (4*x+6 if case=='nested' else x+r*3+17 if case=='control_row' else x+1 if case=='strings' else x*3+7)
  if case=='strings':expected['tag_length']+=len('v:'+str(x))
print(json.dumps(dict(event='environment',label=a.label,case=case,rows=a.rows,cols=a.cols,payload=a.payload,fixture=str(fixture),native_sha256=hashlib.sha256(Path(native.__file__).read_bytes()).hexdigest(),routing_sha256=hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest(),plan_seconds=plan,expected=expected)),flush=True)
for i in range(-1,3):
 wall=time.perf_counter();cpu=time.process_time()
 result=out.aggregate_entries(expression)
 elapsed=time.perf_counter()-wall;used=time.process_time()-cpu
 actual=dict(result.items());assert actual==expected,(actual,expected)
 rss=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
 print(json.dumps(dict(event='measurement',iteration=i,warmup=i<0,wall_seconds=elapsed,cpu_seconds=used,peak_rss_bytes=rss if platform.system()=='Darwin' else rss*1024,result=actual,correctness='pass')),flush=True)
