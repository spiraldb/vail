from pathlib import Path
from collections import defaultdict
import runpy,contextlib,io,json
r=Path(__file__).parent
with contextlib.redirect_stdout(io.StringIO()):a=runpy.run_path(str(r/'analyze.py'))
d=a['d'];cut=a['cut'];end=a['end'];symbol=a['symbol'];totals=defaultdict(float);counts=defaultdict(int);examples={}
for t in d['threads']:
 s=t['samples'];st=t['stackTable'];cache={}
 for i,ts in enumerate(s['time']):
  if i==0 or s['time'][i-1]<cut or ts>end or ts<cut:continue
  w=s['threadCPUDelta'][i];idx=s['stack'][i]
  if w is None or w<=0 or idx is None:continue
  if idx not in cache:
   names=[];n=idx
   while n is not None:names.append(symbol(t,st['frame'][n]));n=st['prefix'][n]
   cache[idx]=names
  names=cache[idx];text='\n'.join(names)
  tests={'arrow_interleave':'interleave' in text,'partial_aggregate_state_gather':'interleave' in text and ('PartialHashAggregateStream' in text or 'grouped_hash::'in text or 'partial::'in text),'convert_to_state':'convert_to_state'in text,'collect_state_exec':'collect_state' in text,'final_array_agg':'array_agg' in text}
  for key,yes in tests.items():
   if yes:
    totals[key]+=w/1e6;counts[key]+=1
    if key not in examples:examples[key]=names
result={'window_cpu':a['total'],'paths':{k:{'cpu_seconds':totals[k],'positive_cpu_samples':counts[k],'percent':100*totals[k]/a['total']}for k in tests},'examples':examples,'scope':'Approximate sampled stack attribution; absent samples are not proof of absence. Source/actual plan establish no partial gather call; final/interpartition gathers can remain.'}
(r/'profile-paths.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result['paths'],indent=2))
