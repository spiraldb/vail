from pathlib import Path
from collections import Counter
from bisect import bisect_right
import gzip,hashlib,json
r=Path(__file__).parent;root=r;profile=root/'collect-profile.json.gz';d=json.loads(gzip.decompress(profile.read_bytes()));syms=json.loads((root/'collect-profile.json.syms.json').read_text());worker=json.loads((r/'profile-worker-result.json').read_text());cut=worker['warm_epoch']*1000-d['meta']['startTime'];end=worker['end_epoch']*1000-d['meta']['startTime']
tables={}
for lib in syms['data']:
 tables[(lib['debug_name'],lib['debug_id'].replace('-','').upper())]=(lib['symbol_table'],[x['rva'] for x in lib['symbol_table']])
cache={}
def symbol(t,frame):
 a=t['frameTable'];func=a['func'][frame];res=t['funcTable']['resource'][func];raw=t['stringArray'][t['funcTable']['name'][func]]
 if res is None or res<0:return raw
 libidx=t['resourceTable']['lib'][res]
 if libidx is None or libidx<0:return raw
 lib=d['libs'][libidx];addr=a['address'][frame];key=(libidx,addr)
 if key in cache:return cache[key]
 tab=tables.get((lib['debugName'],lib['breakpadId'][:32]))
 name=lib['debugName']+'!'+raw
 if tab and addr is not None:
  symbols,rv=tab;i=bisect_right(rv,addr)-1
  if i>=0 and (not symbols[i]['size'] or addr<symbols[i]['rva']+symbols[i]['size']):name=syms['string_table'][symbols[i]['symbol']]
 cache[key]=name;return name
self_cpu=Counter();inclusive=Counter();threads=[];total=0;samples=0
for tid,t in enumerate(d['threads']):
 st=t['stackTable'];s=t['samples'];stackcache={}
 def names(idx):
  if idx in stackcache:return stackcache[idx]
  seq=[];n=idx
  while n is not None:
   seq.append(symbol(t,st['frame'][n]));n=st['prefix'][n]
  stackcache[idx]=seq;return seq
 cpu=0;n=0
 for i,ts in enumerate(s['time']):
  if i==0 or s['time'][i-1]<cut or ts>end or ts<cut:continue
  weight=s['threadCPUDelta'][i]
  if weight is None or weight<=0 or s['stack'][i] is None:continue
  frames=names(s['stack'][i]);seconds=weight/1e6;self_cpu[frames[0]]+=seconds
  for name in set(frames):inclusive[name]+=seconds
  cpu+=seconds;total+=seconds;n+=1;samples+=1
 if cpu:threads.append({'index':tid,'name':t['name'],'tid':t['tid'],'observed_cpu_seconds':cpu,'samples':n})
threads.sort(key=lambda t:t['observed_cpu_seconds'],reverse=True)
def rows(c):return [{'function':name,'cpu_seconds':seconds,'percent':100*seconds/total} for name,seconds in c.most_common(45)]
result={'profile':str(profile),'sha256':hashlib.sha256(profile.read_bytes()).hexdigest(),'symbol_sidecar_sha256':hashlib.sha256((root/'collect-profile.json.syms.json').read_bytes()).hexdigest(),'warm_cutoff_ms':cut,'end_ms':end,'window_observed_thread_cpu_seconds':total,'window_worker_loop_cpu_seconds':worker['loop_cpu'],'positive_cpu_samples':samples,'all_threads':len(d['threads']),'top_self':rows(self_cpu),'top_inclusive':rows(inclusive),'threads':threads,'method':'Use profile-recorded threadCPUDelta microseconds, not raw sample counts. Exclude intervals crossing warm marker/end and each thread first sample. Attribute interval CPU approximately to sampled endpoint stack; inline attribution not reconstructed. Inclusive rows overlap and must not be added.'}
(r/'analysis.json').write_text(json.dumps(result,indent=2)+'\n')
print('OBSERVED CPU',round(total,3),'worker',worker['loop_cpu'],'threads',len(threads),'warmms',round(cut,2));print('SELF')
for x in result['top_self'][:22]:print(round(x['percent'],2),x['function'])
print('INCLUSIVE')
for x in result['top_inclusive'][:24]:print(round(x['percent'],2),x['function'])
print('THREADS',threads[:4])
