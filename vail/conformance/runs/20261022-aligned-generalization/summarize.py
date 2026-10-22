from pathlib import Path
import json,statistics
p=Path(__file__).resolve().parent
summary={};natives=set();routing={}
for f in sorted(p.glob('general*.jsonl')):
 rs=[json.loads(line) for line in f.read_text().splitlines()];assert rs
 env=rs[0];ms=[r for r in rs if r.get('event')=='measurement' and not r['warmup']];assert len(ms)==3,f
 natives.add(env['native_sha256']);routing.setdefault(env['label'],set()).add(env['routing_sha256'])
 key=f"{env['case']}-{env['rows']}x{env['cols']}-w{env['payload']}"+('-repeated' if '-repeat' in f.name else '')
 v=summary.setdefault(key,{}).setdefault(env['label'],dict(wall=[],cpu=[],rss=[],process_medians=[],expected=env['expected']))
 assert v['expected']==env['expected'];v['wall'].extend(r['wall_seconds'] for r in ms);v['cpu'].extend(r['cpu_seconds'] for r in ms);v['rss'].append(max(r['peak_rss_bytes'] for r in ms));v['process_medians'].append(statistics.median(r['wall_seconds'] for r in ms))
assert len(natives)==1,natives
out={}
for key,variants in summary.items():
 assert len(variants)==2,key
 for v in variants.values():
  v['median_wall']=statistics.median(v.pop('wall'));v['median_cpu']=statistics.median(v.pop('cpu'));v['peak_rss']=max(v.pop('rss'))
 b,c=variants['generalbaseline'],variants['generalbatch'];assert b['expected']==c['expected']
 out[key]=dict(variants=variants,speedup=b['median_wall']/c['median_wall'])
 print(key, f"{b['median_wall']*1000:.2f} -> {c['median_wall']*1000:.2f}ms",f"{out[key]['speedup']:.2f}x",f"{b['peak_rss']/2**20:.0f}->{c['peak_rss']/2**20:.0f}MiB",'processes',len(b['process_medians']))
(p/'summary.json').write_text(json.dumps(dict(native_sha256=next(iter(natives)),routing_sha256={k:list(v) for k,v in routing.items()},cases=out),indent=2)+'\n')
