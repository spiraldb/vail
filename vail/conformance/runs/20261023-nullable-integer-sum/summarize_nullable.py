import pathlib,json,statistics,hashlib
p=pathlib.Path('/private/tmp/vail-laptop-dp-profile')
def rows(f):return [json.loads(l) for l in f.read_text().splitlines()]
def med(q):return {k:statistics.median(x[k] for x in q) for k in ['wall','cpu']}
s={'provenance':json.loads((p/'nullable-native-provenance.json').read_text()),'dp':{},'null_rates':[],'all_valid_controls':[],'slice_resources':[],'references':[],'hardware':json.loads((p/'nullable-counter-summary.json').read_text())}
for mode in ['base','candidate']:
 fs=sorted(p.glob(f'nullable-dp-{mode}-*.jsonl'));q=[x for f in fs for x in rows(f) if x.get('phase')=='query'];s['dp'][mode]=med(q)|{'samples':len(q),'median_process_peak_rss':statistics.median(max(x['maxrss'] for x in rows(f) if x.get('phase')=='query') for f in fs)}
for width in [32,64]:
 for rate in [0,1,10,50,90,100]:
  d={'width':width,'missing_threshold':rate}
  for mode in ['base','candidate']:d[mode]=med([x for x in rows(p/f'nullable-rates-{mode}-{width}-{rate}.jsonl') if not x['warmup']])
  d['speedup']=d['base']['wall']/d['candidate']['wall'];s['null_rates'].append(d)
 d={'width':width}
 for mode in ['base','candidate']:d[mode]=med([x for f in sorted(p.glob(f'nullable-zero-control-{mode}-{width}-*.jsonl')) for x in rows(f) if not x['warmup']])
 s['all_valid_controls'].append(d)
for f in sorted(p.glob('nullable-slice*.jsonl')):
 q=[x for x in rows(f) if not x['warmup']];s['slice_resources'].append({'file':f.name,'backing':q[0]['backing'],'visible':q[0]['visible'],**med(q)})
for name in ['primitive','copy']:
 q=rows(p/f'nullable-{name}-reference.jsonl')
 for width in ([32,64] if name=='primitive' else [32]):
  for workers in [1,14]:
   r=[x for x in q if not x['warmup'] and x['workers']==workers and x.get('width',32)==width];d={'kind':name,'width':width,'workers':workers,**med(r)};d['application_bytes']=r[0].get('logical_bytes',r[0].get('read_write_bytes'));d['GB_s']=d['application_bytes']/d['wall']/1e9;s['references'].append(d)
(p/'nullable-summary.json').write_text(json.dumps(s,indent=2))
manifest={}
for root in [pathlib.Path('/private/tmp/vail-nullable-sum-input'),p/'dp500m.mt']:
 for f in sorted(root.rglob('*')):
  if f.is_file():manifest[str(f)]={'bytes':f.stat().st_size,'sha256':hashlib.sha256(f.read_bytes()).hexdigest()}
(p/'nullable-input-manifest.json').write_text(json.dumps(manifest,indent=2))
