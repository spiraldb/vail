import gzip,json,urllib.request,sys,collections
path,server=sys.argv[1:]
p=json.load(gzip.open(path,'rt'));libs=p['libs'];pairs=set()
for t in p['threads']:
 for i,a in enumerate(t['frameTable']['address']):
  f=t['frameTable']['func'][i]
  if f is None:continue
  r=t['funcTable']['resource'][f]
  if r is not None and r>=0:
   lib=t['resourceTable']['lib'][r]
   if lib is not None and lib>=0 and a>=0:pairs.add((lib,a))
pairs=sorted(pairs)
req={'jobs':[{'memoryMap':[[l['debugName'],l['breakpadId']]for l in libs],'stacks':[[list(x)]for x in pairs]}]}
r=json.load(urllib.request.urlopen(urllib.request.Request(server+'/symbolicate/v5',json.dumps(req).encode(),{'Content-Type':'application/json'})))
lookup={pair:stack[0]['function'] for pair,stack in zip(pairs,r['results'][0]['stacks']) if stack and 'function'in stack[0]}
for t in p['threads']:
 for i,a in enumerate(t['frameTable']['address']):
  f=t['frameTable']['func'][i]
  if f is None:continue
  r=t['funcTable']['resource'][f]
  if r is not None and r>=0:
   lib=t['resourceTable']['lib'][r];name=lookup.get((lib,a))
   if name:t['stringArray'][t['funcTable']['name'][f]]=name
out=path.replace('.json.gz','-symbols.json.gz');json.dump(p,gzip.open(out,'wt'))
leaf=collections.Counter();inclusive=collections.Counter()
for t in p['threads']:
 ft,fu,st,sk=t['frameTable'],t['funcTable'],t['stringArray'],t['stackTable']
 for s,w in zip(t['samples']['stack'],t['samples']['threadCPUDelta']):
  if s is None or w is None or w<=0:continue
  names=[]
  while s is not None:
   names.append(st[fu['name'][ft['func'][sk['frame'][s]]]]);s=sk['prefix'][s]
  leaf[names[0]]+=w;inclusive.update({n:w for n in set(names)})
total=sum(leaf.values());summary={'total_cpu_us':total,'leaf':[(k,v,v/total*100)for k,v in leaf.most_common(35)],'inclusive':[(k,v,v/total*100)for k,v in inclusive.most_common(80)]}
json.dump(summary,open(path.replace('.json.gz','-weighted.json'),'w'),indent=2)
print(out)
