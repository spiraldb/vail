import gzip,json,collections,sys
p=json.load(gzip.open(sys.argv[1]));leaf=collections.Counter();inc=collections.Counter();ancestor=collections.Counter()
for t in p['threads']:
 st=t['stringArray'];ft=t['frameTable'];fu=t['funcTable'];sk=t['stackTable']
 for s,w in zip(t['samples']['stack'],t['samples']['threadCPUDelta']):
  if s is None or w is None or w<=0:continue
  names=[]
  while s is not None:
   names.append(st[fu['name'][ft['func'][sk['frame'][s]]]]);s=sk['prefix'][s]
  leaf[names[0]]+=w;inc.update({n:w for n in set(names)})
  if any('GroupValuesColumn' in n and 'intern' in n for n in names):
   for n in names:
    if 'vail_execution::' in n:ancestor[n]+=w;break
n=sum(leaf.values());print(json.dumps({'total_cpu_delta_us':n,'leaf':[(k,v/n*100)for k,v in leaf.most_common(15)],'lookup_inclusive_pct':sum(v for k,v in inc.items() if k.startswith('<vail_execution::collection_lookup::Lookup as'))/n*100,'grouping_ancestor':[(k,v/n*100)for k,v in ancestor.most_common(5)]},indent=2))
