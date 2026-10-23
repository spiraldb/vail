import json,gzip,bisect,collections
p=json.load(gzip.open(__import__('sys').argv[1]));s=json.load(open(__import__('sys').argv[2])); maps={x['debug_id'].replace('-','').lower():x for x in s['data']}
libs=[]
for l in p['libs']:
 m=maps.get(l['breakpadId'].replace('-','').lower())
 if not m:
  m=next((x for x in s['data'] if x['debug_name']==l['debugName']),None)
 libs.append(m)
selfc=collections.Counter();inc=collections.Counter();total=0
for t in p['threads']:
 names=[]
 for i,f in enumerate(t['frameTable']['func']):
  name=t['stringArray'][t['funcTable']['name'][f]]
  res=t['funcTable']['resource'][f]
  if res>=0:
   lib=t['resourceTable']['lib'][res]
   if lib is not None and lib>=0 and libs[lib]:
    tab=libs[lib]['symbol_table']; addr=t['frameTable']['address'][i]; idx=bisect.bisect_right([a['rva'] for a in tab],addr)-1
    if idx>=0:name=s['string_table'][tab[idx]['symbol']]
  names.append(name)
 for st,w in zip(t['samples']['stack'],t['samples']['threadCPUDelta']):
  if st is None:continue
  w=w or 0;chain=[]
  while st is not None:
   chain.append(names[t['stackTable']['frame'][st]]);st=t['stackTable']['prefix'][st]
  if not any('array_aggregate::' in n and ('DenseSum' in n or 'DenseNumeric' in n) for n in chain):continue
  total+=w
  selfc[chain[0]]+=w
  for n in set(chain):inc[n]+=w
print('total',total)
for title,c in [('SELF',selfc),('INCLUSIVE',inc)]:
 print(title)
 for n,v in c.most_common(35):print(round(100*v/total,2),n)
