import gzip,json,bisect,collections
root='/tmp/bgen-order-certificate-20261023/'
p=json.load(gzip.open(root+'profile.json.gz','rt')); sy=json.load(open(root+'profile.json.syms.json')); by={d['debug_name']:d for d in sy['data']}
for t in p['threads']:
 ft=t['frameTable'];fu=t['funcTable'];rt=t['resourceTable'];ss=t['stringArray']
 for i,a in enumerate(ft['address']):
  f=ft['func'][i]; r=fu['resource'][f]
  if r is None or r<0:continue
  lib=p['libs'][rt['lib'][r]];d=by.get(lib['debugName'])
  if not d:continue
  tab=d['symbol_table']; rs=[x['rva'] for x in tab];j=bisect.bisect_right(rs,a)-1
  if j<0:continue
  n=sy['string_table'][tab[j]['symbol']];fu['name'][f]=len(ss);ss.append(n)
json.dump(p,gzip.open(root+'symbolicated.json.gz','wt'))
selfc=collections.Counter();inc=collections.Counter();paths=collections.Counter();threads=collections.Counter();total=0
for t in p['threads']:
 ft=t['frameTable'];fu=t['funcTable'];st=t['stackTable'];ss=t['stringArray'];cache={None:()}
 def path(i):
  if i in cache:return cache[i]
  chain=[];n=i
  while n not in cache:chain.append(n);n=st['prefix'][n]
  v=cache[n]
  for n in reversed(chain):v=v+(ss[fu['name'][ft['func'][st['frame'][n]]]],);cache[n]=v
  return v
 for i,s in enumerate(t['samples']['stack']):
  if s is None:continue
  w=(t['samples']['threadCPUDelta'][i] or 0)/1e6
  if w<=0:continue
  a=path(s);total+=w;threads[t['name']]+=w;selfc[a[-1]]+=w;paths[a]+=w
  for n in set(a):inc[n]+=w
print('TOTALCPU',total)
for label,c in [('SELF',selfc),('INCLUSIVE',inc),('THREADS',threads)]:
 print(label)
 for n,w in c.most_common(35):print(f'{100*w/total:.2f}% {w:.3f} {n}')
json.dump({'total':total,'self':selfc,'inclusive':inc},open(root+'hotspots.json','w'),indent=2)
with open(root+'stacks.folded','w') as f:
 for a,w in paths.items():f.write(';'.join(a)+' '+str(w)+'\n')
