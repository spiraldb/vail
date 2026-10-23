import gzip,json,collections
p=json.load(gzip.open('/tmp/dense-ratio-matrix-profile-symbols.json.gz','rt'));c=collections.Counter();n=0
for t in p['threads']:
 st=t['stackTable'];ft=t['frameTable'];func=t['funcTable'];ss=t['stringArray']
 for stack in t['samples']['stack']:
  if stack is None:continue
  n+=1;names=[]
  while stack is not None:
   names.append(ss[func['name'][ft['func'][st['frame'][stack]]]]);stack=st['prefix'][stack]
  if names and names[0]=='_platform_memmove':c[' <- '.join(names[:8])]+=1
print('all samples',n)
for k,v in c.most_common(12):print(round(v/n*100,3),v,k)
