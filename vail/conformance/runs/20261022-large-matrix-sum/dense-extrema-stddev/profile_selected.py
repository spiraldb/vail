import gzip,json,sys,collections
patterns=['coordinate_extrema','coordinate_stddev','DenseNumeric','CoordinateBundle','group_values','GroupsAccumulator','unnest_list_array','coordinate_indices','take_native','memmove']
for path in sys.argv[1:]:
 p=json.load(gzip.open(path,'rt'));counts=collections.Counter();n=0
 for t in p['threads']:
  st=t['stackTable'];ft=t['frameTable'];fu=t['funcTable'];ss=t['stringArray']
  for stack in t['samples']['stack']:
   if stack is None:continue
   n+=1;names=[]
   while stack is not None:
    f=st['frame'][stack];names.append(ss[fu['name'][ft['func'][f]]]);stack=st['prefix'][stack]
   for pattern in patterns:
    if any(pattern in name for name in names):counts[pattern]+=1
 print(path,'all_samples',n)
 for pat in patterns:print(pat,counts[pat],round(counts[pat]/n*100,3))
