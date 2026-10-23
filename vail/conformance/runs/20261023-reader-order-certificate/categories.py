import gzip,json,collections
p=json.load(gzip.open('/tmp/bgen-order-certificate-20261023/symbolicated.json.gz','rt'));inc=collections.Counter();groups=collections.Counter();tot=0
for t in p['threads']:
 st=t['stackTable'];ft=t['frameTable'];fu=t['funcTable'];ss=t['stringArray'];cache={None:()}
 def path(s):
  if s not in cache:cache[s]=path(st['prefix'][s])+(ss[fu['name'][ft['func'][st['frame'][s]]]],)
  return cache[s]
 for s in t['samples']['stack']:
  if s is None:continue
  a=path(s)
  if any(x in a[-1] for x in ['wait','mach_msg','park','kevent','__semwait','read_nocancel']):continue
  tot+=1
  for n in set(a):inc[n]+=1
  joined=' '.join(a)
  if 'vail_table::bgen_source::scan' in joined:g='BGEN read/decode/build'
  elif 'logical_kernel' in joined:g='semantic expression kernels'
  elif 'sorts::' in joined:g='sort pipeline'
  else:g='other runtime/operator'
  groups[g]+=1
print('active samples',tot,groups)
for pat in ['array_transform::ArrayTransform','array_filter::ArrayFilter','array_index_rows','general_array_length','array_fold','array_reduce','logical_kernel','bgen_source::scan','sorts::sort::SortExec']:
 vals=[(n,v)for n,v in inc.items()if pat in n];print(pat,[(round(v/tot*100,2),n[:200])for n,v in sorted(vals,key=lambda x:-x[1])[:2]])
