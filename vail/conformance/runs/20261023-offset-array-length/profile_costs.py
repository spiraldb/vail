import collections,gzip,json
from pathlib import Path
HERE=Path(__file__).resolve().parent
patterns={'numeric':'columnar_numeric','list_length':'general_array_length','lexical_binder':'ScopedSwitch','boolean_cast':'cast_bool_to_numeric','boolean_build':'Vec<bool>','boolean_access':'BooleanArray>::value','local_cse_planning':'local_cse::','structural_classes':'expression_identity::canonical_nodes','packed_counts':'packed_count::','offset_length':'array_length::'}
out={}
for mode in ['before','after']:
 p=json.load(gzip.open(HERE/(mode+'-profile-symbols.json.gz'),'rt'));leaf=collections.Counter();inc=collections.Counter();total=0
 for t in p['threads']:
  for stack,weight in zip(t['samples']['stack'],t['samples']['threadCPUDelta']):
   if stack is None or weight is None or weight<=0:continue
   total+=weight;names=[]
   while stack is not None:
    f=t['frameTable']['func'][t['stackTable']['frame'][stack]];names.append(t['stringArray'][t['funcTable']['name'][f]]);stack=t['stackTable']['prefix'][stack]
   for label,pattern in patterns.items():
    if pattern in names[0]:leaf[label]+=weight
    if any(pattern in name for name in names):inc[label]+=weight
 out[mode]={'sampled_core_seconds':total/1e6,'costs':{label:{'self_core_seconds':leaf[label]/1e6,'inclusive_core_seconds':inc[label]/1e6} for label in patterns}}
print(json.dumps(out,indent=2));json.dump(out,open(HERE/'profile-costs.json','w'),indent=2)
