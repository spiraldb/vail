import contextlib,io,json
from collections import Counter
with contextlib.redirect_stdout(io.StringIO()): import analyze as a
roles=json.loads((a.r/'threads.json').read_text())['threads'];cpu=Counter();bulk=Counter();records=[]
for t in roles:
 th=a.d['threads'][t['index']];names={a.symbol(th,f) for f in range(th['frameTable']['length'])};active=any('legacy::decode::PartReader' in n or 'legacy::decode::BatchColumn' in n for n in names)
 row=next((x for x in a.result['threads'] if x['index']==t['index']),None)
 if row:cpu[t['role_from_sampled_stacks']]+=row['observed_cpu_seconds']
 if active:bulk[t['role_from_sampled_stacks']]+=1
 records.append(dict(t,has_bulk_row_decode=active))
out={'cpu_seconds_by_role':dict(cpu),'bulk_decode_threads_by_role':dict(bulk),'total_profile_threads':len(roles),'threads':records,'scope':'Threadroles inferredfrom sampledstackroots; CPUtimeweights summedperthread overwarmwindow. Notperfunctionendpointcausality.'};(a.r/'profile-roles.json').write_text(json.dumps(out,indent=2)+'\n');print(json.dumps({k:v for k,v in out.items() if k!='threads'},indent=2))
