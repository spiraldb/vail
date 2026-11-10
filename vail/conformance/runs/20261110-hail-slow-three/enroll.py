from pathlib import Path
import hashlib,json,gzip
r=Path(__file__).parent;p=json.loads((r/'pins-initial.json').read_text())
def sha(q):return hashlib.sha256(Path(q).read_bytes()).hexdigest()
p['fixtures']={}
for name,key,dims,parts in [('gnomad','gnomad_fixture',[250000,1000],32),('profile','profile_fixture',[24885,2535],16)]:
 root=Path(p[key]);assert root.is_dir();files={str(q.relative_to(root)):{'bytes':q.stat().st_size,'sha256':sha(q)} for q in root.rglob('*') if q.is_file()}
 d=json.loads(gzip.decompress((root/'metadata.json.gz').read_bytes()));assert sum(d['components']['partition_counts']['counts'])==dims[0];assert len(d['components']['partition_counts']['counts'])==parts
 p['fixtures'][name]={'root':str(root),'dimensions':dims,'partitions':parts,'files':files}
p['source_sha256']={name:sha(r/name) for name in ['benchmark_table.py','benchmark_matrix_table.py','benchmark_methods.py','fixtures.py']}
assert sha(p['native'])==p['native_sha256'] and sha(p['hail_jar'])==p['hail_jar_sha256']
(r/'pins.json').write_text(json.dumps(p,indent=2)+'\n');print({k:{'files':len(v['files']),'bytes':sum(x['bytes'] for x in v['files'].values()),'dimensions':v['dimensions']} for k,v in p['fixtures'].items()})
