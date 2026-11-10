from pathlib import Path
import hashlib,json,shutil,subprocess,platform
r=Path(__file__).parent;prior=json.loads(Path('/private/tmp/vail-hail-four-20261109/pins.json').read_text());ready=json.loads(Path('/private/tmp/vail-prefix-demand-20261109/ready.json').read_text())
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
identity=json.loads((r/'hail-identity.json').read_text())
pins={k:prior[k] for k in ['vail_fixture']}
pins.update({k:identity[k] for k in ['hail_jar','hail_jar_sha256','hail_version']})
pins['hail_python']=str(r/'hail-env/bin/python')
pins['pyspark_version']=identity['pyspark_version']
root=r/'inputs/hail.mt'
pins['hail_fixture']={'root':str(root),'files':{str(p.relative_to(root)):{'bytes':p.stat().st_size,'sha256':sha(p)} for p in root.rglob('*') if p.is_file()}}
assert len(pins['hail_fixture']['files'])>0
pins.update(vail_python=ready['python'],vail_source=ready['source'],native=ready['identity']['native'],native_sha256=ready['identity']['sha256'])
for mode in ['hail','vail']:
 d=pins[mode+'_fixture'];root=Path(d['root']);assert {str(p.relative_to(root)) for p in root.rglob('*') if p.is_file()}==set(d['files'])
 for rel,expected in d['files'].items():
  p=root/rel;assert {'bytes':p.stat().st_size,'sha256':sha(p)}==expected,rel
assert sha(pins['native'])==pins['native_sha256'] and sha(pins['hail_jar'])==pins['hail_jar_sha256']
source=Path('/private/tmp/vail-list-flatmap-20261106/vail/conformance/runs/20261027-final-supported-scan/protocol/upstream/benchmark_matrix_table.py');shutil.copyfile(source,r/'benchmark_matrix_table.py');pins['benchmark_source_sha256']=sha(source)
shutil.copyfile('/private/tmp/vail-hail-four-20261109/owned.py',r/'owned.py')
(r/'pins.json').write_text(json.dumps(pins,indent=2)+'\n')
(r/'host.json').write_text(json.dumps({'platform':platform.platform(),'python':platform.python_version(),'note':'Current laptop; warmed, read/construct/action timed; initialization outside; native formats differ, same previously converted logical data.'},indent=2)+'\n')
print('ENROLLED',sum(len(pins[x+'_fixture']['files']) for x in ['hail','vail']),'input files')
