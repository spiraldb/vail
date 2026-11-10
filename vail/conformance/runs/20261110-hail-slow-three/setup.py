from pathlib import Path
import ast,hashlib,json,shutil
r=Path(__file__).parent;old=Path('/private/tmp/vail-hail-prefix-20261110');repo=Path('/private/tmp/vail-list-flatmap-20261106/vail');up=repo/'conformance/runs/20261027-final-supported-scan/protocol/upstream'
for name in ['fixtures.py','benchmark_table.py','benchmark_matrix_table.py','benchmark_methods.py']:shutil.copyfile(up/name,r/name)
s=(old/'owned.py').read_text().replace('6*1024**3','12*1024**3').replace('sampledcombinedRSS6GiB','sampledcombinedRSS12GiB');(r/'owned.py').write_text(s)
ready=json.loads(Path('/private/tmp/vail-prefix-demand-20261109/ready.json').read_text());hi=json.loads((old/'hail-identity.json').read_text())
pins={'vail_python':ready['python'],'native':ready['identity']['native'],'native_sha256':ready['identity']['sha256'],'production':ready['source'],'hail_python':str(old/'hail-env/bin/python'),'hail_version':hi['hail_version'],'hail_jar':hi['hail_jar'],'hail_jar_sha256':hi['hail_jar_sha256'],'pyspark_version':hi['pyspark_version'],'profile_fixture':'/Users/danielking/projects/vail/vail/conformance/runs/20261016-hail-paired/data/profile25.mt','gnomad_fixture':str(r/'inputs/gnomad_dp_simulation.mt'),'gnomad_seed':20261110}
for path,key in [(pins['native'],'native_sha256'),(pins['hail_jar'],'hail_jar_sha256')]:assert hashlib.sha256(Path(path).read_bytes()).hexdigest()==pins[key]
(r/'pins-initial.json').write_text(json.dumps(pins,indent=2)+'\n')
