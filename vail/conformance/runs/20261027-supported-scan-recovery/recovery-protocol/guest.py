"""Bounded read-only source metadata inventory; no import/query/build/install."""
import argparse,hashlib,json,os,signal,tarfile,time,sys
from pathlib import Path
def sha256_stream(source):
 digest=hashlib.sha256()
 while True:
  chunk=source.read(1024*1024)
  if not chunk:break
  digest.update(chunk)
 return digest.hexdigest()
p=argparse.ArgumentParser();p.add_argument('--out',required=True);p.add_argument('--expected-sha',required=True);a=p.parse_args()
with Path(__file__).open('rb') as source:
 if sha256_stream(source)!=a.expected_sha:raise RuntimeError('Guest script fingerprint differs')
out=Path(a.out)
if not out.is_absolute() or not out.is_relative_to('/home/danielking') or out.exists():raise RuntimeError('Fresh owned output required')
out.mkdir(parents=True);signal.alarm(120);root=Path('/home/danielking/cloud-scan-20261023');record={'files':{},'stat_only':{},'missing':[],'python':{'version':sys.version,'version_info':list(sys.version_info),'executable':sys.executable,'hashlib_file_digest_available':hasattr(hashlib,'file_digest'),'path_is_relative_to_available':hasattr(Path,'is_relative_to'),'sha256_stream_chunk_bytes':1024*1024}};copied=0;hashed=0
for directory in Path('/proc').iterdir():
 if directory.name.isdigit() and int(directory.name)!=os.getpid():
  try:comm=(directory/'comm').read_text().strip();cmd=(directory/'cmdline').read_bytes().replace(b'\0',b' ').decode(errors='replace')
  except FileNotFoundError:continue
  if comm in {'cargo','rustc','maturin','perf'} or any(x in cmd for x in ('/worker.py','/run_scan.py','/run_supplemental.py')):raise RuntimeError(f'Competing workload:{directory.name}:{comm}')
def collect(path,*,copy=True,cap=1024**2):
 global copied,hashed
 path=Path(path)
 if not path.exists():record['missing'].append(str(path));return None
 resolved=path.resolve()
 if not resolved.is_relative_to('/home/danielking'):raise RuntimeError('Resolved path escaped owned home')
 size=path.stat().st_size
 if not path.is_file() or size>cap:raise RuntimeError(f'Unexpected metadata/artifact size:{path}:{size}')
 hashed+=size
 if hashed>1024**3:raise RuntimeError('1GiB total hash-read cap')
 with path.open('rb') as f:sha=sha256_stream(f)
 entry={'resolved':str(resolved),'bytes':size,'sha256':sha,'copied':copy};record['files'][str(path)]=entry
 if copy:
  copied+=size
  if copied>16*1024**2:raise RuntimeError('16MiB copied-metadata cap')
  target=out/'files'/path.relative_to('/home/danielking');target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(path.read_bytes())
 return path
paths=[]
shared=root/'data/shared-manifests'
if shared.is_dir():
 manifests=sorted(shared.glob('*.json'))
 if len(manifests)>128:raise RuntimeError('128sharedmanifest cap')
 for path in manifests:
  if collect(path):
   data=json.loads(path.read_text())
   for value in data.get('paths',{}).values():paths.extend(value if isinstance(value,list) else [value])
else:record['missing'].append(str(shared))
prepared=[]
for folder in ['hail','vail-from-hail']:
 directory=root/'data/supplemental'/folder
 if directory.is_dir():prepared.extend(directory.glob('*.prepared.json'))
if len(prepared)>128:raise RuntimeError('128preparedmetadata cap')
for path in sorted(prepared):collect(path)
# These are fixture descriptors only; never traverse/read payload part files.
if len(paths)>256:raise RuntimeError('256fixture-reference cap')
for raw in sorted(set(paths)):
 path=Path(raw)
 if not path.is_absolute() or not path.is_relative_to('/home/danielking'):record['stat_only'][raw]={'unsupported_path':True};continue
 if not path.exists():record['missing'].append(raw);continue
 record['stat_only'][raw]={'resolved':str(path.resolve()),'directory':path.is_dir(),'bytes':None if path.is_dir() else path.stat().st_size}
 for candidate in [path,root/'data/vail-from-hail'/path.relative_to(root/'data/hail') if path.is_relative_to(root/'data/hail') else path]:
  if candidate.is_dir():
   for name in ['metadata.json.gz','_vail_table.json','_vail_matrix.json','_SUCCESS']:
    if (candidate/name).is_file():collect(candidate/name)
# Hash existing normal artifacts IN PLACE only; no binary copying/transplant/import.
for native in [root/'vail-env/lib/python3.11/site-packages/vail/_native.abi3.so',Path('/home/danielking/h012-matched-20261027/run-1/gate/candidate-env/lib/python3.12/site-packages/vail/_native.abi3.so')]:collect(native,copy=False,cap=512*1024**2)
for package in [root/'vail-env/lib/python3.11/site-packages',root/'hail-env/lib/python3.11/site-packages',Path('/home/danielking/hail-env/lib/python3.11/site-packages'),Path('/home/danielking/h012-matched-20261027/run-1/gate/candidate-env/lib/python3.12/site-packages')]:
 for pattern in ['vail-*.dist-info/METADATA','hail-*.dist-info/METADATA']:
  matches=sorted(package.glob(pattern))
  if len(matches)>2:raise RuntimeError('Ambiguous installed distribution metadata')
  for path in matches:collect(path)
 for path in [package/'hail/backend/hail-all-spark.jar']:
  if path.exists():collect(path,copy=False,cap=256*1024**2)
for name in ['run_scan.py','upstream_benchmark_coverage.py','benchmark_telemetry.py']:collect(root/'harness'/name)
record.update(copied_bytes=copied,hashed_bytes=hashed,scope='read-only bounded descriptors/artifact hashes; NOpayload hashes/query/import/build')
(out/'inventory.json').write_text(json.dumps(record,indent=2)+'\n')
archive=out.with_suffix('.tar.gz')
with tarfile.open(archive,'w:gz') as target:
 for path in sorted(out.rglob('*')):
  if path.is_file():target.add(path,arcname=str(path.relative_to(out)),recursive=False)
with archive.open('rb') as f:sha=sha256_stream(f)
print(json.dumps({'archive':str(archive),'sha256':sha,'bytes':archive.stat().st_size,'metadata_bytes':copied,'hash_read_bytes':hashed}),flush=True)
