"""One bounded read-only diagnostic recovery; no source repair or compiler."""
import hashlib,json,os,signal,sys,tarfile,time,platform
from importlib import metadata
from pathlib import Path
HERE=Path(__file__).resolve().parent
LOCK=json.loads((HERE/'expected.json').read_text());RUN=Path(LOCK['original_run_root'])
OUT=HERE.parent/'recovered';OUT.mkdir(exist_ok=False);signal.alarm(60)
started=time.monotonic();copied=0;hashed=0;record={'files':{},'missing':[],'hashes':{},'scope':'Read-only known diagnostics/source identities/current process snapshot; no repair/compile/tests/engine imports. New-boot process absence does not reconstruct historical process identity.'}
def limit():
 if time.monotonic()-started>=60:raise TimeoutError('60s diagnostic budget')
def sha(path,cap=2*1024**2):
 global hashed
 before=path.stat();h=hashlib.sha256();read=0
 with path.open('rb') as f:
  while True:
   limit();chunk=f.read(1024**2)
   if not chunk:break
   read+=len(chunk);hashed+=len(chunk)
   if read>cap or hashed>512*1024**2:raise RuntimeError('Per-file/512MiB in-place hash cap')
   h.update(chunk)
 after=path.stat()
 identity=lambda s:(s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns,s.st_ctime_ns)
 if identity(before)!=identity(after):raise RuntimeError('Identity changed while hashing')
 return h.hexdigest()
assets=json.loads((HERE/'ASSETS.json').read_text())
for name,expected in assets.items():
 if sha(HERE/name)!=expected:raise RuntimeError('Recovery asset mismatch')
if sha(RUN/'protocol/ASSETS.json')!=LOCK['original_assets_sha256']:raise RuntimeError('Original protocol manifest differs')
for name,expected in LOCK['original_assets'].items():
 if sha(RUN/'protocol'/name,8*1024**2)!=expected:raise RuntimeError('Original protocol asset differs:'+name)
original=RUN/'protocol/expected.json'
if sha(original)!=LOCK['original_expected_sha256']:raise RuntimeError('Original expected manifest differs')
E=json.loads(original.read_text())
# Only exact known diagnostic paths; no recursion into sources/data/targets.
paths=['result.json']+[label+'/'+name for label in ('tool-cargo','tool-rustc','tool-kache','baseline','candidate') for name in ('command.json','result.json','stdout','stderr')]
for name in paths:
 limit();source=RUN/'receipt'/name
 if not source.is_file():record['missing'].append(name);continue
 size=source.stat().st_size
 if size>2*1024**2 or copied+size>4*1024**2:raise RuntimeError('2MiB file/4MiB total copied-metadata cap')
 digest=sha(source);data=source.read_bytes()
 if hashlib.sha256(data).hexdigest()!=digest:raise RuntimeError('Receipt changed between hash/copy')
 target=OUT/'original'/name;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(data);copied+=size;record['files'][name]={'bytes':size,'sha256':digest}
if 'result.json' not in record['files']:raise RuntimeError('Required original result missing')
# Fixed manifest union only. Hash code bytes in place, do not copy code/binaries.
source=Path(E['stable_source']);names=set(E['candidate_source_files'])|set(E['baseline_source_files'])|set(E['previous_owned_source']['files'])
if len(names)>4096:raise RuntimeError('4096 known-source-file cap')
source_hash_start=hashed
for name in sorted(names):
 if hashed-source_hash_start>64*1024**2:raise RuntimeError('64MiB source hash cap')
 limit();path=source/name
 try:path.resolve().relative_to(source)
 except ValueError:raise RuntimeError('Source escaped fixed root')
 record['hashes'][name]=sha(path) if path.is_file() else None
record['source_matches']={}
for label,expected in [('candidate',E['candidate_source_files']),('baseline',E['baseline_source_files']),('previous517',E['previous_owned_source']['files'])]:
 mismatches=[name for name,expected_hash in expected.items() if record['hashes'].get(name)!=expected_hash]
 record['source_matches'][label]={'manifest_files':len(expected),'matched':len(expected)-len(mismatches),'mismatches':mismatches,'scope':'Known manifest bytes only; unexpected build files inventoried separately'}
# Source filenames only, bounded; never read data or target payloads.
build_files=[];seen=0
for directory,dirs,files in os.walk(source/'vail',followlinks=False):
 limit();base=Path(directory);relative=base.relative_to(source/'vail')
 dirs[:]=[d for d in dirs if d not in {'target','.git','__pycache__'} and not (relative==Path('.') and d in {'conformance','python','benchmarks'})]
 for name in files:
  seen+=1
  if seen>50000:raise RuntimeError('50000 source-name cap')
  path=base/name
  if path.suffix in {'.rs','.toml','.lock'} or (name=='config' and base.name=='.cargo') or name=='rust-toolchain':build_files.append(str(path.relative_to(source)))
record['current_rust_build_file_set']=sorted(build_files)
for label,reference in [('candidate',E['candidate_source_files']),('baseline',E['baseline_source_files']),('previous517',E['previous_owned_source']['files'])]:
 expected_build={n for n in reference if Path(n).suffix in {'.rs','.toml','.lock'} and not n.startswith(('vail/conformance/','vail/python/','vail/benchmarks/'))}
 actual=set(build_files);record['source_matches'][label]['missing_build_files']=sorted(expected_build-actual);record['source_matches'][label]['extra_build_files']=sorted(actual-expected_build)
 record['source_matches'][label]['complete_build_identity_matches']=not record['source_matches'][label]['mismatches'] and expected_build==actual
readonly=E['readonly_scan_artifact'];frontend=Path(readonly['frontend'])
native=frontend/'_native.abi3.so'
record['scan_native_sha256']=sha(native,512*1024**2) if native.is_file() else None
record['scan_native_matches4761']=record['scan_native_sha256']==readonly['native_sha256']
record['scan_frontend_mismatches']=[name for name,expected in readonly['frontend_sha256'].items() if not (frontend/name).is_file() or sha(frontend/name)!=expected]
runtime={'python':sys.version,'executable':sys.executable,'platform':platform.platform(),'packages':sorted([(d.metadata['Name'],d.version) for d in metadata.distributions()])}
record['runtime']=runtime;record['runtime_matches']=runtime==readonly['runtime']
record['compiler_processes_now']=[]
for i,path in enumerate(Path('/proc').iterdir()):
 limit()
 if i>8192:raise RuntimeError('8192 process-directory cap')
 if not path.name.isdigit():continue
 try:comm=(path/'comm').read_text().strip()
 except FileNotFoundError:continue
 if comm in {'cargo','rustc','maturin','kache','perf'} or comm.startswith('entry_consumer'):
  try:
   fields=(path/'stat').read_text().rsplit(')',1)[1].split();uids=next(line.split()[1:] for line in (path/'status').read_text().splitlines() if line.startswith('Uid:'))
   record['compiler_processes_now'].append({'pid':int(path.name),'comm':comm,'pgid':os.getpgid(int(path.name)),'sid':os.getsid(int(path.name)),'start_ticks':fields[19],'uids':uids})
  except ProcessLookupError:pass
record.update(copied_bytes=copied,hash_read_bytes=hashed,seconds=time.monotonic()-started)
encoded=json.dumps(record,indent=2)
if len(encoded.encode())>2*1024**2:raise RuntimeError('2MiB source diagnostic metadata cap')
(OUT/'inventory.json').write_text(encoded+'\n')
archive=HERE.parent/'recovery.tar.gz'
with tarfile.open(archive,'w:gz') as target:
 for path in sorted(OUT.rglob('*')):
  if path.is_file():target.add(path,arcname=str(path.relative_to(OUT)),recursive=False)
print(json.dumps({'archive':str(archive),'sha256':sha(archive,8*1024**2),'bytes':archive.stat().st_size,'stage':'recover'}),flush=True)
