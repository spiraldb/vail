"""Linux Rust-test counterfactual only; no engine benchmark or Python install."""
import hashlib,io,json,os,re,signal,sys,tarfile,time,shutil,platform
from importlib import metadata
from pathlib import Path
from owned import execute
HERE=Path(__file__).resolve().parent
E=json.loads((HERE/'expected.json').read_text())
OUT=HERE.parent/'receipt';OUT.mkdir(exist_ok=False)
SOURCE=Path(E['stable_source']);PROJECT=SOURCE/'vail'
end=time.monotonic()+E['guest_seconds']
record={'success':False,'error':None,'safe_to_restore':True,'candidate_restored':False,'commands':{},'baseline':None,'candidate':None}
def remaining():
 if time.monotonic()>=end:raise TimeoutError('1800s whole guest budget')
def digest(path):
 h=hashlib.sha256()
 with Path(path).open('rb') as stream:
  while True:
   remaining();data=stream.read(1024**2)
   if not data:break
   h.update(data)
 return h.hexdigest()
def build_file_set():
 found=set();seen=0
 for directory,dirs,files in os.walk(PROJECT,followlinks=False):
  remaining();base=Path(directory);relative=base.relative_to(PROJECT)
  dirs[:]=[d for d in dirs if d not in {'target','.git','__pycache__'} and not (relative==Path('.') and d in {'conformance','python','benchmarks'})]
  if any((base/d).is_symlink() for d in dirs):raise RuntimeError('Unexpected source directory symlink')
  for name in files:
   seen+=1
   if seen>50000:raise RuntimeError('Source inventory cap')
   path=base/name
   if path.suffix in {'.rs','.toml','.lock'} or (name=='config' and base.name=='.cargo') or name=='rust-toolchain':found.add(str(path.relative_to(SOURCE)))
 return found
def verify(files,exact=False):
 if exact and build_file_set()!=set(files):raise RuntimeError('Complete Rust/Cargo source file SET differs')
 for name,expected in files.items():
  remaining()
  if digest(SOURCE/name)!=expected:raise RuntimeError('Complete source mismatch:'+name)
def idle():
 start=time.monotonic();found=[]
 for i,p in enumerate(Path('/proc').iterdir()):
  if time.monotonic()-start>5 or i>8192:raise RuntimeError('Compiler-idle inventory cap')
  if not p.name.isdigit():continue
  try:comm=(p/'comm').read_text().strip()
  except FileNotFoundError:continue
  if comm in {'cargo','rustc','maturin','perf'}:found.append({'pid':int(p.name),'comm':comm})
 if found:raise RuntimeError('Competing or surviving compiler processes:'+json.dumps(found))
def write(name,data):
 path=SOURCE/name
 if not path.resolve().is_relative_to(SOURCE):raise RuntimeError('Source path escaped owned root')
 path.parent.mkdir(parents=True,exist_ok=True)
 if path.is_file() and hashlib.sha256(path.read_bytes()).digest()==hashlib.sha256(data).digest():return
 temporary=path.with_name(path.name+'.nested-gate-tmp');temporary.write_bytes(data);os.replace(temporary,path)
def resources():
 remaining()
 if shutil.disk_usage(SOURCE).free<10*1024**3:raise RuntimeError('10GiB free-disk floor')
 status=Path('/proc/meminfo').read_text()
 available=next(int(line.split()[1])*1024 for line in status.splitlines() if line.startswith('MemAvailable:'))
 if available<8*1024**3:raise RuntimeError('8GiB MemAvailable floor; no allocation/RSS claim')
def scan_identity():
 frozen=E['readonly_scan_artifact'];root=Path(frozen['frontend'])
 for name,sha in frozen['frontend_sha256'].items():
  if digest(root/name)!=sha:raise RuntimeError('Read-only scan frontend changed:'+name)
 if digest(root/'_native.abi3.so')!=frozen['native_sha256']:raise RuntimeError('Read-only scan native changed')
 runtime={'python':sys.version,'executable':sys.executable,'platform':platform.platform(),'packages':sorted([(d.metadata['Name'],d.version) for d in metadata.distributions()])}
 if runtime!=frozen['runtime']:raise RuntimeError('Read-only Python runtime/package inventory drift')
 return {'native_sha256':frozen['native_sha256'],'frontend_files':len(frozen['frontend_sha256']),'runtime':runtime}
def interrupted(signum,frame):raise InterruptedError('signal'+str(signum))
handled=(signal.SIGINT,signal.SIGTERM,signal.SIGHUP,signal.SIGALRM)
previous={sig:signal.signal(sig,interrupted) for sig in handled};signal.alarm(E['guest_seconds'])
candidate_data=None
try:
 assets=json.loads((HERE/'ASSETS.json').read_text())
 for name,sha in assets.items():
  if digest(HERE/name)!=sha:raise RuntimeError('Protocol fingerprint mismatch:'+name)
 if Path(sys.executable).absolute()!=Path(E['python']).absolute():raise RuntimeError('Exact existing Python interpreter required')
 if not SOURCE.is_dir() or SOURCE.resolve()!=SOURCE or not Path(E['target']).is_dir():raise RuntimeError('Known stable source/cache required; no new cold path')
 idle();resources();record['scan_identity_before']=scan_identity()
 # The installed frozen scan artifact and runtime environment are read-only.
 native=Path(E['python']).parent.parent/'lib/python3.12/site-packages/vail/_native.abi3.so'
 record['scan_native_before']=digest(native)
 if record['scan_native_before']!='4761b8ec4e70325e922bb330987ef87c7a9f50bfba4f93f9a69c0c761337244c':raise RuntimeError('Installed scan artifact changed')
 verify(E['previous_owned_source']['files']);record['previous_source_files_verified']=len(E['previous_owned_source']['files'])
 source_archive=HERE/'source.tar.gz'
 if source_archive.stat().st_size!=E['source_archive_bytes'] or digest(source_archive)!=E['source_archive_sha256']:raise RuntimeError('Candidate source archive mismatch')
 prepared_data={}
 with tarfile.open(source_archive) as archive:
  for member in archive.getmembers():
   remaining()
   if not member.isfile() or member.name not in E['candidate_source_files']:raise RuntimeError('Unexpected source archive member')
   data=archive.extractfile(member).read()
   if hashlib.sha256(data).hexdigest()!=E['candidate_source_files'][member.name]:raise RuntimeError('Archive source bytes mismatch')
   prepared_data[member.name]=data
 if set(prepared_data)!=set(E['candidate_source_files']):raise RuntimeError('Incomplete candidate archive')
 candidate_data=prepared_data
 for name,data in candidate_data.items():write(name,data)
 verify(E['candidate_source_files'],exact=True);record['candidate_source_files_verified_before_baseline']=len(E['candidate_source_files'])
 os.chdir(PROJECT)
 os.environ.update(PATH='/home/danielking/.cargo/bin:/home/danielking/.local/bin:'+os.environ.get('PATH','/usr/bin:/bin'),RUSTFLAGS='-C target-cpu=native',RUSTC_WRAPPER='kache',CARGO_BUILD_JOBS='8',CARGO_INCREMENTAL='0',CARGO_TARGET_DIR=E['target'],PYO3_PYTHON=E['python'],CARGO_TERM_COLOR='never')
 os.environ.pop('PYTHONPATH',None)
 os.environ.pop('CARGO_ENCODED_RUSTFLAGS',None)
 os.environ.pop('RUSTC_WORKSPACE_WRAPPER',None)
 def command(label,args,budget):
  idle();remaining();record['safe_to_restore']=False
  result,text=execute(args,OUT/label,budget,deadline=min(end,time.monotonic()+budget+12),check=resources)
  record['commands'][label]=result
  if not result['owned_group_clean']:raise RuntimeError('Owned command group unresolved')
  idle();record['safe_to_restore']=True;resources()
  if result['error']:raise RuntimeError(label+':'+result['error'])
  return result,text
 record['tools']={}
 for tool in ('cargo','rustc','kache'):
  result,text=command('tool-'+tool,[tool,'--version'],15)
  if result['returncode']:raise RuntimeError('Existing tool version failed:'+tool)
  record['tools'][tool]={'path':shutil.which(tool),'version':text.strip()}
 def run(label):
  args=['cargo','test','--release','--locked','--offline','-p','vail-table','--test','entry_consumer_projection','--message-format=json-render-diagnostics','--','--test-threads=1','--nocapture']
  result,text=command(label,args,E['cargo_command_seconds'])
  stderr=(OUT/label/'stderr').read_text();combined=text+'\n'+stderr
  expected_failed=2 if label=='baseline' else 0;expected_passed=6 if label=='baseline' else 8
  match=re.search(r'test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored;',combined)
  if not match or tuple(map(int,match.groups()[1:]))!=(expected_passed,expected_failed,0):raise RuntimeError('Unexpected '+label+' public test summary')
  if result['returncode']!=(101 if expected_failed else 0):raise RuntimeError('Unexpected '+label+' Cargo status')
  if expected_failed:
   names=set(re.findall(r'^    ([a-zA-Z_][a-zA-Z_0-9]*)\s*$',combined,re.M))
   if names!=set(E['baseline_failures']):raise RuntimeError('Wrong baseline failed tests:'+str(names))
   if 'unused nested entry payload grew allocations:' not in combined or 'unused nested owner reached temporary output:' not in combined:raise RuntimeError('Expected public resource assertions absent')
  binaries=[]
  for line in text.splitlines():
   if not line.startswith('{'):continue
   message=json.loads(line)
   if message.get('reason')=='compiler-artifact' and message.get('target',{}).get('name')=='entry_consumer_projection' and message.get('executable'):binaries.append(message['executable'])
  if len(set(binaries))!=1:raise RuntimeError('Exact natural test executable not identified')
  record[label]={'passed':expected_passed,'failed':expected_failed,'test_sha256':E['test_sha256'],'source':E[label],'executable':binaries[0],'executable_sha256':digest(Path(binaries[0])),'scope':'Normal cargo Rust test executable hash only; no binary copied/installed'}
 # Baseline uses identical final test bytes; only six production source overlays.
 try:
  for name,overlay in E['baseline_overlays'].items():
   data=(HERE/overlay['asset']).read_bytes()
   if hashlib.sha256(data).hexdigest()!=overlay['sha256']:raise RuntimeError('Baseline overlay mismatch')
   write(name,data)
  verify(E['baseline_source_files'],exact=True);run('baseline');verify(E['baseline_source_files'],exact=True);record['baseline_source_files_verified']=len(E['baseline_source_files'])
 finally:
  # Repeated handled signals cannot interrupt known-safe restoration. Unknown
  # compiler/group ownership skips writes and forces host STOP first instead.
  for sig in handled:signal.signal(sig,signal.SIG_IGN)
  if record['safe_to_restore']:
   idle()
   for name,data in candidate_data.items():write(name,data)
   verify(E['candidate_source_files'],exact=True);record['candidate_restored']=True
  else:record['restoration_error']='Skipped: compiler ownership/quiescence unverified'
  for sig in handled:signal.signal(sig,interrupted)
 if not record['candidate_restored']:raise RuntimeError('Candidate restoration not positively verified')
 run('candidate');verify(E['candidate_source_files'],exact=True)
 record['scan_native_after']=digest(native)
 if record['scan_native_after']!=record['scan_native_before']:raise RuntimeError('Installed scan native artifact changed')
 record['scan_identity_after']=scan_identity()
 if record['scan_identity_after']!=record['scan_identity_before']:raise RuntimeError('Read-only scan environment changed')
 record['success']=True
except BaseException as error:record['error']=repr(error)
finally:
 for sig in handled:signal.signal(sig,signal.SIG_IGN)
 # Always reverify/restore all candidate bytes after a known-quiescent command,
 # including ordinary test failures. Unknown compiler/group ownership never
 # permits more source writes or collection before provider STOP.
 record['scope_clean']=False
 if candidate_data is not None and record['safe_to_restore']:
  try:
   record['candidate_restored']=False;idle()
   for name,data in candidate_data.items():write(name,data)
   verify(E['candidate_source_files'],exact=True);record['candidate_restored']=True
   record['scan_identity_after']=scan_identity()
   if record['scan_identity_after']!=record['scan_identity_before']:raise RuntimeError('Read-only scan environment changed')
   record['scope_clean']=all(c['owned_group_clean'] for c in record['commands'].values())
  except BaseException as error:record['restoration_error']=repr(error)
 (OUT/'result.json').write_text(json.dumps(record,indent=2)+'\n')
if not record['scope_clean']:raise SystemExit(1)
remaining();archive=HERE.parent/'receipt.tar.gz';total=0
with tarfile.open(archive,'w:gz') as target:
 for path in sorted(OUT.rglob('*')):
  if path.is_file():
   total+=path.stat().st_size
   if total>20*1024**2:raise RuntimeError('20MiB receipt cap')
   target.add(path,arcname=str(path.relative_to(OUT)),recursive=False)
print(json.dumps({'archive':str(archive),'sha256':digest(archive),'bytes':archive.stat().st_size,'stage':'rust-tests','gate_passed':record['success'],'scope_clean':record['scope_clean'],'candidate_restored':record['candidate_restored']}),flush=True)
