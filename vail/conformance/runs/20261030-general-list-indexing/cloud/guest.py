"""Six-workload matched rerun. Builds/tests finish before any timed job."""
import hashlib,json,math,os,shutil,signal,subprocess,sys,tarfile,time,zipfile
from pathlib import Path
from owned import execute
HERE=Path(__file__).resolve().parent;ROOT=HERE.parent;OUT=ROOT/'receipts';OUT.mkdir(exist_ok=False)
END=time.monotonic()+2400
lock=json.loads((HERE/'artifact-lock.json').read_text());source=json.loads((HERE/'source-lock.json').read_text())
record={'success':False,'error':None,'commands':{},'results':[]}
def sha(path):
 with Path(path).open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
def resources():
 if time.monotonic()>=END:raise TimeoutError('2400s guest deadline')
 if shutil.disk_usage(ROOT).free<10*1024**3:raise RuntimeError('10GiB disk floor')
def command(label,args,budget,env=None):
 resources();result,text=execute(args,OUT/label,budget,deadline=min(END,time.monotonic()+budget+12))
 record['commands'][label]=result
 if not result['owned_group_clean'] or result['error'] or result['returncode']:
  raise RuntimeError(label+':'+json.dumps(result))
 return text

def baseline_identity():
 b=lock['baseline'];base=Path(b['frontend'])
 for name,expected in b['frontend_sha256'].items():
  if sha(base/name)!=expected:raise RuntimeError('Frozen frontend changed:'+name)
 if sha(base/'_native.abi3.so')!=b['native_sha256']:raise RuntimeError('Frozen native changed')
 if sha(lock['hail']['jar'])!=lock['hail']['jar_sha256']:raise RuntimeError('Hail jar changed')
 return {'native':b['native_sha256'],'frontend_files':len(b['frontend_sha256']),'hail_jar':lock['hail']['jar_sha256']}
def inputs():
 roster=json.loads((HERE/'inputs.json').read_text());actual=set()
 for raw in roster['roots']:
  for f in Path(raw).rglob('*'):
   resources()
   if f.is_file():actual.add(str(f.resolve()))
 if actual!=set(roster['files']):raise RuntimeError('Input canonical file SET changed')
 for raw,meta in roster['files'].items():
  resources();f=Path(raw);before=f.stat()
  if sha(f)!=meta['sha256']:raise RuntimeError('Input SHA changed:'+raw)
  after=f.stat()
  if (before.st_size,before.st_mtime_ns,before.st_ctime_ns)!=(after.st_size,after.st_mtime_ns,after.st_ctime_ns):raise RuntimeError('Input changed while hashing')
 for raw in roster['aliases']:
  if str(Path(raw).resolve()) not in roster['roots']:raise RuntimeError('Fixture alias changed')
 manifest=Path('/home/danielking/cloud-scan-20261023/data/shared-manifests/3049e48c6709d186bd4511f5651fa61775b70a4ad241121f085b2e7c2602913b.json')
 if sha(manifest)!=source['manifest_sha256']:raise RuntimeError('Fixture manifest changed')
 return {'files':len(actual),'bytes':sum(v['identity'][2] for v in roster['files'].values()),'full_sha256':True}
def interrupted(sig,frame):raise InterruptedError('signal '+str(sig))
for sig in (signal.SIGALRM,signal.SIGTERM,signal.SIGINT,signal.SIGHUP):signal.signal(sig,interrupted)
signal.alarm(2400)
try:
 assets=json.loads((HERE/'ASSETS.json').read_text())
 for name,expected in assets.items():
  if sha(HERE/name)!=expected:raise RuntimeError('Protocol asset mismatch:'+name)
 record['baseline_before']=baseline_identity();record['inputs_before']=inputs()
 record['hardware']=command('hardware',['sh','-c','uname -a; lscpu'],20)
 manifest=json.loads((HERE/'source-manifest.json').read_text());dest=ROOT/'source'
 if dest.exists():raise RuntimeError('Fresh source required')
 dest.mkdir()
 if sha(HERE/'source.tar.gz')!=source['source_archive_sha256']:raise RuntimeError('Source archive mismatch')
 with tarfile.open(HERE/'source.tar.gz') as archive:
  members=archive.getmembers()
  if {m.name for m in members}!=set(manifest) or any(not m.isfile() for m in members):raise RuntimeError('Source member SET mismatch')
  for m in members:
   if m.size>64*1024**2:raise RuntimeError('64MiB source member cap')
   data=archive.extractfile(m).read()
   if hashlib.sha256(data).hexdigest()!=manifest[m.name]:raise RuntimeError('Source member hash mismatch')
   target=dest/m.name
   if not target.resolve().is_relative_to(dest.resolve()):raise RuntimeError('Source escaped')
   target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(data)
 os.environ.update(PATH='/home/danielking/.cargo/bin:/home/danielking/.local/bin:'+os.environ.get('PATH','/usr/bin:/bin'),RUSTFLAGS='-C target-cpu=native',RUSTC_WRAPPER='kache',CARGO_BUILD_JOBS='8',CARGO_INCREMENTAL='0',CARGO_TARGET_DIR='/home/danielking/vail-presence-20261026/target',CARGO_TERM_COLOR='never')
 os.environ.pop('PYTHONPATH',None);os.environ.pop('CARGO_ENCODED_RUSTFLAGS',None);os.chdir(dest/'vail')
 py=lock['baseline']['python'];wheels=ROOT/'wheels'
 command('build',[py,'-m','maturin','build','--release','--locked','--offline','--interpreter',py,'--out',str(wheels)],1200)
 wheel,=wheels.glob('*.whl');candidate=ROOT/'candidate'
 with zipfile.ZipFile(wheel) as z:z.extractall(candidate)
 record['candidate']={'source_commit':source['commit'],'wheel_sha256':sha(wheel),'native_sha256':sha(candidate/'vail/_native.abi3.so')}
 for f in (candidate/'vail').rglob('*.py'):
  relative='vail/python/'+str(f.relative_to(candidate))
  if sha(f)!=manifest.get(relative):raise RuntimeError('Candidate frontend not exact source:'+relative)
 os.environ['PYTHONPATH']=str(candidate)
 command('split-correctness',[py,'-m','pytest','-q',str(dest/'vail/python/tests/test_split_multi_hts.py')],240)
 text=command('index-correctness',['cargo','test','--release','--locked','--offline','-p','vail-execution','--test','list_indexing','--test','call_allocations'],360)
 if '4 passed; 0 failed' not in text or '11 passed; 0 failed' not in text:raise RuntimeError('Expected15 indexing/allocation gate tests')
 # No compiler/timing overlap: all builds and tests have completed above.
 start=time.monotonic()
 for i,p in enumerate(Path('/proc').iterdir()):
  if i>8192 or time.monotonic()-start>5:raise RuntimeError('Compiler inventory cap')
  if not p.name.isdigit():continue
  try:name=(p/'comm').read_text().strip()
  except FileNotFoundError:continue
  if name in {'cargo','rustc','maturin','perf'}:raise RuntimeError('Competing compiler/profile process')
 os.environ.update(OMP_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1',MKL_NUM_THREADS='1',NUMEXPR_NUM_THREADS='1',RAYON_NUM_THREADS='32',HAIL_BENCHMARK_CORES='32',HAIL_BENCHMARK_MEMORY='16g',VAIL_BENCHMARK_MAX_RSS_GIB='96',VAIL_BENCHMARK_UPSTREAM=str(HERE/'upstream'))
 schedule=json.loads((HERE/'schedule.json').read_text())
 preflight=[{'case_id':c,'version':'candidate','mode':mode,'repetition':0,'purpose':'preflight'} for c in schedule['cases'] for mode in ['vail-vail','vail-hail']]
 for idx,job in enumerate(preflight+[{**j,'purpose':'timed'} for j in schedule['jobs']]):
  resources();label=f'case-{idx:03}';folder=OUT/label;version=job['version'];engine='hail' if version=='hail' else 'vail'
  if version=='candidate':os.environ['PYTHONPATH']=str(candidate)
  elif version=='baseline':os.environ['PYTHONPATH']=str(Path(lock['baseline']['frontend']).parent)
  else:os.environ.pop('PYTHONPATH',None)
  os.environ.update(FINAL_SCAN_VERSION=version,VAIL_SCAN_MODE=job['mode'],FINAL_SCAN_CASE_DIRECTORY=str(folder),VAIL_BENCHMARK_TELEMETRY_DIR=str(folder/'telemetry'))
  command(label,[lock[version]['python'],str(HERE/'upstream_benchmark_coverage.py'),'--engine',engine,'--case',job['case_id'],'--timeout','120','--fixture-timeout','120','--data-dir','/home/danielking/cloud-scan-20261023/data','--output',str(folder/'observation.json')],270)
  cleanup=json.loads((folder/'worker-cleanup.json').read_text())
  if not cleanup.get('clean'):raise RuntimeError('Worker cleanup unverified')
  report=json.loads((folder/'observation.json').read_text());observation,=report['cases']
  positive=lambda n:isinstance(n,(int,float)) and not isinstance(n,bool) and math.isfinite(n) and n>0
  eligible=observation.get('status')=='passed' and positive(observation.get('benchmark_seconds')) and positive((observation.get('telemetry') or {}).get('cpu_seconds'))
  record['results'].append({**observation,**job,'sample_eligible_for_timed_ratio':eligible and job['purpose']=='timed','actual_sample':eligible,'cross_engine_result_compared':False})
  (OUT/'results.json').write_text(json.dumps(record['results'],indent=2)+'\n')
  print(json.dumps({'completed':idx+1,'total':59,'case':job['case_id'],'version':version,'mode':job['mode'],'status':observation.get('status')}),flush=True)
  if job['purpose']=='preflight' and not eligible:raise RuntimeError('Candidate preflight failed; retain and stop')
 record['inputs_after']=inputs();record['baseline_after']=baseline_identity();record['success']=True
except BaseException as error:
 record['error']=repr(error);raise
finally:
 (OUT/'summary.json').write_text(json.dumps(record,indent=2)+'\n')
if record['success']:
 archive=ROOT/'receipts.tar.gz'
 with tarfile.open(archive,'w:gz') as a:
  total=0
  for f in OUT.rglob('*'):
   if f.is_file():
    total+=f.stat().st_size
    if total>1024**3:raise RuntimeError('1GiB receipt cap')
    a.add(f,arcname=str(f.relative_to(OUT)),recursive=False)
 print(json.dumps({'archive':str(archive),'sha256':sha(archive),'bytes':archive.stat().st_size}),flush=True)
