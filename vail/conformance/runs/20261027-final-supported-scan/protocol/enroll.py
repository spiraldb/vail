"""Finite input/artifact enrollment outside all benchmark action windows."""
import argparse,hashlib,json,os,sys,time
from pathlib import Path
HERE=Path(__file__).resolve().parent
MAX_BYTES=256*1024**3;MAX_FILES=250000;SECONDS=3600;MAX_JSON=128*1024**2

def sha(path,deadline=None,limit=512*1024**2):
 digest=hashlib.sha256();read=0
 with path.open('rb') as stream:
  while True:
   if deadline is not None and time.monotonic()>=deadline:raise TimeoutError('Hash deadline')
   data=stream.read(1024**2)
   read+=len(data)
   if read>limit:raise RuntimeError('Hash byte cap')
   if not data:break
   digest.update(data)
 return digest.hexdigest()

def artifacts():
 lock=json.loads((HERE/'artifact-lock.json').read_text());observed={}
 for name in ('current','old'):
  entry=lock[name];root=Path(entry['frontend'])
  for relative,expected in entry['frontend_sha256'].items():
   if sha(root/relative)!=expected:raise RuntimeError('Frontend mismatch:'+name+':'+relative)
  native=root/'_native.abi3.so'
  if sha(native)!=entry['native_sha256']:raise RuntimeError('Native mismatch:'+name)
  if not Path(entry['python']).is_file():raise RuntimeError('Missing frozen interpreter:'+name)
  observed[name]=entry
 hail=lock['hail']
 if sha(Path(hail['jar']))!=hail['jar_sha256']:raise RuntimeError('Hail jar mismatch')
 if not Path(hail['python']).is_file():raise RuntimeError('Missing Hail interpreter')
 observed['hail']=hail
 return observed

def input_roots():
 maps=json.loads((HERE/'input-maps.json').read_text());pool=json.loads((HERE/'positive-support-pool.json').read_text())
 keys={c['fixture_manifest_key'] for c in pool['cases']};roots=set();manifests=[]
 for entry in maps['shared_manifests']:
  if Path(entry['manifest']).stem not in keys:continue
  original=Path('/home/danielking')/Path(entry['manifest']).relative_to('recovered/files')
  if sha(original)!=entry['sha256']:raise RuntimeError('Shared manifest mismatch:'+str(original))
  manifests.append(str(original))
  for raw in entry['paths'].values():
   for name in raw if isinstance(raw,list) else [raw]:
    if name=='__worker_temporary__':continue
    source=Path(name)
    if not source.is_absolute() or not source.exists():raise RuntimeError('Missing input:'+str(source))
    roots.add(str(source))
    if source.suffix in ('.ht','.mt'):
     base=Path('/home/danielking/cloud-scan-20261023/data')
     target=base/'vail-from-hail'/source.relative_to(base/'hail')
     marker='_vail_table.json' if source.suffix=='.ht' else '_vail_matrix.json'
     if not (target/marker).is_file():raise RuntimeError('Native fixture missing; no conversion:'+str(target))
     roots.add(str(target))
 if len(manifests)!=len(keys):raise RuntimeError('Not every supported fixture key enrolled')
 return sorted(roots),manifests

def roster():
 start=time.monotonic();rows={};total=0;roots,manifests=input_roots()
 for raw in roots:
  root=Path(raw)
  # Only exact supported fixture roots, no home/disk blanket walk.
  paths=[root] if root.is_file() else root.rglob('*')
  for path in paths:
   if time.monotonic()-start>SECONDS:raise TimeoutError('3600s input enrollment cap')
   resolved=path.resolve()
   try:resolved.relative_to('/home/danielking')
   except ValueError:raise RuntimeError('Input escaped owned home')
   if not path.is_file() or str(resolved) in rows:continue
   before=path.stat();total+=before.st_size
   if total>MAX_BYTES or len(rows)>=MAX_FILES:raise RuntimeError('256GiB/250000-file input enrollment cap')
   digest=sha(path,start+SECONDS,before.st_size);after=path.stat()
   identity=lambda s:[s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns,s.st_ctime_ns]
   if identity(before)!=identity(after):raise RuntimeError('Input changed while hashing')
   rows[str(resolved)]={'identity':identity(after),'sha256':digest}
 result={'roots':roots,'shared_manifests':manifests,'files':rows,'bytes':total,'seconds':time.monotonic()-start,'scope':'Full SHA256 of supported input files at enrollment; not historical content identity. Recheck stat identities each chunk, full rehash after scan. Transient same-content restores are not observable.'}
 if len(json.dumps(result))>MAX_JSON:raise RuntimeError('128MiB roster cap')
 return result

def check(roster_path,full=False):
 data=json.loads(Path(roster_path).read_text());start=time.monotonic()
 for raw,entry in data['files'].items():
  if time.monotonic()-start>SECONDS:raise TimeoutError('3600s input recheck cap')
  p=Path(raw);s=p.stat()
  if [s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns,s.st_ctime_ns]!=entry['identity']:raise RuntimeError('Input stat changed:'+raw)
  if full and sha(p,start+SECONDS,entry['identity'][2])!=entry['sha256']:raise RuntimeError('Input content changed:'+raw)
 return {'files':len(data['files']),'full_sha256':full,'seconds':time.monotonic()-start}

if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--out',required=True);p.add_argument('--check');p.add_argument('--full',action='store_true');a=p.parse_args()
 result={'artifacts':artifacts(),'inputs':check(a.check,a.full) if a.check else roster(),'python':sys.version}
 Path(a.out).write_text(json.dumps(result,indent=2)+'\n')
