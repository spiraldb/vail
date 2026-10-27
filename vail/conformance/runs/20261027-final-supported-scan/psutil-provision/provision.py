"""Only install one hash-pinned ordinary telemetry dependency in current env."""
import hashlib,json,os,signal,sys,tarfile
from pathlib import Path
HERE=Path(__file__).resolve().parent
PROTOCOL=Path('/home/danielking/final-supported-scan-20261027/protocol')
sys.path.insert(0,str(PROTOCOL))
from enroll import artifacts,check
from owned import execute
from packaging.tags import sys_tags
from packaging.utils import parse_wheel_filename
OUT=HERE/'receipt';OUT.mkdir(exist_ok=False);signal.alarm(300)
record={'success':False,'error':None,'owned_groups_clean':False};commands=[]
def digest(path):
 h=hashlib.sha256()
 with Path(path).open('rb') as stream:
  while True:
   data=stream.read(1024**2)
   if not data:break
   h.update(data)
 return h.hexdigest()
def run(name,command,budget):
 result,text=execute(command,OUT/name,budget);commands.append(result)
 if result['error'] or result['returncode']!=0 or not result['owned_group_clean']:raise RuntimeError(name+':'+str(result))
 return text
try:
 assets=json.loads((HERE/'ASSETS.json').read_text())
 for name,expected in assets.items():
  if digest(HERE/name)!=expected:raise RuntimeError('Provision asset mismatch:'+name)
 lock=json.loads((HERE/'wheel-lock.json').read_text());wheel=HERE/lock['filename']
 if wheel.stat().st_size!=lock['bytes'] or digest(wheel)!=lock['sha256']:raise RuntimeError('Wheel hash/size mismatch')
 _,_,_,tags=parse_wheel_filename(wheel.name)
 if not tags.intersection(sys_tags()):raise RuntimeError('Pinned wheel incompatible with this interpreter')
 current=json.loads((PROTOCOL/'artifact-lock.json').read_text())['current']
 if Path(sys.executable).absolute()!=Path(current['python']).absolute():raise RuntimeError('Frozen current interpreter required')
 enrollment=PROTOCOL.parent/'results/enrollment.json'
 record['enrollment_sha256_before']=digest(enrollment)
 before=artifacts();record['before']=before;record['input_before']=check(enrollment)
 if any(name.lower()=='psutil' for name,version in before['current']['runtime']['packages']):raise RuntimeError('Expected absent psutil; no overwrite/retry')
 candidates=[Path('/home/danielking/.local/bin/uv'),Path('/home/danielking/.cargo/bin/uv')]
 uv=next((p for p in candidates if p.is_file() and os.access(p,os.X_OK)),None)
 if uv is None:raise RuntimeError('Existing normal uv not found; no installer substitution')
 record['installer']={'path':str(uv),'sha256':digest(uv),'version':run('installer-version',[str(uv),'--version'],15)}
 # Offline, no dependencies/build/cache/network; same interpreter/environment.
 run('install',[str(uv),'pip','install','--python',current['python'],'--no-index','--no-deps','--no-build','--no-cache',str(wheel)],120)
 record['psutil_import']=json.loads(run('psutil-import',[current['python'],'-c',"import json,psutil;print(json.dumps({'version':psutil.__version__,'file':psutil.__file__}))"],15))
 if record['psutil_import']['version']!='7.2.2':raise RuntimeError('Imported version mismatch')
 after=artifacts();record['after']=after;record['input_after']=check(enrollment)
 before_packages=before['current']['runtime']['packages'];after_packages=after['current']['runtime']['packages']
 if sorted(after_packages)!=sorted(before_packages+[['psutil','7.2.2']]):raise RuntimeError('Unexpected current package inventory change')
 for name in ('old','hail'):
  if after[name]!=before[name]:raise RuntimeError('Untargeted runtime changed:'+name)
 record['enrollment_sha256_after']=digest(enrollment)
 if record['enrollment_sha256_after']!=record['enrollment_sha256_before']:raise RuntimeError('Initial input lock changed')
 record['owned_groups_clean']=all(c['owned_group_clean'] for c in commands);record['success']=True
except BaseException as error:record['error']=repr(error)
finally:
 (OUT/'result.json').write_text(json.dumps(record,indent=2)+'\n')
if not record['success']:raise SystemExit(1)
archive=HERE/'receipt.tar.gz';total=0
with tarfile.open(archive,'w:gz') as target:
 for path in sorted(OUT.rglob('*')):
  if path.is_file():
   total+=path.stat().st_size
   if total>20*1024**2:raise RuntimeError('20MiB receipt cap')
   target.add(path,arcname=str(path.relative_to(OUT)),recursive=False)
print(json.dumps({'archive':str(archive),'sha256':digest(archive),'bytes':archive.stat().st_size,'stage':'provision'}),flush=True)
