"""Pin the accepted runtime after normal psutil provisioning, then original stage."""
import argparse,hashlib,json,os,signal,sys
from pathlib import Path
HERE=Path(__file__).resolve().parent
ORIGINAL=HERE.parent/'protocol'
p=argparse.ArgumentParser();p.add_argument('stage',choices=['preflight','rep1','rep2','rep3','final']);a=p.parse_args()
signal.alarm(60)
def digest(path):
 h=hashlib.sha256()
 with Path(path).open('rb') as stream:
  while True:
   data=stream.read(1024**2)
   if not data:break
   h.update(data)
 return h.hexdigest()
assets=json.loads((HERE/'ASSETS.json').read_text())
for name,expected in assets.items():
 if digest(HERE/name)!=expected:raise RuntimeError('Runtime gate asset mismatch:'+name)
state=json.loads((HERE/'accepted-state.json').read_text())
if digest(ORIGINAL/'ASSETS.json')!=state['original_assets_sha256']:raise RuntimeError('Original accepted protocol manifest changed')
if json.loads((ORIGINAL/'ASSETS.json').read_text())!=state['original_assets']:raise RuntimeError('Original accepted asset entries changed')
for name,expected in state['original_assets'].items():
 if digest(ORIGINAL/name)!=expected:raise RuntimeError('Original accepted protocol bytes changed:'+name)
enrollment=ORIGINAL.parent/'results/enrollment.json'
if digest(enrollment)!=state['enrollment_sha256']:raise RuntimeError('Original input enrollment changed')
if digest(HERE/'accepted-after-runtime.json')!=state['accepted_after_sha256']:raise RuntimeError('Accepted after-runtime receipt changed')
# Imported original helper bytes are verified first; no engine import/query.
sys.path.insert(0,str(ORIGINAL))
from enroll import artifacts,check
observed=artifacts();accepted=json.loads((HERE/'accepted-after-runtime.json').read_text())
if observed!=accepted:raise RuntimeError('Runtime/artifact drift since accepted psutil provisioning')
inputs=check(enrollment)
receipt={'accepted_after_sha256':state['accepted_after_sha256'],'original_enrollment_sha256':state['enrollment_sha256'],'artifacts_equal_accepted':True,'input_check':inputs,'stage':a.stage}
proof=HERE/(a.stage+'-gate.json');proof.write_text(json.dumps(receipt,indent=2)+'\n')
print(json.dumps({'runtime_gate_receipt':receipt,'sha256':digest(proof)}),flush=True)
# Exact original stage/action code remains unchanged and checks its own assets.
python=observed['current']['python']
if Path(sys.executable).absolute()!=Path(python).absolute():raise RuntimeError('Frozen interpreter required')
os.execv(python,[python,str(ORIGINAL/'stage.py'),a.stage])
