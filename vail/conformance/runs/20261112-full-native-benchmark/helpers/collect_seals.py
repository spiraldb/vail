"""After final hashing, preserve bounded post-packaging stage metadata."""
import hashlib,json,tarfile,time
from pathlib import Path
B=Path('/home/danielking/native-benchmark-20261112-attempt2')
def read_bounded(path):
 size=path.stat().st_size
 assert size<=32*1024**2
 with path.open('rb')as stream:data=stream.read(32*1024**2+1)
 assert len(data)==size and len(data)<=32*1024**2
 return data
names=['enroll','preflight','rep1','rep2','rep3','final']
paths=[]
for name in names:
 result=B/'results'/(name+'-supervisor')/'result.json'
 assert json.loads(result.read_text())['success']is True
 paths += [result,B/'results'/(name+'-packed.json'),B/(name+'-launch.log')]
assets=json.loads((B/'protocol/ASSETS.json').read_text())
for name,expected in assets.items():
 path=B/'protocol'/name
 assert hashlib.sha256(read_bounded(path)).hexdigest()==expected,name
 paths.append(path)
paths += [B/'protocol/ASSETS.json',B/'artifact-lock.json',B/'runtime-ready.exit',
          B/'results/runtime-gate/complete.json',B/'results/final-enrollment.json']
assert json.loads((B/'results/final-enrollment.json').read_text())['inputs']['full_sha256']is True
files={}
total=0
for path in paths:
 assert path.is_file()
 planned=path.stat().st_size
 assert planned<=32*1024**2 and total+planned<=64*1024**2
 data=read_bounded(path);total+=len(data)
 assert len(data)<=32*1024**2 and total<=64*1024**2
 files[str(path.relative_to(B))]={'bytes':len(data),'sha256':hashlib.sha256(data).hexdigest()}
receipt={'completed_unix':time.time(),'all_stage_post_pack_results_success':True,
         'final_input_full_sha256':True,'final_protocol_assets_match':True,'files':files}
receipt_path=B/'post-pack-seal.json';receipt_path.write_text(json.dumps(receipt,indent=2)+'\n')
archive=B/'post-pack-seals.tar.gz'
with tarfile.open(archive,'w:gz')as t:
 for path in paths+[receipt_path]:t.add(path,arcname=str(path.relative_to(B)),recursive=False)
print(json.dumps({'archive':str(archive),'bytes':archive.stat().st_size,'sha256':hashlib.sha256(archive.read_bytes()).hexdigest(),'files':len(files)}),flush=True)
