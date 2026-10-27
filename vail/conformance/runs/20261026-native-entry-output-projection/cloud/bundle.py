"""Package exactly the complete source and protocol assets, without building."""
import argparse,hashlib,json,tarfile
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--root',required=True);p.add_argument('--out',required=True);a=p.parse_args()
root=Path(a.root).resolve();here=Path(__file__).resolve().parent;out=Path(a.out).resolve();out.mkdir(parents=True,exist_ok=False)
e=json.loads((here/'expected.json').read_text())
for name,sha in e['candidate_source_files'].items():
    if hashlib.sha256((root/name).read_bytes()).hexdigest()!=sha:raise RuntimeError(f'Source asset differs:{name}')
with tarfile.open(out/'source.tar.gz','w:gz',dereference=True) as archive:
    for name in sorted(e['candidate_source_files']):archive.add(root/name,arcname=name,recursive=False)
assets=json.loads((here/'ASSETS.json').read_text())
for name,sha in assets['protocol_files'].items():
    if hashlib.sha256((here/name).read_bytes()).hexdigest()!=sha:raise RuntimeError(f'Protocol asset differs:{name}')
with tarfile.open(out/'protocol.tar.gz','w:gz') as archive:
    for name in sorted(assets['protocol_files']):archive.add(here/name,arcname=name,recursive=False)
    archive.add(here/'ASSETS.json',arcname='ASSETS.json')
print(json.dumps({name:hashlib.sha256((out/name).read_bytes()).hexdigest() for name in ('source.tar.gz','protocol.tar.gz')}))
