from pathlib import Path
import argparse,hashlib,json,tarfile
p=argparse.ArgumentParser();p.add_argument('host_output',type=Path);a=p.parse_args();r=a.host_output
receipt=json.loads((r/'result.json').read_text())
if not (receipt['error'] is None and receipt['guest_complete'] and receipt['download_verified'] and receipt['provider_stopped'] and receipt['provider_final_status']=='TERMINATED'):raise RuntimeError('Host stage is not accepted complete/stopped')
archive=r/'stage.tar.gz'
with archive.open('rb') as f:
 if hashlib.file_digest(f,'sha256').hexdigest()!=receipt['guest_result']['sha256']:raise RuntimeError('Archive SHA mismatch')
out=r/'recovered';out.mkdir(exist_ok=False);total=0;seen=set()
with tarfile.open(archive) as t:
 for m in t:
  target=out/m.name
  canonical=Path(m.name)
  if canonical.is_absolute() or any(x in ('..','.') for x in m.name.split('/')) or str(canonical)!=m.name or m.name in seen or not m.isfile() or not target.resolve().is_relative_to(out.resolve()):raise RuntimeError('Noncanonical/duplicate/unsafe archive path')
  seen.add(m.name)
  total+=m.size
  if total>1024**3:raise RuntimeError('Extraction size cap')
  target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(t.extractfile(m).read())
print(json.dumps(dict(stage=receipt['stage'],members=len(seen),bytes=total,provider_stopped=True)))
