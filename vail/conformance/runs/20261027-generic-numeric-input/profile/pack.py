"""Pack receipts only, never environments, wheels, source or native binaries."""
import argparse
import json
from pathlib import Path
import tarfile
from common import digest
p = argparse.ArgumentParser()
p.add_argument('out')
a = p.parse_args()
out = Path(a.out).resolve()
files = [path for path in out.rglob('*') if path.is_file() and path.name != 'receipt-manifest.json'
         and not path.is_symlink() and path.relative_to(out).parts[:2] not in
         {('gate', 'candidate-env'), ('gate', 'wheels')}]
manifest = {str(path.relative_to(out)): dict(bytes=path.stat().st_size, sha256=digest(path)) for path in sorted(files)}
manifest_path = out / 'receipt-manifest.json'
manifest_path.write_text(json.dumps(manifest, indent=2)+'\n')
files.append(manifest_path)
archive = out.with_suffix('.receipts.tar.gz')
with tarfile.open(archive, 'w:gz') as target:
    for path in files:
        target.add(path, arcname=str(path.relative_to(out)), recursive=False)
print(json.dumps(dict(archive=str(archive), sha256=digest(archive), bytes=archive.stat().st_size)))
