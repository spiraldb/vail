"""Create isolated matched-native routing packages; run from recorded revision."""
import argparse,shutil,subprocess
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--native',type=Path,required=True);p.add_argument('--package-root',type=Path,required=True);a=p.parse_args()
evidence=Path(__file__).resolve().parent
source=evidence.parents[2]/'python/vail'
for label in ['generalbaseline','generalbatch']:
 destination=a.package_root/label/'vail'
 if destination.exists():raise SystemExit(f'Refusing to overwrite {destination}')
 shutil.copytree(source,destination)
 shutil.copy2(a.native,destination/'_native.abi3.so')
 if label=='generalbatch':subprocess.run(['patch','-p4','-i',str(evidence/'prototype.patch')],cwd=destination,check=True)
