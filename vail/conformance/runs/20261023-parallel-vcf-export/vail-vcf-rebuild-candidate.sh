#!/bin/bash
set -euo pipefail
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
b=/home/danielking/parallel-vcf-export
"$b/env/bin/python" - <<'PY'
import pathlib,re,shutil
b=pathlib.Path('/home/danielking/parallel-vcf-export/results'); invalid=b/'invalid-attempt-source-mtime';invalid.mkdir(exist_ok=True)
for p in b.iterdir():
 if re.fullmatch(r'(candidate|baseline|hail)-(legacy|native)-\d+',p.name):shutil.move(str(p),invalid/p.name)
p=pathlib.Path('/home/danielking/parallel-vcf-measure.log')
if p.exists(): shutil.move(str(p),invalid/'measurement.log')
PY
export CARGO_BUILD_JOBS=16 CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/home/danielking/entries-count-candidate/target
export RUSTFLAGS='-C target-cpu=native -C link-arg=-fuse-ld=lld'
cd "$b/source/vail"
touch crates/vail-table/src/vcf_export.rs crates/vail-table/src/compressed_text.rs
"$b/env/bin/maturin" build --release --locked --interpreter "$b/env/bin/python" --out "$b/wheels"
uv pip install --reinstall-package vail --python "$b/env/bin/python" "$b"/wheels/*.whl
sha256sum "$b"/env/lib/python*/site-packages/vail/_native.abi3.so > "$b/results/native-sha256.txt"
"$b/env/bin/python" - <<'PY'
import hashlib,pathlib,zipfile
b=pathlib.Path('/home/danielking/parallel-vcf-export')
w=next((b/'wheels').glob('*.whl'))
with zipfile.ZipFile(w) as z:
 n=next(n for n in z.namelist() if n.endswith('_native.abi3.so'));wheel=hashlib.sha256(z.read(n)).hexdigest()
installed=hashlib.sha256(next((b/'env/lib').glob('python*/site-packages/vail/_native.abi3.so')).read_bytes()).hexdigest()
baseline=hashlib.sha256(next((b/'baseline-env/lib').glob('python*/site-packages/vail/_native.abi3.so')).read_bytes()).hexdigest()
assert installed==wheel,(installed,wheel)
assert installed!=baseline,(installed,baseline)
(b/'results/native-verified.json').write_text(__import__('json').dumps(dict(installed=installed,wheel=wheel,baseline=baseline),indent=2))
PY
"$b/env/bin/python" -m pytest python/tests/test_vcf_export_native.py -q > "$b/results/vcf-final-tests.log" 2>&1
printf '%s\n' ready > "$b/results/candidate-rebuild-complete"
