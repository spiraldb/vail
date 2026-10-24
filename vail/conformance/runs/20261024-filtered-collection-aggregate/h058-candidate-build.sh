#!/bin/bash
set -euo pipefail
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
root=/home/danielking/h058-filtered-aggregate
src=/home/danielking/finite-stddev-20261024/source/vail
while ! test -f "$root/results/baseline-complete"; do sleep 5; done
mkdir -p "$root/wheels"
python3 - <<'PY'
import hashlib,json,pathlib,tarfile,os
src=pathlib.Path('/home/danielking/finite-stddev-20261024/source/vail')
paths=lambda:[p for p in src.rglob('*') if p.is_file() and (p.suffix in ['.rs','.toml'] or p.name=='Cargo.lock')]
old={str(p.relative_to(src)):hashlib.sha256(p.read_bytes()).hexdigest() for p in paths()}
with tarfile.open('/home/danielking/h058-candidate-source.tar') as archive:archive.extractall(src.parent)
changed=[]
for p in paths():
 new=hashlib.sha256(p.read_bytes()).hexdigest();key=str(p.relative_to(src))
 if old.get(key)!=new:
  os.utime(p,None);changed.append({'path':key,'before':old.get(key),'after':new,'mtime_ns':p.stat().st_mtime_ns})
pathlib.Path('/home/danielking/h058-filtered-aggregate/results/changed-source-files.json').write_text(json.dumps(changed,indent=2)+'\n')
PY
UV_VENV_CLEAR=1 uv venv --python /home/danielking/finite-stddev-20261024/base-env/bin/python "$root/env"
uv pip install --python "$root/env/bin/python" numpy pyarrow pytest scipy pandas
export CARGO_BUILD_JOBS=16 CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/home/danielking/cloud-scan-20261023/target
export RUSTFLAGS='-C target-cpu=native'
cd "$src"
/home/danielking/cloud-scan-20261023/vail-env/bin/maturin build --release --locked --interpreter "$root/env/bin/python" --out "$root/wheels"
uv pip install --reinstall-package vail --python "$root/env/bin/python" "$root"/wheels/*.whl
sha256sum crates/vail-execution/src/expressions.rs crates/vail-execution/src/expression_effects.rs crates/vail-python/src/collection_aggregate.rs > "$root/results/candidate-source.sha256"
set +e
"$root/env/bin/python" -m pytest -q python/tests/test_collection_filter_execution.py python/tests/test_collection_aggregate.py python/tests/test_array_agg_coordinate_callbacks.py python/tests/test_matrix_array_agg.py python/tests/test_randomness.py python/tests/test_reduction_expression_sharing.py --junitxml "$root/results/candidate-tests.xml" > "$root/results/candidate-public-tests.log" 2>&1
status=$?
set -e
if test "$status" -gt 1; then exit "$status"; fi
printf '%s\n' "$status" > "$root/results/candidate-pytest-exitcode"
python3 - <<'PYTEST'
import json,pathlib,xml.etree.ElementTree as ET
root=pathlib.Path('/home/danielking/h058-filtered-aggregate/results')
summary={}
for version in ['baseline','candidate']:
 tree=ET.parse(root/f'{version}-tests.xml'); failures=[];passed=skipped=0
 for case in tree.findall('.//testcase'):
  module=next((x for x in case.attrib.get('classname','').split('.') if x.startswith('test_')),case.attrib.get('classname',''))
  key=module+'::'+case.attrib['name']
  assert case.find('error') is None,(version,key)
  if case.find('failure') is not None:failures.append(key)
  elif case.find('skipped') is not None:skipped+=1
  else:passed+=1
 summary[version]={'passed':passed,'skipped':skipped,'failures':sorted(failures)}
new=set(summary['candidate']['failures'])-set(summary['baseline']['failures']);assert not new,new
(root/'test-comparison.json').write_text(json.dumps(summary,indent=2)+'\n')
PYTEST
printf 'ready\n' > "$root/results/candidate-build-complete"
