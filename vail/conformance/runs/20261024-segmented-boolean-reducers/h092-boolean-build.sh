#!/bin/bash
set -euo pipefail
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
root=/home/danielking/h092-boolean-reducers
src=/home/danielking/finite-stddev-20261024/source/vail
mkdir -p "$root/results" "$root/wheels"
sudo shutdown -c || true
sudo shutdown -h +240
# Baseline uses the independently installed integer candidate, before production overlay.
cp /home/danielking/h092-boolean-test.py "$src/python/tests/test_segmented_boolean_reducers.py"
cd "$src"
set +e
/home/danielking/h092-segmented-reducers/env/bin/python -m pytest -q python/tests/test_segmented_boolean_reducers.py python/tests/test_segmented_integer_reducers.py python/tests/test_collection_filter_execution.py python/tests/test_collection_aggregate.py python/tests/test_array_agg_coordinate_callbacks.py python/tests/test_matrix_array_agg.py python/tests/test_randomness.py python/tests/test_reduction_expression_sharing.py --junitxml "$root/results/baseline-tests.xml" > "$root/results/baseline-tests.log" 2>&1
status=$?
set -e
if test "$status" -gt 1; then exit "$status"; fi
python3 - <<'CHECK'
import xml.etree.ElementTree as ET
root=ET.parse('/home/danielking/h092-boolean-reducers/results/baseline-tests.xml')
failures=[c.get('name') for c in root.findall('.//testcase') if c.find('failure') is not None or c.find('error') is not None]
assert failures == ['test_collection_aggregate_matrix_scope_and_lookup_context'],failures
CHECK
python3 - <<'PY'
import hashlib,json,pathlib,tarfile,os
src=pathlib.Path('/home/danielking/finite-stddev-20261024/source/vail')
with tarfile.open('/home/danielking/h092-boolean-source.tar') as archive:
 paths=[src.parent/m.name for m in archive.getmembers() if m.isfile()]
 old={str(p.relative_to(src)):hashlib.sha256(p.read_bytes()).hexdigest() if p.exists() else None for p in paths}
 archive.extractall(src.parent)
 changed=[]
 for p in paths:
  new=hashlib.sha256(p.read_bytes()).hexdigest();key=str(p.relative_to(src))
  if old.get(key)!=new:
   os.utime(p,None);changed.append({'path':key,'before':old.get(key),'after':new,'mtime_ns':p.stat().st_mtime_ns})
 pathlib.Path('/home/danielking/h092-boolean-reducers/results/source-overlay.json').write_text(json.dumps(changed,indent=2)+'\n')
PY
uv venv --python /home/danielking/finite-stddev-20261024/base-env/bin/python "$root/env"
uv pip install --python "$root/env/bin/python" numpy pyarrow pytest scipy pandas
export CARGO_BUILD_JOBS=16 CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/home/danielking/cloud-scan-20261023/target
export RUSTFLAGS='-C target-cpu=native'
cd "$src"
date -u > "$root/results/build-start.txt"
/home/danielking/cloud-scan-20261023/vail-env/bin/maturin build --release --locked --interpreter "$root/env/bin/python" --out "$root/wheels"
date -u > "$root/results/build-end.txt"
uv pip install --reinstall-package vail --python "$root/env/bin/python" "$root"/wheels/*.whl
set +e
"$root/env/bin/python" -m pytest -q python/tests/test_segmented_boolean_reducers.py python/tests/test_segmented_integer_reducers.py python/tests/test_collection_filter_execution.py python/tests/test_collection_aggregate.py python/tests/test_array_agg_coordinate_callbacks.py python/tests/test_matrix_array_agg.py python/tests/test_randomness.py python/tests/test_reduction_expression_sharing.py --junitxml "$root/results/candidate-tests.xml" > "$root/results/candidate-tests.log" 2>&1
status=$?
set -e
if test "$status" -gt 1; then exit "$status"; fi
printf 'ready\n' > "$root/results/build-complete"
