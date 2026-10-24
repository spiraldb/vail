#!/bin/bash
set -euo pipefail
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
R="$HOME/h089-boolean-integration"
S="$HOME/finite-stddev-20261024/source"
python3 - <<'PY'
import pathlib,hashlib,tarfile,os,json
s=pathlib.Path.home()/'finite-stddev-20261024/source/vail';r=pathlib.Path.home()/'h089-boolean-integration'
def files():return [p for p in s.rglob('*') if p.is_file() and (p.suffix in ['.rs','.toml'] or p.name=='Cargo.lock')]
old={str(p.relative_to(s)):hashlib.sha256(p.read_bytes()).hexdigest() for p in files()}
with tarfile.open(r/'source.tar.gz') as a:a.extractall(s.parent)
changed=[]
for p in files():
 key=str(p.relative_to(s));sha=hashlib.sha256(p.read_bytes()).hexdigest()
 if old.get(key)!=sha:os.utime(p,None);changed.append({'file':key,'before':old.get(key),'after':sha})
(r/'results/source-overlay.json').write_text(json.dumps(changed,indent=2)+'\n')
PY
uv venv "$R/env" --python "$HOME/h092-segmented-reducers/env/bin/python"
uv pip install --python "$R/env/bin/python" numpy pytest scipy pandas psutil
export CARGO_TARGET_DIR="$HOME/cloud-scan-20261023/target" CARGO_BUILD_JOBS=16 CARGO_INCREMENTAL=0
export RUSTFLAGS='-C target-cpu=native'
mkdir -p "$R/wheels"
cd "$S/vail"
"$HOME/cloud-scan-20261023/vail-env/bin/maturin" build --release --locked --interpreter "$R/env/bin/python" --out "$R/wheels" > "$R/results/candidate-build.log" 2>&1
uv pip install --python "$R/env/bin/python" "$R"/wheels/*.whl
set +e
"$R/env/bin/python" -m pytest -q python/tests/test_composite_entry_totals.py python/tests/test_segmented_boolean_reducers.py python/tests/test_segmented_integer_reducers.py python/tests/test_call_index.py python/tests/test_call_value.py python/tests/test_genotype_index_call.py python/tests/test_split_multi_hts.py python/tests/test_genotype_likelihoods.py python/tests/test_finite_integer_stddev.py python/tests/test_coordinate_stddev.py python/tests/test_agg_numeric.py python/tests/test_row_fraction_shared_contexts.py python/tests/test_reduction_expression_sharing.py python/tests/test_randomness.py python/tests/test_list_collect_identity.py python/tests/test_shared_list_reducers.py python/tests/test_nullable_integer_sum.py python/tests/test_matrix_entry_sum.py python/tests/test_collection_filter_execution.py python/tests/test_collection_aggregate.py python/tests/test_array_agg_coordinate_callbacks.py python/tests/test_matrix_array_agg.py --junitxml /home/danielking/h089-boolean-integration/results/tests.xml > "$R/results/tests.log" 2>&1
status=$?
set -e
[ "$status" -le 1 ]
printf "%s\n" "$status" > "$R/results/test-exitcode"
touch "$R/complete"
