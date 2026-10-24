#!/bin/bash
set -euo pipefail
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
STDROOT="$HOME/finite-stddev-20261024"
mkdir -p "$STDROOT/source" "$STDROOT/reference" "$STDROOT/evidence" "$STDROOT/base-wheels" "$STDROOT/candidate-wheels"
tar -xzf "$STDROOT/main.tar.gz" -C "$STDROOT/source"
tar -xzf "$STDROOT/candidate.tar.gz" -C "$STDROOT/reference"
cp "$STDROOT/reference/vail/python/tests/test_finite_integer_stddev.py" "$STDROOT/source/vail/python/tests/"
cp "$STDROOT/reference/vail/crates/vail-execution/tests/finite_stddev.rs" "$STDROOT/source/vail/crates/vail-execution/tests/"
uv venv "$STDROOT/base-env" --python "$HOME/cloud-scan-20261023/vail-env/bin/python"
uv venv "$STDROOT/candidate-env" --python "$HOME/cloud-scan-20261023/vail-env/bin/python"
uv pip install --python "$STDROOT/base-env/bin/python" pytest numpy scipy pandas psutil
uv pip install --python "$STDROOT/candidate-env/bin/python" pytest numpy scipy pandas psutil
export CARGO_TARGET_DIR="$HOME/cloud-scan-20261023/target" CARGO_BUILD_JOBS=24 CARGO_INCREMENTAL=0
export RUSTFLAGS='-C target-cpu=native'
cd "$STDROOT/source/vail"
sha256sum crates/vail-execution/src/native_stddev.rs > "$STDROOT/evidence/baseline-source.sha256"
"$HOME/cloud-scan-20261023/vail-env/bin/maturin" build --release --locked --interpreter "$STDROOT/base-env/bin/python" --out "$STDROOT/base-wheels" > "$STDROOT/evidence/base-build.log" 2>&1
uv pip install --python "$STDROOT/base-env/bin/python" "$STDROOT"/base-wheels/*.whl
cargo test --release --locked -p vail-execution --test finite_stddev > "$STDROOT/evidence/base-rust-tests.log" 2>&1
"$STDROOT/base-env/bin/python" -m pytest -q python/tests/test_finite_integer_stddev.py python/tests/test_coordinate_stddev.py python/tests/test_agg_numeric.py python/tests/test_row_fraction_shared_contexts.py python/tests/test_reduction_expression_sharing.py python/tests/test_randomness.py > "$STDROOT/evidence/base-python-tests.log" 2>&1
"$STDROOT/base-env/bin/python" "$STDROOT/measure.py" prepare --path "$STDROOT/data.ht" > "$STDROOT/evidence/prepare.json"
"$STDROOT/base-env/bin/python" "$STDROOT/measure.py" global --path "$STDROOT/data.ht" > "$STDROOT/evidence/base-global.jsonl"
"$STDROOT/base-env/bin/python" "$STDROOT/measure.py" grouped --path "$STDROOT/data.ht" > "$STDROOT/evidence/base-grouped.jsonl"
cp "$STDROOT/reference/vail/crates/vail-execution/src/native_stddev.rs" crates/vail-execution/src/native_stddev.rs
sha256sum crates/vail-execution/src/native_stddev.rs > "$STDROOT/evidence/candidate-source.sha256"
"$HOME/cloud-scan-20261023/vail-env/bin/maturin" build --release --locked --interpreter "$STDROOT/candidate-env/bin/python" --out "$STDROOT/candidate-wheels" > "$STDROOT/evidence/candidate-build.log" 2>&1
uv pip install --python "$STDROOT/candidate-env/bin/python" "$STDROOT"/candidate-wheels/*.whl
cargo test --release --locked -p vail-execution --test finite_stddev > "$STDROOT/evidence/candidate-rust-tests.log" 2>&1
cargo test --release --locked -p vail-execution --test core_aggregates population_stdev > "$STDROOT/evidence/candidate-existing-rust-tests.log" 2>&1
"$STDROOT/candidate-env/bin/python" -m pytest -q python/tests/test_finite_integer_stddev.py python/tests/test_coordinate_stddev.py python/tests/test_agg_numeric.py python/tests/test_row_fraction_shared_contexts.py python/tests/test_reduction_expression_sharing.py python/tests/test_randomness.py > "$STDROOT/evidence/candidate-python-tests.log" 2>&1
"$STDROOT/candidate-env/bin/python" "$STDROOT/measure.py" global --path "$STDROOT/data.ht" > "$STDROOT/evidence/candidate-global.jsonl"
"$STDROOT/candidate-env/bin/python" "$STDROOT/measure.py" grouped --path "$STDROOT/data.ht" > "$STDROOT/evidence/candidate-grouped.jsonl"
"$STDROOT/candidate-env/bin/python" - <<'PY'
import hashlib,json,pathlib,platform,subprocess
root=pathlib.Path.home()/'finite-stddev-20261024'
receipts={}
for mode in ['base','candidate']:
 code='import hashlib,pathlib,json,vail,vail._native as n;print(json.dumps({"python":vail.__file__,"native":n.__file__,"native_sha256":hashlib.sha256(pathlib.Path(n.__file__).read_bytes()).hexdigest()}))'
 receipts[mode]=json.loads(subprocess.check_output([str(root/f'{mode}-env/bin/python'),'-c',code],text=True))
receipts['machine']=platform.uname()._asdict()
receipts['baseline_source']='0eec407731c5b39829316f8edf3f463e544b6888'
receipts['candidate_source']='ded3db88810feee749e8ffa617c0e8f20a4ecd8b'
receipts['source_sha256']={p:hashlib.sha256((root/'source/vail'/p).read_bytes()).hexdigest() for p in ['crates/vail-execution/src/native_stddev.rs','crates/vail-execution/tests/finite_stddev.rs','python/tests/test_finite_integer_stddev.py','Cargo.lock']}
assert receipts['base']['native_sha256']!=receipts['candidate']['native_sha256']
(root/'evidence/provenance.json').write_text(json.dumps(receipts,indent=2)+'\n')
PY
touch "$STDROOT/complete"
