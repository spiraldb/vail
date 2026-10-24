#!/bin/bash
set -euo pipefail
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
R="$HOME/call-stddev-integration-20261024"
S="$HOME/parallel-vcf-export/source"
mkdir -p "$R/evidence" "$R/wheels"
tar -xzf "$R/source.tar.gz" -C "$S"
cd "$S/vail"
touch crates/vail-execution/src/{calls,expressions,native_stddev}.rs crates/vail-execution/tests/{call_allocations,finite_stddev}.rs
sha256sum crates/vail-execution/src/{calls,expressions,native_stddev}.rs crates/vail-execution/tests/{call_allocations,finite_stddev}.rs Cargo.lock > "$R/evidence/source.sha256"
uv venv "$R/env" --python "$HOME/h086-direct-call-index/candidate-env/bin/python"
uv pip install --python "$R/env/bin/python" pytest numpy scipy pandas psutil
export CARGO_TARGET_DIR="$HOME/entries-count-candidate/target" CARGO_BUILD_JOBS=16 CARGO_INCREMENTAL=0
export RUSTFLAGS='-C target-cpu=native -C link-arg=-fuse-ld=lld'
"$HOME/parallel-vcf-export/env/bin/maturin" build --release --locked --interpreter "$R/env/bin/python" --out "$R/wheels" > "$R/evidence/build.log" 2>&1
uv pip install --python "$R/env/bin/python" "$R"/wheels/*.whl
cargo test --release --locked -p vail-execution --lib direct_call_index -- --nocapture > "$R/evidence/call-rust-tests.log" 2>&1
cargo test --release --locked -p vail-execution --test call_allocations direct_call_index -- --nocapture > "$R/evidence/call-allocation-tests.log" 2>&1
cargo test --release --locked -p vail-execution --test finite_stddev > "$R/evidence/stddev-rust-tests.log" 2>&1
cargo test --release --locked -p vail-execution --test core_aggregates population_stdev > "$R/evidence/stddev-existing-rust-tests.log" 2>&1
"$R/env/bin/python" -m pytest -q python/tests/test_call_index.py python/tests/test_call_value.py python/tests/test_genotype_index_call.py python/tests/test_split_multi_hts.py python/tests/test_genotype_likelihoods.py python/tests/test_finite_integer_stddev.py python/tests/test_coordinate_stddev.py python/tests/test_agg_numeric.py python/tests/test_row_fraction_shared_contexts.py python/tests/test_reduction_expression_sharing.py python/tests/test_randomness.py python/tests/test_list_collect_identity.py python/tests/test_shared_list_reducers.py python/tests/test_nullable_integer_sum.py python/tests/test_matrix_entry_sum.py > "$R/evidence/python-tests.log" 2>&1
"$R/env/bin/python" - <<'PY'
import vail,vail._native as n,pathlib,hashlib,json,zipfile,platform
r=pathlib.Path.home()/'call-stddev-integration-20261024'
h=lambda x:hashlib.sha256(x).hexdigest()
w=next((r/'wheels').glob('*.whl'))
with zipfile.ZipFile(w) as z:
 name=next(x for x in z.namelist() if x.endswith('_native.abi3.so'))
 wheelhash=h(z.read(name))
nativehash=h(pathlib.Path(n.__file__).read_bytes());assert nativehash==wheelhash
(r/'evidence/provenance.json').write_text(json.dumps({'source':'6c6038be6c044fde30af7812b010ad902ee2c13b','python':vail.__file__,'native':n.__file__,'native_sha256':nativehash,'wheel_native_sha256':wheelhash,'wheel_sha256':h(w.read_bytes()),'machine':platform.uname()._asdict()},indent=2)+'\n')
PY
touch "$R/complete"
