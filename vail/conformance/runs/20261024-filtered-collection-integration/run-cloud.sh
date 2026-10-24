#!/bin/bash
set -euo pipefail
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
R="$HOME/filtered-collection-integration-20261024"
S="$HOME/finite-stddev-20261024/source"
mkdir -p "$R/evidence" "$R/wheels"
tar -xzf "$R/source.tar.gz" -C "$S"
cd "$S/vail"
touch crates/vail-execution/src/{calls,expressions,expression_effects,native_stddev}.rs crates/vail-python/src/collection_aggregate.rs crates/vail-execution/tests/{call_allocations,finite_stddev}.rs
sha256sum crates/vail-execution/src/{calls,expressions,expression_effects,native_stddev}.rs crates/vail-python/src/collection_aggregate.rs crates/vail-execution/tests/{call_allocations,finite_stddev}.rs Cargo.lock > "$R/evidence/source.sha256"
uv venv "$R/env" --python "$HOME/h058-filtered-aggregate/env/bin/python"
uv pip install --python "$R/env/bin/python" pytest numpy scipy pandas psutil
export CARGO_TARGET_DIR="$HOME/cloud-scan-20261023/target" CARGO_BUILD_JOBS=16 CARGO_INCREMENTAL=0
export RUSTFLAGS='-C target-cpu=native'
"$HOME/cloud-scan-20261023/vail-env/bin/maturin" build --release --locked --interpreter "$R/env/bin/python" --out "$R/wheels" > "$R/evidence/build.log" 2>&1
uv pip install --python "$R/env/bin/python" "$R"/wheels/*.whl
cargo test --release --locked -p vail-execution --lib direct_call_index -- --nocapture > "$R/evidence/call-rust-tests.log" 2>&1
cargo test --release --locked -p vail-execution --test call_allocations direct_call_index -- --nocapture > "$R/evidence/call-allocation-tests.log" 2>&1
cargo test --release --locked -p vail-execution --test finite_stddev > "$R/evidence/stddev-rust-tests.log" 2>&1
cargo test --release --locked -p vail-execution --test core_aggregates population_stdev > "$R/evidence/stddev-existing-rust-tests.log" 2>&1
set +e
"$R/env/bin/python" -m pytest -q python/tests/test_call_index.py python/tests/test_call_value.py python/tests/test_genotype_index_call.py python/tests/test_split_multi_hts.py python/tests/test_genotype_likelihoods.py python/tests/test_finite_integer_stddev.py python/tests/test_coordinate_stddev.py python/tests/test_agg_numeric.py python/tests/test_row_fraction_shared_contexts.py python/tests/test_reduction_expression_sharing.py python/tests/test_randomness.py python/tests/test_list_collect_identity.py python/tests/test_shared_list_reducers.py python/tests/test_nullable_integer_sum.py python/tests/test_matrix_entry_sum.py python/tests/test_collection_filter_execution.py python/tests/test_collection_aggregate.py python/tests/test_array_agg_coordinate_callbacks.py python/tests/test_matrix_array_agg.py --junitxml "$R/evidence/tests.xml" > "$R/evidence/python-tests.log" 2>&1
status=$?
set -e
[ "$status" -le 1 ]
"$R/env/bin/python" - <<'PY'
import vail,vail._native as n,pathlib,hashlib,json,zipfile,platform,xml.etree.ElementTree as ET
r=pathlib.Path.home()/'filtered-collection-integration-20261024'
summary={'passed':0,'failures':[],'errors':[],'skipped':0}
for case in ET.parse(r/'evidence/tests.xml').findall('.//testcase'):
 key=case.attrib.get('classname','')+'::'+case.attrib['name']
 if case.find('failure') is not None:summary['failures'].append(key)
 elif case.find('error') is not None:summary['errors'].append(key)
 elif case.find('skipped') is not None:summary['skipped']+=1
 else:summary['passed']+=1
(r/'evidence/test-summary.json').write_text(json.dumps(summary,indent=2)+'\n')
assert not summary['errors'],summary
assert len(summary['failures'])==1 and summary['failures'][0].endswith('test_collection_aggregate::test_collection_aggregate_matrix_scope_and_lookup_context'),summary
h=lambda x:hashlib.sha256(x).hexdigest()
w=next((r/'wheels').glob('*.whl'))
with zipfile.ZipFile(w) as z:
 name=next(x for x in z.namelist() if x.endswith('_native.abi3.so'))
 wheelhash=h(z.read(name))
nativehash=h(pathlib.Path(n.__file__).read_bytes());assert nativehash==wheelhash
(r/'evidence/provenance.json').write_text(json.dumps({'source':'0379cd059927aaf3893967b956206b534186eba7','python':vail.__file__,'native':n.__file__,'native_sha256':nativehash,'wheel_native_sha256':wheelhash,'wheel_sha256':h(w.read_bytes()),'machine':platform.uname()._asdict()},indent=2)+'\n')
PY
touch "$R/complete"
