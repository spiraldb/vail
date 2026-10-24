#!/bin/bash
set -euo pipefail
R="$HOME/segmented-integer-integration-20261024"
S="$HOME/finite-stddev-20261024/source/vail"
P="$HOME/h092-segmented-reducers/env/bin/python"
mkdir -p "$R"
cd "$S"
sha256sum crates/vail-execution/src/{calls,expression_effects,expressions,list_aggregate,native_stddev}.rs crates/vail-execution/src/list_aggregate/direct.rs crates/vail-python/src/collection_aggregate.rs Cargo.lock > "$R/source.sha256"
"$P" - <<'PY'
import pathlib,hashlib,json,vail,vail._native as n
h=hashlib.sha256(pathlib.Path(n.__file__).read_bytes()).hexdigest()
assert h=='4d46e99d743e7acac6dc3fe3b9a8fe9e2fe70e93d87fc14357ee75634525fd9f',h
r=pathlib.Path.home()/'segmented-integer-integration-20261024'
(r/'provenance.json').write_text(json.dumps({'source':'af97646b1391d722ec4c49e4f4e1702accc10078','python':vail.__file__,'native':n.__file__,'native_sha256':h,'build':'exact unchanged approved normal H092 installed environment; no source edits or rebuild'},indent=2)+'\n')
PY
set +e
"$P" -m pytest -q python/tests/test_segmented_integer_reducers.py python/tests/test_call_index.py python/tests/test_call_value.py python/tests/test_genotype_index_call.py python/tests/test_split_multi_hts.py python/tests/test_genotype_likelihoods.py python/tests/test_finite_integer_stddev.py python/tests/test_coordinate_stddev.py python/tests/test_agg_numeric.py python/tests/test_row_fraction_shared_contexts.py python/tests/test_reduction_expression_sharing.py python/tests/test_randomness.py python/tests/test_list_collect_identity.py python/tests/test_shared_list_reducers.py python/tests/test_nullable_integer_sum.py python/tests/test_matrix_entry_sum.py python/tests/test_collection_filter_execution.py python/tests/test_collection_aggregate.py python/tests/test_array_agg_coordinate_callbacks.py python/tests/test_matrix_array_agg.py --junitxml "$R/tests.xml" > "$R/python-tests.log" 2>&1
status=$?
set -e
[ "$status" -le 1 ]
"$P" - <<'PY'
import pathlib,json,xml.etree.ElementTree as ET
r=pathlib.Path.home()/'segmented-integer-integration-20261024'
s={'passed':0,'failures':[],'errors':[],'skipped':0}
for c in ET.parse(r/'tests.xml').findall('.//testcase'):
 key=c.attrib.get('classname','')+'::'+c.attrib['name']
 if c.find('failure') is not None:s['failures'].append(key)
 elif c.find('error') is not None:s['errors'].append(key)
 elif c.find('skipped') is not None:s['skipped']+=1
 else:s['passed']+=1
(r/'summary.json').write_text(json.dumps(s,indent=2)+'\n')
assert not s['errors'],s
assert len(s['failures'])==1 and s['failures'][0].endswith('test_collection_aggregate::test_collection_aggregate_matrix_scope_and_lookup_context'),s
PY
touch "$R/complete"
