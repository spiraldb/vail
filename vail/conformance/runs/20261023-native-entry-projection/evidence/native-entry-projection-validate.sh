#!/bin/bash
set -euo pipefail
ENTRY_PROJ="$HOME/native-entry-projection"
mkdir -p "$ENTRY_PROJ/results"
"$ENTRY_PROJ/env/bin/python" - <<'PROVENANCE'
import hashlib, json, pathlib, platform, subprocess
import vail._native as native
root=pathlib.Path('/home/danielking/native-entry-projection')
base_code='import hashlib,pathlib,vail._native as n;print(hashlib.sha256(pathlib.Path(n.__file__).read_bytes()).hexdigest())'
baseline=subprocess.check_output([str(root/'baseline-env/bin/python'),'-c',base_code],text=True).strip()
candidate=hashlib.sha256(pathlib.Path(native.__file__).read_bytes()).hexdigest()
assert baseline != candidate, 'Stale candidate: baseline and candidate native hashes match'
files=['crates/vail-table/src/join_interval.rs','crates/vail-table/src/matrix_entries.rs','crates/vail-execution/src/expressions.rs']
receipt={'baseline_native_sha256':baseline,'candidate_native_sha256':candidate,'candidate_native_origin':native.__file__,'source_sha256':{f:hashlib.sha256((root/'source/vail'/f).read_bytes()).hexdigest() for f in files},'executed_public_test_sha256':hashlib.sha256(pathlib.Path('/home/danielking/test_entry_projection.py').read_bytes()).hexdigest(),'machine':platform.uname()._asdict()}
(root/'results/provenance.json').write_text(json.dumps(receipt,indent=2)+'\n')
PROVENANCE
"$ENTRY_PROJ/env/bin/python" -m pytest -q "$HOME/test_entry_projection.py" "$ENTRY_PROJ/source/vail/python/tests/test_matrix_layout.py" "$ENTRY_PROJ/source/vail/python/tests/test_table_joins.py" "$ENTRY_PROJ/source/vail/python/tests/test_interval_partition_joins.py" "$ENTRY_PROJ/source/vail/python/tests/test_interval_reads.py" "$ENTRY_PROJ/source/vail/python/tests/test_partition_operations.py" "$ENTRY_PROJ/source/vail/python/tests/test_count_projection.py" "$ENTRY_PROJ/source/vail/python/tests/test_projection_broadcast.py" > "$ENTRY_PROJ/results/public-tests.log" 2>&1
for case in direct_sum entry_sum entry_sum_entryonly entry_sum_rowonly entry_sum_colonly entry_sum_narrow; do
 "$ENTRY_PROJ/env/bin/python" "$HOME/native-entry-profile.py" "$case" --repeat 4 > "$ENTRY_PROJ/results/$case.jsonl" 2> "$ENTRY_PROJ/results/$case.stderr"
done
touch "$ENTRY_PROJ/validation-complete"
