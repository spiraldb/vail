#!/bin/bash
set -euo pipefail
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
ENTRY_PROJ="$HOME/native-entry-projection"
test -e "$ENTRY_PROJ/validation-complete"
mkdir -p "$ENTRY_PROJ/baseline-wheels"
tar -xzf "$HOME/native-entry-projection-baseline-source.tar.gz" -C "$ENTRY_PROJ/source"
uv venv "$ENTRY_PROJ/baseline-env" --python "$ENTRY_PROJ/env/bin/python"
uv pip install --python "$ENTRY_PROJ/baseline-env/bin/python" numpy scipy pandas psutil pytest
cd "$ENTRY_PROJ/source/vail"
export CARGO_BUILD_JOBS=24 CARGO_INCREMENTAL=0 CARGO_TARGET_DIR="$HOME/legacy-join-scaling/target"
export RUSTFLAGS='-C target-cpu=native -C link-arg=-fuse-ld=lld'
"$HOME/legacy-join-scaling/env/bin/maturin" build --release --locked --interpreter "$ENTRY_PROJ/baseline-env/bin/python" --out "$ENTRY_PROJ/baseline-wheels"
uv pip install --python "$ENTRY_PROJ/baseline-env/bin/python" "$ENTRY_PROJ"/baseline-wheels/*.whl
mkdir -p "$ENTRY_PROJ/baseline-results"
for case in direct_sum entry_sum entry_sum_entryonly entry_sum_rowonly entry_sum_colonly entry_sum_narrow; do
 "$ENTRY_PROJ/baseline-env/bin/python" "$HOME/native-entry-profile.py" "$case" --repeat 4 > "$ENTRY_PROJ/baseline-results/$case.jsonl" 2> "$ENTRY_PROJ/baseline-results/$case.stderr"
done
touch "$ENTRY_PROJ/baseline-complete"
tar -xzf "$HOME/native-entry-projection-final-changes.tar.gz" -C "$ENTRY_PROJ/source"
"$HOME/legacy-join-scaling/env/bin/maturin" build --release --locked --interpreter "$ENTRY_PROJ/env/bin/python" --out "$ENTRY_PROJ/wheels"
uv pip install --python "$ENTRY_PROJ/env/bin/python" "$ENTRY_PROJ"/wheels/*.whl
mv "$ENTRY_PROJ/results" "$ENTRY_PROJ/early-keyby-fix-results"
bash "$HOME/native-entry-projection-validate.sh"
touch "$ENTRY_PROJ/paired-complete"
