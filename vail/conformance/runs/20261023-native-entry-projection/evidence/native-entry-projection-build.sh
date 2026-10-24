#!/bin/bash
set -euo pipefail
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
ENTRY_PROJ="$HOME/native-entry-projection"
mkdir -p "$ENTRY_PROJ/source" "$ENTRY_PROJ/wheels"
tar -xzf "$HOME/native-entry-projection-source.tar.gz" -C "$ENTRY_PROJ/source"
tar -xzf "$HOME/native-entry-projection-changes.tar.gz" -C "$ENTRY_PROJ/source"
uv venv "$ENTRY_PROJ/env" --python "$HOME/legacy-join-scaling/env/bin/python"
uv pip install --python "$ENTRY_PROJ/env/bin/python" numpy scipy pandas psutil pytest
cd "$ENTRY_PROJ/source/vail"
export CARGO_BUILD_JOBS=24 CARGO_INCREMENTAL=0 CARGO_TARGET_DIR="$HOME/legacy-join-scaling/target"
export RUSTFLAGS='-C target-cpu=native -C link-arg=-fuse-ld=lld'
"$HOME/legacy-join-scaling/env/bin/maturin" build --release --locked --interpreter "$ENTRY_PROJ/env/bin/python" --out "$ENTRY_PROJ/wheels"
uv pip install --python "$ENTRY_PROJ/env/bin/python" "$ENTRY_PROJ"/wheels/*.whl
sha256sum "$ENTRY_PROJ"/wheels/*.whl > "$ENTRY_PROJ/wheel.sha256"
touch "$ENTRY_PROJ/build-complete"
