export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
ENTRY_PROJ="$HOME/native-entry-projection"
set -euo pipefail
cd "$ENTRY_PROJ/source/vail"
export CARGO_BUILD_JOBS=24 CARGO_INCREMENTAL=0 CARGO_TARGET_DIR="$HOME/legacy-join-scaling/target"
export RUSTFLAGS='-C target-cpu=native -C link-arg=-fuse-ld=lld'
"$HOME/legacy-join-scaling/env/bin/maturin" build --release --locked --interpreter "$ENTRY_PROJ/env/bin/python" --out "$ENTRY_PROJ/wheels"
uv pip install --python "$ENTRY_PROJ/env/bin/python" "$ENTRY_PROJ"/wheels/*.whl
sha256sum "$ENTRY_PROJ"/wheels/*.whl > "$ENTRY_PROJ/wheel.sha256"
touch "$ENTRY_PROJ/build-complete"
