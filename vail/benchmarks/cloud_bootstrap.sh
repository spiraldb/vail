#!/usr/bin/env bash
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
sudo shutdown -h +180
sudo apt-get update
sudo apt-get install -y build-essential pkg-config libssl-dev cmake clang lld \
  git curl unzip python3-venv openjdk-11-jre-headless protobuf-compiler \
  flatbuffers-compiler linux-tools-common "linux-tools-$(uname -r)"
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain 1.98.0 --profile minimal
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
uv python install 3.11
uv venv --python 3.11 "$HOME/vail-env"
uv venv --python 3.11 "$HOME/hail-env"
uv pip install --python "$HOME/vail-env/bin/python" maturin==1.15.0 numpy pyarrow pandas psutil matplotlib pytest
uv pip install --python "$HOME/hail-env/bin/python" hail psutil
sudo sysctl -w kernel.perf_event_paranoid=-1
perf stat -e instructions,cycles,cache-misses -- sleep 0.2 2> "$HOME/perf-availability.txt" || true
cd "$HOME/vail"
RUSTC_WRAPPER='' CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=32 \
  RUSTFLAGS='-C target-cpu=native -C link-arg=-fuse-ld=lld' \
  "$HOME/vail-env/bin/maturin" build --release --locked \
  --interpreter "$HOME/vail-env/bin/python" --out "$HOME/wheels"
uv pip install --python "$HOME/vail-env/bin/python" "$HOME"/wheels/*.whl
"$HOME/vail-env/bin/python" -m pytest python/tests/test_parallel_execution.py -q
touch "$HOME/setup-complete"
