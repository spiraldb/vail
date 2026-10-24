#!/bin/bash
set -euo pipefail
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
b=/home/danielking/parallel-vcf-export
mkdir -p "$b/source" "$b/wheels" "$b/results"
tar -xzf /home/danielking/vail-parallel-vcf-source.tar.gz -C "$b/source"
tar -xzf /home/danielking/vail-parallel-vcf-overlay.tar.gz -C "$b/source"
uv venv --python /home/danielking/cloud-scan-20261023/vail-env/bin/python "$b/env"
uv pip install --python "$b/env/bin/python" maturin==1.15.0 numpy pyarrow pytest
export CARGO_BUILD_JOBS=16 CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/home/danielking/entries-count-candidate/target
export RUSTFLAGS='-C target-cpu=native -C link-arg=-fuse-ld=lld'
cd "$b/source/vail"
"$b/env/bin/maturin" build --release --locked --interpreter "$b/env/bin/python" --out "$b/wheels"
uv pip install --python "$b/env/bin/python" "$b"/wheels/*.whl
"$b/env/bin/python" -m pytest python/tests/test_vcf_export_native.py -q > "$b/results/vcf-tests.log" 2>&1
sha256sum "$b"/env/lib/python*/site-packages/vail/_native.abi3.so > "$b/results/native-sha256.txt"
printf '%s\n' ready > "$b/results/build-complete"
