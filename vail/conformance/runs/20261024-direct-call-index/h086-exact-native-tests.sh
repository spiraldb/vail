#!/bin/bash
set -euo pipefail
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
root=/home/danielking/h086-direct-call-index
src=/home/danielking/parallel-vcf-export/source/vail
cat /proc/meminfo > "$root/results/meminfo.txt"
ram_kib=$(awk '/MemTotal/ {print $2}' /proc/meminfo)
if test "$ram_kib" -ge 120000000; then export CARGO_BUILD_JOBS=16; else export CARGO_BUILD_JOBS=8; fi
export CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/home/danielking/entries-count-candidate/target
export RUSTFLAGS='-C target-cpu=native -C link-arg=-fuse-ld=lld'
cd "$src"
touch crates/vail-execution/src/calls.rs crates/vail-execution/src/expressions.rs crates/vail-execution/tests/call_allocations.rs
sha256sum crates/vail-execution/src/calls.rs crates/vail-execution/src/expressions.rs crates/vail-execution/tests/call_allocations.rs > "$root/results/exact-native-test-source.sha256"
cargo test --release --locked -p vail-execution --lib direct_call_index -- --nocapture > "$root/results/candidate-rust-tests.log" 2>&1
cargo test --release --locked -p vail-execution --test call_allocations direct_call_index -- --nocapture > "$root/results/candidate-allocation-test.log" 2>&1
printf 'ready\n' > "$root/results/exact-native-tests-complete"
