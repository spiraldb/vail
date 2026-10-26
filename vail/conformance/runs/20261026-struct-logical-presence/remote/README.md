# Frozen cloud gate (prepared only; not executed)

Run only after the VM's current compute owner explicitly releases its slot.
No local Rust build, timing run, new VM or source publication is performed by
these preparation scripts. Root must freeze/review the source revision first.

Requirements: Linux POSIX process groups; Python3.11+ for the driver, supported
Python3.12 for the wheel/test environment; uv; kache; the workspace-pinned
Rust1.98.0 toolchain; ordinary native build tools (C/C++ compiler, linker,
pkg-config and SSL headers). The caller supplies cold/warm build budgets,
runtime budget, job count, writable stable target path and free-space floor.
The archived Cargo.lock/patches and exact public test list are retained.
Network is needed only for normal toolchain/crate/Python dependency resolution.
No private data fixtures or cloud service credentials are needed by these tests.

Prepare from a reviewed committed ref (uncompressed tracked source export,
roughly30MB; no submodule histories, benchmarks, profiles or compiled artifacts):

```sh
python3 make_source_bundle.py --repo /path/to/repo --ref REVIEWED_COMMIT --out /path/to/source.tar
```

Transfer source.tar and its JSON/SHA sidecars. Verify the sidecar checksum before
extracting. The archive contains vail/ with its source and frozen gate scripts;
source-manifest.json binds the exported commit and critical production/test files.
The generator requires its own scripts/test list to be committed at the chosen
ref. It refuses to overwrite an existing export.

From the extracted vail/ directory, run the driver with explicit approved budgets:

```sh
python3 conformance/runs/20261026-struct-logical-presence/remote/run_cloud_gate.py \
  --out /absolute/new-run-directory --target /absolute/stable-target \
  --python /path/to/python3.12 --jobs JOBS --disk-floor-gib FLOOR \
  --wheel-budget SECONDS --table-budget SECONDS --storage-budget SECONDS \
  --runtime-budget SECONDS
```

The output directory must be new, preventing accidental duplicate/resumed runs.
The driver verifies source hashes, records versions/host/flags, builds a normal
locked wheel, installs it into an independent environment and verifies import
provenance. It runs all33frozen Python test files (at least431cases, no skips),
then the whole public Rust targets: encoded_projection(at least34serial),
struct_field_projection_contract(at least4), native_list_evolution(at least2).
It never filters individual cases or retries a failed stage. Budget/floor failure
terminates only its own process group and preserves output before stopping.
status.json and per-stage progress.jsonl support read-only progress reports.
All final wheel/native/executable and log hashes are recorded.

The large IO fixture counts actual requested/completed/unique application-reader
traffic and independently checks values. Tiny/many-file windows are limited
startup/reuse observations, not randomized trials or throughput/core-second/ALU
claims. Linux/cloud timings must not be combined with earlier M3 timings as a
speedup. No native capability/translator/admission expansion is included.

## Approved cloud budgets and cache-child verification

cloud-plan.json and invoke_cloud_gate.sh freeze jobs8, wheel1200s,
table600s, storage600s, runtime180s and10GiBfree-space floor. Root supplies only
the extracted source, new output, stable target and existing Python3.12paths.
Do not upload/start until root releases the upstream owner's first compute slot.

Timeout/floor/error stops only the driver's own process group. Persistent kache
may already have dispatched compiler children in another group. The driver then
observes same-user compilers and kache descendants through Linux/proc for at most
180s, recording PID/start ticks, without signaling the cache daemon. Remaining
workers make the gate blocked and prohibit automatic handoff. Successful final
completion also verifies compiler-child idleness. This is not a general disk,
network, cache-upload-thread or CPU-idle assertion; root still owns VM scheduling.
