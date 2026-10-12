# Vail workspace

This directory contains upstream source checkouts and the new Vail project:

| Directory | Purpose |
| --- | --- |
| `hail/` | Hail Python API, semantics, implementation, and reference tests |
| `datafusion/` | DataFusion execution and ordering implementation |
| `vortex/` | Vortex arrays, files, and DataFusion integration |
| `iceberg/` | Iceberg specification and Java reference implementation |
| `iceberg-rust/` | Iceberg native Rust implementation and integration points |
| `vail/` | Vail source, tests, recorded results, and documentation |

Start with [Vail's build instructions and implemented subset](vail/README.md), then [the implementation plan](vail/docs/PLAN.md) and [agent assignments](vail/docs/TASKS.md).

The five upstream directories are Git submodules pinned to the inspected revisions in [upstreams.lock.json](vail/upstreams.lock.json). They are research baselines, not a tested runtime dependency combination. The Vail runtime instead uses its Cargo lockfile and the documented patched Iceberg PR snapshot under `vail/patches/`.

Clone this repository and fetch the five upstream checkouts with:

```sh
git clone git@github.com:danking/vail.git
cd vail
git submodule update --init
```

Vail's tests need these upstream source checkouts; their own nested submodules are not required for the current Vail checks. Build commands run from `vail/`. Build outputs and local environments are ignored; written documentation and conformance run records are committed.

Vail's original code is licensed under [MIT](LICENSE). Upstream submodules and the vendored Iceberg patch retain their own licenses and notices.
