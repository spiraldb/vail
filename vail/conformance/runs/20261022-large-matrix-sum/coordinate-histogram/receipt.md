# Coordinate histogram and configured callback dispatch

Production material: 40cffb28 histogram layout adapter; 1c55a74b general bare configured callback lowering. Test HEAD edc6df5152099bb9267d8344f4b236b91236a9d3. Normal Rust 1.98.0 release maturin develop, own worktree-local target, existing kache. Installed native SHA256: d3cde4f30988806608f3b45a7f4578da3fbf18bdae1bcbfcfb70194927ef5fce. No manual native copying.

147 Python tests passed (77 coordinate/registry/callback/DAG/matrix +70 numeric/matrix-array/parallel-stats), and 13 native tests passed (3 library +6 public factory +4 public partial-state protocols). Logs included. Build completed35.37s. Commands: normal `maturin develop --release --locked`; Python pytest file groups shown in test logs/provenance below; native `cargo test --release --locked --manifest-path vail/Cargo.toml -p vail-execution --lib --test aggregate_factories --test coordinate_reducer_states coordinate` with macOS dynamic_lookup link flags.

The direct adapter shares existing native HistogramConfig, histogram_bin, add_row, merge_row and output helpers. It dispatches primitive dtype once per batch and uses implicit coordinate positions; no per-entry group-ID vector or Float64 cast buffer. Partial states remain native List<Int64> and are consumed by actual native grouped histogram in the public protocol test.

Configured bare callbacks previously bypassed coordinate_bundle. The general fallback now tries that existing machinery after preserved simple fast paths; existing eligibility/ordered/native identity checks remain. Public bare histogram and collect/take/set/counter results equal wrapped callbacks.

Profile workload: four public 100000-row x100-position histogram actions,32partitions, no fixtures. 1kHz samply, saved embedded symbols, cold process setup included. Baseline is the previous reviewed collect implementation; final uses exact native above. Profile proportions are diagnostic, not paired performance timings; runs were not reserved quiet windows. profile-counts.json records actual callers, including removed group-values/UNNEST and new native histogram path. All sample denominators include setup and waiting.

Remaining structured work is explicit: bounded unordered/ordered take, true value-hash set/counter, native covariance/correlation, sketches and regression state adapters. Matrix global collect retains relational fallback to avoid a new global List i32-offset limit. No claim those algorithms are complete or that genuine hash/value ordering can be removed.
