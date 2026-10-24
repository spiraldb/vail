# Parallel VCF export and legacy join integration

Combined source revision: `883da740fb2302e6affdbfb2368cb744fb1d4630`, based on
main `6b6eb3b6125ff843cd7c0309dedb4bbcb257c209`.

The independently reviewed VCF candidate `d011471e5617d4d1df87ad3ef3d6f3aafb0c7bc5`
and legacy join candidate `9cee0305d69f4ea8955737f4dd4f5dd65141c668` merge cleanly.
No production or test interaction edits were needed. Their original benchmark
results and limitations remain in the adjacent candidate reports.

## Combined validation

Normal kache/maturin release build completed in 11m20s, Python 3.12.12,
`RUSTFLAGS='-C target-cpu=native'`, two Cargo jobs and no incremental compilation.
The completed integration target was reused sequentially with its owner's
permission. No compiled native artifact was copied or manually cached. A first
invocation with default flags was cancelled before installation, and its log is
preserved separately. The final build/install and native/source fingerprints are
recorded in `build.log`, `provenance.json`, and `source.sha256.json`.

**278 public tests passed in 162.52 seconds**, with no failures or skips:
VCF export (including real tabix queries), legacy partition joins (including
concurrent actions), existing legacy readers, table joins, interval partition
joins and VCF parsing. Exact command is in `run-public.sh` and complete output
in `public-tests.log`.

The shared legacy session retains immutable plugin registries by runtime; every
scan keeps its own cursor, decoder and buffers. Export workers receive immutable
Arrow batches and have private formatter/compression buffers. Neither change
shares mutable read cursors with the export workers.

## Scope and tradeoffs

The original performance results are not new combined benchmarks. Native VCF
export's three-sample median improved 42.42→5.64s, with approximately 9.5% more
CPU; legacy export improved 49.76→12.24s with approximately 24.4% more CPU and
remains slower than the fresh Hail baseline. Queue limits bound chunks, not
strict bytes; a single wide row can exceed the nominal entry tile.

Legacy 1000/1000 join's current-main baseline is one 140.03s sample versus three
candidate samples with median 2.89s. Its Hail comparison uses archived scan
measurements. Constructor/timer scopes and uncontrolled OS cache are disclosed
in that report. No additional ordering certificate or benchmark shortcut was
introduced by integration.

Required review thread `01a14128-21b8-7a53-b399-4ad8f9b31f4f` cleared the exact
combined source assembly. Root owns the final evidence review and main update;
this subagent did not update main.
