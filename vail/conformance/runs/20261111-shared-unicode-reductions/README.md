# Shared Unicode producers

This extends the shared reduction DAG to borrowed **Utf8 character-length inputs**, using DataFusion's existing Unicode kernel. The fixed generated workloads improve by **22.3% wall / 28.0% CPU for short ASCII** and **19.3% / 24.5% for Unicode**, compared with the merged numeric checkpoint. The three existing aggregation controls have adverse point estimates, all retained below. This is an incremental producer/type extension, not universal aggregation fusion or a new Hail comparison.

Production `85afc23c06647d43f4aedb5aec5d98023091914e`; normal installed native `488249ddf69c8c00af76f341c332c6257d7f83eeb260923ff6820bc1bd8e30b7`. Baseline `c0233972584ae3bf54b6f4b9ebb9068577b6e6d2`, native `1d287bbe5511cabd62b40638f743b29b2ce6992e9e38b69683b0aa4299a4a433`. Both use mimalloc and the same environment and input files. No builds or profiles overlap timed workers.

## Measured results

| Work | Old wall s | New wall s | Wall change | Old core-s | New core-s | CPU change |
|---|---:|---:|---:|---:|---:|---:|
| Generated short ASCII | .016247 | .012627 | −22.282% | .045004 | .032423 | −27.955% |
| Generated Unicode | .012905 | .010420 | −19.259% | .029355 | .022151 | −24.538% |
| Many row aggregations, Hail input | .725552 | .743616 | +2.490% | 7.033724 | 7.111601 | +1.107% |
| Many row aggregations, native input | .432362 | .434165 | +.417% | 4.998174 | 5.042830 | +.893% |
| Sex-specific QC, Hail input | .455435 | .467422 | +2.632% | 4.486181 | 4.502529 | +.364% |

These are medians of process medians from **two fresh processes per version**, B/C/C/B, one formal warmup then three timed actions per process: **20 workers / 60 calls**. There is no significance claim, noise dismissal, or regression-free claim. Independent review accepted the scoped Unicode gains with these controls. Both candidate process medians improve on both baseline process medians for each generated workload.

The generated cases use range matrices with nested nullable string payloads and filtered entries: ASCII 1024×512, Unicode 256×512. Unicode values have 320, 256, 256 and zero codepoints, plus missing, with a threshold of 257; combining characters remain distinct codepoints. Each generated worker performs an additional untimed full collect against an independent Python codepoint/count/fraction oracle **before** the formal warmup. Initial range/literal/expression-object construction is outside timers; source evaluation, row-aggregate construction and force-count are inside. These generated checks are not upstream Hail benchmarks. Existing controls retain their original benchmark ASTs/actions. Existing Tokio/Rayon4 configuration is not a four-core limit; observed CPU endpoint accounting remains a lower bound for transient children. All workers have positive cleanup receipts, and all **469** enrolled input files and exact file sets match before and after.

## Implementation and validation

The compiler identifies a shared character-length producer in the existing same-owner/lexical/filter domain. It prepares one native UDF, argument/output fields and configuration per program. Bounded windows become StringView descriptors sharing one original Utf8 payload buffer. Physical and ancestor null masks apply before sparse descriptor selection and the Unicode scan. Selection copies descriptors rather than string payloads. Both generated public Matrix queries actually enter **four sinks / nine registers / one shared character-length producer**; this is not a fallback timing.

The workspace estimate adds 34 bytes per lane for coexisting input/selected 16-byte views and masks; ordinary output slots and existing temporary/header allowances remain charged. Native API calls still allocate small argument vectors and required output arrays. Descriptor construction may inspect bounded prefixes/inline bytes before ancestor masking; no zero-hidden-byte-access, zero-allocation, owner-release, RSS, or I/O claim follows. Inputs remain retained. Only Utf8 with i32 offsets is admitted; native StringView, LargeUtf8, dictionary and cast-dependent sources retain ordinary execution. Count-where/fraction are still the only shared reducer contracts; arbitrary reducers and all types remain unfinished.

The final normal build passed **68 Rust / 122 Python tests**. Two new public Python cases also pass on unchanged baseline Vail. A public Rust test evaluates both direct and logical expression routes on nonzero sliced offsets, leaf/parent nulls, empty/missing lists, sparse and all-false domains. Its input fixture and preparation precede allocation counting: one selected 8 MiB string and one null-parent 8 MiB string. All three candidate domains keep allocation growth below the fixed 512 KiB allowance. The identical test on baseline fails: its dense-domain allocations grow from **20,606 / 24,243 bytes to 50,352,446 / 50,356,083 bytes**. This establishes payload-sized allocation avoidance, not CPU cost of hidden bytes or end-to-end memory savings.

The first compile found a duplicate import shared through an included execution module; that failed receipt is preserved. The only correction was removing the redundant import before the final normal build. No latent semantic fix, source-certificate bypass, allocator change or benchmark-specific field/value admission was bundled.

## Evidence

[Results](results.json) preserve every process median and adverse control. [Manifest](manifest.json) pins production, normal native, inputs, test overlay and **142 archive members**. [Evidence](evidence.zip), SHA256 `67a2d8474aa5abc962f85c23f21e812a32f608f89600f694bbcd771924fe9269`, contains source harnesses, gates, route proof, failed compile, baseline resource counterfactual and fresh raw workers, without compiled artifacts. Independent reviewer `01a14128-21b8-7a53-b399-4ad8f9b31f4f` cleared source/resource/performance admission with the stated adverse controls; final sealed-report review precedes root-only main integration.

The separate [hardware bottleneck investigation](../20261111-aggregation-bottlenecks/README.md) uses the numeric checkpoint and is not causal evidence for these Unicode speedups. The prior [numeric comparison with Hail](../20261111-shared-list-reductions/README.md) remains a separate measured cohort.
