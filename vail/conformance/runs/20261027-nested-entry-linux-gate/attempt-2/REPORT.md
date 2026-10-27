# Linux nested-entry public test gate

Exact protocol acf460bb6e677efafc2e4a03bfb705ee43607b81 executed once. Both normal Cargo builds and the complete public integration target ran on Linux, offline with the frozen source, test, flags and shared cache. No Python install, engine benchmark or engine query was run. Build-plus-test elapsed time is not a comparative performance measurement.

Baseline a43ceae110728e89a9e7a8e09a79cb5cf9a40d74: 6 passed, 2 expected named failures, 0 ignored, exit101. The only failures were `nested_struct_leaf_projection_omits_unused_payload_before_entry_materialization` and `nested_leaf_run_keeps_original_spill_charges_and_omits_unused_output`. The allocation assertion observed [1459063, 28118903] bytes; the temporary-output assertion observed 330039296 written bytes. These are the authored public boundaries, not RSS/source-I/O or end-to-end speedup measurements.

Candidate e54cf99558db09c76b2505b3ad087098cac773d6 (production0504498f8): 8 passed, 0 failed/ignored/filtered, exit0. The shared top-field spill positive control passed BOTH revisions. Candidate nested-spill pass establishes the public positive-write and <8MiB temporary-output assertion with unchanged original source charges. Both revisions consumed identical public test source SHA30652162c10f6b97c4b9afcdec18b29c813c49b0a48d7c3d7eff43769dc29a6c.

Natural baseline test executable SHA95e679f061c54700e34581372f83e340eb8469508f3929169441d0cf57f3bd16; candidate SHAca969f0b63379eb920d883410713bc4f647cc353858ba348d6f3eb42d8e90718. No binary was copied or installed. Cargo elapsed104.269440337s baseline and80.595179416s candidate includes compilation and tests.

Before writes, all2,109 previous source fingerprints and exact1,766 Rust/Cargo file set passed. Candidate and baseline each verified1,768 build inputs. Candidate bytes were restored and reverified after tests. The existing installed scan native4761b8ec4e70325e922bb330987ef87c7a9f50bfba4f93f9a69c0c761337244c, all69 frontend files and runtime inventory are identical before/after. All five guest command groups retired cleanly; host ownership/probes clean. Provider positively TERMINATED with no cleanup errors.

Downloaded archive SHA9e86d0ca46c19c1de858d64dd9d553cf574321301a0376b3affac53dc56b251a (83,637 bytes) verified before extraction. Prior failed attempt and diagnostic recovery are unchanged. Independent review and root acceptance remain required before merge.
