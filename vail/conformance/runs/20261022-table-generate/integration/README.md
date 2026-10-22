# Table._generate integration with current main

Candidate `ff6205f6606e4701dd8b7ba1dae0ab76fb8089b9` is based on main
`138f830b33cb61816bd5b7c9ce4b4b03126cc8df`. It cherry-picks only the
Table._generate implementation, behavioral tests and original evidence. Main's
VDS import is preserved. No consumer replay or audit-harness changes are included.
The `_generate` AST and complete test file match scoped-approved `861320e7`.

Fresh validation: **99 focused tests passed in 1.86s**, and **36 original upstream
table randomness tests passed in 1.99s**, including the original TableGen UID case.
`focused.log` and `upstream/` retain outcomes and harness adaptation manifests.
Keyed interval partitioners remain explicitly unsupported; contexts/per-context
outputs are materialized native arrays, with no streaming guarantee.

The native extension is main's recorded release build from `ac709e13`, SHA-256
`68254f5ee21c520edc7e3be33a2d74b496c7a41c477d2408106a7262bda8e12b`.
Rust, Cargo and patch inputs are unchanged between that built revision, current
main and this integration candidate. Build evidence is in the VDS integration
run's `integration-final-build.log`. Python imports use this checkout's absolute
path. No new native build is necessary for this Python-only feature.

The upstream harness was copied from the earlier audit branch to a temporary
folder; unrelated harness changes are not part of the main integration diff.
The original Hail source and behavioral assertions remain unchanged except for
the previously authorized removal of proven structural IR assertions.

Required review thread: `01a14128-21b8-7a53-b399-4ad8f9b31f4f`.
Source/test clearance for `ff6205f6` against `138f830b` found no new blocker.
Final disposition: **FINAL MAIN-INTEGRATION APPROVAL** for exact evidence child
`5164b849e9397d803f145f40c9b154736c145a22` against main
`138f830b33cb61816bd5b7c9ce4b4b03126cc8df`; all findings are closed
(`review.json`). The reviewer inspected source and evidence and ran no builds/tests.
Unrelated dirty linreg evidence in the existing main checkout is preserved.
