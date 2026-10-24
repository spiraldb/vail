# StageA composition receipt

Source4a28412bd78558e65bb7346a3465e0e2046d57d6 is a clean automatic merge of
StageA main1af71dc1 and the a47+encoded assembly76727f12. No manual source edits.
Normal kache locked build1m44s; native SHA256
`a4ab27766c04772d34b3ba66b611c84692bf164606c492cd3a0f550173fd5713`.
173Python tests pass, including all five StageA aggregate-input projection tests;
the same previously documented floating broadcast baseline failure remains.
All nine Rust tests pass. Subsequent main4bd968e8 fixes that broadcast failure;
this receipt does not substitute for its final combined gate.

Exact500M sums pass all12warm measured actions: eligible63.661ms/0.728276CPU s;
filtered ordinary fallback81.003ms/0.912958CPU s. Earlier a47+encoded assembly was
62.287ms/0.737285 and77.788ms/0.920271 in separate runs. Wall increased~2.2%/4.1%
while CPU slightly decreased; these are disclosed separate runs, not additive
speedup claims or a controlled StageA-only A/B.

The existing807MiB wide native `entries-profile25.mt` provides a representative
fallback multi-input query. Seven warm actions each:

- Bare DP:172.424ms/2.028537CPU s, result353709503.
- SUM(DP),SUM(GQ),COUNT:187.978ms/2.064656CPU s, result
  dp353709503/gq1246497536/n63083475. Each component matches a separate scalar
  aggregate, including count with its independent validity/container dependency.

A historical H078 reference successfully ran bare DP at294.186ms/3.366769CPU s,
with the same result. Its composite comparison is unavailable: the editable
Python source has since gained `NativeExpr.is_pure`, absent in that historical
native artifact. The full failure is retained; no source monkeypatch or disabled
optimization was used to fabricate a comparison. There is no exact local
StageA-only baseline artifact. The recorded wide CPU sample is qualitative;
Arrow allocation/offset validation/casting remains visible, and no claim of
complete GT elimination or arithmetic saturation is made. Linux `/proc` I/O
counters are unavailable on this Mac and are explicitly null in the harness.

Source inspection preserves the order: computed MapRows hook precedes ordinary
nested projection; direct-total computed hook precedes ordinary aggregation.
StageA only changes immediate ordinary Read lowering and avoids a table-only
cache for consumer-specific projection. Encoded fallback continues to use the
shared dependency/storage-path analyzer via map_rows_storage_paths. Unsupported
multi-aggregate Struct does not enter the direct-total hook and retains StageA's
ordinary lowering. No fallback redesign was made without evidence of regression.

Raw logs/results/profile/checksums: [stage-a](stage-a/).
