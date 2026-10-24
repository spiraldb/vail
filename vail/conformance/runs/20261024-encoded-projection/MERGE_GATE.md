# Final current-main gate: green

Final production assembly `8ea1859e769b8ec90962e2d48cac434bc0bf2265` cleanly
merges main `4bd968e831e114c1ae9d0433dea73078c445bcb0` with StageA+encoded
`6af69d40f07435ff1b5a9ec7c8fcd1cef234011a`. No conflicts/manual production edits.
The scalar-main change is confined to aligned_matrix_map.rs plus its tests.

Normal locked kache build completed in2m25s, same native CPU flags/jobs2/profile.
Installed native SHA256:
`0f86a632262bb073797934cddac75660e3c41b8367c7936ba3d59ee8e8f9b6d9`.
No compiled artifact was copied or retained as a manual cache.

- **388Python tests pass,0fail**, across29files: the exact25-file successful
  scalar-main broad gate union the four additional pushdown files. The historical
  composite floating/broadcast failure now passes. Includes StageA aggregate
  pruning, typed reducers, arrays, calls/genotypes, split multiallelics, RNG,
  shared contexts, logical nulls, stored constants, globals and all encoded
  projection regressions. The complete file list and JUnit XML are retained.
- **9Rust public integration tests pass,0fail**.
- Final exact-artifact500M-entry smoke queries both pass independent DP sum
  **20,969,995,813**, for eligible dense and ordinary filtered-fallback fixtures.
  These were correctness smokes, not a new timing campaign.

The accepted isolated performance/resource/startup boundary is documented in
[FINAL.md](FINAL.md); the StageA composition's actual wide/native fallback results
and separate-run timing variation are in [STAGE_A.md](STAGE_A.md). The scalar
alignment merge changes no encoded kernel/storage/capability production source,
so those measurements retain their exact recorded provenance rather than being
relabelled as final-artifact timing. Tiny-query/filtered slowdowns and forced
native-Pco producer coverage limitations remain explicit.

All source compositions were submitted to the required reviewer; parent owns
final review disposition and the actual main merge. This branch does not update
main itself. Final build/test/smoke logs and checksums: [merge-gate](merge-gate/).
