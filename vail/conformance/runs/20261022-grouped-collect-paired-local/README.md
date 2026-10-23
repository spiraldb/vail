# Representative grouped collect, fresh laptop pair

**Not the full upstream fixture.** This is the exact `group_by_collect_per_row` query shape over a deterministic persisted 20,000-row × 1,000-column matrix, entry `x=(row_idx+col_idx)%91`, 32 partitions. The full upstream random gnomAD fixture is 250,000 × 1,000 and unavailable locally; no extrapolation or silent substitution.

Both engines read equivalent native stored matrices, localize entries into `e` and columns into `c`, group by the row key, `agg.collect(row_value)`, and force-count output groups. Result independently checked as20,000 on all6attempts. Hail matching fixture was generated once before timings; Vail reused `/tmp/grouped-collect-followup-20k.mt` (41MB). No fixture generation inside measured scopes.

| Engine | Wall median (range), s | Core-seconds median (range) | Peak tree RSS |
|---|---:|---:|---:|
| Hail | .55413 (.37067–.66375) | 5.20292 (3.91159–5.76039) | 1.279GiB |
| Vail | .02053 (.01958–.05220) | .19633 (.19197–.29610) | .231GiB |

3fresh repetitions each, engines serial. Hail/Vail ratios **26.997 wall /26.500 CPU**. Stock PyPI Hail0.2.139 Java11/Spark local[32]24g; Vail EXECUTION_CONCURRENCY32, normal nativeSHA f32664067c0761f1982884d4a95fc6dc092be46942bea22815abb81ec6aafd14, source3373d094 reviewed and merged. This artifact predates unrelated subsequent main work but includes exact grouped-collect fix.

10Hz process-tree CPU retains exited children by(pid,creationtime), including HailJVM. Vail repetitions shorter than100ms can have no interior interval samples: terminal counters provide core-seconds and RSS endpoints, so brief RSS peaks may be missed. Unrelated legacy compiler (~onecore) and Git activity existed; this is not an otherwise-idle performance claim. Raw driver/output/telemetry and summary included. No cloud resources used.
