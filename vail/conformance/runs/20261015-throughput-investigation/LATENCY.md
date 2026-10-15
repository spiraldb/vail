# Small-input total aggregation latency

The initial protected 32-worker fanout added about 5 ms to known tiny inputs. A shared predicate now skips fanout when the table has a known row count fitting one default DataFusion batch. Large or unknown inputs retain the fanout policy. Both target selection and repartition insertion use that predicate.

Matching corrected native SHA256: `180f6f25bf286f96fd346450d9d2fcbc54d8903b5e705827527667d6ce6cafd9`. Separate-process ABBA runs use five warmup actions and 160 measured actions per case per phase (320 observations per version per case). Public sum results are checked in every action.

| Rows | Before median ms | After median ms | Added median ms |
|---:|---:|---:|---:|
| 0 | 3.830 | 3.689 | -0.141 |
| 1 | 3.726 | 3.552 | -0.174 |
| 32 | 3.659 | 3.525 | -0.133 |
| 1024 | 3.858 | 3.662 | -0.195 |

All cases meet the predeclared diagnostic threshold of at most 1 ms added median latency. This is not an API latency guarantee. The shared host was running unrelated work: tails were noisy, and an earlier 40-observation-per-phase run failed the 32-row case (+2.01 ms) while the other three passed. Both runs are preserved; no tail-latency improvement is claimed. The original unconditional-fanout comparison is also retained.

Artifacts: `latency-comparison/` (original), `latency-small-input-final/` (40 per phase), `latency-small-input-repeat/` (160 per phase). Reproduce with `run_latency_comparison.py`; package roots and native hashes are recorded in every phase. These packages use the older expression boundary and do not certify the incoming native-handle planner integration.
