# Range join investigation

Exact fixture-free upstream query: `range_table(1_000_000_000).join(range_table(1_000_000_000), 'inner').count()`.

Baseline diagnostic profile uses existing normally built SUM environment native SHA256 `ba4ca8743a6a7ddf02fb432d30cd15a3020ae3de100af8c24ed998329ee2161e`, source worktree HEAD `fab696fe112e14b2a3608807d04dcbf769cf0061`. Unrelated SUM changes are present; this is a bottleneck attribution baseline, not a controlled speedup measurement against exact main. No cloud resources used.

Query wall 3.795s, process CPU 46.121s, peak process RSS 609566720 bytes. Samply 1kHz profile saved with symbol sidecar. Leaf samples: key interval predicates ~20%, native sort-merge stream comparisons/group advance/pair emission ~60%. No hashing. Predicates redundantly compare every row to intervals already enforced by range generator clipping, or entirely covering certified/observed partition bounds.

Candidate removes these redundant residuals only when source clipping is exact (integer range generator with forwarding key-preserving maps), or partition bounds prove full containment. Other clipped reads and partial overlaps retain residuals. No range algebra/count shortcut.

Candidate build, tests, and post-change profile pending.

## Matching candidate validation

Candidate production `9bf9b864`, test-only child `922e6c33` (same production), based on main `270e6d43`. Normal isolated worktree release build with Rust 1.98.0, kache preserved, completed in 11m47s. Python 3.14 abi3 environment `/private/tmp/range-join-env`; native SHA256 `227d10a99fed459d422356df1d1ad6632e10764f716ae7dbc5ca5c6dcb64c7a3`. No compiled binary copying or shared Cargo target.

Public Python suites `test_interval_partition_joins.py` and `test_table_joins.py`: **48 passed in 10.99s**. Includes whole/partial partition overlap, materialized input, nested/null/prefix keys, join modes, and an 8,388,608-row split boundary regression.

Exact 1B×1B diagnostic after-profile returned count 1,000,000,000: wall **3.607s**, process CPU **45.504s**, peak RSS **756334592 bytes**. KeyPredicate samples disappeared entirely, versus ~20% leaf samples before. The dominant remaining work is native DataFusion sort-merge row comparison/group advancement/output pair construction (~75% sampled leaves), not hashing. Profiles were collected under unrelated builds; source-baseline and candidate have other unrelated differences and Python versions differ. Do not present the single before/after wall/CPU values as a controlled speedup or a new Hail comparison. The demonstrated fix is removal of redundant per-row guard work with public result preservation.

Source/test review `9bf9b864` cleared by required reviewer; final runtime evidence receipt pending root integration. No main changes made by this agent.
