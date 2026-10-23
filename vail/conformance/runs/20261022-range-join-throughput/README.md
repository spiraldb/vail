# Range join investigation

Exact fixture-free upstream query: `range_table(1_000_000_000).join(range_table(1_000_000_000), 'inner').count()`.

Baseline diagnostic profile uses existing normally built SUM environment native SHA256 `ba4ca8743a6a7ddf02fb432d30cd15a3020ae3de100af8c24ed998329ee2161e`, source worktree HEAD `fab696fe112e14b2a3608807d04dcbf769cf0061`. Unrelated SUM changes are present; this is a bottleneck attribution baseline, not a controlled speedup measurement against exact main. No cloud resources used.

Query wall 3.795s, process CPU 46.121s, peak process RSS 609566720 bytes. Samply 1kHz profile saved with symbol sidecar. Leaf samples: key interval predicates ~20%, native sort-merge stream comparisons/group advance/pair emission ~60%. No hashing. Predicates redundantly compare every row to intervals already enforced by range generator clipping, or entirely covering certified/observed partition bounds.

Candidate removes these redundant residuals only when source clipping is exact (integer range generator with forwarding key-preserving maps), or partition bounds prove full containment. Other clipped reads and partial overlaps retain residuals. No range algebra/count shortcut.

Candidate build, tests, and post-change profile pending.
