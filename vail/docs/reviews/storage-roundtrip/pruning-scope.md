# Pruning scope of the nullable-struct correction

The parent-validity dependency changes a nullable one-field strict predicate
from a single data-field expression to an expression depending on both data and
parent validity. `StructReader::pruning_evaluation` conservatively returns its
input mask for a multi-partition expression. This patch does not add a general
multi-field zone-map proof engine.

The old single-field path is not evidence of existing payload pruning: its
expression wrapped the primitive in `mask(root, is_not_null(root))`. The current
zone-map binder accepts aggregate-stat inputs only when they are the root, and
no mask-specific stats rewrite was found. Source inspection therefore suggests
that this nullable nested-field case already lacked pruning; it does not prove
before/after I/O equivalence. Existing nullable multi-field predicates were also
conservative.

No claim is made that nullable nested-field payload segments are demonstrably
avoided. That requires a real read-denial control, not an execution-partition
count or an expression-shape assertion. It remains in queue A005. Top-level
nonnull root/key file pruning code is unchanged; its existing file-avoidance
regressions are part of the broader native/local validation. The final report
will identify their actual outcomes separately from this source assessment.
