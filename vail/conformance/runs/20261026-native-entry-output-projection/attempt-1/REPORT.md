# H051 baseline attempt 1: wrong diagnostic guard, stopped before action

Exact protocol f209f3a538ad643f851b000e7eff8020e84c0407 ran on the bounded primary guest. All script/native/frontend/input/GQ checks passed. Confirmation stopped before its first aggregate action because the diagnostic-only plan API prints `UnboundActionExec: MatrixEntriesByKeyRuns semantic operation; action preparation remains unbound`, not `MatrixEntriesByKeyRunsExec`. This is a protocol guard error, NOT evidence that the public custom path is absent. The plan includes that semantic node and its downstream GQ projection.

No aggregate action, CPU sample or permission mutation occurred. Captured paranoid4 remained4, confirmation process-group cleanup was positively verified, downloaded original receipt archive SHA was verified, and host proactively stopped the VM and verified TERMINATED (provider30m STOP). No retry occurred.

Source trace: logical.rs1719 creates unbound diagnostic labels from VailNode node_name, preserving schema/inputs without action preparation. partitioned.rs1296 matches the same TableNode::MatrixEntriesByKeyRuns; config1338 and matrix_entries::lower1379 prepare the actual action-bound operator. Diagnostic marker correction is source-only and requires new review/root authorization; it is not a test oracle or runtime proof.
