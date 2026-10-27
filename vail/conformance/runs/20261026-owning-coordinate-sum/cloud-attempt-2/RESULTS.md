# Second cloud attempt: baseline preflight passed; build tooling PATH failed

Root authorized complete-source provisioning and one fresh unchangedf90 attempt.
All2,097 source files matched exact00a8d7d7 before the protocol started. Complete
archive SHA256 was213efb8fa1444ca771c6f19899f243acc0b09d7f1e162c2b3e05fda5d99331b1.
Exact frozen protocol hashes also matched. Provider45minute STOP was verified
before boot; guest43minute guard was active. Fresh run-2/receipts-2 retained the
first failure separately.

The protocol baseline provenance/input/schema preflight **passed**: native
5a8613… and all frontend/metadata hashes matched; original fixture has DP Int32
and AD array<Int32>, with stored dimensions24,885×2,535. No count or aggregation
query ran in this schema check.

The next venv stage failed immediately with `FileNotFoundError`: the
noninteractive SSH PATH did not contain the existing `uv` executable. No
candidate installation, Rust compilation, correctness test or timed action ran.
No control/query/result/guard was substituted and no retry occurred.

Host finally packed/downloaded and SHA-verified receipts, proactively stopped
the VM and verified **TERMINATED**. Provider45minute STOP remains configured.
Raw baseline preflight and venv failure are inside host/receipts.tar.gz; the
outer host error is in host/prepare/stderr. There are no benchmark results.

Next prerequisite requires root authorization: deterministic build-tool PATH
including the existing user Cargo/local-bin locations, with no installation or
workload/budget change, then a fresh output-directory attempt. Production00a
remains unchanged and locally validated; this is an execution-provisioning issue.
