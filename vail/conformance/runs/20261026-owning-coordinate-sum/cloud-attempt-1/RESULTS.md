# First cloud attempt: source precondition failed; no workload ran

Exact reviewed executable protocolf90c231e; production candidate00a8d7d7.
Root authorized one guarded attempt. Provider max-run45minute STOP was set and
verified while TERMINATED, before boot. An earlier43minute guest safety timer
was armed after SSH was ready.

The existing stable source directory is named `source-8dcd426b`, not the
illustrative `source` location. Targeted directory metadata discovery established
that path, which was passed through the reviewed --remote-source parameter.
No query or build was used to find it. The exact six-file production overlay and
f90 protocol archives were uploaded and their SHA256 values verified.

The frozen prepare.py source check failed immediately: this existing cloud
source was a filtered archive and does not contain
`vail/benchmarks/benchmark_policy.py`, one of the2,097 frozen source files.
The program refused to continue. **No baseline DP/schema preflight, environment
install, Rust compilation, correctness test, query or timed observation ran.**
No manifest, query, expected value, resource limit or build budget was weakened.
No retry was attempted.

The host outer finally packed/downloaded and hash-verified the available remote
failure receipt archive, then proactively stopped the VM. Provider-after status
is **TERMINATED**, with45minute STOP bound retained. The actual failure/traceback
is in `host/prepare/stderr`; the remote archive contains only its empty receipt
manifest because source verification failed before a stage was started. Local
host command/error/cleanup/provider receipts retain the complete failure.

There are no comparative wall/CPU/memory results to interpret. Mac correctness
and resource tests in the earlier report remain separate evidence.

Next prerequisite, requiring root authorization before another attempt: upload
the complete exact00a8 production source manifest, not merely the six-file overlay
onto the old filtered source. The already reviewed protocol/source/query need
no change; its source guard correctly caught the packaging omission.
