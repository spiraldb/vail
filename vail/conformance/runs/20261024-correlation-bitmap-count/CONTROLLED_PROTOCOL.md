# One final controlled trial (predeclared)

Source and normally built artifacts are frozen: baseline669/native9ee229e8 and candidate9a/nativeab991ed6. No rebuild/source variants. Run only after ArrayLength's critical gate/measurements release local CPU.

Eight independent A/B process pairs, balanced fourAB/fourBA in seeded random order; four same-artifact A/A pairs interspersed, also balanced. Each process executes20queries with4warmups then16recorded steady queries. Thus24fresh processes/480queries, expected about2–3minutes. Primary latency statistic is each process's arithmetic mean, compared as paired geometric ratios with a fixed-seed paired bootstrap95%interval. Median latency and CPU seconds are secondary; all raw iterations retained. A/A compares the identical baseline env/native under two nominal labels to expose process/runtime variability. Fixed seed45020261024 determines order before observation. No outlier removal, no selective windows, and no additional repetitions after this trial.

Both environments cap OPENBLAS_NUM_THREADS/OMP_NUM_THREADS to1. Native hashes and exact public query outputs are asserted per process. Capture load average/swap metadata before each process and keep faults/RSS from every query. No other owned builds/tests/profiles overlap. Hardware PMU claims remain unsupported.

Report intervals and paired ratios honestly; do not infer an improvement from an interval including both regression and improvement. If the resource gate remains inconclusive, leave the source unmerged and finish H045 investigation; do not tune another source variant.
