Final bounded PCO probe experiment, fixed before execution

Base: develop910f9933. Adverse previous: 9f468abe16dd070d2d8eed5df4f45709b50ae375.
Production change: separate stateless cache construction and fallible misses into inline(never) ordinary helpers; retain validated boolean all-null proof instead of cached generic Scalar clone. No bounds removal, cold annotation, force inline, new representation or threshold.

Run unchanged pco-compat workflow and current public harness with identical bench settings: ABBA two processes/version, eight iterations/process, all cold and warm rows preserved. Report warm per-process medians from iterations>=2 using the already frozen analyzer, plus raw rows, exact source/executable hashes. Compare develop/current paired receipts; retain prior 9f paired receipts without treating cross-run differences as causal. Include first probe, scattered large/mixed takes, compact ranges, near-full slices, long-null and all-null, writer/full-scan controls. CPU100Hz ticks are quantized; no zero-CPU claim. Whole-process RSS is not operation peak.

Run usual CI, actual unchanged old-reader compatibility both directions and CodSpeed. Repeat bounded software attribution and linked assembly with source/executable provenance; no latency claims from profiled times. Final disposition if warm regression remains: stop tuning and submit explicit measured latency/resource tradeoff or design limitation to reviewer. No further variants or post-hoc windows.
