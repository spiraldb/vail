
<details>
<summary><b>4 warnings, 1 note</b></summary>

> :warning: **Walltime results may be unreliable**
>
> Some benchmarks used the Walltime instrument outside CodSpeed Macro Runners, which can make the results inconsistent.
>
> [Learn more about Macro Runners](https://codspeed.io/docs/features/macro-runners?utm_source=github&utm_medium=check&utm_content=walltime_unknown_machine)
>
> [See affected benchmarks](https://app.codspeed.io/vortex-data/vortex/branches/dk%2Flogical-row-counts?q=has%3Awalltime_unknown_machine&utm_source=github&utm_medium=check&utm_content=affected_benchmarks)

> :warning: **Unable to measure the execution of 1 benchmark**
>
> This benchmark measured no execution time, so its result is not comparable and counts as unchanged. This usually happens when the compiler removes the code under test.
>
> [Prevent compiler optimizations](https://codspeed.io/docs/troubleshooting?utm_source=github&utm_medium=check&utm_content=optimized_out#optimized-out-benchmarks)
>
> [See affected benchmarks](https://app.codspeed.io/vortex-data/vortex/branches/dk%2Flogical-row-counts?q=has%3Aoptimized_out&utm_source=github&utm_medium=check&utm_content=affected_benchmarks)

> :warning: **18 benchmarks are dominated by system calls**
>
> System calls cannot be simulated consistently and are left out of the measure. The reported value understates the real cost. Switch to the Walltime instrument to measure them.
>
> [Learn more about system calls](https://codspeed.io/docs/instruments/cpu?utm_source=github&utm_medium=check&utm_content=significant_sys_time#system-calls)
>
> [See affected benchmarks](https://app.codspeed.io/vortex-data/vortex/branches/dk%2Flogical-row-counts?q=has%3Asignificant_sys_time&utm_source=github&utm_medium=check&utm_content=affected_benchmarks)

> :warning: **Base and head ran on different runner types**
>
> Some benchmarks with significant performance changes were compared across different runner types, so the changes might not come from the code. Run base and head on the same runner type, or use CodSpeed Macro Runners for more stable results.
>
> [Learn more about Macro Runners](https://codspeed.io/docs/features/macro-runners)
>
> [Open the report in CodSpeed to investigate](https://app.codspeed.io/vortex-data/vortex/branches/dk%2Flogical-row-counts?utm_source=github&utm_medium=check&utm_content=comparison_issues)

> :information_source: **40 benchmarks have no flamegraph**
>
> The flamegraph wasn't generated: the symbols of the profiling trace could not be resolved, because no information about the language frames was collected.
>
> [See affected benchmarks](https://app.codspeed.io/vortex-data/vortex/branches/dk%2Flogical-row-counts?q=has%3Acallgraph_generation_failure&utm_source=github&utm_medium=check&utm_content=affected_benchmarks)

</details>


`⚡ 2` improved benchmarks  
`❌ 8` regressed benchmarks  
`✅ 2398` untouched benchmarks  
`⏩ 12` skipped benchmarks[^skipped]  


> [!WARNING]
> Please fix the performance issues or [acknowledge them on CodSpeed](https://app.codspeed.io/vortex-data/vortex/branches/dk%2Flogical-row-counts?q=is%3Aregression&utm_source=github&utm_medium=check&utm_content=acknowledge).

### Performance Changes

|     | Mode | Benchmark | `BASE` | `HEAD` | Efficiency |
| --- | ---- | --------- | ------ | ------ | ---------- |
| ❌ | Walltime | [`` filter_neon[8] ``](https://app.codspeed.io/vortex-data/vortex/branches/dk%2Flogical-row-counts?uri=encodings%2Ffastlanes%2Fbenches%2Fbitpacking_filter.rs%3A%3Aneon%3A%3Afilter_neon%255B8%255D&runnerMode=WallTime&utm_source=github&utm_medium=check&utm_content=benchmark) | 889 ns | 1,814 ns | -50.99% |
| ❌ | Walltime | [`` threshold_neon[8] ``](https://app.codspeed.io/vortex-data/vortex/branches/dk%2Flogical-row-counts?uri=encodings%2Ffastlanes%2Fbenches%2Fbitpacking_take.rs%3A%3Aneon%3A%3Athreshold_neon%255B8%255D&runnerMode=WallTime&utm_source=github&utm_medium=check&utm_content=benchmark) | 1.4 µs | 2.6 µs | -48.13% |
| ❌ | Walltime | [`` filter_neon[64] ``](https://app.codspeed.io/vortex-data/vortex/branches/dk%2Flogical-row-counts?uri=encodings%2Ffastlanes%2Fbenches%2Fbitpacking_filter.rs%3A%3Aneon%3A%3Afilter_neon%255B64%255D&runnerMode=WallTime&utm_source=github&utm_medium=check&utm_content=benchmark) | 1.7 µs | 2.7 µs | -35.93% |
| ❌ | Walltime | [`` threshold_neon[80] ``](https://app.codspeed.io/vortex-data/vortex/branches/dk%2Flogical-row-counts?uri=encodings%2Ffastlanes%2Fbenches%2Fbitpacking_take.rs%3A%3Aneon%3A%3Athreshold_neon%255B80%255D&runnerMode=WallTime&utm_source=github&utm_medium=check&utm_content=benchmark) | 3.5 µs | 5.1 µs | -30.44% |
| ❌ | Walltime | [`` filter_neon[80] ``](https://app.codspeed.io/vortex-data/vortex/branches/dk%2Flogical-row-counts?uri=encodings%2Ffastlanes%2Fbenches%2Fbitpacking_filter.rs%3A%3Aneon%3A%3Afilter_neon%255B80%255D&runnerMode=WallTime&utm_source=github&utm_medium=check&utm_content=benchmark) | 2.6 µs | 3.5 µs | -27.25% |
| ❌ | CPU Simulation | [`` chunked_dict_primitive_canonical_into[f64, (1000, 1000, 10)] ``](https://app.codspeed.io/vortex-data/vortex/branches/dk%2Flogical-row-counts?uri=vortex-array%2Fbenches%2Fchunked_dict_builder.rs%3A%3Achunked_dict_primitive_canonical_into%255Bf64%252C%2520%281000%252C%25201000%252C%252010%29%255D&runnerMode=Simulation&utm_source=github&utm_medium=check&utm_content=benchmark) | 151.9 µs | 205.2 µs | -25.98% |
| ❌ | Walltime | [`` threshold_neon[64] ``](https://app.codspeed.io/vortex-data/vortex/branches/dk%2Flogical-row-counts?uri=encodings%2Ffastlanes%2Fbenches%2Fbitpacking_take.rs%3A%3Aneon%3A%3Athreshold_neon%255B64%255D&runnerMode=WallTime&utm_source=github&utm_medium=check&utm_content=benchmark) | 2.6 µs | 3.1 µs | -15.99% |
| ❌ | Walltime | [`` decode_chunked_neon[i64, 524288] ``](https://app.codspeed.io/vortex-data/vortex/branches/dk%2Flogical-row-counts?uri=encodings%2Ffastlanes%2Fbenches%2Ffor_decode.rs%3A%3Aneon%3A%3Adecode_chunked_neon%255Bi64%252C%2520524288%255D&runnerMode=WallTime&utm_source=github&utm_medium=check&utm_content=benchmark) | 13.6 µs | 15.1 µs | -10.16% |
| ⚡ | CPU Simulation | [`` test_scan_filter[int] ``](https://app.codspeed.io/vortex-data/vortex/branches/dk%2Flogical-row-counts?uri=vortex-python%2Fbenchmark%2Ftest_filter.py%3A%3Atest_scan_filter%255Bint%255D&runnerMode=Simulation&utm_source=github&utm_medium=check&utm_content=benchmark) | 3.6 ms | 3 ms | +16.75% |
| ⚡ | Walltime | [`` bitpack_blocked_compress_avx512 ``](https://app.codspeed.io/vortex-data/vortex/branches/dk%2Flogical-row-counts?uri=encodings%2Ffastlanes%2Fbenches%2Fbitpack_blocked.rs%3A%3Aavx512%3A%3Abitpack_blocked_compress_avx512&runnerMode=WallTime&utm_source=github&utm_medium=check&utm_content=benchmark) | 5.5 µs | 4.8 µs | +14.89% |

> [!TIP]
> Investigate this regression by commenting `@codspeedbot fix this regression` on this PR, or directly use the [CodSpeed MCP](https://codspeed.io/docs/ai/mcp?utm_source=github&utm_medium=check&utm_content=mcp_tip_regression) with your agent.

---

<sub>Comparing <code>dk/logical-row-counts</code> (58fd61e) with <code>develop</code> (910f993)</sub>

<a href="https://app.codspeed.io/vortex-data/vortex/branches/dk%2Flogical-row-counts?utm_source=github&utm_medium=check&utm_content=button">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://codspeed.io/pr-report/open-in-codspeed-dark.svg">
    <source media="(prefers-color-scheme: light)" srcset="https://codspeed.io/pr-report/open-in-codspeed-light.svg">
    <img alt="Open in CodSpeed" src="https://codspeed.io/pr-report/open-in-codspeed-light.svg" width="169" height="32">
  </picture>
</a>


[^skipped]: 12 benchmarks were skipped, so the baseline results were used instead. If they were deleted from the codebase, [click here and archive them to remove them from the performance reports](https://app.codspeed.io/vortex-data/vortex/branches/dk%2Flogical-row-counts?q=is%3Askipped&utm_source=github&utm_medium=check&utm_content=archive).

