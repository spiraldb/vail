# Final bounded PCO experiment

PR https://github.com/vortex-data/vortex/pull/10771 remains DRAFT. Source 58fd61e34d181553a60423defaebb381e9213b74; CI merge615a967dfe3f77447d53aaedc44096c3b3977034.

59PCO tests,1doctest and four2/2 public file/actual unchanged-reader phases PASS. Resource40121399516, attribution40121399664 SUCCESS. CodSpeed FAILURE; usual remaining jobs listed separately.

Final fixed iteration has ended. No further probe variants or post-hoc windows. Final reviewer disposition required for residual warm latency.

|Operation|Pattern|Base warmµs|Current warmµs|Speedup|
|---|---|---:|---:|---:|
|full|all-null|3.5292|3.1937|1.1050|
|full|compressed|50374.8898|50267.5062|1.0021|
|full|dense|56412.1100|57284.3397|0.9848|
|full|long-null|146.9080|150.4067|0.9767|
|full|nullable|103591.6520|104588.0557|0.9905|
|full|nullable-legacy|106720.6883|105529.3635|1.0113|
|probe|all-null|18.8057|16.6077|1.1323|
|probe|compressed|36.2058|34.9895|1.0348|
|probe|dense|29.8825|34.4503|0.8674|
|probe|long-null|35.5553|34.9657|1.0169|
|probe|nullable|33.5540|39.2173|0.8556|
|probe|nullable-legacy|34.1405|40.2338|0.8486|
|probe-first|all-null|0.4175|0.1067|3.9110|
|probe-first|compressed|104.2107|4.1160|25.3185|
|probe-first|dense|93.8623|3.8348|24.4768|
|probe-first|long-null|15.6740|3.9470|3.9711|
|probe-first|nullable|118.6437|0.4897|242.2537|
|probe-first|nullable-legacy|119.8430|9.4525|12.6784|
|slice-near-full|all-null|3.8065|3.5257|1.0796|
|slice-near-full|compressed|51377.5017|21115.5860|2.4332|
|slice-near-full|dense|56067.9730|37885.4160|1.4799|
|slice-near-full|long-null|147.2220|139.4315|1.0559|
|slice-near-full|nullable|106793.9628|44071.5207|2.4232|
|slice-near-full|nullable-legacy|105981.4385|42148.4328|2.5145|
|take-contiguous|all-null|33494.5263|5747.5360|5.8276|
|take-contiguous|compressed|84919.0835|53277.2667|1.5939|
|take-contiguous|dense|83768.7867|66227.0560|1.2649|
|take-contiguous|long-null|33682.2770|30425.0668|1.1071|
|take-contiguous|nullable|132587.2327|74528.3193|1.7790|
|take-contiguous|nullable-legacy|135524.2607|68280.7752|1.9848|
|take-large|all-null|15561.6125|298.2603|52.1746|
|take-large|compressed|62918.1042|63425.8557|0.9920|
|take-large|dense|66597.3138|66573.8087|1.0004|
|take-large|long-null|15818.2470|15915.9657|0.9939|
|take-large|nullable|116215.2392|115562.9223|1.0056|
|take-large|nullable-legacy|114798.4865|115694.7855|0.9923|
|take-mixed|all-null|15554.6275|296.8720|52.3951|
|take-mixed|compressed|63896.1390|65196.4448|0.9801|
|take-mixed|dense|66935.2560|67155.7897|0.9967|
|take-mixed|long-null|15917.2933|15900.7007|1.0010|
|take-mixed|nullable|118339.4235|119250.6705|0.9924|
|take-mixed|nullable-legacy|117398.7995|119440.6180|0.9829|
|take-range|all-null|57678.1822|3.7890|15222.5342|
|take-range|compressed|101981.1838|21138.1808|4.8245|
|take-range|dense|108888.1822|37893.0780|2.8736|
|take-range|long-null|55325.8350|151.3888|365.4554|
|take-range|nullable|157148.7867|43939.2595|3.5765|
|take-range|nullable-legacy|155494.0650|42104.4970|3.6931|
|take-repeated|all-null|634.6898|300.2135|2.1141|
|take-repeated|compressed|54270.7948|55001.2540|0.9867|
|take-repeated|dense|59132.9125|59359.9388|0.9962|
|take-repeated|long-null|785.6995|802.2208|0.9794|
|take-repeated|nullable|111381.9460|108389.0702|1.0276|
|take-repeated|nullable-legacy|109129.3617|106967.8005|1.0202|
|write|all-null|0.4388|0.4980|0.8810|
|write|compressed|67506.6185|68160.5642|0.9904|
|write|dense|83321.0528|83616.6860|0.9965|
|write|long-null|303.4183|319.9307|0.9484|
|write|nullable|117112.8438|106899.1548|1.0955|
|write|nullable-legacy|106780.6803|106858.2432|0.9993|

Protocol /private/tmp/pco-final-probe-protocol.md; raw /private/tmp/pco-resource-final; analyzer output /private/tmp/pco-resource-final-summary.json; exact assembly/profile /private/tmp/pco-attribution-final. Two processes/version,8iterations/process, medians of warm iterations>=2 fixed in advance. CPU100Hz quantization means zeros are resolution-limited; RSS whole-process incl fixture compression and inspection, not operation peak. No IO claim.

Final hot frame152B vs previous632B/base504B; all-null generic Scalar clone removed. This fixes all-null warmed latency but dense/nullable/legacy remain+4.46/+5.53/+5.95ns per probe. Do not self-waive confirmed regressions or infer causal cross-run9f improvement.

Correction: probe-first selects rows−2048. For nullable fixtures that index is missing, so the242× result is first missing-value resolution, NOT first defined decode. Dense row is defined. First mode creates stateless probe each iteration, warmed mode retains a probe; do not combine different patterns into a break-even estimate. Complete open+first is separate, buffers/validity already materialized; noIO claim.

Final internal resource review ACCEPTED EXPLICIT TRADEOFF at58fd/615a, advisory only; NOT GitHub approval, performance-CI waiver or readiness to merge. PR remains DRAFT. Exact final checks77successful/11skipped; CodSpeed FAIL8regressions2improvements; two-human-review policy pending. Maintainers must decide performance-gate acceptance. Warm probe-dominated pipelines remain15–18% slower, metadata-only open13–15% slower, fullscan flat; no causal writer-win claim. No further variants, unsolicited review requests/DMs or agent approval/merge. Prior adverse evidence retained.
