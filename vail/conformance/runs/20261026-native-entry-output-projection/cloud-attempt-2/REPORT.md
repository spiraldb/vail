# H051 final matched resource evidence

Exact reviewed e3977aed protocol, one fresh attempt; no variants/retries. Candidate production6aac81e1/test4902bc56 normalLinuxnativeec71f970260859c2510490dc8d48d48538d45bad1b364887a60f60f1479e2b63; baseline6cb1fe23 productionequivalent mainc3f. Complete2100sourcehashes and69frontend/input fingerprints verified. Protected baseline overlay restoration positive, allgroupsclean. Candidate freshNORMALwheel build/enrollment, Linux4Rust/438PythonPASS before timings. Baseline identicalpublictest2semanticPASS/2resourceFAIL: growingunusedallocation and observableforcedspill. Candidate positive temporarywrites<8MiB versus baseline330,039,296bytes under EXACTunchanged originalsource charges (not an exactcandidateIOmeasurement). Candidateallocatedbytes1.375965→1.288185MB on growinghidden/definedunusedowner fixture; fixtureconstruction outsideaccounting.

Eight frozenbalanced independent processes,4/version; each includesidenticalwarmups+4repeatedtimedactions perquery. Everyfull3result matched `{total:1246497536,present:57513340,high:24865771}` (recordedcompletebaseline reference, not independentoracle). Results below are means of4independent-processmedians, not16independenttrials.

|Publicquery|Baseline wall|Candidate wall|Baseline core-sec|Candidate core-sec|
|---|---:|---:|---:|---:|
|entries().select(GQ), fullaggregate|0.951717s|0.594192s|19.521867|10.822081|
|ALLfieldprojection control, identicalaggregate|0.952351s|0.956928s|19.671049|19.891288|

Projected wall1.602x/core1.804x mean-ratio improvement; all4adjacent pairs improve wall(1.569,1.607,1.576,1.654x) andCPU(1.786,1.778,1.766,1.887x). Fullprojectioncontrol is adverse in aggregate: wall+0.48%,CPU+1.12%; wallbaseline/candidatepairs1.0002,0.9687,0.9862,1.0258, CPU0.9859,0.9753,0.9657,1.0289. Preserve alloutcomes; no no-regression/noise/causalexplanationclaim. WholeprocesspeakRSSmean3.366296GB→3.261405GB (~3.12%lower), INCLUDINGbothqueries/construction/warmups, notprojectedquerymemoryalone.

The improvement is general consumer-local fieldprojection before customentryoperatorconcat/spill/take, not benchmarkmatching. Sourceproducer execution/sharedcache/order and originalfullbatchbytes/id64MiB/8192input-retention decisions remain unchanged; selectedoutput Arrow estimate under32MiB/8192heuristic is an explicitapprovedresourcepolicychange. Fulloutputestimators unchanged; preciseoldchunkboundary/dominance not promised. Publiccorrectnesscoversaliases, sharedwideconsumeractualpayload, logicalnullancestors,LargeList, variablewidth/skew4097rows andcomputedfallback. Allocation/tempIO/processRSS are separateevidence.

No sourceIO/decode pruning, fullownerrelease, postcandidateCPUframe-elimination, SIMD/ALUsaturation, zero-copy, universalpipeline speedup orHailcomparison is claimed. Baselineprofilebee7e4e4 establishescopy-heavycallerclass; thisattempt containsnooptionalprofile/sysctlchanges. Priorattempt1 invalidtstruct.fields constructorfailedBEFOREtimedactions andremainsfrozen. Fullprojectionpolicytradeoff remains forindependent/rootacceptance.

Hostdownloadhashverified originalarchive; allownedcommandsincludingbaselineexpectedfailurecertifiedclean; provider-after verifiesTERMINATED/2700sSTOP. No VMleftactive. ScopedH051casecompletependingreview; implicitaggregateprojection/source-layout H028/H012/H008/H052 andbroaderH051remainopen.
