# H051 cloud1: correctness passes, matched constructor protocol error

Exact53374 protocol. BaselineRust2semanticPASS/2resourceFAIL asrequired: allocationgrowth, forcedmulti-batchtemporarywrites330,039,296bytes. Candidate source4902 normalLinuxwheel/nativeec71f970 built1m13s, Rust4/4PASS/Python438PASS. Candidateallocation1.375965→1.288185MB; candidate forcedspilltest provespositiveprocesswrites<8MiB, not an exact byte estimate.2100source hashes restored/verified, all69front+input fingerprints passed.

Matched comparison stopped BEFORE its first action: full-control constructor accessed nonexistent `t.row.dtype.fields` and `t.key.dtype.fields`; publictstruct is Mapping and must be iterated directly. Thus NOtimingresult/processpairs exist and no improvement/regression claim is supported. This is a protocol API error, not a production-reader failure. Firstbaselineprocess constructorfailure is preserved. No retry.

All owned groups positivelyclean; downloaded originalreceiptarchivehashverified; sourceoverlayrestorationproofsuccess; provider-after verifiesTERMINATED/2700sSTOP. No sysctl/profile changes. Future minimalconstructorfix needs exactreview+rootGO; existingnormalcandidateartifact can only be reused with explicit protocol re-enrollment, never silent substitution.
