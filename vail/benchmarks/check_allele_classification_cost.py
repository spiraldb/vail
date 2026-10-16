import json,time,resource
import vail as hl
for length in [128,512,2048]:
    t=hl.Table.parallelize([{'ref':'A'*length,'alt':'A'*(length-1)+'G'} for _ in range(16)])
    start=time.perf_counter()
    out=t.select(kind=hl.allele_type(t.ref,t.alt),transition=hl.is_transition(t.ref,t.alt),transversion=hl.is_transversion(t.ref,t.alt)).collect()
    assert all(r.kind=='SNP' and r.transition and not r.transversion for r in out)
    print(json.dumps({'rows':16,'allele_length':length,'seconds':time.perf_counter()-start,'max_rss_bytes':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}),flush=True)
