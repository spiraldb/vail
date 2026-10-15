import json,time,resource
import vail as hl
for n in [1000,10000]:
    t=hl.range_table(n)
    start=time.perf_counter()
    c=hl.call((t.idx*7+3)%100, (t.idx*11+5)%100, phased=True)
    out=t.select(c=c,phase=c.phased,n=c.n_alt_alleles()).collect()
    assert len(out)==n and all(r.phase and r.n == sum(a>0 for a in r.c.alleles) for r in out)
    print(json.dumps({'rows':n,'seconds':time.perf_counter()-start,'max_rss_bytes':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}))
