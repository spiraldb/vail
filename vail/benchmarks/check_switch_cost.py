import json,time,resource
import vail as hl
for n in [16,64,256]:
    table=hl.range_table(100)
    start=time.perf_counter()
    builder=hl.switch((table.idx*7+3)%100)
    for i in range(n):builder.when(i,i+1)
    result=builder.default(-1).collect()
    assert result==[((i*7+3)%100)+1 if (i*7+3)%100<n else -1 for i in range(100)]
    print(json.dumps({'branches':n,'rows':100,'seconds':time.perf_counter()-start,'max_rss_bytes':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}),flush=True)
