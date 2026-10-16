import json,time,resource
import vail as hl
for n in [16,64,256]:
    table=hl.Table.parallelize([{'text':'A'*8192} for _ in range(16)])
    start=time.perf_counter()
    selector=hl.hamming(table.text,'C'*8192)
    switch=hl.switch(selector)
    for i in range(n):switch.when(i,i)
    result=switch.default(-1).collect()
    assert result==[-1]*16
    print(json.dumps({'workload':'switch','branches':n,'rows':16,'codepoints':8192,'seconds':time.perf_counter()-start,'max_rss_bytes':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}),flush=True)
for n in [16,64,256]:
    table=hl.Table.parallelize([{'text':'A'*8192} for _ in range(16)])
    start=time.perf_counter()
    expression=hl.case(missing_false=True)
    for i in range(n):expression.when(hl.hamming(table.text,'C'*8192)<0,i)
    result=expression.default(-1).collect()
    assert result==[-1]*16
    print(json.dumps({'workload':'case_missing_false','branches':n,'rows':16,'codepoints':8192,'seconds':time.perf_counter()-start,'max_rss_bytes':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}),flush=True)
