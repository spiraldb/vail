import json,time,resource
import vail as hl
for length in [128,512,2048]:
    text=('a😀b中'*(length//4))
    t=hl.Table.parallelize([{'s':text} for _ in range(16)]).annotate_globals(mapping=hl.literal({'a':'XY','😀':'中é'},hl.tdict(hl.tstr,hl.tstr)))
    start=time.perf_counter()
    out=t.select(step=t.s[::-2],translated=t.s.translate(t.mapping)).collect()
    expected=''.join({'a':'XY','😀':'中é'}.get(c,c) for c in text)
    assert all(r.step==text[::-2] and r.translated==expected for r in out)
    print(json.dumps({'rows':16,'codepoints':length,'seconds':time.perf_counter()-start,'max_rss_bytes':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}),flush=True)
