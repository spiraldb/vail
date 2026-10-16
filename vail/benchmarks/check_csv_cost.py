import json,time,tempfile,pathlib,resource
import vail as hl
from vail.text import split_line
for width in [64,256,1024]:
    with tempfile.TemporaryDirectory() as directory:
        path=pathlib.Path(directory)/'header.tsv';path.write_text('\t'.join(['a']*width)+'\n')
        start=time.perf_counter();table=hl.import_table(path)
        assert list(table.row)==['a']+[f'a_{i}' for i in range(1,width)]
        print(json.dumps({'workload':'header','fields':width,'seconds':time.perf_counter()-start,'max_rss_bytes':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}),flush=True)
for count in [16,256,4096]:
    table=hl.Table.parallelize([{'line':','.join(['x']*256)} for _ in range(64)])
    markers=[f'missing-{i}' for i in range(count)]
    start=time.perf_counter();rows=split_line(table.line,',',missing=markers).collect()
    assert rows==[['x']*256]*64
    print(json.dumps({'workload':'markers','markers':count,'rows':64,'fields':256,'seconds':time.perf_counter()-start,'max_rss_bytes':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}),flush=True)
