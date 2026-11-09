import json,os,sys,time,pathlib,resource,hashlib
import vail as hl,vail._native as native
mode,case,fixture,out=sys.argv[1:]
def query():
    mt=hl.read_matrix_table(fixture)
    if case.startswith('unknown_'):
        mt=mt.annotate_rows(replica=hl.array([0])).explode_rows('replica')
    elif case.startswith('duplicate_'):
        mt=mt.annotate_rows(replica=hl.array([0,1])).explode_rows('replica')
    entries=mt.entries()
    if case.endswith('_show'):
        entries.show(handler=lambda text:None)
    elif case.endswith('_take'):
        return entries.head(8192).to_arrow().num_rows
    elif case.endswith('_count'):
        return entries._force_count()
samples=[]
for i in range(6):
    c=time.process_time();t=time.perf_counter();value=query();w=time.perf_counter()-t;c=time.process_time()-c
    if i>=2:samples.append({'wall':w,'cpu':c,'rows':value,'maxrss':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss})
r={'mode':mode,'case':case,'native':native.__file__,'native_sha256':hashlib.sha256(pathlib.Path(native.__file__).read_bytes()).hexdigest(),'allocator':native._benchmark_allocator,'samples':samples}
pathlib.Path(out).write_text(json.dumps(r,indent=2)+'\n')
