import argparse, json, time, resource, subprocess, threading
from pathlib import Path
import vail as hl
p=argparse.ArgumentParser(); p.add_argument('--output',required=True);p.add_argument('--warmup',type=int,default=1);p.add_argument('--runs',type=int,default=3);p.add_argument('--single-partition',action='store_true'); a=p.parse_args()
t=hl.read_table('/Users/danielking/projects/vail/vail/local-data/upstream-benchmark-20261015/vail/many_ints_ht.ht')
if a.single_partition:t=t.naive_coalesce(1)
print('source_partitions',t.n_partitions(),flush=True)
e=hl.agg.array_agg(lambda i:hl.agg.linreg(t.i0+i,[t.i1,t.i2,t.i3,t.i4]),hl.range(75))
results=[]
def compiler_active():
    return any(line.strip().rsplit('/',1)[-1]=='rustc' for line in subprocess.check_output(['ps','-axo','comm'],text=True).splitlines())
i=0
while i<a.warmup+a.runs:
    while compiler_active():time.sleep(.5)
    stop, interference=threading.Event(),threading.Event()
    def monitor():
        while not stop.wait(.1):
            if compiler_active():interference.set()
    thread=threading.Thread(target=monitor,daemon=True);thread.start()
    c=time.process_time(); w=time.perf_counter(); result=t.aggregate(e); elapsed=time.perf_counter()-w;cpu=time.process_time()-c
    stop.set();thread.join()
    if interference.is_set() or compiler_active():
        print('retrying compiler overlap',i,elapsed,flush=True);continue
    def plain(v):
        if isinstance(v,(str,int,float,bool)) or v is None:return v
        if isinstance(v,dict) or hasattr(v,'items'):return {k:plain(x) for k,x in v.items()}
        if isinstance(v,(list,tuple)):return [plain(x) for x in v]
        return repr(v)
    row=dict(wall_seconds=elapsed,cpu_seconds=cpu,warmup=i<a.warmup,result=plain(result))
    results.append(row);print(i,elapsed,cpu,flush=True)
    Path(a.output).write_text(json.dumps(dict(source_partitions=t.n_partitions(),samples=results),indent=2)+'\n')
    i+=1
