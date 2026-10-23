import vail as hl
import time,json,threading,psutil,sys
p=psutil.Process();results=[]
for repetition in range(3):
    samples=[];stop=threading.Event()
    def sample():
        while not stop.is_set():
            samples.append({'elapsed':time.perf_counter()-start,'rss':p.memory_info().rss});stop.wait(.1)
    start=time.perf_counter();cpu=p.cpu_times();thread=threading.Thread(target=sample);thread.start()
    t=hl.utils.range_table(100000,n_partitions=32)
    t=t.annotate(xs=hl.range(100).map(lambda i:hl.struct(value=t.idx+i,key=(hl.int64(t.idx)*17+i)%100003)))
    result=t.aggregate(hl.agg.array_agg(lambda x:hl.agg.take(x.value,3,ordering=x.key),t.xs))
    end=time.perf_counter();after=p.cpu_times();stop.set();thread.join();samples.append({'elapsed':end-start,'rss':p.memory_info().rss})
    for i,values in enumerate(result):
        keys=[((v-i)*17+i)%100003 for v in values]
        assert len(values)==3 and keys==sorted(keys) and max(keys)<10
    row={'label':sys.argv[1],'rep':repetition+1,'wall_s':end-start,'core_s':after.user+after.system-cpu.user-cpu.system,'rss_sampled_max':max(s['rss'] for s in samples),'samples':samples}
    results.append(row);print(json.dumps(row),flush=True)
