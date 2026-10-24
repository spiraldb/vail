import argparse,os,pathlib,time,json,resource
import vail as hl
p=argparse.ArgumentParser();p.add_argument('action',choices=['prepare','run']);p.add_argument('--path',default='/private/tmp/vail-wide-native-key.ht');p.add_argument('--repeat',type=int,default=5);a=p.parse_args()
if a.action=='prepare':
 t=hl.range_table(1000000,16)
 t=t.annotate(key=hl.struct(id=t.idx,payload=hl.range(32).map(lambda _:hl.rand_int32(2147483647,seed=17))),value=t.idx%13)
 t=t.key_by('key').select('value');t.write(a.path)
 print(json.dumps({'path':a.path,'rows':1000000,'bytes':sum(f.stat().st_size for f in pathlib.Path(a.path).rglob('*') if f.is_file())}))
else:
 t=hl.read_table(a.path)
 print(json.dumps({'pid':os.getpid(),'phase':'ready'}),flush=True)
 for i in range(a.repeat):
  wall=time.perf_counter();cpu=time.process_time()
  v=t.aggregate(hl.agg.sum(t.value))
  print(json.dumps({'iteration':i,'result':v,'seconds':time.perf_counter()-wall,'cpu_seconds':time.process_time()-cpu,'maxrss':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}),flush=True)
