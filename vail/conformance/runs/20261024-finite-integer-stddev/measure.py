import argparse,json,pathlib,time,resource,os
import vail as hl
p=argparse.ArgumentParser();p.add_argument('action',choices=['prepare','global','grouped']);p.add_argument('--path',default='/private/tmp/vail-stddev-500m.ht');p.add_argument('--repeat',type=int,default=4);a=p.parse_args()
if a.action=='prepare':
 t=hl.range_table(500000000,16)
 t=t.annotate(x=hl.or_missing(t.idx%7!=0,hl.int32(t.idx%1000)),g=hl.int32(t.idx%32)).key_by().select('x','g')
 t.write(a.path)
 print(json.dumps({'rows':500000000,'bytes':sum(f.stat().st_size for f in pathlib.Path(a.path).rglob('*') if f.is_file())}))
else:
 t=hl.read_table(a.path)
 print(json.dumps({'pid':os.getpid(),'phase':'ready'}),flush=True)
 for i in range(a.repeat):
  wall=time.perf_counter();cpu=time.process_time()
  if a.action=='global': result=t.aggregate(hl.agg.stats(t.x).stdev)
  else:
   r=t.group_by(t.g).aggregate(sd=hl.agg.stats(t.x).stdev)
   result=sum(row.sd for row in r.collect())
  print(json.dumps({'iteration':i,'seconds':time.perf_counter()-wall,'cpu_seconds':time.process_time()-cpu,'result':result,'maxrss':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}),flush=True)
