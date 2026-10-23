import os,sys,json,time,resource,threading,traceback
sys.path.append('/private/tmp/linreg-hail-pypi-latest/lib/python3.12/site-packages')
import psutil
engine,label=sys.argv[1:3]
if engine=='hail':
 import hail as hl
 hl.init(master='local[32]',quiet=True,log='/tmp/coordinate-collect-comparison/hail.log',spark_conf={'spark.driver.memory':'24g','spark.executor.memory':'24g','spark.local.dir':'/tmp/coordinate-collect-comparison/spark','spark.driver.extraJavaOptions':'-XX:ActiveProcessorCount=32','spark.executor.extraJavaOptions':'-XX:ActiveProcessorCount=32'})
else:
 import vail as hl
def query():
 table=hl.utils.range_table(100000,n_partitions=32)
 table=table.annotate(xs=hl.range(100).map(lambda i:hl.or_missing((table.idx+i)%5!=0,table.idx+i)))
 result=table.aggregate(hl.agg.array_agg(lambda x:hl.struct(size=hl.len(hl.agg.collect(x))),table.xs))
 assert len(result)==100 and all(x.size==100000 for x in result)
 return dict(width=len(result),length=result[0].size)
class Monitor:
 def __init__(self, case, rep):
  self.case=case;self.rep=rep;self.values={};self.rows=[];self.stop=threading.Event()
  self.sample();self.base=sum(self.values.values());self.maxrss=0
 def sample(self):
  root=psutil.Process(os.getpid());rss=0
  try:processes=[root]+root.children(recursive=True)
  except psutil.Error:processes=[root]
  for p in processes:
   try:
    key=(p.pid,p.create_time());c=p.cpu_times();self.values[key]=max(self.values.get(key,0),c.user+c.system);rss+=p.memory_info().rss
   except psutil.Error:pass
  return rss
 def run(self):
  while not self.stop.wait(.1):
   rss=self.sample();self.maxrss=max(self.maxrss,rss);self.rows.append(dict(monotonic=time.monotonic(),core=sum(self.values.values())-self.base,rss=rss,retained_processes=len(self.values)))
 def finish(self):
  self.stop.set();self.thread.join();rss=self.sample();self.maxrss=max(self.maxrss,rss)
  path=f"/tmp/coordinate-collect-comparison/{label}-{self.case}-{self.rep}-telemetry.jsonl"
  with open(path,'w') as f:
   for row in self.rows:f.write(json.dumps(row)+'\n')
  return dict(tree_core=sum(self.values.values())-self.base,tree_peak_rss=self.maxrss,telemetry=path)
 def start(self):
  self.thread=threading.Thread(target=self.run,daemon=True);self.thread.start()


for rep in range(1,4):
 monitor=Monitor('coordinate_collect',rep);monitor.start();start=time.perf_counter()
 try:
  result=query();wall=time.perf_counter()-start;metrics=monitor.finish()
  print(json.dumps(dict(label=label,engine=engine,rep=rep,status='pass',wall=wall,result=result,**metrics)),flush=True)
 except Exception as error:
  metrics=monitor.finish();traceback.print_exc();print(json.dumps(dict(label=label,engine=engine,rep=rep,status='fail',error=str(error),**metrics)),flush=True)
if engine=='hail':hl.stop()
