import os,sys,json,time,resource,threading,shutil
sys.path.append('/private/tmp/linreg-hail-pypi-latest/lib/python3.12/site-packages')
import psutil
engine=sys.argv[1]
if engine=='hail':
 import hail as hl
 hl.init(master='local[32]',quiet=True,log='/tmp/writer-paired-20261022/hail.log',spark_conf={'spark.driver.memory':'24g','spark.executor.memory':'24g','spark.driver.extraJavaOptions':'-XX:ActiveProcessorCount=32','spark.local.dir':'/tmp/writer-paired-20261022/spark'})
else:
 import vail as hl
base='/tmp/writer-paired-20261022/'+engine+'-input.mt'
m=hl.utils.range_matrix_table(10000,1000,n_partitions=16)
m=m.annotate_entries(x=hl.float64((m.row_idx*31+m.col_idx*17)%10007)/10007,dp=hl.int32((m.row_idx+m.col_idx)%100))
m.write(base)
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
  path=f"/tmp/writer-paired-20261022/{engine}-{self.case}-{self.rep}-telemetry.jsonl"
  with open(path,'w') as f:
   for row in self.rows:f.write(json.dumps(row)+'\n')
  return dict(tree_core=sum(self.values.values())-self.base,tree_peak_rss=self.maxrss,telemetry=path)
 def start(self):
  self.thread=threading.Thread(target=self.run,daemon=True);self.thread.start()


for rep in range(1,4):
 out='/tmp/writer-paired-20261022/'+engine+'-out.mt'
 m=hl.read_matrix_table(base)
 monitor=Monitor('representative_profile_write',rep);monitor.start();start=time.perf_counter()
 try:
  m.write(out)
  wall=time.perf_counter()-start;metrics=monitor.finish()
  print(json.dumps(dict(engine=engine,rep=rep,status='pass',wall=wall,**metrics)),flush=True)
 finally:
  shutil.rmtree(out,ignore_errors=True)
shutil.rmtree(base,ignore_errors=True)
if engine=='hail':hl.stop()
