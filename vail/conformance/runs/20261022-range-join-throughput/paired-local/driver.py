import os,sys,json,time,resource,traceback,threading
sys.path.append("/private/tmp/linreg-hail-pypi-latest/lib/python3.12/site-packages")
import psutil
engine=sys.argv[1]
if engine=='hail':
 import hail as hl
 hl.init(master='local[32]', quiet=True, log='/tmp/local-range-join-paired-20261022/hail.log', spark_conf={
  'spark.driver.memory':'24g','spark.executor.memory':'24g',
  'spark.local.dir':'/tmp/local-range-join-paired-20261022/spark',
  'spark.driver.extraJavaOptions':'-XX:ActiveProcessorCount=32',
  'spark.executor.extraJavaOptions':'-XX:ActiveProcessorCount=32'})
else:
 import vail as hl

def query(case):
 left=hl.utils.range_table(1000000000)
 right=hl.utils.range_table(1000000000)
 result=left.join(right, 'inner').count()
 assert result==1000000000
 return result

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
  path=f"/tmp/local-range-join-paired-20261022/{engine}-{self.case}-{self.rep}-telemetry.jsonl"
  with open(path,'w') as f:
   for row in self.rows:f.write(json.dumps(row)+'\n')
  return dict(tree_core=sum(self.values.values())-self.base,tree_peak_rss=self.maxrss,telemetry=path)
 def start(self):
  self.thread=threading.Thread(target=self.run,daemon=True);self.thread.start()

print(json.dumps(dict(event='initialized',engine=engine)),flush=True)
while not os.path.exists('/tmp/local-range-join-paired-20261022/GO'):
 time.sleep(.1)
for case in ['test_table_range_join_1B_1B']:
 for rep in range(1,4):
  monitor=Monitor(case,rep);monitor.start()
  before=resource.getrusage(resource.RUSAGE_SELF);start=time.perf_counter()
  try:
   result=query(case);wall=time.perf_counter()-start;after=resource.getrusage(resource.RUSAGE_SELF);metrics=monitor.finish()
   print(json.dumps(dict(event='done',engine=engine,case=case,rep=rep,status='pass',result=result,wall=wall,self_core=after.ru_utime+after.ru_stime-before.ru_utime-before.ru_stime,self_maxrss=after.ru_maxrss,**metrics)),flush=True)
  except Exception as error:
   wall=time.perf_counter()-start;metrics=monitor.finish();traceback.print_exc()
   print(json.dumps(dict(event='done',engine=engine,case=case,rep=rep,status='fail',error=str(error),wall=wall,**metrics)),flush=True)
if engine=='hail':hl.stop()
