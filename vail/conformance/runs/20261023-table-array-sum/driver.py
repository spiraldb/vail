import os,sys,json,time,resource,traceback,threading
sys.path.append("/private/tmp/linreg-hail-pypi-latest/lib/python3.12/site-packages")
import psutil
engine=sys.argv[1]
if engine=='hail':
 import hail as hl
 hl.init(master='local[32]', quiet=True, log='/tmp/table-array-sum-followup/hail.log', spark_conf={
  'spark.driver.memory':'24g','spark.executor.memory':'24g',
  'spark.local.dir':'/tmp/table-array-sum-followup/spark',
  'spark.driver.extraJavaOptions':'-XX:ActiveProcessorCount=32',
  'spark.executor.extraJavaOptions':'-XX:ActiveProcessorCount=32'})
else:
 import vail as hl

def query(case):
 if case=='test_large_range_matrix_table_sum':
  m=hl.utils.range_matrix_table(n_cols=500000,n_rows=10000,n_partitions=2500)
  m=m.annotate_entries(x=m.col_idx+m.row_idx)
  return m.annotate_cols(foo=hl.agg.sum(m.x))._force_count_cols()
 if case=='test_table_aggregate_array_sum':
  t=hl.utils.range_table(10000000)
  values=t.aggregate(hl.agg.array_sum(hl.range(0,100)))
  assert list(values)==[10000000*i for i in range(100)]
  return dict(length=len(values),last=values[-1])
 if case=='representative_grouped_coordinate_mean':
  m=hl.utils.range_matrix_table(10000,1000,n_partitions=250)
  m=m.annotate_entries(x=hl.float64(m.row_idx+m.col_idx))
  return m.group_rows_by(g=m.row_idx%3).aggregate(x=hl.agg.mean(m.x))._force_count_rows()
 raise ValueError(case)

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
  path=f"/tmp/table-array-sum-followup/{engine}-{self.case}-{self.rep}-telemetry.jsonl"
  with open(path,'w') as f:
   for row in self.rows:f.write(json.dumps(row)+'\n')
  return dict(tree_core=sum(self.values.values())-self.base,tree_peak_rss=self.maxrss,telemetry=path)
 def start(self):
  self.thread=threading.Thread(target=self.run,daemon=True);self.thread.start()

print(json.dumps(dict(event='initialized',engine=engine)),flush=True)
while not os.path.exists('/tmp/table-array-sum-followup/GO'):
 time.sleep(.1)
for case in ['test_table_aggregate_array_sum']:
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
