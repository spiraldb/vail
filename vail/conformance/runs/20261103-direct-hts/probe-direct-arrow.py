import sys,os,time,json
from pathlib import Path
import pyarrow as pa
import vail as hl
r=Path(__file__).parent
mt=hl.read_matrix_table('/Users/danielking/.cache/vail-benchmarks/indexed-20261103/profile-first-200.mt')
def query():return hl.split_multi_hts(mt)
def action(q):return q._mir.core.to_arrow()
file=r/'direct-arrow-reference.arrow'
if not os.environ.get('VAIL_EXPERIMENT_DIRECT_HTS'):
 expected=action(query())
 if not file.exists():
  with pa.OSFile(str(file),'wb') as sink:
   with pa.ipc.new_file(sink,expected.schema) as writer:writer.write_table(expected)
 else:
  with pa.memory_map(str(file),'r') as source:assert expected.equals(pa.ipc.open_file(source).read_all())
else:
 with pa.memory_map(str(file),'r') as source:expected=pa.ipc.open_file(source).read_all()
assert action(query()).equals(expected)
t=time.perf_counter();warm=0
while time.perf_counter()-t<1:
 assert action(query()).equals(expected);warm+=1
print(json.dumps({'warmup':warm,'rows':expected.num_rows,'bytes':expected.nbytes}),flush=True)
for sample in range(7):
 t=time.perf_counter();c=time.process_time();q=query();p=time.perf_counter();pc=time.process_time();result=action(q);end=time.perf_counter();ec=time.process_time();assert result.equals(expected)
 print(json.dumps({'sample':sample,'wall':end-t,'cpu':ec-c,'action':end-p,'action_cpu':ec-pc,'construction':p-t,'rows':result.num_rows,'bytes':result.nbytes}),flush=True)
