import sys,importlib.abc,importlib.util,hashlib,json,time
from pathlib import Path
mode,rep=sys.argv[1:]
root=Path('/tmp/bgen-order-certificate-20261023')
native_path='/Users/danielking/.codex/worktrees/shared-annotation-aggregates/vail/vail/python/vail/_native.abi3.so'
reader_path=root/('reader_'+mode+'.py')
class Overlay(importlib.abc.MetaPathFinder):
 def find_spec(self,fullname,path=None,target=None):
  if fullname=='vail._native':return importlib.util.spec_from_file_location(fullname,native_path)
  if fullname=='vail.bgen':return importlib.util.spec_from_file_location(fullname,reader_path)
sys.meta_path.insert(0,Overlay())
sys.path.insert(0,'/Users/danielking/.codex/worktrees/ordered-prefetch-cores/vail/vail/python')
import vail as hl
import vail._native as native
sys.path.insert(0,'/tmp/full-local-upstream-prep-20261022/harness')
from benchmark_telemetry import Measurement
mt=hl.import_bgen('/tmp/full-local-upstream-cache-20261022/sim_ukb/sim_ukb.bgen',sample_file='/tmp/full-local-upstream-cache-20261022/sim_ukb/sim_ukb.sample',entry_fields=['GP'],n_partitions=8)
mt=mt.annotate_rows(info_score=hl.agg.info_score(mt.GP));t=mt.rows().select('info_score')
receipt=dict(mode=mode,rep=rep,native=native.__file__,native_sha256=hashlib.sha256(open(native.__file__,'rb').read()).hexdigest(),reader=str(reader_path),reader_sha256=hashlib.sha256(reader_path.read_bytes()).hexdigest())
measurement=Measurement(root/'paired'/mode/rep).start();start=time.perf_counter()
count=t._force_count()
result=measurement.stop();receipt.update(count=count,wall_seconds=time.perf_counter()-start,measurement=result)
(root/'paired'/mode/rep/'receipt.json').write_text(json.dumps(receipt,indent=2))
print(json.dumps(receipt),flush=True)
