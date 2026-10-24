import gzip, hashlib, importlib, json, os, pathlib, sys, time
sys.path.insert(0,'/home/danielking/cloud-scan-20261023/supplemental')
from benchmark_telemetry import Measurement
b=pathlib.Path('/home/danielking/parallel-vcf-export'); mode, storage, repetition=sys.argv[1:]; out=b/'results'/f'{mode}-{storage}-{repetition}';out.mkdir(exist_ok=True)
hl=importlib.import_module('hail' if mode=='hail' else 'vail')
if mode=='hail':
 hl.init(master='local[32]',quiet=True,log=str(out/'hail.log'),spark_conf={'spark.driver.memory':'64g','spark.executor.memory':'64g','spark.driver.maxResultSize':'8g','spark.ui.enabled':'false','spark.sql.shuffle.partitions':'32'})
input_path=b/'data'/('profile-native.mt' if storage=='native' else 'profile.mt')
output=out/'out.vcf.bgz'
m=Measurement(out/'telemetry').start(); start=time.perf_counter()
try:
 matrix=hl.MatrixTable.read_legacy(input_path) if mode!='hail' and storage=='legacy' else hl.read_matrix_table(str(input_path))
 hl.export_vcf(matrix,str(output))
finally:
 elapsed=time.perf_counter()-start; telemetry=m.stop()
h=hashlib.sha256();record_count=0
with gzip.open(output,'rb') as f:
 for line in f:
  h.update(line)
  record_count+=not line.startswith(b'#')
result={'mode':mode,'storage':storage,'repetition':repetition,'seconds':elapsed,'telemetry':telemetry,'bytes':output.stat().st_size,'decompressed_sha256':h.hexdigest(),'records':record_count,'native_module':str(getattr(hl,'_native',None))}
(out/'result.json').write_text(json.dumps(result,indent=2));print(json.dumps(result),flush=True)
if mode=='hail': hl.stop()
