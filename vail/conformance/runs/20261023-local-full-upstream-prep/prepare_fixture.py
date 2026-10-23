"""Bounded, offline preparation of explicitly selected exact native fixtures."""
import argparse,json,os,signal,subprocess,sys,time
from pathlib import Path
HERE=Path(__file__).resolve().parent

def main():
 p=argparse.ArgumentParser();p.add_argument('--engine',choices=['vail','hail'],required=True)
 p.add_argument('--fixture',choices=['many_partitions_ht','many_ints_ht','profile25_mt','gnomad_dp_sim'],required=True)
 p.add_argument('--partitions',type=int);p.add_argument('--data-dir',type=Path,required=True)
 p.add_argument('--manifest',type=Path,default=HERE/'fixture-manifest.json');p.add_argument('--output',type=Path,required=True)
 p.add_argument('--worker',action='store_true');p.add_argument('--timeout',type=float,default=1800)
 p.add_argument('--minimum-free-gib',type=int,default=8)
 a=p.parse_args()
 if not a.worker:
  import shutil
  if shutil.disk_usage(a.data_dir.parent).free<a.minimum_free_gib*2**30:raise RuntimeError('disk reserve unavailable before fixture preparation')
  a.output.mkdir(parents=True,exist_ok=True);cmd=[sys.executable,str(Path(__file__)),*sys.argv[1:],'--worker']
  with (a.output/'worker.log').open('w') as log:
   process=subprocess.Popen(cmd,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
   try:code=process.wait(a.timeout)
   except subprocess.TimeoutExpired:code=124
   finally:
    try:os.killpg(process.pid,signal.SIGTERM)
    except ProcessLookupError:pass
    if process.poll() is None:
     try:process.wait(3)
     except subprocess.TimeoutExpired:
      try:os.killpg(process.pid,signal.SIGKILL)
      except ProcessLookupError:pass
      process.wait()
  if code:raise SystemExit(code)
  return
 os.environ.update(LOCAL_BENCHMARK_FIXTURE_MANIFEST=str(a.manifest),LOCAL_BENCHMARK_ALLOW_FIXTURE_GENERATION='1',LOCAL_BENCHMARK_OFFLINE='1',LOCAL_BENCHMARK_MIN_FREE_GIB=str(a.minimum_free_gib),VAIL_BENCHMARK_MAX_RSS_GIB='48')
 sys.path.insert(0,str(HERE/'harness'))
 import upstream_benchmark_coverage as coverage
 from benchmark_telemetry import Measurement
 if a.engine=='hail':
  import hail as hl
  hl.init(quiet=True,master='local[32]',log=str(a.output/'hail.log'),spark_conf={'spark.driver.memory':'24g','spark.executor.memory':'24g','spark.ui.enabled':'false','spark.driver.extraJavaOptions':'-XX:ActiveProcessorCount=32'})
 else:import vail as hl
 measurement=Measurement(a.output/'telemetry').start();start=time.perf_counter()
 parameters={a.fixture:a.partitions} if a.partitions else {}
 try:
  paths=coverage._make_exact_fixtures(hl,a.engine,[a.fixture],a.data_dir,parameters)
  receipt=dict(engine=a.engine,fixture=a.fixture,parameters=parameters,paths=paths,wall_seconds=time.perf_counter()-start,telemetry=measurement.stop())
  if a.engine=='vail':
   import vail._native as native,hashlib
   receipt['native_sha256']=hashlib.sha256(Path(native.__file__).read_bytes()).hexdigest()
  (a.output/'receipt.json').write_text(json.dumps(receipt,indent=2)+'\n')
 finally:
  if a.engine=='hail':hl.stop()
if __name__=='__main__':main()
