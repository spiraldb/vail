import os,sys,threading,json,argparse
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[3]/'benchmarks'))
args=argparse.ArgumentParser()
args.add_argument('--package',default='/private/tmp/vail-profile-baseline-package-20261015')
args.add_argument('--label',default='before')
args.add_argument('--seconds',type=float,default=45)
args.add_argument('--data-dir',type=Path,default=Path('/Users/danielking/projects/vail/vail/local-data/upstream-benchmark-20261015'))
args=args.parse_args()
package=Path(args.package).resolve()
if not (package/'vail'/'_native.abi3.so').is_file(): raise FileNotFoundError(package)
sys.path.insert(0,str(package))
import upstream_benchmark_coverage as runner
out=Path(__file__).resolve().parent
os.environ['VAIL_BENCHMARK_TELEMETRY_DIR']=str(out/(args.label+'-telemetry'))
os.environ['VAIL_BENCHMARK_MAX_RSS_GIB']='16'
class Queue:
    def put(self,value):
        print(json.dumps(value),flush=True)
        if value.get("status")!="ready": (out/(args.label+"-result.json")).write_text(json.dumps(value,indent=2)+"\n")
        if value.get('status')=='ready':
            timer=threading.Timer(args.seconds,lambda:os._exit(124));timer.daemon=True;timer.start()
case=next(c for c in runner.inventory() if c['name']=='test_table_aggregate_linreg')
runner._worker(case,'vail',False,str(args.data_dir.resolve()),Queue())
