"""Serial full-size upstream attempts on one laptop; preparation only until freeze."""
import argparse,hashlib,json,os,platform,shutil,subprocess,sys,time
from pathlib import Path
import psutil
HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE/'harness'))
import upstream_benchmark_coverage as coverage

def write(path,value):
 path.parent.mkdir(parents=True,exist_ok=True);path.write_text(json.dumps(value,indent=2)+'\n')

def main():
 p=argparse.ArgumentParser()
 for name in ['vail-python','hail-python','output','commit','upstream','data-dir']:p.add_argument('--'+name,required=True)
 p.add_argument('--fixture-manifest',default=str(HERE/'fixture-manifest.json'))
 p.add_argument('--repetitions',type=int,default=3);p.add_argument('--cores',type=int,default=32)
 p.add_argument('--heap',default='24g');p.add_argument('--rss-gib',type=int,default=48)
 p.add_argument('--timeout',type=float,default=250);p.add_argument('--fixture-timeout',type=float,default=1800)
 p.add_argument('--allow-fixture-generation',action='store_true');p.add_argument('--resume',action='store_true')
 p.add_argument('--minimum-free-gib',type=int,default=8)
 p.add_argument('--case',action='append',help='ExactcaseID; validationonly unlessfullselection')
 p.add_argument('--validation-only',action='store_true')
 p.add_argument('--deadline-hours',type=float,default=6)
 a=p.parse_args();deadline=time.monotonic()+a.deadline_hours*3600;out=Path(a.output).resolve();out.mkdir(parents=True,exist_ok=True)
 if shutil.disk_usage(out).free<a.minimum_free_gib*2**30:raise RuntimeError('insufficient free disk; prepare space before exact fixture/run work')
 builds=[x.info for x in psutil.process_iter(['pid','name']) if x.info['name'] in {'cargo','rustc','clang','ld'}]
 if builds and not a.validation_only:raise RuntimeError(f'finish builds before timed suite: {builds}')
 env=os.environ.copy();env.pop('PYTHONPATH',None)
 env.update(VAIL_BENCHMARK_UPSTREAM=a.upstream,LOCAL_BENCHMARK_FIXTURE_MANIFEST=a.fixture_manifest,
  HAIL_BENCHMARK_CORES=str(a.cores),HAIL_BENCHMARK_MEMORY=a.heap,
  VAIL_BENCHMARK_MAX_RSS_GIB=str(a.rss_gib),EXECUTION_CONCURRENCY=str(a.cores),LOCAL_BENCHMARK_MIN_FREE_GIB=str(a.minimum_free_gib),LOCAL_BENCHMARK_OFFLINE='1',
  JAVA_HOME='/Library/Java/JavaVirtualMachines/temurin-11.jre/Contents/Home')
 if a.allow_fixture_generation:env['LOCAL_BENCHMARK_ALLOW_FIXTURE_GENERATION']='1'
 coverage.UPSTREAM=Path(a.upstream);cases=coverage.inventory()
 if a.case:
  requested=set(a.case);cases=[c for c in cases if c['id'] in requested]
  if {c['id'] for c in cases}!=requested:raise ValueError('unknownselectedcase')
 versions={e:subprocess.check_output([getattr(a,e+'_python'),'-c',f'import importlib.metadata;print(importlib.metadata.version("{e}"))'],env=env,text=True).strip() for e in ['vail','hail']}
 native_receipt=subprocess.check_output([a.vail_python,'-c','import vail._native as n,hashlib,json;print(json.dumps(dict(path=n.__file__,sha256=hashlib.sha256(open(n.__file__,"rb").read()).hexdigest())))'],env=env,text=True)
 files=[Path(__file__),*sorted((HERE/'harness').glob('*.py')),Path(a.fixture_manifest)]
 settings=dict(vars(a));settings.pop('resume')
 provenance=dict(native=json.loads(native_receipt),settings=settings,background_builds=builds,platform=platform.platform(),logical_cpus=os.cpu_count(),memory=psutil.virtual_memory().total,versions=versions,
  hashes={str(f):hashlib.sha256(f.read_bytes()).hexdigest() for f in files},
  source_hashes={f:hashlib.sha256((Path(a.upstream)/f).read_bytes()).hexdigest() for f in [*coverage.SOURCES,'fixtures.py']})
 if a.resume and (out/'provenance.json').exists():
  previous=json.loads((out/'provenance.json').read_text());comparison=dict(provenance);comparison['background_builds']=previous.get('background_builds',[])
  if previous!=comparison:raise RuntimeError('resume provenance mismatch')
 else:write(out/'provenance.json',provenance)
 write(out/'selection.json',cases)
 for rep in range(1,a.repetitions+1):
  repdir=out/f'rep-{rep:02}';write(repdir/'selection.json',cases)
  reports={e:dict(engine=e,cases=[]) for e in ['vail','hail']}
  write(repdir/'settings.json',dict(title='Local upstream validation' if a.validation_only else 'Full local upstream comparison',comparison_note='Fresh independentworkers; exactinputs,serialengines,threeattempts. Missingfixturesandfailuresseparate. Validationselectionisnotafullsuite.'))
  for index,case in enumerate(cases):
   vail_pass=False
   for engine in ['vail','hail']:
    if time.monotonic()>deadline:raise RuntimeError('finite suite deadline reached; existing reports preserved')
    if shutil.disk_usage(out).free<a.minimum_free_gib*2**30:raise RuntimeError('disk reserve reached; existing reports preserved')
    path=out/f'rep-{rep:02}'/'attempts'/engine/f'{index:03}.json';path.parent.mkdir(parents=True,exist_ok=True)
    if engine=='hail' and not vail_pass:
     result=dict(case,status='not_run_vail_failed',comparable=False);write(path,dict(cases=[result]));reports[engine]['cases'].append(result);write(repdir/f'{engine}.json',reports[engine]);continue
    if a.resume and path.exists():report=json.loads(path.read_text())
    else:
     env['VAIL_BENCHMARK_TELEMETRY_DIR']=str(out/f'rep-{rep:02}'/'telemetry')
     cmd=[getattr(a,engine+'_python'),str(HERE/'harness/upstream_benchmark_coverage.py'),'--engine',engine,'--case',case['id'],'--timeout',str(a.timeout),'--fixture-timeout',str(a.fixture_timeout),'--data-dir',a.data_dir,'--output',str(path)]
     print(f'START rep{rep} {engine} {case["id"]}',flush=True)
     with path.with_suffix('.log').open('w') as log:run=subprocess.run(cmd,env=env,stdout=log,stderr=subprocess.STDOUT)
     if not path.exists():write(path,dict(cases=[dict(case,status='harness_failed',returncode=run.returncode,comparable=False)]))
     report=json.loads(path.read_text())
    result=report['cases'][0];result.update(source_commit=a.commit,comparable=True,baseline_hardware='sameM3Maxlaptop;seeprovenanceforbackgroundactivity');reports[engine]['cases'].append(result);write(repdir/f'{engine}.json',reports[engine])
    status=result['status'];print(f'DONE rep{rep} {engine} {case["id"]} {status}',flush=True)
    if engine=='vail':vail_pass=status=='passed'
  subprocess.run([sys.executable,str(HERE/'harness/render_affected_benchmarks.py'),str(repdir)],check=True)
 subprocess.run([sys.executable,str(HERE/'aggregate_runs.py'),str(out)],check=True)
if __name__=='__main__':main()
