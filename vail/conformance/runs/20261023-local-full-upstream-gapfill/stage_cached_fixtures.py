"""Offline cached-input fixture setup, only after the initial timings finish."""
import argparse,json,os,subprocess,sys
from pathlib import Path
import psutil
HERE=Path(__file__).resolve().parent
p=argparse.ArgumentParser();p.add_argument('--vail-python',required=True);p.add_argument('--hail-python',required=True);p.add_argument('--initial',type=Path,required=True);p.add_argument('--cache',type=Path,required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--minimum-free-gib',type=int,default=2);a=p.parse_args()
for process in psutil.process_iter(['pid','cmdline']):
 args=process.info.get('cmdline') or []
 if process.pid!=os.getpid() and any(x.endswith('/run_local_upstream.py') for x in args):raise RuntimeError('initial timing runner is still active; no concurrent fixture CPU')
if not (a.initial/'aggregate-summary.json').is_file():raise RuntimeError('initial inventory run has not produced its final summary')
a.output.mkdir(parents=True,exist_ok=True)
bgen=a.cache/'hail-bgen/sim_ukb.bgen';bgen.parent.mkdir(parents=True,exist_ok=True)
original=Path('/tmp/bgen-throughput-data/sim_ukb.bgen')
if bgen.exists():
 if bgen.resolve()!=original.resolve():raise RuntimeError('refuse replacement of BGEN alias')
else:bgen.symlink_to(original)
work=[('hail','sim_ukb_bgen',None),('hail','profile25_mt',None),('hail','many_ints_ht',None)]
work += [(engine,'many_partitions_ht',n) for engine in ['vail','hail'] for n in [10,100,1000]]
results=[]
for engine,fixture,n in work:
 label=f'{engine}-{fixture}'+(f'-p{n}' if n else '');out=a.output/label
 cmd=[getattr(a,engine+'_python'),str(HERE/'prepare_fixture.py'),'--engine',engine,'--fixture',fixture,'--data-dir',str(a.cache),'--manifest',str(HERE/'fixture-manifest.json'),'--output',str(out),'--minimum-free-gib',str(a.minimum_free_gib)]
 if n:cmd += ['--partitions',str(n)]
 env=os.environ.copy();env.update(JAVA_HOME='/Library/Java/JavaVirtualMachines/temurin-11.jre/Contents/Home',EXECUTION_CONCURRENCY='32')
 print('PREPARE',label,flush=True);r=subprocess.run(cmd,env=env);results.append(dict(engine=engine,fixture=fixture,partitions=n,returncode=r.returncode,receipt=str(out/'receipt.json')))
 (a.output/'preparation-status.json').write_text(json.dumps(results,indent=2)+'\n')
 if r.returncode in [87,124] or r.returncode<0:raise RuntimeError(f'bounded preparation stopped: {label} code{r.returncode}')
# Select every case benefiting from these exact cached-input fixtures.
selection=json.loads((a.initial/'selection.json').read_text())
gap=[c['id'] for c in selection if any(f in {'profile25_mt','many_ints_ht','many_partitions_ht','many_partitions_ht1','many_partitions_ht2','sim_ukb_bgen'} for f in c['fixtures'])]
(a.output/'gap-case-ids.json').write_text(json.dumps(gap,indent=2)+'\n')
print('Selected',len(gap),'exact followup cases',flush=True)
