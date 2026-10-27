"""Frozen supported schedule; public upstream bodies/asserts remain unchanged."""
import argparse,json,os,signal,subprocess,time
from pathlib import Path
HERE=Path(__file__).resolve().parent
THREADS={'OMP_NUM_THREADS':'1','OPENBLAS_NUM_THREADS':'1','MKL_NUM_THREADS':'1','NUMEXPR_NUM_THREADS':'1','RAYON_NUM_THREADS':'32','HAIL_BENCHMARK_CORES':'32','HAIL_BENCHMARK_MEMORY':'16g','VAIL_BENCHMARK_MAX_RSS_GIB':'96'}

def main():
 p=argparse.ArgumentParser();p.add_argument('--phase',choices=['preflight','rep1','rep2','rep3'],required=True);p.add_argument('--out',required=True);p.add_argument('--enrollment',required=True);a=p.parse_args()
 out=Path(a.out);out.mkdir(exist_ok=False);started=time.monotonic();cap=3600 if a.phase=='preflight' else 7200
 lock=json.loads((HERE/'artifact-lock.json').read_text());schedule=json.loads((HERE/'schedule.json').read_text())['jobs'];tiers={c['case_id']:c for c in json.loads((HERE/'evidence-tiers.json').read_text())['cases']}
 from enroll import artifacts,check
 artifacts();check(a.enrollment)
 jobs=[j for j in schedule if (j['phase']=='preflight' if a.phase=='preflight' else j['phase']=='timed' and j['repetition']==int(a.phase[-1]))]
 preflight_path=out.parent/'preflight'/'results.json'
 preflight=[] if a.phase=='preflight' else json.loads(preflight_path.read_text())
 passed={(j['case_id'],j['mode']) for j in preflight if j.get('status')=='passed'}
 results=[];failure=None
 try:
  for index,job in enumerate(jobs):
   if time.monotonic()-started>cap-780:raise TimeoutError('No remaining phase budget for complete case cap/cleanup')
   prior=[]
   if a.phase!='preflight':
    for rep in range(1,job['repetition']):
     previous=out.parent/f'rep{rep}'/'results.json'
     prior.extend(json.loads(previous.read_text()))
   nonpass=next((r for r in prior if (r.get('case_id'),r.get('version'),r.get('mode'))==(job['case_id'],job['version'],job['mode']) and r.get('status')!='passed'),None)
   if nonpass is not None:
    results.append({**job,'status':nonpass['status'],'actual_sample':False,'reused_nonpass_status':True});continue
   required='vail-vail' if job['version']=='hail' else job['mode']
   if a.phase!='preflight' and (job['case_id'],required) not in passed:
    results.append({**job,'status':'withheld_current_preflight_failed','actual_sample':False});continue
   folder=out/f'case-{index:04}';folder.mkdir();engine='hail' if job['version']=='hail' else'vail'
   env=os.environ.copy();env.pop('PYTHONPATH',None);env.update(THREADS);env.update(VAIL_SCAN_MODE=job['mode'],VAIL_BENCHMARK_UPSTREAM=str(HERE/'upstream'),VAIL_BENCHMARK_TELEMETRY_DIR=str(folder/'telemetry'),FINAL_SCAN_CASE_DIRECTORY=str(folder))
   command=[lock[job['version']]['python'],str(HERE/'upstream_benchmark_coverage.py'),'--engine',engine,'--case',job['case_id'],'--timeout','600','--fixture-timeout','120','--data-dir','/home/danielking/cloud-scan-20261023/data','--output',str(folder/'result.json')]
   (folder/'command.json').write_text(json.dumps(command)+'\n')
   with (folder/'stdout').open('wb') as stdout,(folder/'stderr').open('wb') as stderr:
    proc=subprocess.Popen(command,env=env,stdout=stdout,stderr=stderr,start_new_session=True)
    try:proc.wait(timeout=750)
    except BaseException:
     # Outer ownership/cleanup is uncertain: retain it, abort phase; host STOP FIRST.
     proc.kill()
     try:proc.wait(timeout=2)
     except BaseException:pass
     raise RuntimeError('Case supervisor failed/timed out; worker session requires provider STOP')
   cleanup=folder/'worker-cleanup.json'
   if not cleanup.exists() or not json.loads(cleanup.read_text()).get('clean'):raise RuntimeError('Case session cleanup unverified; provider STOP first')
   if proc.returncode!=0:raise RuntimeError('Case supervisor failed; stop rather than infer query status')
   report=json.loads((folder/'result.json').read_text())
   if len(report['cases'])!=1:raise RuntimeError('Expected one case receipt')
   result={**job,**report['cases'][0],'actual_sample':True,'evidence_tier':tiers[job['case_id']],'cross_engine_result_compared':False}
   results.append(result);(out/'results.json').write_text(json.dumps(results,indent=2)+'\n')
  check(a.enrollment)
 except BaseException as error:failure=repr(error);raise
 finally:
  (out/'phase-result.json').write_text(json.dumps({'failure':failure,'seconds':time.monotonic()-started,'jobs_completed':len(results),'job_count':len(jobs),'requires_stop_before_collection':failure is not None})+'\n')
if __name__=='__main__':main()
