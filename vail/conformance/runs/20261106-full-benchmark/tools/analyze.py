"""Summarize all observations; no query execution or outcomes-based enrollment."""
import argparse,collections,csv,hashlib,json,math,statistics
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--reps',default='1,2,3');p.add_argument('--acceptance',type=Path);a=p.parse_args();out=a.output;out.mkdir(exist_ok=False);reps=[int(x) for x in a.reps.split(',')]
if reps!=[1,2,3]:raise RuntimeError('Final report requires exactly distinct repetitions1,2,3')
protocol=Path(__file__).parent/'protocol'
schedule=json.loads((protocol/'schedule.json').read_text())['jobs']
source_lock=json.loads((protocol/'source-lock.json').read_text())
assets=json.loads((protocol/'ASSETS.json').read_text())
for name,digest in assets.items():
 if hashlib.sha256((protocol/name).read_bytes()).hexdigest()!=digest:raise RuntimeError('Frozen protocol asset mismatch:'+name)
if len(json.loads((protocol/'policy.json').read_text())['cases'])!=39:raise RuntimeError('Frozen exclusions differ')
provenance_acceptance=None
if a.acceptance:
 provenance_acceptance=json.loads(a.acceptance.read_text())
 if provenance_acceptance.get('final_input_integrity_accepted') is not True:raise RuntimeError('Explicit final integrity acceptance required')
 for label,entry in provenance_acceptance['sealed_files'].items():
  target=Path(entry['path'])
  if hashlib.sha256(target.read_bytes()).hexdigest()!=entry['sha256']:raise RuntimeError('Accepted receipt mismatch:'+label)
 for stage in ('prepare','preflight','rep1','rep2','rep3','final'):
  entry=provenance_acceptance['sealed_files']['host-'+stage];receipt=json.loads(Path(entry['path']).read_text())
  if receipt.get('stage')!=stage or receipt.get('error') or receipt.get('provider_final_status')!='TERMINATED' or not receipt.get('provider_stopped') or not receipt.get('guest_complete') or not receipt.get('download_verified') or receipt.get('protocol_assets')!=assets:raise RuntimeError('Accepted stage/STOP/assets mismatch:'+stage)
 final=json.loads(Path(provenance_acceptance['sealed_files']['final-enrollment']['path']).read_text())
 if final['inputs'].get('full_sha256') is not True or final['inputs'].get('canonical_set_verified') is not True or final['inputs'].get('files')!=17568:raise RuntimeError('Final full input integrity incomplete')
key=lambda r:(r['case_id'],r['version'],r['mode'])
expected_current={key(r) for r in schedule if r['phase']=='preflight'}
if len(expected_current)!=179:raise RuntimeError('Frozen preflight schedule differs')
provenance={};identities=set();process_receipts=[];verified_processes=set()
lock_path=a.root/'artifact-lock.json'
lock=json.loads(lock_path.read_text())
if lock['current']['compiled_source']!=source_lock['commit']:raise RuntimeError('Current source lock mismatch')
if lock['old']!=json.loads((protocol/'baseline-lock.json').read_text())['old']:raise RuntimeError('Baseline lock mismatch')
provenance['artifact_lock_sha256']=hashlib.sha256(lock_path.read_bytes()).hexdigest()
if provenance_acceptance:
 if provenance_acceptance['sealed_files']['artifact-lock']['sha256']!=provenance['artifact_lock_sha256'] or final['artifacts']['current']['native_sha256']!=lock['current']['native_sha256']:raise RuntimeError('Accepted runtime artifact mismatch')
 provenance['integrity_acceptance_path']=str(a.acceptance);provenance['integrity_acceptance_sha256']=hashlib.sha256(a.acceptance.read_bytes()).hexdigest()
prepath=a.root/'preflight/results.json';pre=json.loads(prepath.read_text())
if len(pre)!=179 or {key(r) for r in pre}!=expected_current or any(r['repetition']!=0 or r.get('sample_eligible_for_timed_ratio') for r in pre):raise RuntimeError('Preflight receipt/schedule mismatch')
provenance['preflight_sha256']=hashlib.sha256(prepath.read_bytes()).hexdigest()
if provenance_acceptance and provenance_acceptance['sealed_files']['preflight-results']['sha256']!=provenance['preflight_sha256']:raise RuntimeError('Accepted preflight measurements differ from analyzed bytes')
preflight_passed={(r['case_id'],r['mode']) for r in pre if r.get('status')=='passed'}
records=[];inputs=[]
for rep in reps:
 path=a.root/f'rep{rep}/results.json';raw=path.read_bytes()
 if provenance_acceptance and provenance_acceptance['sealed_files'][f'rep{rep}-results']['sha256']!=hashlib.sha256(raw).hexdigest():raise RuntimeError('Accepted timed measurements differ from analyzed bytes')
 rows=json.loads(raw);expected=[r for r in schedule if r['phase']=='timed' and r['repetition']==rep]
 if len(rows)!=len(expected) or {key(r) for r in rows}!={key(r) for r in expected} or len({key(r) for r in rows})!=len(expected) or any(r['repetition']!=rep for r in rows):raise RuntimeError('Timed schedule key SET/repetition mismatch')
 indexes={key(r):i for i,r in enumerate(expected)}
 for row in rows:
  if not row.get('fresh_attempt'):continue
  folder=path.parent/f'case-{indexes[key(row)]:04}';ip=folder/'worker-identity.json';cp=folder/'worker-cleanup.json';identity=json.loads(ip.read_text());cleanup=json.loads(cp.read_text())
  unique=(identity['pid'],identity['start'])
  if unique in identities or identity['pid']!=identity['pgid'] or identity['pid']!=identity['sid'] or not cleanup.get('clean'):raise RuntimeError('Duplicate process/session or incomplete cleanup')
  identities.add(unique);verified_processes.add((*key(row),rep));process_receipts.append(dict(repetition=rep,key=key(row),identity=identity,identity_sha256=hashlib.sha256(ip.read_bytes()).hexdigest(),cleanup_sha256=hashlib.sha256(cp.read_bytes()).hexdigest()))
 records+=rows;inputs.append(dict(rep=rep,path=str(path),sha256=hashlib.sha256(raw).hexdigest()))
groups=collections.defaultdict(list)
for r in records:groups[r['case_id'],r['version'],r['mode']].append(r)
def eligible(r):
 return r.get('fresh_attempt') is True and (r['case_id'],r['version'],r['mode'],r['repetition']) in verified_processes and (r['case_id'],'vail-vail' if r['version']=='hail' else r['mode']) in preflight_passed and r.get('sample_eligible_for_timed_ratio') is True and r.get('actual_sample') is True and r.get('planning_sample') is False and r.get('status')=='passed'
metrics={};nonpasses=[];planning=[];withheld=[]
for key,rows in groups.items():
 if sorted(r['repetition'] for r in rows)!=reps:raise RuntimeError('Duplicate/missing group repetitions')
 rows.sort(key=lambda r:r['repetition'])
 if all(eligible(r) for r in rows):
  values={'wall_s':[r['benchmark_seconds'] for r in rows],'cpu_s':[r['telemetry']['cpu_seconds'] for r in rows],'rss_gib':[r['telemetry']['peak_rss_gib'] for r in rows]}
  if not all(isinstance(v,(int,float)) and not isinstance(v,bool) and math.isfinite(v) and v>0 for vs in values.values() for v in vs):raise RuntimeError('Eligible metric nonfinite/nonpositive')
  metrics[key]={m:dict(median=statistics.median(vs),min=min(vs),max=max(vs),samples=vs,by_repetition={str(rep):v for rep,v in zip(reps,vs)}) for m,vs in values.items()}
 else:
  withheld.append(dict(key=key,reason='not all three observations eligible',eligible_observations=sum(eligible(r) for r in rows),observations=rows))
  for r in rows:
   if r.get('planning_sample'):planning.append(r)
   elif not eligible(r):nonpasses.append(r)
rows=[]
for case,version,mode in sorted(groups):
 if version!='current':continue
 row=dict(case_id=case,mode=mode,enrolled_repetitions=len(reps),current=metrics.get((case,'current',mode)),old=metrics.get((case,'old',mode)),hail=metrics.get((case,'hail','hail-hail')))
 row['comparisons']={}
 if row['current']:
  for comparator in ('old','hail'):
   if row[comparator]:
    row['comparisons'][comparator]={m:dict(speedup=row[comparator][m]['median']/row['current'][m]['median'],current_over=row['current'][m]['median']/row[comparator][m]['median'],per_repetition_current_over_comparator={str(rep):row['current'][m]['by_repetition'][str(rep)]/row[comparator][m]['by_repetition'][str(rep)] for rep in reps}) for m in ('wall_s','cpu_s')}
 rows.append(row)
if len(rows)!=179:raise RuntimeError('Current enrolled group count differs')
stats={};adverse=[]
for mode in ('vail-vail','vail-hail'):
 stats[mode]={}
 for comparator in ('old','hail'):
  paired=[r for r in rows if r['mode']==mode and comparator in r['comparisons']];entry=dict(paired_cases=len(paired),enrolled_cases=sum(r['mode']==mode for r in rows))
  for m in ('wall_s','cpu_s'):
   ratios=[r['comparisons'][comparator][m]['speedup'] for r in paired]
   entry[m]=dict(median_speedup=statistics.median(ratios) if ratios else None,geomean_speedup=math.exp(statistics.mean(math.log(v) for v in ratios)) if ratios else None,sum_old_medians=sum(r[comparator][m]['median'] for r in paired),sum_current_medians=sum(r['current'][m]['median'] for r in paired),slower_cases=sum(r['comparisons'][comparator][m]['current_over']>1 for r in paired),at_least_10x_cases=sum(v>=10 for v in ratios))
   for r in paired:
    q=r['comparisons'][comparator][m]
    if q['current_over']>1:adverse.append(dict(case_id=r['case_id'],mode=mode,comparator=comparator,metric=m,**q,current_s=r['current'][m]['median'],comparator_s=r[comparator][m]['median']))
  stats[mode][comparator]=entry
summary=dict(repetitions=reps,current_revision=lock['current']['compiled_source'],baseline_revision=lock['old']['compiled_source'],baseline_native=lock['old']['native_sha256'],scope='cumulative since last full-suite baseline, not isolated flatmap',status='FINAL_ACCEPTED' if provenance_acceptance else 'PROVISIONAL_UNTIL_FINAL_INTEGRITY_AND_PROVIDER_STOP_ACCEPTANCE',provenance=provenance,statistics=stats,inputs=inputs,enrolled_native=112,enrolled_legacy=67,planning_observations=len(planning),nonpass_observations=len(nonpasses),policy_exclusions=39,supplemental_omitted=85,ratio='comparator median divided by current median across three independent processes',validation='Original upstream actions/asserts; no cross-engine result equality claim',cpu='Observed Python+descendant10Hz lower bound; unseen short-lived children may be omitted',rss='Summed sampled RSS, not allocation/PSS/unique RAM or initialization memory')
(out/'withheld-groups.json').write_text(json.dumps(withheld,indent=2)+'\n');(out/'observations.json').write_text(json.dumps(records,indent=2)+'\n');(out/'process-receipts.json').write_text(json.dumps(process_receipts,indent=2)+'\n')
(out/'summary.json').write_text(json.dumps(summary,indent=2)+'\n');(out/'all-cases.json').write_text(json.dumps(rows,indent=2)+'\n');(out/'all-regressions.json').write_text(json.dumps(sorted(adverse,key=lambda x:x['current_over'],reverse=True),indent=2)+'\n');(out/'nonpasses.json').write_text(json.dumps(nonpasses,indent=2)+'\n');(out/'planning.json').write_text(json.dumps(planning,indent=2)+'\n')
fields=['case_id','mode','current_wall_s','current_cpu_s','old_wall_s','old_cpu_s','hail_wall_s','hail_cpu_s','wall_speedup_vs_old','cpu_speedup_vs_old','wall_speedup_vs_hail','cpu_speedup_vs_hail']
with (out/'all-cases.csv').open('w') as f:
 w=csv.DictWriter(f,fieldnames=fields);w.writeheader()
 for r in rows:
  flat={k:r[k] for k in ('case_id','mode')}
  for v in ('current','old','hail'):
   for m in ('wall_s','cpu_s'):flat[v+'_'+m]=r[v][m]['median'] if r[v] else None
  for v in ('old','hail'):
   for m in ('wall','cpu'):flat[m+'_speedup_vs_'+v]=r['comparisons'].get(v,{}).get(m+'_s',{}).get('speedup')
  w.writerow(flat)
lines=['# Full supported benchmark rerun','', '**Integrity status: '+summary['status']+'**','', 'Current `c82d03056`; baseline last full-suite `517c8753`/native4761. Three fresh processes per version/case, original upstream actions. Old-to-current measures cumulative changes.','', '| Input | Comparator | Pairs | Median wall speedup | Median CPU speedup | Slower wall | Higher CPU |','|---|---|---:|---:|---:|---:|---:|']
for mode in stats:
 for comparator,e in stats[mode].items():
  w=e['wall_s'];c=e['cpu_s'];lines.append(f"| {mode} | {comparator} | {e['paired_cases']}/{e['enrolled_cases']} | {w['median_speedup']:.3f}× | {c['median_speedup']:.3f}× | {w['slower_cases']} | {c['slower_cases']} |" if e['paired_cases'] else f'| {mode} | {comparator} | 0 | — | — | — | — |')
lines+=['','## split_multi_hts','']
for r in rows:
 if 'split_multi_hts' in r['case_id']:lines.append('```json\n'+json.dumps(r,indent=2)+'\n```')
lines+=['','## Largest wall-time regressions versus Hail','', '| Case | Input | Vail seconds | Hail seconds | Vail/Hail |','|---|---|---:|---:|---:|']
for r in sorted((x for x in adverse if x['metric']=='wall_s' and x['comparator']=='hail'),key=lambda x:x['current_over'],reverse=True):lines.append(f"| {r['case_id']} | {r['mode']} | {r['current_s']:.6g} | {r['comparator_s']:.6g} | {r['current_over']:.3f}× |")
lines+=['',f'Planning observations: {len(planning)}; nonpass/withheld observations: {len(nonpasses)}. All39policy exclusions and85supplemental omissions retained. Missing/failed samples never receive invented timings.','', 'CPU is observed descendant accounting at10Hz, a lower bound. Sampled summed RSS can double-count shared pages. No cross-engine semantic equality claim; original action/assertion evidence only. Fixed cyclic order and uncontrolled OS cache. All adverse wall/CPU observations remain in all-regressions.json; all cases and samples are linked in CSV/JSON. Final input integrity acceptance is recorded separately.']
(out/'SUMMARY.md').write_text('\n'.join(lines)+'\n')
(out/'SHA256.json').write_text(json.dumps({p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in out.iterdir() if p.is_file()},indent=2)+'\n')
print(json.dumps(stats,indent=2))
