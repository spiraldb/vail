#!/usr/bin/env python3
"""Read-only receipt analysis. No benchmark execution or source mutation."""
import argparse,collections,csv,hashlib,json,math,pathlib,statistics

def summary(v):
 assert v and all(math.isfinite(x) and x>0 for x in v)
 ordered=sorted(v);n=len(v);med=statistics.median(v)
 # Independent median implementation cross-check, including even-count scaffolding runs.
 check=ordered[n//2] if n%2 else (ordered[n//2-1]+ordered[n//2])/2
 assert med==check
 return dict(n=n,median=med,min=min(v),max=max(v),relative_range=(max(v)-min(v))/med,
             sample_cv=statistics.stdev(v)/statistics.mean(v) if n>1 else None)

def distribution(v):
 return dict(n=len(v),median=statistics.median(v),arithmetic_mean=statistics.mean(v),
             geometric_mean=math.exp(statistics.mean(math.log(x) for x in v)),min=min(v),max=max(v)) if v else dict(n=0)

def analyze(root,reps,out,accepted,acceptance_record=None):
 acceptance=None
 if accepted:
  assert acceptance_record is not None,'Final requires explicit external acceptance record'
  acceptance=json.loads(acceptance_record.read_text());assert acceptance['final_input_integrity_accepted'] is True
  receipt=root/'final-attempt-1/recovered/final-enrollment.json';assert hashlib.sha256(receipt.read_bytes()).hexdigest()==acceptance['receipt_sha256']
  integrity=json.loads(receipt.read_text());assert integrity['inputs']['canonical_set_verified'] is True and integrity['inputs']['full_sha256'] is True and integrity['inputs']['files']==17568
  assert integrity['artifacts']['current']['native_sha256']==acceptance['frozen_native_sha256']
  stage_path=root/'final-attempt-1/summary.json';assert hashlib.sha256(stage_path.read_bytes()).hexdigest()==acceptance['stage_summary_sha256']
  stage=json.loads(stage_path.read_text());assert stage['provider_final_status']=='TERMINATED' and stage['full_input_sha_runtime_passed'] is True
 out.mkdir(parents=True,exist_ok=True);samples=collections.defaultdict(list);excluded=[];manifest=[];allrows=[]
 for rep in reps:
  path=root/f'rep{rep}-attempt-1/recovered/rep{rep}/results.json';raw=path.read_bytes();rows=json.loads(raw)
  manifest.append(dict(repetition=rep,path=str(path),sha256=hashlib.sha256(raw).hexdigest(),records=len(rows)))
  seen=set()
  for x in rows:
   key=(x['case_id'],x['version'],x['mode']);assert key not in seen,(rep,key);seen.add(key)
   assert x['repetition']==rep
   if not (x.get('actual_sample') is True and x.get('planning_sample') is False and x.get('sample_eligible_for_timed_ratio') is True and x['status']=='passed'):
    excluded.append(dict(repetition=rep,case_id=x['case_id'],version=x['version'],mode=x['mode'],status=x['status'],actual_sample=x.get('actual_sample'),planning_sample=x.get('planning_sample'),reason=x.get('reason',x.get('error',x.get('reused_nonpass_status')))))
    continue
   assert x.get('measurement_complete_finite_positive') is True
   metrics={'wall_seconds':x['benchmark_seconds'],'observed_cpu_seconds':x['telemetry']['cpu_seconds'],'sampled_peak_summed_rss_gib':x['telemetry']['peak_rss_gib']}
   assert all(isinstance(y,(int,float)) and math.isfinite(y) and y>0 for y in metrics.values()),key
   assert x['evidence_tier']['cross_engine_result_compared'] is False
   samples[key].append(dict(rep=rep,metrics=metrics,evidence_tier=x['evidence_tier']['evidence_tier']))
  allrows+=rows
 agg={}
 for k,v in samples.items():
  assert sorted(x['rep'] for x in v)==sorted(reps),(k,'incomplete_repetitions')
  agg[k]={m:summary([x['metrics'][m] for x in v]) for m in v[0]['metrics']}
 native=[];legacy=[]
 for (case,version,mode),cur in sorted(agg.items()):
  if version!='current':continue
  old=agg.get((case,'old',mode));assert old
  hail=agg.get((case,'hail','hail-hail'))
  r={'case_id':case,'mode':mode,'repetitions':len(reps),'cross_engine_result_compared':False,'evidence_tier':samples[(case,version,mode)][0]['evidence_tier'],'current':cur,'old':old,'hail':hail}
  r['comparisons']={}
  for label,base in [('old',old),('hail',hail)]:
   if not base:continue
   z={}
   for metric in cur:
    c,b=cur[metric]['median'],base[metric]['median'];z[metric]={'current_over_comparator':c/b,'comparator_over_current':b/c,'current_greater':c>b}
    # Positive arithmetic identity, not rounded displayed values, determines adverse lists.
    assert math.isclose((c/b)*(b/c),1,rel_tol=1e-12)
   r['comparisons'][label]=z
  (native if mode=='vail-vail' else legacy).append(r)
 assert len(native)==111 and len(legacy)==67,(len(native),len(legacy))
 assert sum(r['hail'] is not None for r in native)==110
 for group,rows in [('native',native),('legacy',legacy)]:
  (out/f'{group}-all-cases.json').write_text(json.dumps(rows,indent=2))
  fields=['case_id','mode','repetitions','evidence_tier']
  fields+=['hail_normalized_wall','hail_normalized_observed_cpu','old_over_hail_wall','old_over_hail_observed_cpu']
  for v in ['old','current','hail']:
   for m in ['wall_seconds','observed_cpu_seconds','sampled_peak_summed_rss_gib']:fields += [f'{v}_{m}_{stat}' for stat in ['median','min','max','relative_range','sample_cv']]
  for b in ['old','hail']:
   for m in ['wall_seconds','observed_cpu_seconds']:fields += [f'current_over_{b}_{m}',f'{b}_over_current_{m}']
  with (out/f'{group}-all-cases.csv').open('w') as f:
   w=csv.DictWriter(f,fieldnames=fields,lineterminator="\n");w.writeheader()
   for r in rows:
    flat={k:r[k] for k in fields[:4]}
    if r['hail']:
     flat.update(hail_normalized_wall=1.0,hail_normalized_observed_cpu=1.0,old_over_hail_wall=r['old']['wall_seconds']['median']/r['hail']['wall_seconds']['median'],old_over_hail_observed_cpu=r['old']['observed_cpu_seconds']['median']/r['hail']['observed_cpu_seconds']['median'])
    for v in ['old','current','hail']:
     if r[v]:
      for m in r[v]:
       for stat in ['median','min','max','relative_range','sample_cv']:flat[f'{v}_{m}_{stat}']=r[v][m][stat]
    for b,z in r['comparisons'].items():
     for m in ['wall_seconds','observed_cpu_seconds']:
      flat[f'current_over_{b}_{m}']=z[m]['current_over_comparator'];flat[f'{b}_over_current_{m}']=z[m]['comparator_over_current']
    w.writerow(flat)
  for b in ['old','hail']:
   for m in ['wall_seconds','observed_cpu_seconds']:
    adverse=[{'case_id':r['case_id'],**r['comparisons'][b][m],'old':r['old'][m]['median'],'current':r['current'][m]['median'],'hail':r['hail'][m]['median'] if r['hail'] else None} for r in rows if b in r['comparisons'] and r['comparisons'][b][m]['current_greater']]
    (out/f'{group}-current-higher-{m}-than-{b}.json').write_text(json.dumps(sorted(adverse,key=lambda x:x['current_over_comparator'],reverse=True),indent=2))
  fast=[{'case_id':r['case_id'],'hail_over_current_wall':r['comparisons']['hail']['wall_seconds']['comparator_over_current'],'hail_wall':r['hail']['wall_seconds']['median'],'current_wall':r['current']['wall_seconds']['median']} for r in rows if 'hail' in r['comparisons'] and r['comparisons']['hail']['wall_seconds']['comparator_over_current']>=10]
  (out/f'{group}-at-least-10x-faster-than-hail.json').write_text(json.dumps(sorted(fast,key=lambda x:x['hail_over_current_wall'],reverse=True),indent=2))
 (out/'planning-observations.json').write_text(json.dumps([x for x in allrows if x.get('planning_sample') is True],indent=2))
 stats={}
 for label,rs in [('native',native),('legacy',legacy)]:
  stats[label]={}
  for b in ['old','hail']:
   paired=[r for r in rs if b in r['comparisons']];z={'paired_cases':len(paired)}
   for m in ['wall_seconds','observed_cpu_seconds']:
    z[m]={'speedup_distribution':distribution([r['comparisons'][b][m]['comparator_over_current'] for r in paired]),'current_normalized_distribution':distribution([r['comparisons'][b][m]['current_over_comparator'] for r in paired]),'sum_comparator_medians':sum(r[b][m]['median'] for r in paired),'sum_current_medians':sum(r['current'][m]['median'] for r in paired),'current_higher_cases':sum(r['comparisons'][b][m]['current_greater'] for r in paired)}
    z[m]['ratio_summed_medians']=z[m]['sum_comparator_medians']/z[m]['sum_current_medians']
   stats[label][b]=z
 packet={'status':'FINAL_ACCEPTED' if accepted else 'PROVISIONAL_NOT_FINAL_INTEGRITY_ACCEPTED','integrity_acceptance':acceptance,'repetitions':reps,'inputs':manifest,'ratio_definition':'ratio of per-version medians; NOT median of per-repetition ratios','planning_logical_cases_excluded':1,'planning_excluded':sum(x.get('planning_sample') is True for x in excluded),'excluded_records':excluded,'native_execution_pairs':111,'native_hail_pairs':110,'legacy_pairs':67,'supplemental_cases_omitted':85,'statistics':stats,'limits':['Old Python3.11 vs current3.12 is whole-bundle comparison, not Rust-only attribution.','Observed CPU at10Hz lower bound incl JVM and telemetry; short-lived descendants can be missed.','Summed sampled RSS is not PSS, allocated bytes, unique physical memory or process-initialization peak.','Fixed cyclic version order; uncontrolled OS cache; no causal or no-noise/no-regression claim.','All cross-engine result comparisons false; execution-only tiers preserved.','One intentional planning benchmark excluded; Hail gVCF unavailable after one actual failure and later cached status.']}
 (out/'summary.json').write_text(json.dumps(packet,indent=2));print(json.dumps({k:packet[k] for k in ['status','repetitions','native_execution_pairs','native_hail_pairs','legacy_pairs','planning_excluded']}));return packet

if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--root',type=pathlib.Path,required=True);p.add_argument('--repetitions',default='1,2,3');p.add_argument('--out',type=pathlib.Path,required=True);p.add_argument('--final-sha-accepted',action='store_true');p.add_argument('--acceptance-record',type=pathlib.Path);a=p.parse_args();reps=[int(x) for x in a.repetitions.split(',')]
 if a.final_sha_accepted:assert reps==[1,2,3],'Final requires all3repetitions AND external integrity approval'
 analyze(a.root,reps,a.out,a.final_sha_accepted,a.acceptance_record)
