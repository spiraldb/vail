from pathlib import Path
import json,statistics
r=Path(__file__).parent;first=json.loads((r/'bench-results.json').read_text());repeat=json.loads((r/'repeat-results.json').read_text());summary={}
for case in first['summary']:
 row={};workers=[x for x in first['results']+repeat['results']if x['case']==case]
 for mode in ['baseline','candidate']+(['hail']if case=='collect_hail'else []):
  selected=[x for x in workers if x['mode']==mode];medians=[{k:statistics.median(s[k]for s in x['samples'])for k in ['wall_seconds','cpu_seconds']}for x in selected]
  row[mode]={k:statistics.median(x[k]for x in medians)for k in ['wall_seconds','cpu_seconds']};row[mode].update(process_medians=medians,processes=len(selected),timed_calls=3*len(selected))
 for key in ['wall','cpu']:row[key+'_change_percent']=100*(row['candidate'][key+'_seconds']/row['baseline'][key+'_seconds']-1)
 summary[case]=row
result={'summary':summary,'first_cohort':first['summary'],'reverse_cohort':repeat['summary'],'scope':'First balanced18workers54calls plus focused8workers24calls reverseorder repeat of collect_hail/take_rekey due unresolvedcontrol; 4freshprocesses/version for those two, 2/version otherwise, 2Hail; median of per-process median3calls after1warmup. Both cohorts retained; no control adjustment. Exact upstream actions; execution tests, not cross-engine values. Hail CPU includes observed stable JVM endpoint lowerbound. Native is preexisting1000x2535subset; Hailinput is250000x1000.','receipts':first['receipts']+repeat['receipts'],'all_inputs_verified_before_after':first['all_inputs_verified_before_after']and repeat['all_inputs_verified_before_after']}
(r/'combined-results.json').write_text(json.dumps(result,indent=2)+'\n')
for case,row in summary.items():print(case,'wall%',round(row['wall_change_percent'],3),'cpu%',round(row['cpu_change_percent'],3),'B',row['baseline']['wall_seconds'],row['baseline']['cpu_seconds'],'C',row['candidate']['wall_seconds'],row['candidate']['cpu_seconds'])
