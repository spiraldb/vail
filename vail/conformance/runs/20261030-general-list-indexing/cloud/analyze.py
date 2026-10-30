import json,statistics,sys,hashlib,csv
from pathlib import Path
root=Path(sys.argv[1]);records=json.loads((root/'results.json').read_text());summary=json.loads((root/'summary.json').read_text());assert summary['success']
timed=[r for r in records if r['purpose']=='timed'];pre=[r for r in records if r['purpose']=='preflight'];assert len(timed)==51 and len(pre)==8
assert all(r['status']=='passed' and r['actual_sample'] for r in pre)
rows=[]
for case in sorted({r['case_id'] for r in timed if r['version']!='hail'}):
 for mode in ['vail-vail','vail-hail']:
  groups={v:sorted([r for r in timed if r['case_id']==case and r['mode']==mode and r['version']==v],key=lambda r:r['repetition']) for v in ['baseline','candidate']}
  assert all(len(g)==3 for g in groups.values())
  row={'case':case,'mode':mode,'samples':groups,'eligible':all(r['sample_eligible_for_timed_ratio'] for g in groups.values() for r in g)}
  if row['eligible']:
   for name,read in [('wall',lambda r:r['benchmark_seconds']),('cpu',lambda r:r['telemetry']['cpu_seconds']),('rss',lambda r:r['telemetry']['peak_rss_gib'])]:
    b=[read(r) for r in groups['baseline']];c=[read(r) for r in groups['candidate']];bm=statistics.median(b);cm=statistics.median(c)
    row[name]={'baseline':bm,'candidate':cm,'speedup':bm/cm,'change_pct':100*(cm/bm-1),'paired_candidate_over_baseline':[y/x for x,y in zip(b,c)]}
   hail=[r for r in timed if r['case_id']==case and r['version']=='hail' and r['sample_eligible_for_timed_ratio']]
   if hail:
    assert len(hail)==3;row['hail_wall']=statistics.median(r['benchmark_seconds'] for r in hail);row['candidate_speedup_over_hail']=row['hail_wall']/row['wall']['candidate']
  rows.append(row)
out={'status_counts':{s:sum(r['status']==s for r in timed) for s in {r['status'] for r in timed}},'preflight':len(pre),'timed':len(timed),'rows':rows,'summary_sha256':hashlib.sha256((root/'summary.json').read_bytes()).hexdigest(),'inputs_verified_before_after':summary['inputs_before']==summary['inputs_after'],'candidate_artifact':summary['candidate'],'limits':'Three repetitions; observed descendant CPU lower bound; execution-only evidence; fixed cyclic order; no statistical significance or cross-engine output equality claim'}
(root.parent/'analysis.json').write_text(json.dumps(out,indent=2)+'\n')
for r in rows:
 if not r['eligible']:print(r['case'],r['mode'],'NONPASS');continue
 print(r['case'].split('::')[1],r['mode'],f"wall {r['wall']['baseline']:.4f}→{r['wall']['candidate']:.4f}s ({r['wall']['speedup']:.3f}x), CPU {r['cpu']['baseline']:.2f}→{r['cpu']['candidate']:.2f} ({r['cpu']['change_pct']:+.2f}%)",f"Hail {r.get('hail_wall',0):.3f}s" if 'hail_wall' in r else '')
