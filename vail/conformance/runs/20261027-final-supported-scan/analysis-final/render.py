import pathlib,json,math,csv,statistics,collections,argparse,hashlib
p=argparse.ArgumentParser();p.add_argument('output',type=pathlib.Path);a=p.parse_args();o=a.output;s=json.load(open(o/'summary.json'));raw={}
for inp in s['inputs']:
 rows=json.load(open(inp['path']))
 for x in rows:
  if x.get('sample_eligible_for_timed_ratio') is True and x.get('actual_sample') is True and x['status']=='passed':raw[(x['case_id'],x['version'],x['mode'],x['repetition'])]=x
lines=['# Three-repetition benchmark results','', '**Integrity status: '+s['status']+'**. Frozen benchmark native4761 / production517c8753, BEFORE pruning73201cc4c. These are not results for latest main.','', 'Three eligible observations per version/case; ratio of medians. All cross-engine result comparisons remain false.','', '| Comparison | Cases | Median wall speedup | Geometric mean wall speedup | Summed-median wall speedup | Median observed-CPU speedup | Wall slower | CPU higher |','|---|---:|---:|---:|---:|---:|---:|---:|']
for g in ['native','legacy']:
 for b,z in s['statistics'][g].items():
  w=z['wall_seconds'];c=z['observed_cpu_seconds'];lines.append(f"| Frozen {g} vs {b} | {z['paired_cases']} | {w['speedup_distribution']['median']:.3f}× | {w['speedup_distribution']['geometric_mean']:.3f}× | {w['ratio_summed_medians']:.3f}× | {c['speedup_distribution']['median']:.3f}× | {w['current_higher_cases']} | {c['current_higher_cases']} |")
lines+=['','## Hail = 1.0 normalization','', 'Median of per-case normalized ratios; lower is better. This is not the inverse of the arithmetic mean speedup and not a ratio of aggregate totals.','', '| Input mode | Cases | Hail wall | Old wall/Hail | Frozen wall/Hail | Hail observed CPU | Old CPU/Hail | Frozen CPU/Hail |','|---|---:|---:|---:|---:|---:|---:|---:|']
for g in ['native','legacy']:
 rr=[r for r in json.load(open(o/f'{g}-all-cases.json')) if r['hail']]
 vals=[statistics.median(r[v][m]['median']/r['hail'][m]['median'] for r in rr) for m in ['wall_seconds','observed_cpu_seconds'] for v in ['old','current']]
 lines.append(f'| {g} | {len(rr)} | 1.0 | {vals[0]:.3f} | {vals[1]:.3f} | 1.0 | {vals[2]:.3f} | {vals[3]:.3f} |')
if s.get('integrity_acceptance'):
 integrity=s['integrity_acceptance'];lines += ['', 'Final full-input integrity was independently accepted at evidence revision `'+integrity['accepted_evidence_revision']+'`:17,568 files, canonical set and full SHA256 matched; provider terminated. The immutable stage summary originally recorded review pending; this report references the subsequent explicit root/reviewer acceptance.', '', 'Raw action receipt SHA256s:']
 for source in s['inputs']:lines.append('- Rep'+str(source['repetition'])+': `'+source['sha256']+'`')
descriptive=[]
for g in ['native','legacy']:
 rs=json.load(open(o/f'{g}-all-cases.json'))
 for r in rs:
  for base in ['old','hail']:
   if base not in r['comparisons']:continue
   for metric in ['wall_seconds','observed_cpu_seconds']:
    q=r['comparisons'][base][metric]
    if not q['current_greater']:continue
    def val(version,rep):
     x=raw[(r['case_id'],version,'hail-hail' if version=='hail' else r['mode'],rep)]
     return x['benchmark_seconds'] if metric=='wall_seconds' else x['telemetry']['cpu_seconds']
    ratios=[val('current',rep)/val(base,rep) for rep in s['repetitions']]
    descriptive.append(dict(group=g,case_id=r['case_id'],comparator=base,metric=metric,median_ratio=q['current_over_comparator'],per_repetition_ratios=ratios,at_least_10_percent_and_all_three_directions=q['current_over_comparator']>=1.1 and all(x>1 and not math.isclose(x,1,rel_tol=1e-12,abs_tol=0) for x in ratios)))
(o/'all-adverse-with-directions.json').write_text(json.dumps(descriptive,indent=2))
lines+=['','A descriptive highlight means median slowdown>=10% AND all3 individual repetition ratios>1 (within1e-12 of1 is an arithmetic tie, not a statistical threshold). It is not a significance test, confidence interval or proof that smaller differences are noise. Every adverse ratio is retained.','']
for g in ['native','legacy']:
 lines += [f'## {g.title()}: every adverse comparison','', '| Case | Comparator | Metric | Current/comparator | Repetition ratios | >=10%, all3 directions |','|---|---|---|---:|---|---|']
 for x in sorted((x for x in descriptive if x['group']==g),key=lambda x:(x['comparator'],x['metric'],-x['median_ratio'])):
  lines.append(f"| {x['case_id']} | {x['comparator']} | {x['metric']} | {x['median_ratio']:.3f} | {', '.join(f'{y:.3f}' for y in x['per_repetition_ratios'])} | {'yes' if x['at_least_10_percent_and_all_three_directions'] else 'no'} |")
 fast=json.load(open(o/f'{g}-at-least-10x-faster-than-hail.json'));lines += ['',f'## {g.title()}: Hail/current wall speedup >=10× ({len(fast)} cases)','', '| Case | Hail/current | Hail median seconds | Frozen Vail median seconds |','|---|---:|---:|---:|']
 for r in fast:lines.append(f"| {r['case_id']} | {r['hail_over_current_wall']:.3f}× | {r['hail_wall']:.6g} | {r['current_wall']:.6g} |")
# Native Hail speedup distribution artifacts, no plotting dependency and no runtime measurement.
rs=json.load(open(o/'native-all-cases.json'));vals=sorted(r['comparisons']['hail']['wall_seconds']['comparator_over_current'] for r in rs if 'hail' in r['comparisons'])
with (o/'native-speedup-ecdf.csv').open('w') as f:
 w=csv.writer(f,lineterminator="\n");w.writerow(['hail_over_current_wall_speedup','empirical_cdf','n']);w.writerows((x,(i+1)/len(vals),len(vals)) for i,x in enumerate(vals))
lo=math.floor(math.log2(min(vals)));hi=math.ceil(math.log2(max(vals)));edges=[2**(lo+i/2) for i in range(2*(hi-lo)+1)];counts=[0]*(len(edges)-1)
for x in vals:
 i=min(len(counts)-1,max(0,int((math.log2(x)-lo)*2)));counts[i]+=1
assert sum(counts)==len(vals)==110
with (o/'native-speedup-histogram.csv').open('w') as f:
 w=csv.writer(f,lineterminator="\n");w.writerow(['lower_inclusive','upper_exclusive_last_inclusive','count','fraction','n']);w.writerows((edges[i],edges[i+1],n,n/len(vals),len(vals)) for i,n in enumerate(counts))
lines+=['','## Provenance and limits','', '111 native old/current execution pairs;110 Hail/current pairs;67 legacy storage pairs. One intentional planning logical case is separate (9planning observations).1410 scheduled records comprise1408 fresh worker attempts,1407 passed records,1 actual Hail helper failure and2 cached nonpass records.1398 execution observations enter timing ratios. No cached record receives invented CPU/wall.85 supplemental cases omitted.','', 'Old Python3.11 vs frozen Vail Python3.12 is a whole-bundle comparison. Observed CPU is a10Hz worker+observed-descendant lower bound including JVM and telemetry; sampled summed RSS is not PSS/allocatedbytes/uniquephysical memory or initialization peak. Fixed cyclic version order and uncontrolled OS cache: no causal/no-noise/no-regression claim. Existing evidence tiers remain execution-only except one count15 assertion and planning; no cross-engine semantic validation.','', 'All-case CSV files include Hail=1 normalized old/current wall and observedCPU, all3 medians/min/max/relative range/sampleCV. Histogram/ECDF CSVs include110 paired execution cases and exclude planning/missing Hail. Final integrity approval is external to this calculation.']
(o/'THREE-REP-REPORT.md').write_text('\n'.join(lines)+'\n')
manifest={str(p.relative_to(o)):hashlib.sha256(p.read_bytes()).hexdigest() for p in o.iterdir() if p.is_file() and p.name!='DERIVED-SHA256.json'};(o/'DERIVED-SHA256.json').write_text(json.dumps(manifest,indent=2))
print('native>=10x',len(json.load(open(o/'native-at-least-10x-faster-than-hail.json'))),'legacy>=10x',len(json.load(open(o/'legacy-at-least-10x-faster-than-hail.json'))),'descriptive_adverses',sum(x['at_least_10_percent_and_all_three_directions'] for x in descriptive))
