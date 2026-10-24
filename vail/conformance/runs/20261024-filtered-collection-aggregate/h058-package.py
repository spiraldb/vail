import pathlib,json,hashlib,zipfile,gzip,statistics,tarfile,subprocess
root=pathlib.Path('/home/danielking/h058-filtered-aggregate');p=root/'results';out={}
for query in ['ad_sum','ad_bundle','pl_count']:
 versions={}
 for version in ['baseline','candidate']:
  rows=[x for line in (p/f'{version}-{query}.jsonl').read_text().splitlines() if 'iteration' in (x:=json.loads(line))];assert len(rows)==6
  measured=rows[1:]
  versions[version]={key:statistics.median(x[key] for x in measured) for key in ['seconds','cpu_seconds','minor_faults','major_faults']}
  versions[version].update(peak_rss_kib=max(x['maxrss_kib'] for x in rows),result=rows[0]['result'])
  assert len({x['result'] for x in rows})==1
 assert versions['baseline']['result']==versions['candidate']['result']
 versions.update(speedup=versions['baseline']['seconds']/versions['candidate']['seconds'],cpu_reduction=1-versions['candidate']['cpu_seconds']/versions['baseline']['cpu_seconds'],minor_fault_reduction=1-versions['candidate']['minor_faults']/versions['baseline']['minor_faults'])
 out[query]=versions
(p/'summary.json').write_text(json.dumps(out,indent=2)+'\n')
paired={}
if (p/'paired-complete').exists():
 for query in ['ad_sum','ad_bundle','pl_count']:
  groups={v:[] for v in ['baseline','candidate']}
  for f in sorted((p/'paired').glob(query+'-*.jsonl')):
   version=f.stem.split('-')[-1]
   rows=[x for line in f.read_text().splitlines() if 'iteration' in (x:=json.loads(line))][1:]
   groups[version].append({'file':f.name,**{key:statistics.median(x[key] for x in rows) for key in ['seconds','cpu_seconds','minor_faults']}})
  both={v:{key:statistics.median(x[key] for x in rows) for key in ['seconds','cpu_seconds','minor_faults']} for v,rows in groups.items()}
  both['speedup']=both['baseline']['seconds']/both['candidate']['seconds'];both['cpu_reduction']=1-both['candidate']['cpu_seconds']/both['baseline']['cpu_seconds'];both['process_medians']=groups
  paired[query]=both
 (p/'paired-summary.json').write_text(json.dumps(paired,indent=2)+'\n')

prov={}
for version,env,wheels in [('baseline',pathlib.Path('/home/danielking/finite-stddev-20261024/base-env'),pathlib.Path('/home/danielking/finite-stddev-20261024/base-wheels')),('candidate',root/'env',root/'wheels')]:
 native=next((env/'lib').glob('python*/site-packages/vail/_native.abi3.so'));wheel=next(wheels.glob('*.whl'))
 with zipfile.ZipFile(wheel) as z:
  item=next(n for n in z.namelist() if n.endswith('_native.abi3.so'));wh=hashlib.sha256(z.read(item)).hexdigest()
 installed=hashlib.sha256(native.read_bytes()).hexdigest();assert installed==wh
 prov[version]={'installed_native_sha256':installed,'wheel_native_sha256':wh,'native_path':str(native),'wheel_path':str(wheel)}
assert prov['baseline']['installed_native_sha256']!=prov['candidate']['installed_native_sha256']
(p/'native-provenance.json').write_text(json.dumps(prov,indent=2)+'\n')
fixture=pathlib.Path('/home/danielking/cloud-scan-20261023/data/vail-from-hail/profile25_mt.mt');fs=[{'path':str(x.relative_to(fixture)),'bytes':x.stat().st_size,'mtime_ns':x.stat().st_mtime_ns} for x in fixture.rglob('*') if x.is_file()]
(p/'fixture-files.json').write_text(json.dumps({'path':str(fixture),'rows':24885,'columns':2535,'entries':24885*2535,'total_bytes':sum(x['bytes'] for x in fs),'files':fs},indent=2)+'\n')
(p/'machine.txt').write_text(subprocess.check_output(['lscpu'],text=True))
for version in ['baseline','candidate']:
 name=f'{version}-perf-report.txt'
 with gzip.open(p/(name+'.gz'),'wb') as f:f.write((p/name).read_bytes())
with tarfile.open('/home/danielking/h058-evidence.tar.gz','w:gz') as t:
 for x in p.rglob('*'):
  if x.is_file() and x.suffix!='.data' and x.name not in ['baseline-perf-report.txt','candidate-perf-report.txt']:t.add(x,arcname='results/'+str(x.relative_to(p)))
 for name in ['h058-baseline-run.log','h058-candidate-build.log','h058-candidate-run.log','h058-measure.py']:t.add('/home/danielking/'+name,arcname=name)
print(json.dumps({"initial":out,"paired":paired},indent=2))
