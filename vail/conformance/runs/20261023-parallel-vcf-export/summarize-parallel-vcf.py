import collections,json,pathlib,statistics
b=pathlib.Path('/home/danielking/parallel-vcf-export/results')
g=collections.defaultdict(list)
for p in b.glob('*-*-*/result.json'):
 if p.parent.name.endswith('-profile'):continue
 j=json.loads(p.read_text());g[j['mode'],j['storage']].append(j)
rows=[]
for (mode,storage),samples in sorted(g.items()):
 assert len(samples)==3,(mode,storage,len(samples))
 rows.append(dict(mode=mode,storage=storage,samples=3,median_seconds=statistics.median(j['seconds'] for j in samples),median_cpu_seconds=statistics.median(j['telemetry']['cpu_seconds'] for j in samples),peak_rss_gib=max(j['telemetry']['peak_rss_gib'] for j in samples),records=sorted({j['records'] for j in samples}),decompressed_sha256=sorted({j['decompressed_sha256'] for j in samples}),output_bytes=sorted({j['bytes'] for j in samples})))
assert len(rows)==5
for storage in ['legacy','native']:
 c=next(r for r in rows if r['mode']=='candidate' and r['storage']==storage)
 a=next(r for r in rows if r['mode']=='baseline' and r['storage']==storage)
 assert c['decompressed_sha256']==a['decompressed_sha256'],(c,a)
 assert c['records']==a['records']==[24885]
 c['speedup_vs_baseline']=a['median_seconds']/c['median_seconds']
 c['relative_time_vs_hail']=c['median_seconds']/next(r for r in rows if r['mode']=='hail')['median_seconds']
(b/'summary.json').write_text(json.dumps(rows,indent=2)+'\n');print(json.dumps(rows,indent=2))
