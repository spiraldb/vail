import glob,hashlib,json,pathlib,statistics,subprocess,tarfile,xml.etree.ElementTree as ET,zipfile
def stream_digest(f):
 h=hashlib.sha256()
 for chunk in iter(lambda:f.read(1024*1024),b''):h.update(chunk)
 return h.hexdigest()
root=pathlib.Path('/home/danielking/h092-boolean-reducers');out=root/'results'
assert (out/'measurements-complete').exists()
summary={}
for q in ['ad_any','pl_bundle']:
 values={}
 for v in ['baseline','candidate']:
  groups=[]
  for p in out.glob(f'paired-{v}-*-{q}.jsonl'):
   rows=[json.loads(x) for x in p.read_text().splitlines()][2:]
   assert len(rows)==3
   groups.append({k:statistics.median(r[k] for r in rows) for k in ['seconds','cpu_seconds','maxrss_kib','minor_faults','major_faults']})
   pass # compare exact integer fields and parallel float sums below
  assert len(groups)==2
  values[v]={k:statistics.median(r[k] for r in groups) for k in groups[0]}
 b,c=values['baseline'],values['candidate'];values['wall_speedup']=b['seconds']/c['seconds'];values['cpu_reduction_fraction']=1-c['cpu_seconds']/b['cpu_seconds'];summary[q]=values
import math
references={}
for q in ['ad_any','pl_bundle']:
 for p in out.glob(f'paired-*-*-{q}.jsonl'):
  rows=[json.loads(x) for x in p.read_text().splitlines()][1:]
  for r in rows:
   if q not in references: references[q]=r['result']
   a,b=references[q],r['result']
   if q=='ad_any':assert a==b
   else:
    assert all(a[k]==b[k] for k in ['some','every','n'])
    assert math.isclose(a['fraction'],b['fraction'],rel_tol=1e-11,abs_tol=1e-7)
summary['results']=references
(out/'paired-summary.json').write_text(json.dumps(summary,indent=2)+'\n')
checks={}
for v,p in [('baseline','baseline-tests.xml'),('candidate','candidate-tests.xml')]:
 cases=ET.parse(out/p).findall('.//testcase');fail=[];passed=skipped=0
 for c in cases:
  assert c.find('error') is None
  if c.find('failure') is not None:fail.append(c.get('classname')+'::'+c.get('name'))
  elif c.find('skipped') is not None:skipped+=1
  else:passed+=1
 checks[v]={'passed':passed,'skipped':skipped,'failures':fail}
assert checks['baseline']==checks['candidate'],checks
(out/'test-comparison.json').write_text(json.dumps(checks,indent=2)+'\n')
prov={}
for v,path in [('baseline',pathlib.Path('/home/danielking/h092-segmented-reducers')),('candidate',root)]:
 native=next((path/'env').glob('lib/python*/site-packages/vail/_native*.so'))
 wheel=next((path/'wheels').glob('*.whl'))
 with native.open('rb') as f: installed=stream_digest(f)
 with zipfile.ZipFile(wheel) as z:
  member=next(n for n in z.namelist() if n.endswith('.so') and '/_native' in n)
  with z.open(member) as f: compiled=stream_digest(f)
 assert installed==compiled
 prov[v]={'native_path':str(native),'wheel_path':str(wheel),'installed_native_sha256':installed,'wheel_native_sha256':compiled}
(out/'native-provenance.json').write_text(json.dumps(prov,indent=2)+'\n')
raw={}
for p in out.glob('*-perf.data'):
 raw[str(p)]=subprocess.check_output(['sudo','sha256sum',str(p)],text=True).split()[0]
(out/'raw-profile-sha256.json').write_text(json.dumps(raw,indent=2)+'\n')
source=pathlib.Path('/home/danielking/finite-stddev-20261024/source/vail')
paths=['crates/vail-execution/src/list_aggregate/direct.rs','python/tests/test_segmented_boolean_reducers.py']
(out/'candidate-source-sha256.json').write_text(json.dumps({p:hashlib.sha256((source/p).read_bytes()).hexdigest() for p in paths},indent=2)+'\n')
with tarfile.open('/home/danielking/h092-boolean-receipts.tar.gz','w:gz') as archive:
 for p in out.iterdir():
  if '-perf.data' in p.name or p.name.endswith('-perf-flat.txt') or p.name.endswith('-perf-callgraph.txt'):continue
  archive.add(p,arcname='results/'+p.name)
 for name in ['h092-boolean-build.log','h092-boolean-run.log']:
  p=pathlib.Path('/home/danielking')/name
  archive.add(p,arcname=name)
print(json.dumps(summary,indent=2))
