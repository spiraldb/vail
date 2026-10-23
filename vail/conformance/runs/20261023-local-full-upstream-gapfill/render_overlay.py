"""Select latest explicit phase per case, without double-counting reused samples."""
import argparse,json,subprocess,sys
from pathlib import Path
HERE=Path(__file__).resolve().parent
p=argparse.ArgumentParser();p.add_argument('original',type=Path);p.add_argument('followup',type=Path);p.add_argument('output',type=Path);a=p.parse_args()
def read(path):return json.loads(path.read_text())
def write(path,value):path.parent.mkdir(parents=True,exist_ok=True);path.write_text(json.dumps(value,indent=2)+'\n')
selection=read(a.original/'selection.json');followup_ids={c['id'] for c in read(a.followup/'selection.json')}
write(a.output/'selection.json',selection)
write(a.output/'provenance.json',dict(original=read(a.original/'provenance.json'),followup=read(a.followup/'provenance.json'),policy='One sample per engine/case/repetition. Followup cases replace original reports; reused Vail samples retain original timing/telemetry provenance. No sample duplication.'))
for rep in sorted(a.original.glob('rep-*')):
 if not (rep/'selection.json').is_file():continue
 out=a.output/rep.name;write(out/'selection.json',selection)
 write(out/'settings.json',dict(title='Latest full local benchmark comparison',comparison_note='Exact frozen source with explicit fixture/index followup. Missing fixtures and failures remain separate; no representative substitution.'))
 for engine in ['vail','hail']:
  original={c['id']:c for c in read(rep/f'{engine}.json')['cases']}
  newpath=a.followup/rep.name/f'{engine}.json';new={c['id']:c for c in read(newpath)['cases']} if newpath.exists() else {}
  cases=[]
  for spec in selection:
   ident=spec['id'];source=newpath if ident in followup_ids and ident in new else rep/f'{engine}.json'
   item=dict(new[ident] if ident in followup_ids and ident in new else original[ident]);item['report_source']=str(source);cases.append(item)
  write(out/f'{engine}.json',dict(engine=engine,cases=cases))
 subprocess.run([sys.executable,str(HERE/'harness/render_affected_benchmarks.py'),str(out)],check=True)
subprocess.run([sys.executable,str(HERE/'aggregate_runs.py'),str(a.output)],check=True)
