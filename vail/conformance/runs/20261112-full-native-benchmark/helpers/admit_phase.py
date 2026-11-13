"""Check a completed phase's schedule and positive cleanup before continuing."""
import argparse,collections,hashlib,json,tarfile
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('phase');p.add_argument('sha256');a=p.parse_args()
B=Path(__file__).parent;archive=B/(a.phase+'-attempt2.tar.gz');E=B/'evidence-attempt2'
assert hashlib.sha256(archive.read_bytes()).hexdigest()==a.sha256
with tarfile.open(archive)as t:
 for m in t.getmembers():
  assert not Path(m.name).is_absolute()and'..'not in Path(m.name).parts
  assert m.isfile()or m.isdir()
 t.extractall(E)
schedule=json.loads((B/'protocol-attempt2/schedule.json').read_text())['jobs']
expected=[j for j in schedule if j['phase']=='preflight']if a.phase=='preflight'else[j for j in schedule if j['phase']=='timed'and j['repetition']==int(a.phase[-1])]
folder=E/'results'/a.phase;rows=json.loads((folder/'results.json').read_text())
key=lambda r:(r['case_id'],r['version'],r['mode'],r['repetition'])
assert len(rows)==len(expected)and len({key(r)for r in rows})==len(rows)
assert {key(r)for r in rows}=={key(r)for r in expected}
assert [key(r)for r in rows]==[key(r)for r in expected]
phase=json.loads((folder/'phase-result.json').read_text());assert phase['failure']is None and phase['jobs_completed']==len(expected)and phase['job_count']==len(expected)
owned=json.loads((E/'results'/(a.phase+'-supervisor')/'command/result.json').read_text())
assert owned['error']is None and owned['returncode']==0 and owned['owned_group_clean']
fresh=0
for index,r in enumerate(rows):
 if r.get('fresh_attempt')is not True:
  assert r.get('actual_sample')is False
  assert (r.get('reused_nonpass_status')is True and r['status']!='passed')or r['status']=='withheld_current_preflight_failed'
  continue
 fresh+=1;cleanup=json.loads((folder/f'case-{index:04}'/'worker-cleanup.json').read_text());assert cleanup['clean']
assert len(list(folder.glob('case-*/worker-cleanup.json')))==fresh
receipt={'phase':a.phase,'archive_sha256':a.sha256,'jobs':len(rows),'fresh_attempts':fresh,
         'statuses':dict(collections.Counter(r['status']for r in rows)),
         'eligible_execution_samples':sum(r.get('sample_eligible_for_timed_ratio',False)for r in rows),
         'planning_samples':sum(r.get('planning_sample',False)for r in rows),
         'reused_nonpasses':sum(r.get('reused_nonpass_status',False)for r in rows),
         'positive_worker_cleanups':fresh,'stage_owned_cleanup':True,'seconds':phase['seconds']}
(B/(a.phase+'-attempt2-admission.json')).write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps(receipt,indent=2))
