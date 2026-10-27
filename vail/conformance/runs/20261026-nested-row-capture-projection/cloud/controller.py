"""One correctness-then-comparison attempt with positive child cleanup proof."""
import argparse,json,signal
from pathlib import Path
from common import EXPECTED,HERE,environment,guarded
p=argparse.ArgumentParser();p.add_argument('--source',required=True);p.add_argument('--out',required=True);a=p.parse_args()
out=Path(a.out);out.mkdir(parents=True,exist_ok=False)
record={'owned_groups_clean':False,'error':None,'profile_attempted':False,'profile_safe':True}
def interrupted(signum,frame):raise InterruptedError(f'signal {signum}')
for sig in (signal.SIGINT,signal.SIGTERM,signal.SIGHUP):signal.signal(sig,interrupted)
try:
    guarded([EXPECTED['baseline_python'],HERE/'prepare.py','--source',a.source,'--out',out/'gate'],
            out/'prepare-supervisor',out,environment(),1200)
    guarded([EXPECTED['baseline_python'],HERE/'compare.py','--gate',out/'gate','--out',out/'comparison'],
            out/'compare-supervisor',out,environment(),480)
    record['profile_attempted']=True
    guarded([EXPECTED['baseline_python'],HERE/'attribution.py','--gate',out/'gate','--output',out/'attribution'],
            out/'attribution-supervisor',out,environment(),180)
except BaseException as error:record['error']=repr(error)
finally:
    try:
        commands=list(out.rglob('command.json'))
        record['owned_groups_clean']=bool(commands) and all(
            json.loads((command.parent/'result.json').read_text()).get('owned_groups_clean') is True
            for command in commands)
    except BaseException as error:record['error']=f"{record['error']}; cleanup verification:{error!r}"
    if record['profile_attempted']:
        record['profile_safe']=False
        try:
            scope=json.loads((out/'attribution'/'scope-result.json').read_text())
            record['profile_safe']=(scope.get('owned_groups_clean') is True and scope.get('restored') is True
                and scope.get('original')==4 and scope.get('restored_value')==4 and scope.get('pending_command_groups')==[])
        except BaseException as error:record['error']=f"{record['error']}; attribution scope:{error!r}"
        record['owned_groups_clean'] &= record['profile_safe']
    (out/'scope-result.json').write_text(json.dumps(record,indent=2)+'\n')
    print(json.dumps(record),flush=True)
if record['error'] or not record['owned_groups_clean']:raise SystemExit(1)
