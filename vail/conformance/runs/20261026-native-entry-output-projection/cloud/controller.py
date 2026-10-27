"""One correctness-then-comparison attempt with positive child cleanup proof."""
import argparse,json,signal
from pathlib import Path
from common import EXPECTED,HERE,environment,guarded
p=argparse.ArgumentParser();p.add_argument('--source',required=True);p.add_argument('--out',required=True);a=p.parse_args()
out=Path(a.out);out.mkdir(parents=True,exist_ok=False)
record={'owned_groups_clean':False,'error':None}
def interrupted(signum,frame):raise InterruptedError(f'signal {signum}')
for sig in (signal.SIGINT,signal.SIGTERM,signal.SIGHUP):signal.signal(sig,interrupted)
try:
    guarded([EXPECTED['baseline_python'],HERE/'prepare.py','--source',a.source,'--out',out/'gate'],
            out/'prepare-supervisor',out,environment(),1800)
    guarded([EXPECTED['baseline_python'],HERE/'compare.py','--gate',out/'gate','--out',out/'comparison'],
            out/'compare-supervisor',out,environment(),510)
except BaseException as error:record['error']=repr(error)
finally:
    try:
        commands=list(out.rglob('command.json'))
        record['owned_groups_clean']=bool(commands) and all(
            json.loads((command.parent/'result.json').read_text()).get('owned_groups_clean') is True
            for command in commands)
    except BaseException as error:record['error']=f"{record['error']}; cleanup verification:{error!r}"
    (out/'scope-result.json').write_text(json.dumps(record,indent=2)+'\n')
    print(json.dumps(record),flush=True)
if record['error'] or not record['owned_groups_clean']:raise SystemExit(1)
