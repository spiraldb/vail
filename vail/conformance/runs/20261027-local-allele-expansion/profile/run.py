"""Confirm public path, then one scoped unprivileged CPU sample; finally restore."""
import argparse
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import time
from common import EXPECTED, HERE, environment, guarded, stop_owned, digest
p = argparse.ArgumentParser()
p.add_argument('--output', required=True)
a = p.parse_args()
out = Path(a.output).resolve()
out.mkdir(parents=True, exist_ok=False)
record = dict(original=None, write_attempted=False, restored=False, error=None,
              pending_command_groups=[], owned_groups_clean=False, confirmation_cleanup_verified=False, event='cpu-clock:u', frequency=99, requested_sample_seconds=3)
query = sampler = None
def command(args, timeout=10):
    proc = None
    entry = dict(args=args)
    record.setdefault('commands', []).append(entry)
    try:
        proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                text=True, start_new_session=True)
        stdout, stderr = proc.communicate(timeout=timeout)
        entry.update(returncode=proc.returncode, stdout=stdout, stderr=stderr)
        if proc.returncode:
            raise RuntimeError(f'Command failed: {entry}')
        return stdout.strip()
    finally:
        if proc is not None:
            try:
                stop_owned(proc)
            except BaseException:
                record['pending_command_groups'].append(proc.pid)
                raise

def interrupted(signum, _frame):
    raise InterruptedError(f'signal {signum}')
for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP, signal.SIGALRM):
    signal.signal(sig, interrupted)
signal.alarm(420)
try:
    if socket.gethostname().split('.')[0] != 'vail-bench-20261021':
        raise RuntimeError('Exclusive primary guest required')
    assets = json.loads((HERE/'ASSETS.json').read_text())
    for name, sha256 in assets['scripts'].items():
        if digest(HERE/name) != sha256: raise RuntimeError(f'Protocol asset differs: {name}')
    record['protocol_assets'] = assets['scripts']
    try:
        guarded([EXPECTED['python'], HERE/'setup.py'], out/'setup', out, environment(), 180,
                query=True, action_seconds=180, storage=EXPECTED['generated_fixture'])
    finally:
        setup_receipt=json.loads((out/'setup'/'result.json').read_text())
        record['setup_cleanup_verified']=setup_receipt.get('owned_groups_clean') is True
    perf = shutil.which('perf')
    if perf != '/usr/bin/perf':
        raise RuntimeError('Existing perf unavailable/different; no install')
    for tool in ('/usr/bin/perf', '/usr/bin/sudo', '/usr/sbin/sysctl', '/usr/bin/sleep'):
        if not os.access(tool, os.X_OK): raise RuntimeError(f'Existing tool unavailable: {tool}')
    command([perf, '--version'], 5)
    original = int(command(['/usr/sbin/sysctl', '-n', 'kernel.perf_event_paranoid']))
    record['original'] = original
    if original != 4: raise RuntimeError('Expected captured guest setting4; stop')
    command(['/usr/bin/sudo', '-n', 'true'], 5)
    try:
        guarded([EXPECTED['python'], HERE/'worker.py', 'confirm', '--stop-launch', out/'stop-launch'],
                out/'confirm', out, environment(), 60, query=True)
    finally:
        # A failed/absent retained receipt cannot certify the confirmation group.
        confirmation = json.loads((out/'confirm'/'result.json').read_text())
        record['confirmation_cleanup_verified'] = confirmation.get('owned_groups_clean') is True
    events=[json.loads(line) for line in (out/'confirm'/'stdout').read_text().splitlines()]
    actions=[event for event in events if event['phase']=='action']
    if len(actions)!=1:raise RuntimeError('One complete confirmation result required')
    (out/'reference.json').write_text(json.dumps(actions[0]['result'])+'\n')
    record['reference_origin']='independent Python first-match expansion of original real405cells, integer aggregate totals multiplied4096; per-cell arrays preserved in setup reference'
    record['write_attempted'] = True
    command(['/usr/bin/sudo', '-n', '/usr/sbin/sysctl', '-w', 'kernel.perf_event_paranoid=2'])
    if int(command(['/usr/sbin/sysctl', '-n', 'kernel.perf_event_paranoid'])) != 2:
        raise RuntimeError('Temporary guest setting not verified')
    begun = time.monotonic()
    first_completed = None
    sample_begun = None
    with (out/'sample.jsonl').open('w') as log, (out/'sample.stderr').open('w') as err:
        query = subprocess.Popen([EXPECTED['python'], str(HERE/'worker.py'), 'sample',
                                  '--stop-launch', str(out/'stop-launch'), '--reference', str(out/'reference.json')], cwd=out, env=environment(),
                                 stdout=log, stderr=err, start_new_session=True)
        while query.poll() is None:
            now = time.monotonic()
            if now-begun > 60 or shutil.disk_usage(out).free < 10*1024**3:
                raise TimeoutError('External60s/disk10GiB budget')
            try:
                text = Path(f'/proc/{query.pid}/status').read_text()
            except FileNotFoundError:
                text = ''
            rss = next((int(line.split()[1])*1024 for line in text.splitlines() if line.startswith('VmRSS:')), 0)
            record['monitor_maxrss_bytes'] = max(record.get('monitor_maxrss_bytes', 0), rss)
            if rss > 6*1024**3:
                raise MemoryError('6GiB whole-process live RSS')
            text = (out/'sample.jsonl').read_text()
            events = [json.loads(line) for line in text[:text.rfind('\n')+1].splitlines()]
            active = {}
            for event in events:
                if event['phase'] == 'action_start': active[event['id']] = event['monotonic']
                if event['phase'] == 'action':
                    active.pop(event['id'], None)
                    if first_completed is None: first_completed = event['completed_monotonic']
            if active and now-min(active.values()) > 10:
                raise TimeoutError('In-flight action10s budget')
            if first_completed is not None and now-first_completed > 6:
                raise TimeoutError('Hard6s post-first-action window; no extension')
            if first_completed is not None and sampler is None:
                sampler = subprocess.Popen([perf, 'record', '-e', 'cpu-clock:u', '-F', '99',
                    '--call-graph', 'dwarf', '-p', str(query.pid), '-o', str(out/'perf.data'), '--',
                    '/usr/bin/sleep', '3'], stdout=err, stderr=err, start_new_session=True)
                sample_begun = time.monotonic()
            if sampler is not None and sampler.poll() is not None:
                if sampler.returncode:
                    raise RuntimeError('Unprivileged perf failed; stop without retry')
                (out/'stop-launch').touch()
            time.sleep(.05)
        if query.returncode or sampler is None:
            raise RuntimeError('Query failed or no sample')
        if sampler.poll() is None:
            raise RuntimeError('Query ended before complete requested sample')
        final = [json.loads(line) for line in (out/'sample.jsonl').read_text().splitlines()]
        actions = [event for event in final if event['phase']=='action']
        if (not actions or len(actions)>12 or actions[-1]['completed_monotonic']-actions[0]['completed_monotonic']>6
                or sampler.returncode or (out/'perf.data').stat().st_size==0):
            raise RuntimeError('Incomplete/over-budget sample actions')
        record.update(actions=len(actions), sample_elapsed=time.monotonic()-sample_begun,
                      post_first_action_seconds=actions[-1]['completed_monotonic']-actions[0]['completed_monotonic'])
except BaseException as error:
    record['error'] = repr(error)
finally:
    signal.alarm(0)
    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP, signal.SIGALRM): signal.signal(sig, signal.SIG_IGN)
    groups_clean = True
    for proc in (query, sampler):
        try: stop_owned(proc)
        except BaseException as error:
            groups_clean = False
            record['error'] = f'{record["error"]}; cleanup:{error!r}'
    record['owned_groups_clean'] = (groups_clean and record.get('setup_cleanup_verified') is True and record['confirmation_cleanup_verified']
                                    and not record['pending_command_groups'])
    if record['pending_command_groups']:
        record['error'] = f'{record["error"]}; unresolved privileged command group; stop VM immediately'
    elif record['original'] is not None:
        try:
            if record['write_attempted']:
                command(['/usr/bin/sudo', '-n', '/usr/sbin/sysctl', '-w',
                         f'kernel.perf_event_paranoid={record["original"]}'])
            record['restored_value'] = int(command(['/usr/sbin/sysctl', '-n', 'kernel.perf_event_paranoid']))
            record['restored'] = record['restored_value']==record['original'] and not record['pending_command_groups']
            if not record['restored']: raise RuntimeError('Restoration verification failed')
        except BaseException as error: record['error'] = f'{record["error"]}; restore:{error!r}'
    if record['write_attempted'] and not record['restored']:
        record['error'] = f'{record["error"]}; guest permission not verified restored; stop VM immediately'
    (out/'scope-result.json').write_text(json.dumps(record, indent=2)+'\n')
    print(json.dumps(record))
if record['error']: raise SystemExit(1)
