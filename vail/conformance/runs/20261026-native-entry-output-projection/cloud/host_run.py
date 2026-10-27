"""Execute only on an already authorized running VM; always request STOP."""
import argparse
import json
from pathlib import Path
import shlex
import signal
from common import EXPECTED, HERE, digest, environment, guarded
p = argparse.ArgumentParser()
p.add_argument('--remote-source', required=True)
p.add_argument('--remote-protocol', required=True)
p.add_argument('--remote-out', required=True)
p.add_argument('--local-out', required=True)
a = p.parse_args()
for path in (a.remote_source, a.remote_protocol, a.remote_out):
    if not Path(path).is_absolute() or not Path(path).is_relative_to('/home/danielking'):
        raise RuntimeError('Remote paths must be owned absolute paths')
out = Path(a.local_out).resolve()
out.mkdir(parents=True, exist_ok=False)
gcloud = '/Users/danielking/projects/google-cloud-sdk/bin/gcloud'
vm = 'vail-bench-20261021'
identity = ['--project=pyspiral-dev', '--zone=us-east1-b']
def local(name, command, seconds):
    return guarded(command, out / name, HERE, environment(), seconds)
def remote(name, argv, seconds):
    return local(name, [gcloud, 'compute', 'ssh', vm, *identity, '--command', shlex.join(list(map(str, argv)))], seconds)
def interrupted(signum, _frame):
    raise InterruptedError(f'signal {signum}')
for sig in (signal.SIGTERM, signal.SIGINT):
    signal.signal(sig, interrupted)
failed = None
authorized_guest = False
scope_verified = False
def verified_scope():
    # Consume only the already captured supervisor output: no extra guest job
    # is allowed while privileged cleanup/restoration is unknown.
    lines = (out / 'profile' / 'stdout').read_text().splitlines()
    scope = json.loads(lines[-1])
    if not (scope.get('owned_groups_clean') is True):
        raise RuntimeError('Owned child process cleanup unverified')
    return scope
try:
    stage = local('provider-before', [gcloud, 'compute', 'instances', 'describe', vm, *identity,
                                    '--format=json(status,scheduling)'], 30)
    provider = json.loads((stage / 'stdout').read_text())
    if (provider['status'] != 'RUNNING' or provider['scheduling']['instanceTerminationAction'] != 'STOP'
            or not 0 < int(provider['scheduling']['maxRunDuration']['seconds']) <= 2700):
        raise RuntimeError('Running VM with <=45minute provider STOP bound required')
    authorized_guest = True
    controller = EXPECTED['baseline_python']
    remote('profile', [controller, Path(a.remote_protocol)/'controller.py', '--source', a.remote_source,
                       '--out', a.remote_out], 2350)
    verified_scope()
    scope_verified = True
except BaseException as error:
    failed = repr(error)
finally:
    # Unknown/unsafe scope means provider STOP first. Do not start
    # remote packing or downloading; original evidence remains on VM disk.
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, signal.SIG_IGN)
    if authorized_guest and not scope_verified:
        try:
            verified_scope()
            scope_verified = True
        except BaseException as error:
            failed = f'{failed}; unsafe/unknown scope: {error!r}; evidence retained on VM disk'
    if authorized_guest and scope_verified:
        try:
            stage = remote('pack', [EXPECTED['baseline_python'], Path(a.remote_protocol) / 'pack.py', a.remote_out], 90)
            packed = json.loads((stage / 'stdout').read_text())
            local('download', [gcloud, 'compute', 'scp', *identity,
                               f'{vm}:{packed["archive"]}', str(out / 'receipts.tar.gz')], 120)
            if digest(out / 'receipts.tar.gz') != packed['sha256']:
                raise RuntimeError('Downloaded receipt archive fingerprint differs')
            (out / 'download-verified.json').write_text(json.dumps(packed, indent=2)+'\n')
        except BaseException as error:
            failed = f'{failed}; receipt collection: {error!r}'
    try:
        local('stop', [gcloud, 'compute', 'instances', 'stop', vm, *identity, '--quiet'], 120)
        stage = local('provider-after', [gcloud, 'compute', 'instances', 'describe', vm, *identity,
                                       '--format=json(status,scheduling)'], 30)
        provider = json.loads((stage / 'stdout').read_text())
        if provider['status'] != 'TERMINATED':
            raise RuntimeError(f'VM not verified stopped: {provider}')
    except BaseException as error:
        failed = f'{failed}; VM STOP/verification: {error!r}'
    (out / 'result.json').write_text(json.dumps(dict(error=failed, scope_verified=scope_verified, evidence_retained_on_guest=not scope_verified), indent=2)+'\n')
if failed:
    raise RuntimeError(failed)
