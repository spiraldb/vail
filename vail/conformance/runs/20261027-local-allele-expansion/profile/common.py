"""Bounded owned-process execution; no VM, privilege or perf configuration."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import time

HERE = Path(__file__).resolve().parent
EXPECTED = json.loads((HERE / 'expected.json').read_text())

def digest(path):
    with Path(path).open('rb') as source:
        return hashlib.file_digest(source, 'sha256').hexdigest()

def protocol_fingerprints():
    return {str(path.relative_to(HERE)): digest(path) for path in sorted(HERE.rglob('*'))
            if path.is_file() and path.suffix in {'.py', '.json'} and '__pycache__' not in path.parts}

def environment():
    result = dict(os.environ, **EXPECTED['threads'])
    result.pop('PYTHONPATH', None)
    return result

def stop_owned(process):
    if process is None:
        return
    group = process.pid  # Popen(start_new_session=True), retained after exit.
    def complete():
        leader_exited = process.poll() is not None  # Also reap the leader.
        try:
            os.killpg(group, 0)
        except ProcessLookupError:
            return leader_exited
        return False
    for signum in (signal.SIGTERM, signal.SIGKILL):
        if complete():
            return
        try:
            os.killpg(group, signum)
        except ProcessLookupError:
            pass
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            if complete():
                return
            time.sleep(.05)
    if not complete():
        raise RuntimeError(f'Owned process group {group} unresolved after SIGKILL; leader={process.returncode}')

def guarded(command, output, cwd, env, seconds, *, query=False, action_seconds=10, storage=None):
    """One attempt; always retain status and reap the owned process group."""
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    (output / 'command.json').write_text(json.dumps(list(map(str, command)), indent=2)+'\n')
    process = None
    error = None
    cleanup_verified = False
    started = time.monotonic()
    maxrss = 0
    previous = {}
    def interrupted(signum, _frame):
        raise InterruptedError(f'signal {signum}')
    handlers = {sig: signal.signal(sig, interrupted) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        with (output / 'stdout').open('w') as stdout, (output / 'stderr').open('w') as stderr:
            process = subprocess.Popen(list(map(str, command)), cwd=cwd, env=env,
                                       stdout=stdout, stderr=stderr, start_new_session=True)
            while process.poll() is None:
                now = time.monotonic()
                if now-started > seconds:
                    raise TimeoutError(f'{seconds}s external budget')
                if shutil.disk_usage(output).free < 10*1024**3:
                    raise RuntimeError('10GiB disk floor')
                if storage is not None:
                    used=sum(p.stat().st_size for p in Path(storage).rglob('*') if p.is_file())
                    if used>10*1024**3:raise RuntimeError('10GiB generated-output cap')
                if query:
                    try:
                        status = Path(f'/proc/{process.pid}/status').read_text()
                    except FileNotFoundError:
                        status = ''
                    rss = next((int(line.split()[1])*1024 for line in status.splitlines()
                                if line.startswith('VmRSS:')), 0)
                    maxrss = max(maxrss, rss)
                    if rss > 6*1024**3:
                        raise MemoryError('6GiB whole-process live RSS')
                    text = (output / 'stdout').read_text()
                    complete = text[:text.rfind('\n')+1]
                    events = [json.loads(line) for line in complete.splitlines()]
                    for event in events:
                        if event['phase'] == 'action_start':
                            previous[event['id']] = event['monotonic']
                        elif event['phase'] == 'action':
                            previous.pop(event['id'], None)
                    if previous and now-min(previous.values()) > action_seconds:
                        raise TimeoutError(f'{action_seconds}s in-flight action budget')
                time.sleep(.05)
            if process.returncode:
                raise RuntimeError(f'child exited {process.returncode}')
    except BaseException as failure:
        error = repr(failure)
    finally:
        for sig in handlers:
            signal.signal(sig, signal.SIG_IGN)
        try:
            stop_owned(process)
            cleanup_verified = True
        except BaseException as failure:
            error = f'{error}; cleanup: {failure!r}'
        receipt = dict(command=list(map(str, command)), elapsed_seconds=time.monotonic()-started,
                       returncode=None if process is None else process.returncode,
                       error=error, owned_groups_clean=cleanup_verified, monitor_maxrss_bytes=maxrss,
                       rss_scope='whole child process; includes construction and warmups',
                       monitoring='50ms /proc RSS/action watchdog; overhead not subtracted')
        (output / 'result.json').write_text(json.dumps(receipt, indent=2)+'\n')
        for sig, handler in handlers.items():
            signal.signal(sig, handler)
    if error:
        raise RuntimeError(json.dumps(receipt))
    return output
