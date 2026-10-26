#!/usr/bin/env python3
"""Run the frozen public gate on the owner's exclusive cloud compute slot."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import time
import xml.etree.ElementTree as ET

HERE = Path(__file__).resolve().parent
PACKAGE = HERE.parents[3]


def digest(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--target', type=Path, required=True)
    parser.add_argument('--python', required=True)
    parser.add_argument('--uv', default='uv')
    parser.add_argument('--kache', default='kache')
    parser.add_argument('--jobs', type=int, required=True)
    parser.add_argument('--wheel-budget', type=int, required=True)
    parser.add_argument('--table-budget', type=int, required=True)
    parser.add_argument('--storage-budget', type=int, required=True)
    parser.add_argument('--runtime-budget', type=int, required=True)
    parser.add_argument('--disk-floor-gib', type=float, required=True)
    args = parser.parse_args()
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=False)  # Do not silently duplicate/resume a run.
    target = args.target.resolve()
    target.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ, RUSTFLAGS='-C target-cpu=native', RUSTC_WRAPPER=args.kache,
               CARGO_INCREMENTAL='0', CARGO_BUILD_JOBS=str(args.jobs),
               CARGO_TARGET_DIR=str(target), CARGO_TERM_COLOR='never',
               OPENBLAS_NUM_THREADS='1', OMP_NUM_THREADS='1')
    env.pop('PYTHONPATH', None)
    manifest = json.loads((HERE / 'source-manifest.json').read_text())
    for relative, expected in manifest['files'].items():
        actual = digest(PACKAGE / relative)
        if actual != expected:
            raise RuntimeError(f'Source mismatch: {relative}: {actual} != {expected}')
    (out / 'source-manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    (out / 'host.json').write_text(json.dumps({
        'uname': list(os.uname()), 'arguments': vars(args),
        'env': {k: env[k] for k in ['RUSTFLAGS', 'RUSTC_WRAPPER', 'CARGO_INCREMENTAL',
                                  'CARGO_BUILD_JOBS', 'CARGO_TARGET_DIR']},
    }, default=str, indent=2) + '\n')

    def status(value):
        temporary = out / 'status.tmp'
        temporary.write_text(json.dumps(value, indent=2) + '\n')
        temporary.replace(out / 'status.json')

    def run(name, command, budget, cwd=PACKAGE):
        stage = out / name
        stage.mkdir(parents=True, exist_ok=True)
        command = [str(x) for x in command]
        (stage / 'command.json').write_text(json.dumps(command, indent=2) + '\n')
        started = time.monotonic()
        minimum = shutil.disk_usage(target).free
        reason = None
        with (stage / 'output.log').open('w') as log:
            process = subprocess.Popen(command, cwd=cwd, env=env, stdout=log,
                                       stderr=subprocess.STDOUT, start_new_session=True)
            with (stage / 'progress.jsonl').open('w') as progress:
                while process.poll() is None:
                    elapsed = time.monotonic() - started
                    free = shutil.disk_usage(target).free
                    minimum = min(minimum, free)
                    snapshot = {'stage': name, 'pid': process.pid, 'elapsed': elapsed,
                                'budget': budget, 'free_bytes': free}
                    status(snapshot)
                    progress.write(json.dumps(snapshot) + '\n')
                    progress.flush()
                    if elapsed > budget:
                        reason = 'stage time budget'
                    if free < args.disk_floor_gib * 1024**3:
                        reason = 'approved free-space floor'
                    if reason:
                        os.killpg(process.pid, signal.SIGTERM)
                        try:
                            process.wait(timeout=10)
                        except subprocess.TimeoutExpired:
                            os.killpg(process.pid, signal.SIGKILL)
                            process.wait()
                        break
                    time.sleep(2)
        result = {'stage': name, 'command': command, 'elapsed': time.monotonic() - started,
                  'returncode': process.returncode, 'stop_reason': reason,
                  'minimum_free_bytes': minimum, 'log_sha256': digest(stage / 'output.log')}
        (stage / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
        status(result)
        print(json.dumps(result), flush=True)
        if process.returncode != 0 or reason:
            raise RuntimeError(f'{name} failed; inspect {stage}')
        return stage

    for name, command in [('rustc', ['rustc', '--version']), ('cargo', ['cargo', '--version']),
                          ('kache', [args.kache, '--version']), ('uv', [args.uv, '--version']),
                          ('python', [args.python, '--version'])]:
        run('version-' + name, command, args.runtime_budget)
    candidate = out / 'candidate-env'
    run('venv', [args.uv, 'venv', candidate, '--python', args.python], args.runtime_budget)
    python = candidate / 'bin/python'
    run('dependencies', [args.uv, 'pip', 'install', '--python', python,
                         '-r', HERE / 'requirements.txt'], args.runtime_budget)
    env['PYO3_PYTHON'] = str(python)
    wheels = out / 'wheels'
    run('wheel', [candidate / 'bin/maturin', 'build', '--release', '--locked',
                  '--interpreter', python, '--out', wheels], args.wheel_budget)
    built = list(wheels.glob('*.whl'))
    if len(built) != 1:
        raise RuntimeError(f'Expected one normal wheel, got {built}')
    run('install', [args.uv, 'pip', 'install', '--python', python, built[0]], args.runtime_budget)
    check = ('import vail,vail._native,pathlib,hashlib,json,os; '
             'p=pathlib.Path(vail._native.__file__); '
             'print(json.dumps({"frontend":vail.__file__,"native":str(p),'
             '"native_sha256":hashlib.sha256(p.read_bytes()).hexdigest(),'
             '"pythonpath":os.environ.get("PYTHONPATH")}))')
    provenance_stage = run('native-provenance', [python, '-c', check], args.runtime_budget)
    provenance = json.loads((provenance_stage / 'output.log').read_text())
    for path in ['frontend', 'native']:
        if not Path(provenance[path]).is_relative_to(candidate):
            raise RuntimeError(f'Import escaped normal candidate environment: {provenance}')
    provenance.update(source_revision=manifest['source_revision'], wheel_sha256=digest(built[0]))
    (out / 'native-provenance.json').write_text(json.dumps(provenance, indent=2) + '\n')
    tests = json.loads((HERE / 'public-tests.json').read_text())
    run('python-public', [python, '-m', 'pytest', '-q', *tests,
                          '--junitxml', out / 'python-public.xml'], args.runtime_budget)
    xml = ET.parse(out / 'python-public.xml').getroot()
    cases = list(xml.iter('testcase'))
    if len(cases) < 431 or any(case.find(x) is not None for case in cases
                               for x in ['failure', 'error', 'skipped']):
        raise RuntimeError('Public Python gate narrowed, skipped, or failed')

    artifacts = []
    for name, package, targets, minimums, budget in [
        ('table', 'vail-table', ['encoded_projection', 'struct_field_projection_contract'],
         [34, 4], args.table_budget),
        ('storage', 'vail-storage', ['native_list_evolution'], [2], args.storage_budget),
    ]:
        command = ['cargo', 'test', '--release', '--locked', '-p', package]
        for test in targets:
            command += ['--test', test]
        stage = run(name + '-build', command + ['--no-run'], budget)
        executables = {Path(label).stem: path for label, path in re.findall(
            r'Executable (.*?) \((.*?)\)', (stage / 'output.log').read_text())}
        for test, minimum in zip(targets, minimums):
            executable = Path(executables[test])
            checksum = digest(executable)
            result = run(test, [executable, '--nocapture', '--test-threads=1'], args.runtime_budget)
            match = re.search(r'test result: ok\. (\d+) passed; 0 failed; (\d+) ignored;',
                              (result / 'output.log').read_text())
            if match is None or int(match[1]) < minimum or int(match[2]) != 0:
                raise RuntimeError(f'Rust public target narrowed/skipped: {test}')
            artifacts.append({'target': test, 'passed': int(match[1]),
                              'exe_sha256': checksum, 'log_sha256': digest(result / 'output.log')})
    (out / 'runtime-artifacts.json').write_text(json.dumps(artifacts, indent=2) + '\n')
    status({'status': 'complete', 'source_revision': manifest['source_revision'],
            'python_passed': len(cases), 'rust_artifacts': artifacts})


if __name__ == '__main__':
    main()
