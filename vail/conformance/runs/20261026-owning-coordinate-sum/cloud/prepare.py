"""One normal Linux candidate build/gate; never measures before correctness."""
import argparse
import json
import os
from pathlib import Path
import re
import sys
import xml.etree.ElementTree as ET
from common import EXPECTED, HERE, digest, environment, guarded, protocol_fingerprints

p = argparse.ArgumentParser()
p.add_argument('--source', required=True, help='extracted Git root containing vail/')
p.add_argument('--out', required=True, help='fresh owned run directory')
a = p.parse_args()
source = Path(a.source).resolve()
out = Path(a.out).resolve()
out.mkdir(parents=True, exist_ok=False)
project = source / 'vail'
for relative, sha256 in EXPECTED['candidate_source_files'].items():
    if digest(source / relative) != sha256:
        raise RuntimeError(f'Candidate source differs: {relative}')
env = environment()
env.update(RUSTFLAGS='-C target-cpu=native', RUSTC_WRAPPER='kache',
           CARGO_INCREMENTAL='0', CARGO_BUILD_JOBS='8',
           CARGO_TARGET_DIR='/home/danielking/vail-presence-20261026/target')
python_base = '/home/danielking/.local/share/uv/python/cpython-3.12-linux-x86_64-gnu/bin/python3.12'
candidate = out / 'candidate-env'
python = candidate / 'bin/python'
def run(name, command, budget):
    return guarded(command, out / name, project, env, budget)
run('baseline-input-preflight', [EXPECTED['baseline_python'], HERE / 'preflight.py'], 30)
# Fresh normal environment; no installed native is copied between environments.
run('venv', ['uv', 'venv', '--python', python_base, candidate], 90)
requirements = project / 'conformance/runs/20261026-struct-logical-presence/remote/requirements.txt'
run('dependencies', ['uv', 'pip', 'install', '--python', python, '-r', requirements], 180)
env['PYO3_PYTHON'] = str(python)
run('wheel', [candidate / 'bin/maturin', 'build', '--release', '--locked',
              '--interpreter', python, '--out', out / 'wheels'], 900)
wheels = list((out / 'wheels').glob('*.whl'))
if len(wheels) != 1:
    raise RuntimeError('Expected exactly one normal candidate wheel')
run('install', ['uv', 'pip', 'install', '--python', python, wheels[0]], 90)
# No filter: includes combined hidden outer/inner/unused-sibling allocated-byte
# fixture, short separated-span regression and all lifecycle/merge tests.
rust = run('rust', ['cargo', 'test', '--release', '--locked', '-p', 'vail-table',
                   '-p', 'vail-execution', '--test', 'owning_coordinate_sum'], 300)
rust_text = (rust / 'stdout').read_text() + (rust / 'stderr').read_text()
if 'test result: ok. 8 passed; 0 failed; 0 ignored' not in rust_text:
    raise RuntimeError('Complete eight-case public Rust gate not observed')
files = re.findall(r'python/tests/test_[a-z_]+\.py',
                   (project / 'conformance/runs/20261024-coordinate-current-main-integration/h050-main-gate.sh').read_text())
files += ['python/tests/test_native_array_length_projection.py', 'python/tests/test_batched_list_sets.py',
          str(HERE / 'frozen_public_tests/test_transparent_struct_fields.py'),
          'python/tests/test_matrix_owning_coordinate_sum.py']
run('public', [python, '-m', 'pytest', '-q', *files, '--junitxml', out / 'public.xml'], 120)
suites = ET.parse(out / 'public.xml').getroot()
totals = {key: sum(int(suite.get(key, '0')) for suite in suites.iter('testsuite'))
          for key in ('tests', 'failures', 'errors', 'skipped')}
if totals != dict(tests=435, failures=0, errors=0, skipped=0):
    raise RuntimeError(f'Public gate differs: {totals}')
# Enroll only a normally installed, correctness-tested artifact. Its previously
# unknowable Linux native hash is now frozen before any measurement starts.
probe = """import hashlib,json,pathlib,vail,vail._native
p=pathlib.Path(vail._native.__file__)
print(json.dumps(dict(frontend=str(pathlib.Path(vail.__file__).parent.resolve()),native=str(p.resolve()),native_sha256=hashlib.sha256(p.read_bytes()).hexdigest())))"""
lock = {}
for version, interpreter in [('baseline', EXPECTED['baseline_python']), ('candidate', str(python))]:
    stage = run('probe-'+version, [interpreter, '-c', probe], 30)
    entry = json.loads((stage / 'stdout').read_text())
    expected_root = Path(interpreter).parent.parent.resolve()
    if not Path(entry['frontend']).is_relative_to(expected_root) or not Path(entry['native']).is_relative_to(expected_root):
        raise RuntimeError('Normal import escaped its environment')
    if version == 'baseline' and entry['native_sha256'] != EXPECTED['baseline_native_sha256']:
        raise RuntimeError('Baseline normal native differs')
    for relative, sha256 in EXPECTED[version+'_frontend'].items():
        if digest(Path(entry['frontend']) / relative) != sha256:
            raise RuntimeError(f'{version} installed frontend differs: {relative}')
    entry.update(python=interpreter, source=EXPECTED[version+'_source'])
    lock[version] = entry
lock.update(wheel_sha256=digest(wheels[0]), rust_cases=8, public_cases=435,
            candidate_source_files_verified=len(EXPECTED['candidate_source_files']),
            expected_protocol_sha256=digest(HERE / 'expected.json'),
            protocol_files=protocol_fingerprints())
(out / 'artifact-lock.json').write_text(json.dumps(lock, indent=2)+'\n')
print(json.dumps(lock, indent=2), flush=True)
