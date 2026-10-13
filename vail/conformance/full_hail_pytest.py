"""Attempt upstream Hail Python API pytest modules against Vail.

Imports naming hail are redirected to vail in a temporary source copy. Only
Hail runtime initialization, flag decorators and JVM health fixtures are
omitted. Missing internal modules remain real collection blockers. Each module
runs in its own process so a native failure cannot prevent later collection.
"""
import ast
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
from upstream_paths import doctest_data_dir

PLUGIN = '''
import json, os, signal
from pathlib import Path
def emit(value):
    with open(os.environ['VAIL_PYTEST_EVENTS'], 'a') as f:
        f.write(json.dumps(value) + '\\n')
def pytest_collectreport(report):
    if report.failed:
        emit(dict(kind='collection', nodeid=report.nodeid, outcome='failed', error=str(report.longrepr)))
def pytest_collection_modifyitems(items):
    emit(dict(kind='inventory', items=[i.nodeid for i in items]))
def pytest_runtest_setup(item):
    emit(dict(kind='started', nodeid=item.nodeid))
    def timeout(signum, frame):
        raise TimeoutError('per-test time limit exceeded (45 seconds)')
    signal.signal(signal.SIGALRM, timeout)
    signal.alarm(45)
def pytest_runtest_logreport(report):
    emit(dict(kind='test', nodeid=report.nodeid, when=report.when,
              outcome=report.outcome, duration=report.duration,
              error=str(report.longrepr) if report.longrepr else ''))
    if report.when == 'teardown':
        signal.alarm(0)
'''

CONFTEST = '''
import pytest
def init_hail_scope(fixture_name, config):
    return 'session'
@pytest.fixture(scope='session')
def init_hail():
    yield
@pytest.fixture
def uninitialized():
    yield
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    source = args.source.resolve()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    files = [p for p in sorted(source.rglob('test_*.py')) if 'backend' not in p.relative_to(source).parts]
    inventory = []
    for file in files:
        tree = ast.parse(file.read_text())
        for node in tree.body:
            members = node.body if isinstance(node, ast.ClassDef) else [node]
            for member in members:
                if isinstance(member, (ast.FunctionDef, ast.AsyncFunctionDef)) and member.name.startswith('test_'):
                    name = (node.name + '.' if isinstance(node, ast.ClassDef) else '') + member.name
                    inventory.append(dict(module=str(file.relative_to(source)), name=name,
                                          line=member.lineno, body=ast.unparse(member)))
    (output / 'definition-inventory.json').write_text(json.dumps(inventory, indent=2))
    results = []
    with tempfile.TemporaryDirectory(prefix='vail-full-hail-') as folder:
        root = Path(folder)
        target = root / 'upstream_hail'
        shutil.copytree(source, target)
        for file in target.rglob('*.py'):
            code = file.read_text()
            code = re.sub(r'(?m)^(\s*(?:from|import) )hail(?=[.\s])', r'\1vail', code)
            file.write_text(code)
        # Infrastructure adapters only; leave test bodies and API imports intact.
        helper = target / 'helpers.py'
        tree = ast.parse(helper.read_text())
        for node in tree.body:
            if isinstance(node, ast.ImportFrom) and node.module == 'vail.utils.java':
                node.module = 'vail.utils'
                node.names = [ast.alias(name='Env')]
            if isinstance(node, ast.FunctionDef) and node.name in ('hl_init_for_test', 'hl_stop_for_test'):
                node.body = [ast.Pass()]
            if isinstance(node, ast.FunctionDef) and node.name == 'with_flags':
                node.body = ast.parse('return lambda f: f').body
        helper.write_text('choose_backend = lambda *args: "vail"\n' + ast.unparse(ast.fix_missing_locations(tree)))
        (target / 'conftest.py').write_text(CONFTEST)
        (root / 'vail_pytest_reporter.py').write_text(PLUGIN)
        env = dict(os.environ)
        env['PYTHONPATH'] = str(root) + os.pathsep + env.get('PYTHONPATH', '')
        env['HAIL_TEST_RESOURCES_DIR'] = str(source.parents[2] / 'hail' / 'test' / 'resources')
        env['HAIL_DOCTEST_DATA_DIR'] = doctest_data_dir(source)
        env['PYTEST_DISABLE_PLUGIN_AUTOLOAD'] = '1'
        for index, file in enumerate(files):
            relative = file.relative_to(source)
            label = str(relative).replace('/', '__')
            events = output / (label + '.jsonl')
            env['VAIL_PYTEST_EVENTS'] = str(events)
            command = [sys.executable, '-m', 'pytest', str(target / relative),
                       '-p', 'vail_pytest_reporter', '--continue-on-collection-errors',
                       '-o', 'addopts=', '--tb=short', '-q']
            with (output / (label + '.log')).open('w') as log:
                try:
                    proc = subprocess.run(command, env=env, cwd=root, stdout=log,
                                          stderr=log, timeout=300)
                    result = dict(module=str(relative), exit_code=proc.returncode)
                except subprocess.TimeoutExpired:
                    result = dict(module=str(relative), timeout=True)
            result['source_sha256'] = hashlib.sha256(file.read_bytes()).hexdigest()
            results.append(result)
            (output / 'modules.json').write_text(json.dumps(results, indent=2))
            print(f'{index+1}/{len(files)} {relative}: {result}', flush=True)


if __name__ == '__main__':
    main()
