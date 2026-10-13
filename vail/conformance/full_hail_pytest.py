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
import time
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
def pytest_collection_modifyitems(config, items):
    selection = os.environ.get('VAIL_TEST_SELECTION')
    if selection:
        manifest = json.loads(Path(selection).read_text())
        allowed = {(r['module'], r['name']) for r in manifest['included']}
        kept, excluded = [], []
        for item in items:
            module = str(Path(item.path).relative_to(os.environ['VAIL_UPSTREAM_ROOT']))
            parts = item.nodeid.split('::')[1:]
            parts[-1] = parts[-1].split('[')[0]
            (kept if (module, '.'.join(parts)) in allowed else excluded).append(item)
        items[:] = kept
        config.hook.pytest_deselected(items=excluded)
    emit(dict(kind='inventory', items=[i.nodeid for i in items]))
def pytest_runtest_setup(item):
    emit(dict(kind='started', nodeid=item.nodeid))
    timeout_seconds = int(os.environ.get('VAIL_PER_TEST_TIMEOUT', '45'))
    if timeout_seconds <= 0:
        return
    def timeout(signum, frame):
        raise TimeoutError(f'per-test time limit exceeded ({timeout_seconds} seconds)')
    signal.signal(signal.SIGALRM, timeout)
    signal.alarm(timeout_seconds)
def pytest_runtest_logreport(report):
    emit(dict(kind='test', nodeid=report.nodeid, when=report.when,
              outcome=report.outcome, duration=report.duration,
              error=str(report.longrepr) if report.longrepr else ''))
    if report.when == 'teardown':
        signal.alarm(0)
'''

CONFTEST = '''
import pytest
@pytest.fixture(scope='session', autouse=True)
def vail_sample_snapshots(tmp_path_factory):
    import os
    import vail as hl
    from upstream_paths import FixtureModule
    original = hl.read_matrix_table
    adapter = FixtureModule(hl, os.environ['HAIL_TEST_RESOURCES_DIR'],
                            tmp_path_factory.mktemp('sample-snapshots'))
    hl.read_matrix_table = adapter.read_matrix_table
    try:
        yield
    finally:
        hl.read_matrix_table = original
def init_hail_scope(fixture_name, config):
    return 'session'
@pytest.fixture(scope='session')
def init_hail():
    yield
@pytest.fixture
def uninitialized():
    yield
'''


def run_module(command, env, cwd, log, events, module_timeout, per_test_timeout):
    """Enforce a deadline outside native execution, where SIGALRM can stall."""
    process = subprocess.Popen(command, env=env, cwd=cwd, stdout=log, stderr=log)
    started = time.monotonic()
    active = None
    active_since = None
    offset = 0
    pending = b''
    while process.poll() is None:
        now = time.monotonic()
        if events.exists():
            with events.open('rb') as file:
                file.seek(offset)
                pending += file.read()
                offset = file.tell()
            lines = pending.split(b'\n')
            pending = lines.pop()
            for line in lines:
                event = json.loads(line)
                if event['kind'] == 'started':
                    active, active_since = event['nodeid'], now
                elif event['kind'] == 'test' and event['when'] == 'teardown' and event['nodeid'] == active:
                    active, active_since = None, None
        test_expired = per_test_timeout and active_since is not None and now-active_since > per_test_timeout+5
        module_expired = module_timeout and now-started > module_timeout
        if test_expired or module_expired:
            process.kill()
            process.wait()
            return dict(timeout=True, timeout_scope='test' if test_expired else 'module',
                        nodeid=active, elapsed_seconds=now-(active_since if test_expired else started),
                        declared_limit_seconds=per_test_timeout if test_expired else module_timeout)
        time.sleep(0.2)
    return dict(exit_code=process.returncode)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--selection', type=Path, help='Explicit included/excluded definition manifest')
    parser.add_argument('--module', help='Run only the test module at this path relative to --source')
    parser.add_argument('--module-timeout', type=int, default=300,
                        help='Per-module timeout in seconds; 0 disables it (default: 300)')
    parser.add_argument('--per-test-timeout', type=int, default=45,
                        help='Per-test timeout in seconds; 0 disables it (default: 45)')
    parser.add_argument('--deselect', action='append', default=[],
                        help='Pytest node ID(s) to deselect; may be repeated')
    args = parser.parse_args()
    source = args.source.resolve()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    files = [p for p in sorted(source.rglob('test_*.py')) if 'backend' not in p.relative_to(source).parts]
    if args.module:
        requested = Path(args.module)
        files = [p for p in files if p.relative_to(source) == requested]
        if not files:
            parser.error(f'no eligible test module matches {args.module!r}')
    allowed = None
    if args.selection:
        allowed = {(r['module'], r['name']) for r in json.loads(args.selection.read_text())['included']}
        files = [p for p in files if any(module == str(p.relative_to(source)) for module, _ in allowed)]
    inventory = []
    for file in files:
        tree = ast.parse(file.read_text())
        for node in tree.body:
            members = node.body if isinstance(node, ast.ClassDef) else [node]
            for member in members:
                if isinstance(member, (ast.FunctionDef, ast.AsyncFunctionDef)) and member.name.startswith('test_'):
                    name = (node.name + '.' if isinstance(node, ast.ClassDef) else '') + member.name
                    if allowed is not None and (str(file.relative_to(source)), name) not in allowed:
                        continue
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
        # Hail's decorators only recognize local/batch/spark when selecting
        # timeout presets. Vail executes locally, so present it as ``local``
        # to the test harness; the adapted API imports still target Vail.
        helper.write_text('choose_backend = lambda *args: "local"\n' + ast.unparse(ast.fix_missing_locations(tree)))
        (target / 'conftest.py').write_text(CONFTEST)
        shutil.copyfile(Path(__file__).with_name('upstream_paths.py'), root / 'upstream_paths.py')
        # Hail test modules import shared fixtures as ``test.hail.*``.  The
        # runner copies only the Hail test package, so provide that package
        # name inside the isolated root and point it at the adapted copy.
        test_package = root / 'test'
        test_package.mkdir()
        (test_package / '__init__.py').write_text('')
        # Load upstream immutable-value helpers without exposing the Hail
        # runtime package on PYTHONPATH.
        hailtop = root / 'hailtop'
        hailtop.mkdir()
        (hailtop / '__init__.py').write_text('__path__ = [' + repr(str(source.parents[1] / 'hailtop')) + ']')
        (test_package / 'hail').symlink_to(target, target_is_directory=True)
        (root / 'vail_pytest_reporter.py').write_text(PLUGIN)
        env = dict(os.environ)
        env['PYTHONPATH'] = str(root) + os.pathsep + env.get('PYTHONPATH', '')
        env['HAIL_TEST_RESOURCES_DIR'] = str(source.parents[2] / 'hail' / 'test' / 'resources')
        env['HAIL_DOCTEST_DATA_DIR'] = doctest_data_dir(source)
        env['PYTEST_DISABLE_PLUGIN_AUTOLOAD'] = '1'
        env['VAIL_PER_TEST_TIMEOUT'] = str(args.per_test_timeout)
        if args.selection:
            env['VAIL_TEST_SELECTION'] = str(args.selection.resolve())
            env['VAIL_UPSTREAM_ROOT'] = str(target)
        for index, file in enumerate(files):
            relative = file.relative_to(source)
            label = str(relative).replace('/', '__')
            events = output / (label + '.jsonl')
            env['VAIL_PYTEST_EVENTS'] = str(events)
            command = [sys.executable, '-m', 'pytest', str(target / relative),
                       '-p', 'vail_pytest_reporter', '-p', 'test.hail.conftest',
                       '--continue-on-collection-errors',
                       '-o', 'addopts=', '--tb=short', '-q']
            for nodeid in args.deselect:
                command.extend(['--deselect', nodeid])
            with (output / (label + '.log')).open('w') as log:
                result = dict(module=str(relative), **run_module(
                    command, env, root, log, events, args.module_timeout, args.per_test_timeout))
            result['source_sha256'] = hashlib.sha256(file.read_bytes()).hexdigest()
            results.append(result)
            (output / 'modules.json').write_text(json.dumps(results, indent=2))
            print(f'{index+1}/{len(files)} {relative}: {result}', flush=True)


if __name__ == '__main__':
    main()
