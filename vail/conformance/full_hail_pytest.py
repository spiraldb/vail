"""Attempt upstream Hail Python API pytest modules against Vail.

Imports naming hail are redirected to vail in a temporary source copy. Test
lifecycle and timeout selection use the Vail backend. Hail flag decorators and
JVM health fixtures are omitted. Proven local Hail IR-shape helper calls are
omitted with a separate source-span manifest; behavioral assertions remain.
Missing internal modules remain collection blockers. Each module
runs in its own process so a native failure cannot prevent later collection.
"""
import ast
import argparse
from upstream_scope import exclusion_reason
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
from upstream_backend import adapt_helpers, adapt_class_configuration, adapt_structural_ir_assertions

PLUGIN = '''
import json, os, signal, pytest
from pathlib import Path
from upstream_backend import VailTestBackend
from upstream_scope import exclusion_reason
def pytest_configure(config):
    config.addinivalue_line('markers', 'backend(name): backend used by the upstream test adapter')
    config.addinivalue_line('markers', 'timeout(seconds): upstream timeout preset; externally enforced by the runner')
def emit(value):
    with open(os.environ['VAIL_PYTEST_EVENTS'], 'a') as f:
        f.write(json.dumps(value) + '\\n')
def pytest_collectreport(report):
    if report.failed:
        emit(dict(kind='collection', nodeid=report.nodeid, outcome='failed', error=str(report.longrepr)))
@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    report = (yield).get_result()
    if report.failed and call.excinfo is not None:
        phase = 'body' if call.when == 'call' else call.when
        report.vail_category = VailTestBackend.classify_failure(phase, call.excinfo.value)
def pytest_collection_modifyitems(config, items):
    kept, excluded = [], []
    for item in items:
        module = str(Path(item.path).relative_to(os.environ['VAIL_UPSTREAM_ROOT']))
        parts = item.nodeid.split('::')[1:]
        parts[-1] = parts[-1].split('[')[0]
        name = '.'.join(parts)
        reason = exclusion_reason(module, name)
        if reason:
            emit(dict(kind='scope_excluded', module=module, name=name, reason=reason))
            excluded.append(item)
        else:
            kept.append(item)
    items[:] = kept
    if excluded:
        config.hook.pytest_deselected(items=excluded)
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
              category=getattr(report, 'vail_category', None),
              error=str(report.longrepr) if report.longrepr else ''))
    if report.when == 'teardown':
        signal.alarm(0)
'''

CONFTEST = '''
import pytest
@pytest.fixture(autouse=True)
def reset_global_randomness():
    from upstream_backend import reset_test_randomness
    reset_test_randomness()
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
    from upstream_backend import VailTestBackend
    with VailTestBackend():
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
    parser.add_argument('--include-backend', action='store_true',
                        help='Also attempt Hail backend-specific test modules')
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
    exclusions = [dict(module=str(p.relative_to(source)), reason=exclusion_reason(p.relative_to(source)))
                  for p in sorted(source.rglob('test_*.py')) if exclusion_reason(p.relative_to(source))]
    (output / 'scope-exclusions.json').write_text(json.dumps(exclusions, indent=2) + '\n')
    original_conftest = source / 'conftest.py'
    reset_definition = None
    if original_conftest.exists():
        reset_definition = next((node for node in ast.parse(original_conftest.read_text()).body
                                 if isinstance(node, ast.FunctionDef)
                                 and node.name == 'reset_global_randomness'), None)
    fixture_manifest = dict(randomness_reset=dict(
        replacement='vail.reset_global_randomness', scope='function', autouse=True,
        timing='pytest autouse fixture before each test',
        upstream_fixture='conftest.py::reset_global_randomness' if reset_definition else None,
        upstream_line=reset_definition.lineno if reset_definition else None,
        upstream_conftest_sha256=hashlib.sha256(original_conftest.read_bytes()).hexdigest()
        if original_conftest.exists() else None))
    (output / 'fixture-adaptations.json').write_text(json.dumps(fixture_manifest, indent=2) + '\n')
    files = [p for p in sorted(source.rglob('test_*.py'))
             if not exclusion_reason(p.relative_to(source))
             if args.include_backend or 'backend' not in p.relative_to(source).parts]
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
                    reason = exclusion_reason(file.relative_to(source), name)
                    if reason:
                        exclusions.append(dict(module=str(file.relative_to(source)), name=name,
                                               line=member.lineno, reason=reason))
                        continue
                    if allowed is not None and (str(file.relative_to(source)), name) not in allowed:
                        continue
                    inventory.append(dict(module=str(file.relative_to(source)), name=name,
                                          line=member.lineno, body=ast.unparse(member)))
    (output / 'definition-inventory.json').write_text(json.dumps(inventory, indent=2))
    (output / 'scope-exclusions.json').write_text(json.dumps(exclusions, indent=2) + '\n')
    results = []
    with tempfile.TemporaryDirectory(prefix='vail-full-hail-') as folder:
        root = Path(folder)
        target = root / 'upstream_hail'
        shutil.copytree(source, target)
        structural_manifest = []
        for file in target.rglob('*.py'):
            code = file.read_text()
            code = re.sub(r'(?m)^(\s*(?:from|import) )hail(?=[.\s])', r'\1vail', code)
            tree = ast.parse(code)
            tree, removed = adapt_structural_ir_assertions(tree, file.relative_to(target))
            structural_manifest.extend(removed)
            code = ast.unparse(tree)
            if any(isinstance(node, ast.ClassDef) for node in tree.body):
                tree = adapt_class_configuration(tree)
                code = ('from upstream_backend import backend_api as _vail_backend_api\n'
                        + ast.unparse(tree))
            file.write_text(code)
        (output / 'structural-ir-assertions.json').write_text(json.dumps(
            dict(count=len(structural_manifest), assertions=structural_manifest), indent=2))
        # Infrastructure and documented structural assertions only; API imports remain.
        helper = target / 'helpers.py'
        helper.write_text(adapt_helpers(helper.read_text()))
        (target / 'conftest.py').write_text(CONFTEST)
        shutil.copyfile(Path(__file__).with_name('upstream_paths.py'), root / 'upstream_paths.py')
        shutil.copyfile(Path(__file__).with_name('upstream_backend.py'), root / 'upstream_backend.py')
        shutil.copyfile(Path(__file__).with_name('upstream_scope.py'), root / 'upstream_scope.py')
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
        # Fixture converters retain their checked-in payloads beside their
        # modules. Resolve them from the conformance directory, while the
        # adapted upstream test package remains first on the import path.
        env['PYTHONPATH'] = os.pathsep.join((str(root), str(Path(__file__).resolve().parent),
                                           env.get('PYTHONPATH', '')))
        env['HAIL_TEST_RESOURCES_DIR'] = str(source.parents[2] / 'hail' / 'test' / 'resources')
        env['HAIL_DOCTEST_DATA_DIR'] = doctest_data_dir(source)
        env['PYTEST_DISABLE_PLUGIN_AUTOLOAD'] = '1'
        env['VAIL_PER_TEST_TIMEOUT'] = str(args.per_test_timeout)
        env['VAIL_UPSTREAM_ROOT'] = str(target)
        if args.selection:
            env['VAIL_TEST_SELECTION'] = str(args.selection.resolve())
        for index, file in enumerate(files):
            relative = file.relative_to(source)
            label = str(relative).replace('/', '__')
            events = output / (label + '.jsonl')
            # A rerun replaces evidence for this attempt; appending old passes
            # can mask a new collection failure or confuse timeout tracking.
            events.write_text('')
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
