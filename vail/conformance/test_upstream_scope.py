"""Scope policy is identical in manifests and actual pytest collection."""
import importlib
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest


@pytest.fixture
def policy(monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).parent))
    return importlib.import_module('upstream_scope')


def test_definition_policy_keeps_randomness_and_excludes_mixed_hadoop(policy):
    assert policy.exclusion_reason('utils/test_utils.py', 'Tests.test_hadoop_open[local]')
    assert policy.exclusion_reason('typecheck/test_check.py', 'test_checks')
    assert policy.exclusion_reason('fs/test_local.py', 'test_exists')
    assert policy.exclusion_reason('test_randomness.py', 'test_random_seed') is None
    assert policy.exclusion_reason('expr/test_types.py', 'test_types') is None
    assert policy.exclusion_reason('utils/test_utils.py', 'Tests.test_general') is None


def test_manifest_collection_and_rerun_evidence_agree(tmp_path, monkeypatch, policy):
    monkeypatch.syspath_prepend(str(Path(__file__).parent))
    selection = importlib.import_module('suite_selection')
    suite = tmp_path / 'hail' / 'python' / 'test' / 'hail'
    (suite / 'utils').mkdir(parents=True)
    (suite / '__init__.py').write_text('')
    (suite / 'helpers.py').write_text('')
    module = suite / 'utils' / 'test_utils.py'
    module.write_text('''class TestMixed:
    def test_hadoop_open(self):
        raise AssertionError('must be excluded')
    def test_regular(self):
        assert True

def test_random_seed():
    assert True
''')
    manifest = selection.manifest(suite)
    assert {row['name'] for row in manifest['included']} == {'TestMixed.test_regular', 'test_random_seed'}
    assert {row['name'] for row in manifest['excluded']} == {'TestMixed.test_hadoop_open'}
    project = Path(__file__).resolve().parents[1]
    output = tmp_path / 'output'
    env = dict(os.environ)
    env['PYTHONPATH'] = os.pathsep.join([str(project / 'python'), str(project), env.get('PYTHONPATH', '')])
    command = [sys.executable, str(project / 'conformance' / 'full_hail_pytest.py'),
               '--source', str(suite), '--output', str(output)]
    def run():
        subprocess.run(command, env=env, check=True, capture_output=True, text=True)
        return [json.loads(line) for line in (output / 'utils__test_utils.py.jsonl').read_text().splitlines()]
    events = run()
    calls = [row for row in events if row['kind'] == 'test' and row['when'] == 'call']
    assert len(calls) == 2 and all(row['outcome'] == 'passed' for row in calls)
    assert len([row for row in events if row['kind'] == 'scope_excluded']) == 1
    excluded = json.loads((output / 'scope-exclusions.json').read_text())
    assert excluded[0]['name'] == 'TestMixed.test_hadoop_open'
    module.write_text(module.read_text().replace('def test_regular(self):\n        assert True',
                                                 'def test_regular(self):\n        assert False'))
    rerun = run()
    calls = [row for row in rerun if row['kind'] == 'test' and row['when'] == 'call']
    assert len(calls) == 2, 'the second attempt must not retain first-attempt passes'
    assert sum(row['outcome'] == 'failed' for row in calls) == 1
