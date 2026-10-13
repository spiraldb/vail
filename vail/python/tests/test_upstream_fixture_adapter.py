import json
from pathlib import Path
import sys

import vail as hl

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'conformance'))
from full_hail_bodies import worker
from upstream_paths import FixtureModule, resource_path


def test_original_fixture_dependency_and_parameter_resolution(tmp_path):
    suite = tmp_path / 'hail' / 'python' / 'test' / 'hail'
    directory = suite / 'extract_intervals'
    directory.mkdir(parents=True)
    (suite / 'helpers.py').write_text('')
    (directory / 'conftest.py').write_text('''
import pytest
import hail as hl
@pytest.fixture(scope='session')
def mt():
    return hl.range_matrix_table(2, 3)
@pytest.fixture(scope='session')
def ht(mt):
    return mt.rows()
@pytest.fixture(scope='session', params=[hl.literal(1), 2])
def probe(request):
    return request.param
''')
    source = directory / 'test_fixture.py'
    source.write_text('''
def test_fixture(ht, mt, probe):
    assert ht.count() == 2
    assert mt.count() == (2, 3)
    assert hl.eval(probe) in (1, 2)
''')
    output = tmp_path / 'result.json'
    worker(source, 'test_fixture', output, suite)
    report = json.loads(output.read_text())
    assert report['status'] == 'passed', report
    assert len(report['variants']) == 2
    assert report['setup_errors'] == []


def test_sample_snapshot_path_appended_to_vcf_is_adapted(tmp_path, monkeypatch):
    resources = tmp_path / 'resources'
    resources.mkdir()
    cache = tmp_path / 'fixtures'
    calls = []
    class Matrix:
        def repartition(self, n):
            calls.append(('partitions', n))
            return self
        def write(self, path, overwrite):
            Path(path).mkdir(parents=True)
            (Path(path) / '_vail_matrix.json').write_text('{}')
    monkeypatch.setattr(hl, 'import_vcf', lambda path, **kw: calls.append(('source', path, kw)) or Matrix())
    monkeypatch.setattr(hl, 'read_matrix_table', lambda path, **kw: path)
    module = FixtureModule(hl, resources, cache)
    source = resource_path(resources, 'sample.vcf', cache)
    assert module.read_matrix_table(source + '.mt') == str(cache / 'sample.vcf.mt')
    assert module.read_matrix_table(source + '.mt') == str(cache / 'sample.vcf.mt')
    assert module.read_matrix_table(str(resources / 'sample.vcf-20-partitions.mt')) == str(cache / 'sample.vcf-20-partitions.mt')
    assert [c[1] for c in calls if c[0] == 'partitions'] == [2, 20]
    assert all(c[1] == str(resources / 'sample.vcf') for c in calls if c[0] == 'source')
    assert module.read_matrix_table(str(resources / 'unrelated.mt')) == str(resources / 'unrelated.mt')
