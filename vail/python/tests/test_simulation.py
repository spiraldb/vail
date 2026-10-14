"""Sampling and model behavior, including original upstream statistical checks."""
import ast
import copy
from pathlib import Path
import unittest

import pytest
import vail as hl


@pytest.mark.parametrize('name', [
    'test_balding_nichols_model',
    'test_balding_nichols_model_same_results',
    'test_balding_nichols_model_af_ranges',
    'test_balding_nichols_stats',
])
def test_upstream_balding_nichols(name):
    source = Path(__file__).resolve().parents[3] / 'hail/hail/python/test/hail/methods/test_statgen.py'
    node = copy.deepcopy(next(n for n in ast.walk(ast.parse(source.read_text()))
                              if isinstance(n, ast.FunctionDef) and n.name == name))
    node.decorator_list = []
    scope = {'hl': hl}
    exec(compile(ast.fix_missing_locations(ast.Module(body=[node], type_ignores=[])),
                 str(source), 'exec'), scope)
    hl.reset_global_randomness()
    scope[name](unittest.TestCase())


def test_seeded_table_sample_matches_upstream():
    assert hl.utils.range_table(16).sample(0.25, seed=0).count() == 4


@pytest.mark.parametrize('axis', ['table', 'row', 'col'])
def test_sampling_endpoints_validation_and_replay(axis):
    if axis == 'table':
        value = hl.utils.range_table(32, 3)
        sample = value.sample
        count = lambda x: x.count()
    else:
        value = hl.utils.range_matrix_table(32, 32, 3)
        sample = value.sample_rows if axis == 'row' else value.sample_cols
        count = lambda x: x.count_rows() if axis == 'row' else x.count_cols()
    assert count(sample(0, seed=11)) == 0
    assert count(sample(1, seed=11)) == 32
    for probability in [-0.1, 1.1, float('nan')]:
        with pytest.raises(ValueError, match=r'p.*\[0,1\]'):
            sample(probability)
    first = sample(0.4, seed=11)
    second = sample(0.4, seed=11)
    assert first._same(second)
    assert 0 < count(first) < 32


@pytest.mark.parametrize('mixture', [False, True])
def test_phased_model_support_and_replay(mixture):
    def generate():
        hl.reset_global_randomness()
        return hl.balding_nichols_model(2, 6, 5, n_partitions=3,
                                       mixture=mixture, phased=True)
    model = generate()
    assert model.n_partitions() == 3
    assert model.aggregate_entries(hl.agg.all(model.GT.phased)) is True
    calls = model.entries().select('GT').collect()
    assert len(calls) == 30
    assert all(c.GT.phased and c.GT.ploidy == 2 and set(c.GT.alleles) <= {0, 1}
               for c in calls)
    assert model._same(generate())


@pytest.mark.parametrize('kwargs', [
    {'n_populations': 0}, {'n_samples': 0}, {'n_variants': 0},
    {'n_partitions': 0}, {'pop_dist': [1]}, {'fst': [0, 0.1]}, {'af_dist': 1},
])
def test_model_argument_validation(kwargs):
    options = dict(n_populations=2, n_samples=3, n_variants=4)
    options.update(kwargs)
    with pytest.raises(ValueError):
        hl.balding_nichols_model(**options)


def test_call_expression_accessors_handle_missing_and_multiallelic_calls():
    t = hl.Table.parallelize([
        hl.Struct(gt=hl.Call([0, 2], phased=True)),
        hl.Struct(gt=hl.Call([1, 3])),
        hl.Struct(gt=hl.Call([0])),
        hl.Struct(gt=None),
    ], schema=hl.tstruct(gt=hl.tcall))
    rows = t.select(phase=t.gt.phased, n=t.gt.n_alt_alleles()).collect()
    assert [(r.phase, r.n) for r in rows] == [(True, 1), (False, 2), (False, 0), (None, None)]
