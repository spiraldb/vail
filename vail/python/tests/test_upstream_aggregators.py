"""Unmodified Table-only aggregation bodies from Hail's expression test module.

Imports/decorators/cluster fixtures are not run; these selected bodies need none
of them. This is source-derived conformance, not a live Hail oracle.
"""
import ast
from pathlib import Path
import unittest

import pytest
import vail as hl
from conformance.upstream_table_cases import extract


CASES = (
    'test_agg_array_empty',
    'test_agg_array_non_trivial_post_op',
    'test_agg_array_filter',
    'test_agg_array_group_by',
    'test_agg_array_nested',
    'test_agg_array_take',
    'test_agg_array_init_op',
    'test_aggregators_hist',
    'test_aggregators_hist_neg0',
    'test_aggregators_hist_nan',
    'test_linreg_no_data',
    'test_downsample_aggregator_on_empty_table',
)


@pytest.mark.parametrize('name', CASES)
def test_original_hail_aggregation_body(name):
    path = Path(__file__).resolve().parents[3] / 'hail/hail/python/test/hail/expr/test_expr.py'
    tree = ast.parse(path.read_text())
    fn, _ = extract(tree, 'Tests', name, str(path), {'hl': hl, '__name__': 'upstream_agg_bodies'})
    fn(unittest.TestCase())
