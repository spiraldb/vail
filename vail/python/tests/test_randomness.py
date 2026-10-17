"""Self-contained public random-expression stream and numeric contracts."""
import math
import sys

import numpy as np
import pytest
import vail as hl


@pytest.fixture(autouse=True)
def reset_random_sites():
    hl.reset_global_randomness()
    yield
    hl.reset_global_randomness()


def test_scalar_and_callback_stream_goldens():
    assert hl.eval(hl.rand_unif()) == 0.9828239225846387
    assert hl.eval(hl.rand_unif()) == 0.49094525115847415
    hl.reset_global_randomness()
    assert hl.eval(hl.array([3] * 5).map(lambda _: hl.rand_int32(5))) == [4, 3, 2, 3, 3]
    hl.reset_global_randomness()
    nested = hl.range(5).map(lambda _: hl.range(2).map(lambda _: hl.rand_int32(5)))
    assert hl.eval(nested) == [[0, 3], [2, 4], [1, 4], [4, 2], [4, 4]]


@pytest.mark.parametrize('partitions', [1, 3])
def test_row_seeded_values_survive_actions_filters_and_partitioning(partitions):
    table = hl.range_table(5, n_partitions=partitions)
    table = table.annotate(random=hl.rand_int32(5, seed=-1))
    expected = [4, 3, 2, 3, 3]
    assert table.random.collect() == expected
    assert table.random.collect() == expected
    selected = table.filter(table.idx >= 2)
    assert selected.random.collect() == expected[2:]
    assert {r.idx: r.random for r in table.repartition(2).collect()} == dict(enumerate(expected))
    assert table.head(2).random.collect() == expected[:2]


def test_set_and_dictionary_random_callbacks():
    value = hl.set([3, 1, 2]).map(lambda item: hl.tuple([item, hl.rand_int32(5, seed=-1)]))
    assert hl.eval(value) == {(1, 4), (2, 3), (3, 2)}
    mapping = hl.literal({'z': 30, 'a': 10}, hl.tdict(hl.tstr, hl.tint32))
    result = mapping.map_values(lambda value: hl.struct(original=value, random=hl.rand_int32(5, seed=-1)))
    first = hl.eval(result)
    assert {key: value.original for key, value in first.items()} == {'z': 30, 'a': 10}
    assert {value.random for value in first.values()} == {3, 4}
    assert hl.eval(result) == first


def test_callback_missing_empty_and_repeated_values():
    assert hl.eval(hl.empty_array(hl.tint32).map(lambda _: hl.rand_int32(5))) == []
    assert hl.eval(hl.missing(hl.tarray(hl.tint32)).map(lambda _: hl.rand_int32(5))) is None
    value = hl.literal([None, None], hl.tarray(hl.tint32))
    assert hl.eval(value.map(lambda _: hl.rand_int32(5, seed=-1))) == [4, 3]
    assert hl.eval(hl.range(8).filter(lambda _: hl.rand_bool(1, seed=3))) == list(range(8))
    assert hl.eval(hl.find(lambda _: hl.rand_bool(1, seed=3), hl.array([7, 8]))) == 7
    assert hl.eval(hl.fold(lambda acc, _: acc + hl.rand_int32(5, seed=-1), 0, hl.array([3] * 5))) == 15


def test_ndarray_dimensions_are_exact_and_extreme_uniforms_are_finite():
    dimension = 2**53 + 1
    assert hl.eval(hl.rand_norm(size=(dimension, 0), seed=3)).shape == (dimension, 0)
    assert hl.eval(hl.rand_unif(size=(0, dimension), seed=3)).shape == (0, dimension)
    maximum = sys.float_info.max
    table = hl.range_table(64)
    values = table.annotate(value=hl.rand_unif(-maximum, maximum, seed=7)).value.collect()
    assert all(math.isfinite(value) and -maximum <= value <= maximum for value in values)
    values = hl.eval(hl.rand_unif(-maximum, maximum, seed=7, size=(8, 8)))
    assert np.isfinite(values).all()


def test_bivariate_normal_validates_and_handles_large_or_degenerate_covariance():
    assert hl.eval(hl.rand_norm2d([5, 7], [0, 0, 0], seed=3)) == [5, 7]
    value = hl.eval(hl.rand_norm2d([0, 0], [4, 6, 9], seed=3))
    assert value[1] == pytest.approx(1.5 * value[0])
    value = hl.eval(hl.rand_norm2d([0, 0], [1e308, 1e308, 1e308], seed=3))
    assert all(math.isfinite(x) for x in value)
    assert value[0] == pytest.approx(value[1])
    assert hl.eval(hl.rand_norm2d([5, 7], [0, 0, 4], seed=3))[0] == 5
    for covariance in [[1, 2, 1], [1e308, 1e308, 1], [0, 1e-320, 1e308], [-1, 0, 1], [1, 0]]:
        with pytest.raises(RuntimeError, match='bivariate normal'):
            hl.eval(hl.rand_norm2d([0, 0], covariance, seed=3))


def test_discrete_boundaries_and_compound_samples():
    assert hl.eval(hl.rand_hyper(2_000_000_000, 2_000_000_000, 2_000_000_000, seed=1)) == 2_000_000_000
    assert hl.eval(hl.rand_hyper(2_000_000_000, 0, 2_000_000_000, seed=1)) == 0
    assert hl.eval(hl.rand_multi_hyper([0, 2_000_000_000, 0], 2_000_000_000, seed=1)) == [0, 2_000_000_000, 0]
    assert hl.eval(hl.rand_cat([0, 0, 1], seed=2)) == 2
    draws = hl.eval(hl.rand_multi_hyper([10, 20, 30], 15, seed=4))
    assert sum(draws) == 15 and all(0 <= value <= limit for value, limit in zip(draws, [10, 20, 30]))
    proportions = hl.eval(hl.rand_dirichlet([1., 2., 3.], seed=4))
    assert sum(proportions) == pytest.approx(1) and all(value >= 0 for value in proportions)
    assert hl.eval(hl.rand_pois(0, seed=7)) == 0
    assert hl.eval(hl.rand_bool(0, seed=7)) is False
    assert hl.eval(hl.rand_bool(1, seed=7)) is True


def test_shuffle_preserves_nested_values_and_missing_elements():
    dtype = hl.tarray(hl.tstruct(value=hl.tarray(hl.tint32)))
    value = hl.literal([hl.Struct(value=[1, 2]), None, hl.Struct(value=[])], dtype)
    shuffled = hl.shuffle(value, seed=12)
    result = hl.eval(shuffled)
    assert result == hl.eval(shuffled)
    assert result.count(None) == 1
    assert sorted(tuple(item.value) for item in result if item is not None) == [(), (1, 2)]
    assert hl.eval(hl.shuffle(hl.empty_array(hl.tint32), seed=12)) == []
    assert hl.eval(hl.shuffle(hl.missing(hl.tarray(hl.tint32)), seed=12)) is None


@pytest.mark.parametrize('make', [
    lambda: hl.rand_int32(0), lambda: hl.rand_int64(3, 2),
    lambda: hl.rand_unif(2, 1), lambda: hl.rand_unif(0, float('inf')),
    lambda: hl.rand_norm(sd=-1), lambda: hl.rand_norm(size=(-1, 0)),
    lambda: hl.rand_gamma(0, 1), lambda: hl.rand_beta(-1, 1),
    lambda: hl.rand_cat(hl.empty_array(hl.tfloat64)), lambda: hl.rand_cat([1, -1]),
    lambda: hl.rand_dirichlet([1, -1]), lambda: hl.rand_dirichlet([0, 0]),
    lambda: hl.rand_dirichlet(hl.empty_array(hl.tfloat64)), lambda: hl.rand_hyper(2, 3, 1),
    lambda: hl.rand_multi_hyper([2, -1], 1),
])
def test_invalid_distribution_parameters_fail_when_evaluated(make):
    expression = make()
    with pytest.raises(RuntimeError, match='random distribution'):
        hl.eval(expression)


def test_missing_and_unused_distributions_are_lazy():
    assert hl.eval(hl.rand_unif(hl.missing(hl.tfloat64), 1)) is None
    assert hl.eval(hl.rand_norm(size=(hl.missing(hl.tint64), 0))) is None
    assert hl.eval(hl.rand_norm2d(hl.missing(hl.tarray(hl.tfloat64)), [1, 0, 1])) is None
    assert hl.eval(hl.if_else(False, hl.rand_unif(2, 1), 3.)) == 3
    table = hl.range_table(0).annotate(value=hl.rand_unif(2, 1))
    assert table.collect() == []


@pytest.mark.parametrize('colors,expected', [([0, 2, 3], [0, 0, 2]), ([2, 0, 3], [0, 0, 2]), ([2, 3, 0], [0, 2, 0])])
def test_multivariate_hypergeometric_preserves_seeded_zero_color_streams(colors, expected):
    assert hl.eval(hl.rand_multi_hyper(colors, 2, seed=-1)) == expected


@pytest.mark.parametrize('storage', ['large_string', 'string_view'])
def test_imported_string_list_callback_storage(storage):
    import pyarrow as pa
    dtype = pa.large_string() if storage == 'large_string' else pa.string_view()
    values = pa.array([['α', 'β'], [], None, ['γ']], type=pa.list_(dtype))
    table = hl.Table.parallelize(pa.table({'strings': values}))
    result = table.annotate(mapped=table.strings.map(lambda text: hl.struct(
        text=text, random=hl.rand_int32(5, seed=-1))))
    rows = result.collect()
    assert [[item.text for item in row.mapped] if row.mapped is not None else None for row in rows] == [['α', 'β'], [], None, ['γ']]
    assert rows == result.collect()


def test_shared_random_nodes_keep_input_and_result_aggregate_scopes():
    table = hl.range_table(5, n_partitions=3)
    random = hl.rand_int32(5, seed=-1)
    result = table.aggregate(hl.struct(values=hl.agg.collect(random), scalar=random))
    assert result['values'] == [4, 3, 2, 3, 3]
    assert result.scalar == hl.eval(hl.rand_int32(5, seed=-1))
    grouped = table.group_by(group=table.idx % 2).aggregate(values=hl.agg.collect(random), scalar=random)
    result = {row.group: row for row in grouped.collect()}
    assert sorted(result[0]['values']) == [2, 3, 4]
    assert sorted(result[1]['values']) == [3, 3]
    assert (result[0].scalar, result[1].scalar) == (4, 3)


def test_randomness_in_aggregate_array_and_explode_contexts():
    table = hl.range_table(5, n_partitions=3).annotate(elements=[0, 0])
    result = table.aggregate(hl.agg.array_agg(
        lambda _: hl.agg.sum(table.idx) + hl.rand_int32(5, seed=-1), table.elements))
    assert result == [14, 13]
    result = table.group_by(group=table.idx % 2).aggregate(value=hl.agg.array_agg(
        lambda _: hl.agg.sum(table.idx) + hl.rand_int32(5, seed=-1), table.elements))
    assert {row.group: row.value for row in result.collect()} == {0: [6, 9], 1: [6, 8]}
    result = table.aggregate(hl.agg.explode(
        lambda _: hl.agg.collect(hl.rand_int32(5, seed=-1)), table.elements))
    assert result == [0, 3, 2, 4, 1, 4, 4, 2, 4, 4]


@pytest.mark.parametrize('concentrations', [[1e308, 1e308], [sys.float_info.max, sys.float_info.max / 2],
    [1e-100, 1e-100], [1e-100, 1e-80], [5e-324, 1e-100],
    [5e-324, 5e-324], [0, 1e-100, 0], [5e-324, 1e308]])
def test_dirichlet_extremes_remain_normalized(concentrations):
    values = hl.eval(hl.rand_dirichlet(concentrations, seed=3))
    assert all(math.isfinite(value) and value >= 0 for value in values)
    assert sum(values) == pytest.approx(1)
    assert all(value == 0 for value, concentration in zip(values, concentrations) if concentration == 0)
