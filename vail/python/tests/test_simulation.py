"""Public sampling and Balding–Nichols simulation contracts."""
import math

import pytest
import vail as hl


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
    if axis == 'table':
        assert first.collect() == second.collect()
    elif axis == 'row':
        assert first.rows().collect() == second.rows().collect()
    else:
        assert first.cols().collect() == second.cols().collect()
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
    replayed = generate()
    assert model.rows().collect() == replayed.rows().collect()
    assert model.cols().collect() == replayed.cols().collect()
    assert model.GT.collect() == replayed.GT.collect()


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


@pytest.mark.parametrize('name', ['n_populations', 'n_samples', 'n_variants', 'n_partitions'])
@pytest.mark.parametrize('value', [True, 1.5, '2', 0, -1, 2**31])
def test_counts_checked_before_allocation(name, value):
    options = dict(n_populations=2, n_samples=3, n_variants=4)
    options[name] = value
    error = TypeError if type(value) is not int else ValueError
    with pytest.raises(error, match=name):
        hl.balding_nichols_model(**options)


@pytest.mark.parametrize('name,values', [
    ('pop_dist', [0, 0]), ('pop_dist', [-1, 1]),
    ('pop_dist', [math.inf, 1]), ('pop_dist', [math.nan, 1]),
    ('fst', [math.nan, .1]), ('fst', [math.inf, .1]),
    ('fst', [1, .1]), ('fst', [5e-324, .1]),
])
def test_population_parameter_validation(name, values):
    with pytest.raises(ValueError):
        hl.balding_nichols_model(2, 3, 4, **{name: values})


@pytest.mark.parametrize('mixture', [False, True])
@pytest.mark.parametrize('ancestral', [0., 1.])
def test_fixed_ancestral_frequencies_and_zero_population_weights(mixture, ancestral):
    model = hl.balding_nichols_model(2, 5, 3, mixture=mixture,
                                    pop_dist=[0, 1], af_dist=hl.float64(ancestral))
    assert model.af.collect() == [[ancestral, ancestral]] * 3
    assert all(call.alleles == [int(ancestral)] * 2 for call in model.GT.collect())
    populations = model.pop.collect()
    assert populations == ([[0., 1.]] * 5 if mixture else [1] * 5)


@pytest.mark.parametrize('ancestral', [-.1, 1.1, math.nan, math.inf, -math.inf, None])
def test_realized_ancestral_frequency_checked(ancestral):
    model = hl.balding_nichols_model(2, 2, 2, af_dist=hl.literal(ancestral, hl.tfloat64))
    with pytest.raises(Exception, match='ancestral allele frequency'):
        model.af.collect()


def test_ordinary_seeded_model_compatibility():
    hl.reset_global_randomness()
    model = hl.balding_nichols_model(2, 4, 3, n_partitions=1)
    assert model.ancestral_af.collect() == [
        .381520365258488, .8450705622977756, .633700898973382]
    assert model.af.collect() == [
        [.11000375894739493, .39227775178485935],
        [.9721709367806775, .6550640884736961],
        [.7770338388204026, .392858528947945]]
    assert [c.alleles for c in model.GT.collect()] == [
        [0,0], [0,0], [0,0], [0,0], [1,1], [0,1],
        [1,1], [1,1], [1,1], [0,0], [1,1], [0,1]]


def test_model_frequency_moments_and_partition_coverage():
    hl.reset_global_randomness()
    model = hl.balding_nichols_model(2, 16, 1000, n_partitions=7,
                                    fst=[.1, .2], af_dist=hl.float64(.4))
    rows = model.rows().collect()
    assert len(rows) == 1000
    assert len({row.locus for row in rows}) == 1000
    assert model.n_partitions() == 7
    for population, fst in enumerate([.1, .2]):
        frequencies = [row.af[population] for row in rows]
        assert all(0 <= p <= 1 for p in frequencies)
        mean = sum(frequencies) / len(frequencies)
        variance = sum((p - .4)**2 for p in frequencies) / len(frequencies)
        assert abs(mean - .4) < .04
        assert abs(variance - .4 * .6 * fst) < .01
    calls = model.GT.collect()
    assert len(calls) == 16000
    assert abs(sum(sum(c.alleles) for c in calls) / (2 * len(calls)) - .4) < .04


def test_seeded_model_uneven_partition_compatibility():
    hl.reset_global_randomness()
    model = hl.balding_nichols_model(2, 4, 5, n_partitions=3)
    assert model.ancestral_af.collect() == [
        .381520365258488, .7432643300025062, .7058845354840656,
        .7227470169998954, .21320743991037927]
    assert model.af.collect() == [
        [.11000375894739493, .39227775178485935],
        [.4232802466031035, .8826159957824004],
        [.9189645070512301, .5605766772759937],
        [.8733312592897426, .6621799229887871],
        [.44043935723849026, .2342235310903643]]
    assert [c.alleles for c in model.GT.collect()] == [
        [0,0], [0,0], [0,0], [0,0], [0,1], [1,1], [0,0], [0,1],
        [1,1], [0,1], [1,1], [0,1], [1,1], [0,0], [1,1], [1,1],
        [0,1], [0,0], [0,0], [0,0]]


def test_simulation_prefix_replay_across_generation_batches():
    def generate(variants):
        hl.reset_global_randomness()
        return hl.balding_nichols_model(2, 65, variants, n_partitions=1)
    # Prefix values must survive the generation batch boundary and a larger
    # requested model. Only public rows and calls are compared.
    small = generate(1010)
    prefix = generate(1000000).head(1010)
    assert small.rows().collect() == prefix.rows().collect()
    assert small.cols().collect() == prefix.cols().collect()
    assert small.GT.collect() == prefix.GT.collect()


@pytest.mark.parametrize('mixture', [False, True])
def test_population_distribution_moments(mixture):
    hl.reset_global_randomness()
    model = hl.balding_nichols_model(2, 1000, 1, pop_dist=[1, 3], mixture=mixture)
    populations = model.pop.collect()
    if mixture:
        assert all(len(p) == 2 and all(x >= 0 for x in p) and abs(sum(p) - 1) < 1e-12
                   for p in populations)
        fractions = [p[1] for p in populations]
    else:
        assert set(populations) <= {0, 1}
        fractions = populations
    assert abs(sum(fractions) / len(fractions) - .75) < .06


@pytest.mark.parametrize('mixture', [False, True])
def test_extreme_finite_population_weights(mixture):
    model = hl.balding_nichols_model(2, 16, 1, pop_dist=[1e308, 1e308], mixture=mixture)
    populations = model.pop.collect()
    if mixture:
        assert all(all(math.isfinite(x) and x >= 0 for x in p) and abs(sum(p) - 1) < 1e-12
                   for p in populations)
    else:
        assert set(populations) == {0, 1}
    assert hl.eval(model.index_globals().bn.pop_dist) == [1e308, 1e308]


def test_head_preserves_prefix_scans_randomness_and_captured_globals():
    source = hl.utils.range_table(12, 3)
    total = source.aggregate(hl.agg.sum(source.idx), _localize=False)
    table = source.annotate_globals(total=total)
    table = table.annotate(prefix=hl.scan.sum(table.idx), draw=hl.rand_unif(seed=29))
    expected = table.collect()[:5]
    prefix = table.head(5)
    assert prefix.collect() == expected
    assert hl.eval(prefix.index_globals().total) == 66


def test_head_evaluates_only_retained_projection_values():
    table = hl.utils.range_table(1000000, 7)
    table = table.annotate(value=hl.case().when(table.idx < 3, table.idx).or_error('outside requested prefix'))
    assert [r.value for r in table.head(3).collect()] == [0, 1, 2]
