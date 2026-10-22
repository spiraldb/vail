import pytest
import vail as hl


@pytest.mark.parametrize("pl, expected", [
    ([0, 20, 100], 0.009900990296049406),
    ([20, 0, 100], 0.9900990100009803),
    ([20, 100, 0], 1.980198019704931),
])
def test_pl_dosage_matches_upstream_examples(pl, expected):
    assert hl.eval(hl.pl_dosage(pl)) == pytest.approx(expected)


def test_pl_probabilities_missingness_and_empty_arrays():
    assert hl.eval(hl.pl_to_gp([0, 10, 100])) == pytest.approx([
        0.9090909090082644, 0.09090909090082644, 9.090909090082645e-11,
    ])
    assert hl.eval(hl.pl_to_gp(hl.empty_array(hl.tint32))) == []
    assert hl.eval(hl.pl_to_gp([])) == []
    assert hl.eval(hl.pl_to_gp([None, None])) == [None, None]
    assert hl.eval(hl.pl_to_gp(None)) is None
    assert hl.eval(hl.pl_to_gp(hl.missing(hl.tarray(hl.tint32)))) is None
    assert hl.eval(hl.pl_dosage([20, hl.missing(hl.tint32), 100])) is None
    table = hl.range_table(3)
    assert table.annotate(d=hl.pl_dosage(hl.array([table.idx, 0, 10]))).d.collect() == pytest.approx([
        (1 + 0.2) / (10 ** (-i / 10) + 1 + 0.1) for i in range(3)
    ])


def test_pl_cache_negative_indices_match_hail_without_a_broadcast_cache():
    assert hl.eval(hl.pl_to_gp([-1, 0], _cache_size=2)) == pytest.approx([
        10 ** -0.1 / (1 + 10 ** -0.1), 1 / (1 + 10 ** -0.1),
    ])
    with pytest.raises(RuntimeError, match="out of bounds"):
        hl.eval(hl.pl_to_gp([-3, 0], _cache_size=2))
    with pytest.raises(TypeError, match="array<int32>"):
        hl.pl_dosage([1.0, 2.0, 3.0])
    with pytest.raises(RuntimeError):
        hl.eval(hl.pl_dosage([0, 10]))


@pytest.mark.parametrize('pl', [
    [None, 0, 10], [0, None, 10], [0, 10, None], [None, None, None],
])
def test_pl_dosage_missing_probability_at_every_position(pl):
    assert hl.eval(hl.pl_dosage(hl.literal(pl, hl.tarray(hl.tint32)))) is None


def test_pl_dosage_extreme_likelihoods_and_cache_boundaries():
    import math
    assert math.isnan(hl.eval(hl.pl_dosage([100000, 100000, 100000])))
    assert hl.eval(hl.pl_dosage([-2048, 0, 10])) == pytest.approx(1.2 / 2.1)
    with pytest.raises(RuntimeError, match='out of bounds'):
        hl.eval(hl.pl_dosage([-2049, 0, 10]))
    for pl in ([], [0], [0, 10], [0, 10, 20, 30]):
        with pytest.raises(RuntimeError, match='exactly three'):
            hl.eval(hl.pl_dosage(hl.literal(pl, hl.tarray(hl.tint32))))
