import math

import pytest
import vail as hl


def calls():
    return hl.Table.parallelize(
        [{'gt': hl.Call((0, 0))}, {'gt': hl.Call((0, 1))},
         {'gt': hl.Call((0, 1))}, {'gt': hl.Call((1, 1))}],
        schema=hl.tstruct(gt=hl.tcall), n_partitions=2,
    )


@pytest.mark.parametrize('mode,expected_p', [(False, 0.6571428571428571), (True, 0.5714285714285714)])
def test_hwe_mode_from_expression_and_globals(mode, expected_p):
    table = calls().annotate_globals(mode=mode)
    result = table.aggregate(hl.agg.hardy_weinberg_test(table.gt, one_sided=table.mode))
    # Golden values from Hail's test_hardy_weinberg_test: counts (1, 2, 1).
    assert result.het_freq_hwe == pytest.approx(0.5714285714285714)
    assert result.p_value == pytest.approx(expected_p)
    literal = table.aggregate(hl.agg.hardy_weinberg_test(table.gt, one_sided=hl.literal(mode)))
    assert literal == result
    grouped = table.group_by().aggregate(hwe=hl.agg.hardy_weinberg_test(table.gt, one_sided=table.mode))
    assert grouped.collect()[0].hwe == result


def test_hwe_mode_can_be_an_aggregate_result():
    table = calls()
    result = table.aggregate(hl.agg.hardy_weinberg_test(table.gt, one_sided=hl.agg.count() == 4))
    assert result.p_value == pytest.approx(0.5714285714285714)


def test_hwe_missing_mode_and_empty_input():
    table = calls()
    assert table.aggregate(hl.agg.hardy_weinberg_test(table.gt, one_sided=hl.missing(hl.tbool))) is None
    empty = table.head(0).annotate_globals(mode=True)
    result = empty.aggregate(hl.agg.hardy_weinberg_test(empty.gt, one_sided=empty.mode))
    assert math.isnan(result.het_freq_hwe)
    assert result.p_value == 0.5


def test_hwe_mode_global_dependency_remains_lazy(tmp_path):
    table = calls().annotate_globals(mode=True)
    path = tmp_path / 'calls'
    table.write(path)
    read = hl.read_table(path)
    globals_path = next(path.rglob('globals.arrow'))
    hidden = globals_path.with_suffix('.hidden')
    globals_path.rename(hidden)
    expression = hl.agg.hardy_weinberg_test(read.gt, one_sided=read.mode)
    hidden.rename(globals_path)
    assert read.aggregate(expression).p_value == pytest.approx(0.5714285714285714)


def test_hwe_rejects_non_boolean_mode():
    table = calls()
    with pytest.raises(TypeError, match='bool expression'):
        hl.agg.hardy_weinberg_test(table.gt, one_sided=hl.int32(1))
