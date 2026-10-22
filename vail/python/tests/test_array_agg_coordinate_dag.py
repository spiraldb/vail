"""Shared aggregate DAGs remain shared while lifting coordinate callbacks."""

import vail as hl


def test_reused_aggregate_expression_in_array_callback():
    table = hl.range_table(4, n_partitions=2)
    value = hl.agg.sum(table.idx)
    for _ in range(18):
        value = value + value

    result = table.aggregate(hl.agg.array_agg(lambda _: value, hl.literal([0, 1])))
    assert result == [6 * (1 << 18), 6 * (1 << 18)]
