"""Configured reducers with initializer arguments use the coordinate bundle."""

import vail as hl


def test_composite_corr_and_hist_coordinate_reducers():
    table = hl.Table.parallelize(
        [
            {"xs": [0, 1], "y": 0},
            {"xs": [1, 2], "y": 1},
            {"xs": [2, 3], "y": 2},
            {"xs": [3, 4], "y": 3},
        ],
        schema=hl.tstruct(xs=hl.tarray(hl.tint32), y=hl.tint32),
        n_partitions=2,
    )
    result = table.aggregate(hl.agg.array_agg(
        lambda x: hl.struct(
            correlation=hl.agg.corr(x, table.y),
            histogram=hl.agg.hist(x, 0, 4, 2),
        ),
        table.xs,
    ))

    assert [row.correlation for row in result] == [1.0, 1.0]
    assert [row.histogram.bin_freq for row in result] == [[2, 2], [1, 3]]
