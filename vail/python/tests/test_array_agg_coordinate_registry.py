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


def test_bare_configured_collection_reducers_match_public_wrapped_results():
    table=hl.Table.parallelize([{'xs':[3,6]},{'xs':[1,4]},{'xs':[2,5]}],
        schema=hl.tstruct(xs=hl.tarray(hl.tint32)),n_partitions=1)
    for reducer in [lambda x:hl.agg.collect(x),lambda x:hl.agg.take(x,2),
                    lambda x:hl.agg.collect_as_set(x),lambda x:hl.agg.counter(x)]:
        bare=table.aggregate(hl.agg.array_agg(reducer,table.xs))
        wrapped=table.aggregate(hl.agg.array_agg(lambda x:hl.struct(value=reducer(x)),table.xs))
        assert bare==[x.value for x in wrapped]
    taken=table.aggregate(hl.agg.array_agg(lambda x:hl.agg.take(x,2),table.xs))
    assert taken==[[3,1],[6,4]]
    assert table.aggregate(hl.agg.array_agg(lambda x:hl.agg.take(x,0),table.xs))==[[],[]]
