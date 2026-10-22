import vail as hl

path = "/Users/danielking/projects/vail/vail/local-data/upstream-benchmark-20261015/vail/many_ints_ht.ht"
ht = hl.read_table(path)
result = ht.aggregate(
    hl.agg.array_agg(
        lambda i: hl.agg.linreg(ht.i0 + i, [ht.i1, ht.i2, ht.i3, ht.i4]),
        hl.range(75),
    )
)
assert len(result) == 75
