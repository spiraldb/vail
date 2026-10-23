import vail as hl
for repetition in range(4):
    t=hl.utils.range_table(100000,n_partitions=32)
    t=t.annotate(xs=hl.range(100).map(lambda i:t.idx+i))
    result=t.aggregate(hl.agg.array_agg(lambda x:hl.agg.take(x,3000),t.xs).map(lambda xs:hl.len(xs)))
    assert result==[3000]*100
    print(repetition, result[:3])
