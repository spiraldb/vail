import vail as hl
for repetition in range(4):
    table=hl.utils.range_table(100000,n_partitions=32)
    table=table.annotate(xs=hl.range(100).map(lambda i: hl.or_missing((table.idx+i)%5!=0, table.idx+i)))
    result=table.aggregate(hl.agg.array_agg(lambda x:hl.struct(size=hl.len(hl.agg.collect(x))),table.xs))
    assert len(result)==100 and all(x.size==100000 for x in result)
    print(repetition,result[0],flush=True)
