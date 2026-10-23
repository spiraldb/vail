import vail as hl
for rep in range(4):
    t=hl.utils.range_table(100000,n_partitions=32)
    t=t.annotate(xs=hl.range(100).map(lambda i:t.idx+i))
    result=t.aggregate(hl.agg.array_agg(lambda x:hl.agg.hist(x,0,100100,10),t.xs))
    assert len(result)==100 and all(sum(x.bin_freq)==100000 and x.n_smaller==0 and x.n_larger==0 for x in result)
    print(rep,result[0],flush=True)
