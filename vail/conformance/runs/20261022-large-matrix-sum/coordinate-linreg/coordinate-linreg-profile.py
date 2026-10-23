import vail as hl
for repetition in range(3):
    t=hl.utils.range_table(100000,n_partitions=32)
    x1=hl.float64(t.idx%97);x2=hl.float64(t.idx%7);x3=hl.float64(t.idx%11);x4=hl.float64(t.idx%13)
    y=4.+2.5*x1-1.7*x2+.3*x3+.8*x4
    fits=t.aggregate(hl.agg.array_agg(lambda offset:hl.agg.linreg(y+offset,[1.,x1,x2,x3,x4]),hl.range(75)))
    assert len(fits)==75
    assert all(f.n==100000 and abs(f.beta[0]-(4+i))<1e-7 for i,f in enumerate(fits))
    print(repetition,fits[0].n,fits[0].beta)
