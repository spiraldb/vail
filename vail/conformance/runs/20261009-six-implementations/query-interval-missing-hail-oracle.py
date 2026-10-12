import hail as hl
hl.init(master='local[2]', quiet=True, log='/private/tmp/vail-query-missing-hail.log')
t=hl.Table.parallelize([hl.Struct(k=None,v=0),hl.Struct(k=1,v=1)],schema=hl.tstruct(k=hl.tint32,v=hl.tint32),key='k',n_partitions=1)
p='/private/tmp/vail-query-null-oracle.ht'
t.write(p,overwrite=True)
for name,q in [('missing_scalar',hl.missing(hl.tint32)),('struct_missing',hl.struct(k=hl.missing(hl.tint32))),('missing_struct',hl.missing(hl.tstruct(k=hl.tint32))),('missing_interval',hl.missing(hl.tinterval(hl.tint32))),('missing_start',hl.interval(hl.missing(hl.tint32),1,includes_end=True)),('missing_end',hl.interval(1,hl.missing(hl.tint32),includes_end=True))]:
 try: print(name,hl.eval(hl.query_table(p,q)))
 except Exception as e: print(name,type(e).__name__,str(e).splitlines()[-1])
outer=hl.Table.parallelize([hl.Struct(q=None),hl.Struct(q=1)],schema=hl.tstruct(q=hl.tint32))
for name,q in [('scalar',outer.q),('struct',hl.struct(k=outer.q))]:
 try: print('row_'+name,outer.annotate(matches=hl.query_table(p,q)).collect())
 except Exception as e: print('row_'+name,type(e).__name__,str(e).splitlines()[-1])
hl.stop()
