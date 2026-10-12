import hail as hl
hl.init(master='local[2]',quiet=True,log='/private/tmp/vail-dict-hail.log')
pairs=hl.array([hl.tuple([1,10]),hl.tuple([1,20]),hl.tuple([hl.missing(hl.tint32),30]),hl.missing(hl.ttuple(hl.tint32,hl.tint32))])
d=hl.dict(pairs)
print('dictionary',hl.eval(d))
print('missing_key',hl.eval(d[hl.missing(hl.tint32)]))
print('expression_keys',hl.eval(hl.dict({hl.int32(1):10,hl.int32(1):20})))
hl.stop()
