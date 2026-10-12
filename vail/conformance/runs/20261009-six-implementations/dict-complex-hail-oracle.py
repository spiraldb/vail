import hail as hl
hl.init(master='local[2]',quiet=True,log='/private/tmp/vail-dict-duplicate-hail.log')
for name,value in [('python_pairs',[(1,10),(1,20)]),('array_literal',hl.literal([(1,10),(1,20)])),('array_expr',hl.array([hl.tuple([1,10]),hl.tuple([1,20])])),('expression_dict',{hl.int32(1):10,hl.int32(1):20})]:
 print(name,hl.eval(hl.dict(value)), hl.dict(value)._ir)
t=hl.Table.parallelize([hl.Struct(k=1,a=10,b=20)])
for name,x in [('dynamic_array',hl.array([hl.tuple([t.k,t.a]),hl.tuple([t.k,t.b])])),('dynamic_mapping',{t.k:t.a,t.k+0:t.b})]:
 print(name,t.annotate(d=hl.dict(x)).d.collect())

for name,key1,key2 in [('set_keys',hl.set([1,2]),hl.set([2,1])),('dict_keys',hl.dict({'a':1}),hl.dict({'a':1})),('array_keys',hl.array([1,2]),hl.array([1,2])),('cast_keys',hl.int64(1),hl.int64(1))]:
 x=hl.dict(hl.array([hl.tuple([key1,10]),hl.tuple([key2,20])]))
 print(name,hl.eval(x.values()))

hl.stop()
