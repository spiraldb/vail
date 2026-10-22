import json, struct, zlib, math
import vail as hl
p='/private/tmp/bgen-throughput-data/sim_ukb'
meta=json.load(open(p+'.bgen.idx2'))
with open(p+'.bgen','rb') as f:
    for contig,position,alleles,offset in meta['vail_variant_offsets']:
        f.seek(offset)
        for _ in range(3):f.seek(int.from_bytes(f.read(2),'little'),1)
        f.seek(6,1)
        for _ in range(2):f.seek(int.from_bytes(f.read(4),'little'),1)
        size=int.from_bytes(f.read(4),'little'); block=f.read(size)
        d=zlib.decompress(block[4:]);n=int.from_bytes(d[:4],'little')
        if d[8:8+n].count(130) != n:break
    else:raise AssertionError('fixture has no defined genotypes')
expected=dict(n=0,gp0=0.,gp1=0.,gp2=0.,dosage=0.,defined=0,hom_ref=0,het=0,hom_var=0)
for i in range(n):
    if d[8+i]&128:continue
    a=d[10+n+2*i];b=d[11+n+2*i];c=255-a-b
    expected['n']+=1
    for field,value in [('gp0',a/255),('gp1',b/255),('gp2',c/255),('dosage',(b+2*c)/255)]:expected[field]+=value
    ref=a>b and a>c;het=b>a and b>c;alt=c>a and c>b
    expected['defined']+=ref or het or alt
    expected['hom_ref']+=ref;expected['het']+=het;expected['hom_var']+=alt
mt=hl.import_bgen(p+'.bgen',['GT','GP','dosage'],sample_file=p+'.sample',variants=[hl.Struct(locus=hl.Locus(contig,position),alleles=alleles)],n_partitions=1)
assert mt.count_rows()==1
result=mt.aggregate_entries(hl.struct(n=hl.agg.count(),gp0=hl.agg.sum(mt.GP[0]),gp1=hl.agg.sum(mt.GP[1]),gp2=hl.agg.sum(mt.GP[2]),dosage=hl.agg.sum(mt.dosage),defined=hl.agg.count_where(hl.is_defined(mt.GT)),hom_ref=hl.agg.count_where(mt.GT.is_hom_ref()),het=hl.agg.count_where(mt.GT.is_het()),hom_var=hl.agg.count_where(mt.GT.is_hom_var())))
for k,v in expected.items():assert math.isclose(result[k],v,rel_tol=1e-10,abs_tol=1e-7),(k,result[k],v)
with open(p+'.sample') as f:samples=[line.split()[0] for line in f.readlines()[2:]]
assert mt.s.collect()==samples
print(json.dumps({'variant':[contig,position,alleles],'samples':n,'native':dict(result),'byte_oracle':expected,'sample_order_matches':True},indent=2))
