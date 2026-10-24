import hail as hl,json,pathlib,tarfile,hashlib
root=pathlib.Path('/home/danielking/legacy-join-scaling/fixtures'); root.mkdir(parents=True,exist_ok=True)
hl.init(quiet=True,master='local[4]',tmp_dir=str(root/'tmp'))
inputs={
 'range': (list(range(96)),list(range(13,80)),12,7,hl.tint32),
 'duplicates': ([0,2,4,4,7,10,14,None],[1,4,4,5,7,11,15,None],4,3,hl.tint32),
 'int64': ([-2**63,-2**53,-1,0,2**53+1,2**63-1],[-2**63,-2**53,0,2**53+1,2**63-1],3,2,hl.tint64),
 'empty': ([],[1,2,3],2,2,hl.tint32),
}
oracle={}
for name,(left,right,lp,rp,dtype) in inputs.items():
 tables=[]
 for side,values,parts in [('left',left,lp),('right',right,rp)]:
  field='lv' if side=='left' else 'rv'
  rows=[{'k':v,field:i} for i,v in enumerate(values)]
  t=hl.Table.parallelize(rows,schema=hl.tstruct(k=dtype,**{field:hl.tint32}),n_partitions=parts).key_by('k')
  t.write(str(root/f'{name}-{side}.ht'),overwrite=True);tables.append(t)
 oracle[name]={}
 for how in ('inner','left','right','outer'):
  oracle[name][how]=[dict(row) for row in tables[0].join(tables[1],how).collect()]
(root/'expected.json').write_text(json.dumps(oracle,indent=2))
files={str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in root.rglob('*') if p.is_file() and ('tmp' not in p.relative_to(root).parts) and not p.name.endswith('.crc')}
(root/'manifest.json').write_text(json.dumps({'generator':'Hail 0.2.139','files':files},indent=2))
with tarfile.open(root.parent/'hail-join-fixtures.tar.gz','w:gz') as out:
 for p in root.iterdir():
  if p.name!='tmp':out.add(p,arcname=p.name)
print('fixture complete')
hl.stop()
