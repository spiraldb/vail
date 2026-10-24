import xml.etree.ElementTree as E,collections,json,sys
p=sys.argv[1];r=E.parse(p);ids={e.get('id'):e for e in r.iter() if e.get('id')}
def deref(e):
 while e.get('ref'):e=ids[e.get('ref')]
 return e
def txt(e):return deref(e).text or ''
leaf=collections.Counter();inc=collections.Counter();total=0
for row in r.findall('.//row'):
 bt=row.find('backtrace')
 if bt is None:continue
 bt=deref(bt);fs=[deref(x) for x in bt.findall('frame')];w=row.find('weight');weight=float(txt(w)) if w is not None else 1
 total+=weight
 if fs:leaf[fs[0].get('name','?')]+=weight
 for name in set(f.get('name','?') for f in fs):inc[name]+=weight
print(json.dumps({'total':total,'leaf':leaf.most_common(5000),'inclusive':inc.most_common(5000)},indent=2))
