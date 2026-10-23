import xml.etree.ElementTree as E,collections,json
r=E.parse('/private/tmp/legacy-counter-analysis-metrics.xml').getroot();ids={x.get('id'):x for x in r.iter() if x.get('id')}
def de(x):return ids[x.get('ref')] if x.get('ref') else x
stats=collections.defaultdict(lambda:collections.defaultdict(float));cycles={};doc={};names=set()
for row in r.iter('row'):
 a=list(map(de,row));t,d,event,name,thread,proc,value,core,ratio,documentation=a
 start=int(t.text);duration=int(d.text);names.add(event.text)
 if documentation.tag=='markdown-text':doc[event.text]=' '.join(de(x).text or '' for x in documentation)
 if not 2e9<=start<5e9:continue
 key=(start,thread.get('fmt'),core.get('fmt'));kind='P' if 'P Core' in core.get('fmt','') else 'E';v=float(value.text)
 if event.text=='cycle':cycles[key]=v;stats[kind]['cycles']+=v;stats[kind]['cpu_ns']+=duration
 else:
  c=cycles[key];stats[kind][event.text]+=v*c
out={'window_seconds':[2,5],'recorded_events':sorted(names),'by_coretype':{},'definitions':doc}
for kind in ['P','E','all']:
 s=stats[kind] if kind!='all' else {k:sum(x[k] for x in stats.values()) for k in stats['P']}
 out['by_coretype'][kind]={'cycles':s['cycles'],'cpu_seconds':s['cpu_ns']/1e9,'fractions_cycle_weighted':{k:s[k]/s['cycles'] for k in ['delivery','discarded','processing','useful']}}
open('/private/tmp/legacy-counter-analysis-summary.json','w').write(json.dumps(out,indent=2));print(json.dumps({k:v for k,v in out.items() if k!='definitions'},indent=2));print('defs',list(doc))
