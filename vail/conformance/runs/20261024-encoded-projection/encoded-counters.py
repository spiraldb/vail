import xml.etree.ElementTree as E,collections,json,sys
ids={};cycles=collections.Counter();vals=collections.defaultdict(dict);docs={};rows=0
# MetricTable has perthread core-slice cycle and derived ratios. Weight ratios by same slice cycles.
for ev,e in E.iterparse(sys.argv[1],events=['end']):
 if e.get('id'):ids[e.get('id')]=(e.text,e.get('fmt'))
 if e.tag!='row':continue
 def val(x):
  if x.get('ref'):return ids[x.get('ref')][0]
  return x.text
 ch=list(e)
 if len(ch)!=10:continue
 start,dur,thread,proc,total,value,event,name,core,doc=ch
 key=(val(start),val(dur),thread.get('ref') or thread.get('id'),core.get('ref') or core.get('id'))
 event=val(event)
 try:value=float(val(total) or val(value))
 except (TypeError,ValueError):continue
 vals[key][event]=value;rows+=1
 if doc.tag=='markdown-text' and not doc.get('ref'):docs[event]=doc.get('fmt')
 e.clear()
num=collections.Counter();den=collections.Counter();n=0
for k,v in vals.items():
 c=v.get('cycle',0)
 if not c:continue
 # omitstartup/warmup/pause by relative recordingtime: measured loop ~3.5–23.5 sec
 t=float(k[0])/1e9
 if t<4:continue
 n+=1
 for name,x in v.items():
  if name=='cycle':num[name]+=x
  else:num[name]+=x*c;den[name]+=c
print(json.dumps({'rows':rows,'slices':n,'cycles':num['cycle'],'cycle_weighted_ratios':{k:num[k]/den[k] for k in den},'docs':docs},indent=2))
