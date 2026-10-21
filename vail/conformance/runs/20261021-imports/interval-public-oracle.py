import hail as hl, tempfile, json, os
hl.init(master='local[2]',quiet=True,log='/tmp/vail-interval-probe.hail.log')
results={}
try:
 results['boolean_numeric_equality']=hl.eval(hl.literal(True)==1.0)
 with tempfile.TemporaryDirectory() as d:
  p=os.path.join(d,'input.txt')
  for key,text,kwargs in [('numeric','1:2-5\n',{}),('recode','chr1:2-5\n',{'contig_recoding':{'chr1':'1'}}),('skip','bad:2-5\n',{'skip_invalid_intervals':True})]:
   open(p,'w').write(text)
   try:
    t=hl.import_locus_intervals(p,**kwargs)
    results[key]=[{'start':str(r.interval.start),'end':str(r.interval.end),'includes_start':r.interval.includes_start,'includes_end':r.interval.includes_end} for r in t.collect()]
   except Exception as e:results[key]={'error':type(e).__name__,'message':str(e)[-500:]}
 print(json.dumps(results))
 open('/tmp/vail-interval-probe.json','w').write(json.dumps(results,indent=2)+'\n')
finally:hl.stop()
