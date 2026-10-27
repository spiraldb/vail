import json,time,pathlib,sys
mode=sys.argv[1]
if mode=='output':print(json.dumps({'phase':'payload','value':'x'*70000}),flush=True);time.sleep(1)
elif mode=='slow':print(json.dumps({'phase':'action_start','id':0,'monotonic':time.monotonic()}),flush=True);time.sleep(1)
else:
 if mode=='fragment':
  text=json.dumps({'phase':'payload','value':'z'*40000})+'\n'
  for start in range(0,len(text),512):sys.stdout.write(text[start:start+512]);sys.stdout.flush();time.sleep(.0005)
 i=0
 while not pathlib.Path('stop-launch').exists():
  print(json.dumps({'phase':'action_start','id':i,'monotonic':time.monotonic()}),flush=True)
  time.sleep(.005)
  print(json.dumps({'phase':'action','id':i,'result':[i],'completed_monotonic':time.monotonic()}),flush=True)
  i+=1
