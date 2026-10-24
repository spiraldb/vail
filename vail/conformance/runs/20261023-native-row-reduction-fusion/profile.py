import os,pathlib,subprocess,time
root=pathlib.Path(__file__).resolve().parent
measure=root.parent/'20261023-native-reduction-cse'/'measure.py'
for kind,python in [('baseline','/private/tmp/vail-entry-fixes-integration-env/bin/python'),('candidate','/private/tmp/vail-native-row-fusion-env/bin/python')]:
 logfile=root/f'{kind}-profile-query.jsonl'
 with logfile.open('w') as output:
  p=subprocess.Popen([python,str(measure),'/private/tmp/native-reduction-cse-500m.mt','--rows','200000','--cols','2500','--case','fractions4','--repeat','12'],stdout=output,stderr=subprocess.STDOUT,env={**os.environ,'OPENBLAS_NUM_THREADS':'1'})
  while p.poll() is None:
   if '"iteration": 0' in logfile.read_text(): break
   time.sleep(.1)
  with (root/f'{kind}-sample-tool.log').open('w') as log:
   subprocess.run(['/usr/bin/sample',str(p.pid),'3','-file',str(root/f'{kind}.sample.txt')],stdout=log,stderr=subprocess.STDOUT,check=True)
  assert p.wait()==0
