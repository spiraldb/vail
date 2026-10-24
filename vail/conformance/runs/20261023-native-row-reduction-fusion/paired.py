import os, pathlib, subprocess
root=pathlib.Path(__file__).resolve().parent
measure=root.parent/'20261023-native-reduction-cse'/'measure.py'
envs={'baseline':'/private/tmp/vail-entry-fixes-integration-env/bin/python','candidate':'/private/tmp/vail-native-row-fusion-env/bin/python'}
output=root/'isolated500m'; output.mkdir(exist_ok=True)
for case in ['fractions4','fractions12','mixed','numeric','sum']:
 for kind in (['baseline','candidate'] if case in ['fractions4','mixed'] else ['candidate','baseline']):
  args=[envs[kind],str(root/'mixed.py' if case in ['mixed','numeric'] else measure),'/private/tmp/native-reduction-cse-500m.mt','--rows','200000','--cols','2500']
  if case=='numeric': args+=['--numeric-only']
  elif case!='mixed': args+=['--case',case]
  print(kind,case,flush=True)
  with (output/f'{kind}-{case}.jsonl').open('w') as out,(output/f'{kind}-{case}.stderr').open('w') as err:
   subprocess.run(args,stdout=out,stderr=err,env={**os.environ,'OPENBLAS_NUM_THREADS':'1'},check=True)
