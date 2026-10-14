import json, subprocess, time, sys, hashlib, platform
from pathlib import Path
import psutil
root=Path('/Users/danielking/projects/vail/vail')
out=root/'conformance/runs/20261014-matrix-layout-local'
out.mkdir(parents=True,exist_ok=True)
versions={'before':'/private/tmp/vail-layout-baseline-env/bin/python','after':'/private/tmp/vail-parallel-tests/bin/python'}
configs=[('samples-10',10,100000),('samples-20',20,100000),('samples-40',40,100000),('samples-80',80,100000),('rows-200k',10,200000),('rows-400k',10,400000)]
while any(p.info['name'] in {'rustc','cargo','clang','ld'} for p in psutil.process_iter(['name'])):
 time.sleep(2)
report={'kind':'local diagnostic, not the requested cloud comparison','platform':platform.platform(),'logical_cpus':psutil.cpu_count(),'source_partitions':14,'runtime_workers':32,'warmups':3,'seconds':3,'configs':configs,'runs':[]}
(out/'provenance.json').write_text(json.dumps(report,indent=2)+'\n')
for index,(tag,samples,rows) in enumerate(configs):
 for case in ('matrix_entry_sum','matrix_filter_rows_columns'):
  for version in (('before','after') if index%2==0 else ('after','before')):
   folder=out/tag/(case+'-'+version)
   command=[versions['after'],str(root/'benchmarks/profile_samples.py'),'--engine','vail','--python',versions[version],'--case',case,'--samples',str(samples),'--matrix-rows',str(rows),'--seconds','3','--output',str(folder)]
   print('START',tag,case,version,flush=True)
   result=subprocess.run(command,cwd=root,capture_output=True,text=True)
   record={'config':tag,'case':case,'version':version,'returncode':result.returncode,'command':command}
   report['runs'].append(record);(out/'provenance.json').write_text(json.dumps(report,indent=2)+'\n')
   if result.returncode:
    print(result.stderr,flush=True);sys.exit(result.returncode)
   summary=json.loads((folder/'summary.json').read_text());print('DONE',round(summary['median_seconds'],5),round(summary['peak_rss_mib']),flush=True)
print('COMPLETE',flush=True)
