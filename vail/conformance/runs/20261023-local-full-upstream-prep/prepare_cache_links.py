"""Prepare exact raw/native cache aliases without copying or generating data."""
import argparse,json
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('directory',type=Path);p.add_argument('--apply',action='store_true');a=p.parse_args()
links={
 'many_ints_table/many_ints_table.tsv.bgz':'/Users/danielking/projects/vail/vail/local-data/upstream-benchmark-20261015/many_ints_table/many_ints_table.tsv.bgz',
 'many_ints_table/many_ints_table.tsv':'/Users/danielking/projects/vail/vail/local-data/upstream-benchmark-20261015/many_ints_table/many_ints_table.tsv',
 'profile25/profile.vcf.bgz':'/tmp/vail-matrix-decode-count/fixtures/profile25/profile.vcf.bgz',
 'sim_ukb/sim_ukb.bgen':'/tmp/bgen-throughput-data/sim_ukb.bgen',
 'sim_ukb/sim_ukb.sample':'/tmp/bgen-throughput-data/sim_ukb.sample',
 'sim_ukb/sim_ukb.bgen.idx2':'/tmp/bgen-throughput-data/sim_ukb.bgen.idx2',
 'vail/many_ints_ht.ht':'/Users/danielking/projects/vail/vail/local-data/upstream-benchmark-20261015/vail/many_ints_ht.ht',
 'vail/profile25_mt.mt':'/tmp/entries-profile25.mt'}
for name in ['many_partitions_ht','many_partitions_ht1','many_partitions_ht2']:
 for n in [10,100,1000]:
  source_name='many_partitions_ht1' if name=='many_partitions_ht' else name
  links[f'vail/{name}-p{n}.ht']=f'/private/tmp/hail-upstream-benchmark-data/vail/{source_name}-p{n}.ht'
for name,target in links.items():
 src=Path(target);dst=a.directory/name
 if not src.exists() or (src.is_dir() and not (src/'_vail_table.json').is_file() and not (src/'_vail_matrix.json').is_file()):
  print(json.dumps(dict(destination=str(dst),source=str(src),status='missing_or_incomplete')));continue
 if dst.exists() or dst.is_symlink():
  if dst.resolve()!=src.resolve():raise RuntimeError(f'refuse replacement of {dst}')
 elif a.apply:
  dst.parent.mkdir(parents=True,exist_ok=True);dst.symlink_to(src,target_is_directory=src.is_dir())
 print(json.dumps(dict(destination=str(dst),source=str(src),applied=a.apply)))
