#!/usr/bin/env bash
set -euo pipefail
trap 'touch /home/ubuntu/audit-failed' ERR
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
cd /home/ubuntu/vail
tar -xzf /home/ubuntu/audit-source.tar.gz
find crates -type f -exec touch {} +
RUSTC_WRAPPER='' CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=32 RUSTFLAGS='-C target-cpu=native -C link-arg=-fuse-ld=lld' /home/ubuntu/vail-env/bin/maturin build --release --locked --interpreter /home/ubuntu/vail-env/bin/python --out /home/ubuntu/audit-wheels
uv pip install --python /home/ubuntu/vail-env/bin/python --force-reinstall --no-deps /home/ubuntu/audit-wheels/*.whl
/home/ubuntu/vail-env/bin/python -m pytest python/tests/test_matrix_layout.py python/tests/test_collection_group_indexing.py python/tests/test_matrix_array_agg.py python/tests/test_array_methods.py python/tests/test_locus.py python/tests/test_parallel_execution.py -q
/home/ubuntu/vail-env/bin/python - <<'PY'
from pathlib import Path
import json, shutil
old = Path('/home/ubuntu/after-results')
new = Path('/home/ubuntu/audit-results')
new.mkdir()
report = json.loads((old/'provenance.json').read_text())
report['previous_vail_commit'] = report['commit']
report['commit'] = 'bfe917a3e42448b0963555f2db81ade54cb0367f'
report['hail_comparison'] = {'source':'earlier run on the same VM, same installed Hail wheel and benchmark scripts', 'source_provenance': str(old/'provenance.json'), 'benchmark_code_commit': report['previous_vail_commit']}
report['runs'] = [r for r in report['runs'] if r['engine']=='hail']
report['attempts'] = list(report['runs'])
report['wall_seconds'] = 0
for record in report['runs']:
    tag, case = record['config'], record['case']
    source = old/tag/(case+'-hail')
    target = new/tag/(case+'-hail')
    target.parent.mkdir(parents=True,exist_ok=True)
    shutil.copytree(source,target)
(new/'provenance.json').write_text(json.dumps(report,indent=2)+'\n')
PY
/home/ubuntu/vail-env/bin/python benchmarks/run_cloud_benchmarks.py --output /home/ubuntu/audit-results --commit bfe917a3e42448b0963555f2db81ade54cb0367f --monitor-python /home/ubuntu/vail-env/bin/python --vail-python /home/ubuntu/vail-env/bin/python --hail-python /home/ubuntu/hail-env/bin/python --samples-8x --seconds 10 --resume
touch /home/ubuntu/audit-complete
