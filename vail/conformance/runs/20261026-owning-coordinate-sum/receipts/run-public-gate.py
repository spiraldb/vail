import json, os, pathlib, re, subprocess, sys
root = pathlib.Path('/private/tmp/vail-owning-coordinate-gate')
source = pathlib.Path('/private/tmp/vail-owning-coordinate-sum/vail')
files = re.findall(r'python/tests/test_[a-z_]+\.py', (source/'conformance/runs/20261024-coordinate-current-main-integration/h050-main-gate.sh').read_text())
files += ['python/tests/test_native_array_length_projection.py', 'python/tests/test_batched_list_sets.py', '/private/tmp/vail-df-leaf-main-integration/public-tests/test_transparent_struct_fields.py', 'python/tests/test_matrix_owning_coordinate_sum.py']
command = [sys.argv[1], '-m', 'pytest', '-q', *files, '--junitxml', str(root/'whole-bitmap-public.xml')]
(root/'whole-bitmap-public-command.json').write_text(json.dumps(command, indent=2)+'\n')
with (root/'whole-bitmap-public.log').open('w') as output:
    result = subprocess.run(command, cwd=source, stdout=output, stderr=subprocess.STDOUT,
        env=dict(os.environ, OPENBLAS_NUM_THREADS='1', OMP_NUM_THREADS='1'))
(root/'whole-bitmap-public-exit.txt').write_text(str(result.returncode)+'\n')
sys.exit(result.returncode)
