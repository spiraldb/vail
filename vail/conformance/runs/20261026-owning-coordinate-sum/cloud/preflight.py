"""Read only frozen baseline provenance/metadata/schema, before candidate build."""
import json
import os
from pathlib import Path
from common import EXPECTED, digest
import vail as h
import vail._native as native
if os.environ.get('PYTHONPATH') or os.cpu_count() != 32:
    raise RuntimeError('Unexpected import environment or CPU count')
if digest(native.__file__) != EXPECTED['baseline_native_sha256']:
    raise RuntimeError('Baseline normal native fingerprint differs')
frontend = Path(h.__file__).parent
for relative, sha256 in EXPECTED['baseline_frontend'].items():
    if digest(frontend / relative) != sha256:
        raise RuntimeError(f'Baseline frontend differs: {relative}')
fixture = Path(EXPECTED['fixture'])
for relative, sha256 in EXPECTED['metadata_sha256'].items():
    if digest(fixture / relative) != sha256:
        raise RuntimeError(f'Fixture metadata differs: {relative}')
m = h.read_matrix_table(str(fixture))
if 'DP' not in m.entry.dtype or m.entry.dtype['DP'] != h.tint32:
    raise RuntimeError('Required original DP Int32 control absent/different; stop, no substitute')
if 'AD' not in m.entry.dtype or m.entry.dtype['AD'] != h.tarray(h.tint32):
    raise RuntimeError('Required original AD Int32-array field absent/different')
print(json.dumps(dict(phase='preflight', metadata_dimensions=EXPECTED['dimensions'],
    dimensions_scope='stored metadata fingerprint, not a count query',
    DP=str(m.entry.dtype['DP']), AD=str(m.entry.dtype['AD']), native_sha256=digest(native.__file__))))
