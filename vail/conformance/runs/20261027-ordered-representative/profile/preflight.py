"""Existing accepted artifact checks; no dataset action."""
from pathlib import Path
import os
from common import EXPECTED,digest
def verify():
    if os.environ.get('PYTHONPATH') or os.cpu_count()!=32:raise RuntimeError('Unexpected import/CPU environment')
    for name,value in EXPECTED['threads'].items():
        if os.environ.get(name)!=value:raise RuntimeError(f'Thread setting differs:{name}')
    import vail as h
    import vail._native as native
    frontend=Path(h.__file__).parent.resolve()
    if frontend!=Path(EXPECTED['python']).parent.parent/'lib/python3.12/site-packages/vail':raise RuntimeError('Frontend escaped normal env')
    if digest(native.__file__)!=EXPECTED['native_sha256']:raise RuntimeError('Native differs')
    for name,sha in EXPECTED['frontend'].items():
        if digest(frontend/name)!=sha:raise RuntimeError(f'Frontend differs:{name}')
    return h
