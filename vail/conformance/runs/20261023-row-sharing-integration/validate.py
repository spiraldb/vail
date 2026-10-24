"""Check Python-only assembly against unchanged, normally built main native."""
import hashlib, importlib.util, json, pathlib, subprocess, sys
root=pathlib.Path.cwd()
native=pathlib.Path('/Users/danielking/.codex/worktrees/entry-fixes-integration/vail/vail/python/vail/_native.abi3.so')
expected='77b0c38a0f7bea5308f1651114d0bc8391d2da93beed0b0f4111bb01e7bd7d49'
assert hashlib.sha256(native.read_bytes()).hexdigest()==expected
unchanged=subprocess.check_output(['git','diff','a0996a3','--name-only','--','vail/crates','vail/patches','vail/Cargo.lock','vail/Cargo.toml'],cwd=root.parent,text=True)
assert not unchanged, unchanged
# Load the installed extension in place. No artifact is copied or installed.
spec=importlib.util.spec_from_file_location('vail._native',native)
module=importlib.util.module_from_spec(spec)
sys.modules['vail._native']=module
spec.loader.exec_module(module)
sys.path.insert(0,str(root/'python'))
import vail
assert pathlib.Path(vail.__file__).resolve()==root/'python/vail/__init__.py'
files=['python/vail/matrixtable.py','python/tests/test_row_fraction_shared_contexts.py']
receipt={'assembly':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),'base':'a0996a304a6c505de76dc66b32c32f59bdb07831','native_origin':str(native),'native_sha256':expected,'python_origin':vail.__file__,'rust_cargo_diff_from_base':unchanged,'source_sha256':{f:hashlib.sha256((root/f).read_bytes()).hexdigest() for f in files},'runtime_method':'Python-only correctness checks use explicit assembly Python source and unchanged installed normal main native in place; no native copies or build-cache workaround'}
(root/'conformance/runs/20261023-row-sharing-integration/provenance.json').write_text(json.dumps(receipt,indent=2)+'\n')
import pytest
raise SystemExit(pytest.main(sys.argv[1:]))
