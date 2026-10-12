import importlib.util, sys
from pathlib import Path
runner=Path.cwd()/'conformance/all_upstream_table_cases.py'
spec=importlib.util.spec_from_file_location('runner',runner);mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)
mod.SOURCE=Path(sys.argv[1]);mod.worker(sys.argv[2],sys.argv[3])
