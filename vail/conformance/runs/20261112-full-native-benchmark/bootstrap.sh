#!/bin/bash
set -euo pipefail
BASE=/home/danielking/native-benchmark-20261112
exec > >(tee "$BASE/bootstrap.log") 2>&1
trap 'rc=$?; echo "$rc" > "$BASE/bootstrap.exit"' EXIT
export DEBIAN_FRONTEND=noninteractive
sudo -n systemctl is-active vail-benchmark-stop.timer
sudo -n apt-get update -qq
sudo -n apt-get install -y -qq build-essential pkg-config libssl-dev cmake clang lld git curl unzip python3-venv openjdk-11-jre-headless protobuf-compiler libclang-dev
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain 1.98.0 --profile minimal
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export CARGO_BUILD_JOBS=8 CARGO_INCREMENTAL=0
rustc -Vv
uv --version
# Install the required compiler cache normally; no copied compiler outputs.
timeout 1200 cargo install kache --locked --version 2.0.0
kache --version
uv python install 3.12
uv venv --python 3.12 "$BASE/vail-env"
uv venv --python 3.12 "$BASE/hail-env"
uv pip install --python "$BASE/vail-env/bin/python" maturin==1.15.0 'numpy>=2' 'pyarrow>=20' pandas psutil pytest
uv pip install --python "$BASE/hail-env/bin/python" hail==0.2.139 psutil pytest
cd "$BASE"
sha256sum -c source.sha256
mkdir source
tar -xzf source.tar.gz -C source
python3 - <<'PY'
from pathlib import Path
import json,hashlib
b=Path('/home/danielking/native-benchmark-20261112')
for name,h in json.loads((b/'source-manifest.json').read_text()).items():
 assert hashlib.sha256((b/'source'/name).read_bytes()).hexdigest()==h,name
PY
cd "$BASE/source/vail"
export RUSTUP_TOOLCHAIN=1.98.0 RUSTC_WRAPPER=kache RUSTFLAGS='-C target-cpu=native' CARGO_TARGET_DIR="$BASE/target"
timeout 5400 "$BASE/vail-env/bin/python" -m maturin build --release --locked --interpreter "$BASE/vail-env/bin/python" --out "$BASE/wheels"
uv pip install --no-deps --python "$BASE/vail-env/bin/python" "$BASE"/wheels/*.whl
"$BASE/vail-env/bin/python" - <<'PY'
from pathlib import Path
import json,hashlib,sys,importlib.metadata
import vail,vail._native as n
b=Path('/home/danielking/native-benchmark-20261112');src=b/'source/vail/python/vail';package=Path(vail.__file__).parent
front={str(p.relative_to(package)):hashlib.sha256(p.read_bytes()).hexdigest()for p in package.rglob('*.py')}
for p,h in front.items():assert hashlib.sha256((src/p).read_bytes()).hexdigest()==h,p
(b/'vail-ready.json').write_text(json.dumps({'python':sys.executable,'frontend':str(package),'frontend_sha256':front,'native_sha256':hashlib.sha256(Path(n.__file__).read_bytes()).hexdigest(),'native':n.__file__,'allocator':n._benchmark_allocator,'packages':sorted((x.metadata['Name'],x.version)for x in importlib.metadata.distributions())},indent=2)+'\n')
PY
"$BASE/hail-env/bin/python" - <<'PY'
from pathlib import Path
import json,hashlib,sys,importlib.metadata
import hail
b=Path('/home/danielking/native-benchmark-20261112');j=Path(hail.__file__).parent/'backend/hail-all-spark.jar';assert j.is_file()
(b/'hail-ready.json').write_text(json.dumps({'python':sys.executable,'version':hail.__version__,'jar':str(j),'jar_sha256':hashlib.sha256(j.read_bytes()).hexdigest(),'packages':sorted((x.metadata['Name'],x.version)for x in importlib.metadata.distributions())},indent=2)+'\n')
PY
lscpu > "$BASE/hardware.txt"
uv pip freeze --python "$BASE/vail-env/bin/python" > "$BASE/vail-requirements.txt"
uv pip freeze --python "$BASE/hail-env/bin/python" > "$BASE/hail-requirements.txt"
echo BOOTSTRAP_COMPLETE
