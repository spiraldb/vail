#!/usr/bin/env bash
# Build the pinned Hail checkout and prepare an isolated Python oracle.
set -euo pipefail

vail_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
hail_dir="$(cd "$vail_dir/../hail/hail" && pwd)"
oracle_dir="${VAIL_HAIL_ORACLE_DIR:-/private/tmp/vail-hail-oracle}"
cache_dir="${VAIL_HAIL_BUILD_CACHE:-/private/tmp/vail-hail-cache}"

export XDG_CACHE_HOME="$cache_dir"
export COURSIER_CACHE="${VAIL_HAIL_COURSIER_CACHE:-$cache_dir/coursier}"
export COURSIER_ARCHIVE_CACHE="${VAIL_HAIL_ARCHIVE_CACHE:-$cache_dir/coursier-archive}"

cd "$hail_dir"
java_home="$(bash mill --version | sed -n 's/^java.home: //p' | tail -1)"
if [[ -z "$java_home" ]]; then
    echo 'Mill did not report its JDK home' >&2
    exit 1
fi
export JAVA_HOME="$java_home"
bash mill hail.assembly

python3 - <<'PY'
from pathlib import Path
import shutil
import subprocess

root = Path.cwd()
revision = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()
version = f'0.2.139-{revision[:12]}'
(root / 'python/hail/version.py').write_text(
    f'__pip_version__ = "0.2.139"\n__revision__ = "{revision}"\n'
    f'__version__ = "{version}"\n__spark_version__ = "4.1.2"\n'
)
(root / 'python/hailtop/version.py').write_text(
    f'__pip_version__ = "0.2.139"\n__revision__ = "{revision}"\n'
    f'__version__ = "{version}"\n'
)
shutil.copyfile(root / 'out/hail/assembly.dest/out.jar', root / 'python/hail/backend/hail-all-spark.jar')
print(f'Built Hail {version}')
PY

if [[ ! -x "$oracle_dir/bin/python" ]]; then
    uv venv "$oracle_dir" --python 3.12
fi
uv pip install --python "$oracle_dir/bin/python" -r "$hail_dir/python/requirements.txt"

printf 'Run the oracle with:\nJAVA_HOME=%q SPARK_LOCAL_IP=127.0.0.1 VAIL_HAIL_PYTHON=%q .venv/bin/python -m conformance.run --backend oracle\n' \
    "$java_home" "$oracle_dir/bin/python"
