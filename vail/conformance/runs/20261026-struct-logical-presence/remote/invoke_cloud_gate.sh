#!/bin/sh
set -eu
: "${VAIL_GATE_SOURCE:?Set extracted repository root containing vail/}"
: "${VAIL_GATE_OUT:?Set a new absolute output directory}"
: "${VAIL_GATE_TARGET:?Set the stable writable cloud target directory}"
: "${VAIL_GATE_PYTHON:?Set the existing supported Python3.12 interpreter path}"
exec python3 "$VAIL_GATE_SOURCE/vail/conformance/runs/20261026-struct-logical-presence/remote/run_cloud_gate.py" \
  --out "$VAIL_GATE_OUT" --target "$VAIL_GATE_TARGET" --python "$VAIL_GATE_PYTHON" \
  --jobs 8 --wheel-budget 1200 --table-budget 600 --storage-budget 600 \
  --runtime-budget 180 --disk-floor-gib 10
