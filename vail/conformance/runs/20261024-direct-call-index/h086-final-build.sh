#!/bin/bash
set -euo pipefail
while test -d /proc/3048; do sleep 5; done
cp /home/danielking/h086-candidate-source-final.tar /home/danielking/h086-candidate-source.tar
rm -f /home/danielking/h086-direct-call-index/results/candidate-build-complete
sed 's/uv pip install --python "$root\/candidate-env\/bin\/python" "$root"\/wheels-candidate/uv pip install --reinstall-package vail --python "$root\/candidate-env\/bin\/python" "$root"\/wheels-candidate/' /home/danielking/h086-candidate-build.sh > /home/danielking/h086-final-build-inner.sh
bash /home/danielking/h086-final-build-inner.sh
printf 'ready\n' > /home/danielking/h086-direct-call-index/results/final-build-complete
