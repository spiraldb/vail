#!/bin/bash
set -euo pipefail
systemd-run --unit=vail-benchmark-stop --on-active=21300s /usr/sbin/shutdown -h now
