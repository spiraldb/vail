#!/bin/sh
set -eu
export VAIL_HAIL_RESOURCES=/Users/danielking/projects/vail/hail/hail/hail/test/resources
/private/tmp/vail-export-join-integration-env/bin/python -m pytest -q python/tests/test_vcf_export_native.py python/tests/test_legacy_partition_joins.py python/tests/test_legacy.py python/tests/test_table_joins.py python/tests/test_interval_partition_joins.py python/tests/test_vcf.py
