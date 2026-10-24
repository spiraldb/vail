#!/bin/bash
set -euo pipefail
/private/tmp/vail-native-key-projection-env/bin/python -m pytest -q \
 python/tests/test_native_key_projection.py python/tests/test_count_projection.py \
 python/tests/test_projection_broadcast.py python/tests/test_table_joins.py \
 python/tests/test_interval_partition_joins.py python/tests/test_interval_reads.py \
 python/tests/test_partition_operations.py python/tests/test_matrix_layout.py \
 python/tests/test_entry_projection.py python/tests/test_randomness.py \
 python/tests/test_computed_keys_order.py python/tests/test_matrix_reader_order.py python/tests/test_table_distinct_order.py \
 > conformance/runs/20261024-native-key-projection/public-tests.log 2>&1
