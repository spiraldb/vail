#!/bin/bash
set -euo pipefail
ENVROOT=/private/tmp/vail-entry-fixes-integration-env
"$ENVROOT/bin/python" -m pytest -q \
 python/tests/test_entry_projection.py \
 python/tests/test_matrix_layout.py \
 python/tests/test_table_joins.py \
 python/tests/test_interval_partition_joins.py \
 python/tests/test_interval_reads.py \
 python/tests/test_partition_operations.py \
 python/tests/test_count_projection.py \
 python/tests/test_projection_broadcast.py \
 python/tests/test_reduction_expression_sharing.py \
 python/tests/test_array_map.py python/tests/test_array_filter.py \
 python/tests/test_array_agg_coordinate_callbacks.py \
 python/tests/test_array_agg_coordinate_dag.py \
 python/tests/test_bind.py python/tests/test_scalar_extrema.py \
 python/tests/test_agg_composition_execution.py \
 python/tests/test_aggregate_execution.py \
 --durations=8 > conformance/runs/20261023-entry-fixes-integration/public-tests.log 2>&1
"$ENVROOT/bin/python" -m pytest -q python/tests/test_randomness.py > conformance/runs/20261023-entry-fixes-integration/random-tests.log 2>&1
