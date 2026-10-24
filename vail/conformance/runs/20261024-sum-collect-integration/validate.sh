#!/bin/bash
set -uo pipefail
/private/tmp/vail-sum-collect-integration-env/bin/python -m pytest -q \
 python/tests/test_nullable_integer_sum.py python/tests/test_list_collect_identity.py python/tests/test_sum_collect_integration.py \
 python/tests/test_collection_aggregate.py python/tests/test_collection_aggregate_kernels.py \
 python/tests/test_matrix_array_agg.py python/tests/test_agg_collections.py \
 python/tests/test_row_fraction_shared_contexts.py python/tests/test_reduction_expression_sharing.py \
 python/tests/test_aggregate.py python/tests/test_aggregate_execution.py \
 python/tests/test_agg_numeric.py python/tests/test_agg_composition_execution.py \
 python/tests/test_array_map.py python/tests/test_array_agg_coordinate_callbacks.py \
 python/tests/test_array_agg_coordinate_dag.py python/tests/test_bind.py \
 python/tests/test_matrix_layout.py python/tests/test_randomness.py --durations=8 \
 > conformance/runs/20261024-sum-collect-integration/public-tests.log 2>&1
