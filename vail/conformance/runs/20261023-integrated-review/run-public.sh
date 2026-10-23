#!/bin/sh
set -eu
/private/tmp/vail-integrated-review-env/bin/python -m pytest -q \
 python/tests/test_integrated_compatibility.py \
 python/tests/test_python_api_gaps.py python/tests/test_numeric_coercion.py \
 python/tests/test_vds_legacy.py python/tests/test_matrix_layout.py \
 python/tests/test_matrix_table.py python/tests/test_matrix_reader_order.py \
 python/tests/test_eval_typed.py python/tests/test_globals.py \
 python/tests/test_call_value.py python/tests/test_genotype_index_call.py \
 python/tests/test_dictionary_set_operations.py python/tests/test_collections.py \
 python/tests/test_table_distinct_order.py
