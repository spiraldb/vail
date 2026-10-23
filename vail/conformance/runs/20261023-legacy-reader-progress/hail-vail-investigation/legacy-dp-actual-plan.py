"""Plan the exact aggregate_entries(sum(DP)) lowering without scanning entries.

Run from /Users/danielking/.codex/worktrees/legacy-layout/vail/vail:
  PYTHONPATH=python /private/tmp/vail-legacy-review-env/bin/python /private/tmp/legacy-dp-actual-plan.py > /private/tmp/legacy-dp-actual-plan.txt

MatrixTable._aggregate's direct_entry_sum branch constructs these same row_total
and aggregate_sum expressions. NativeTable.aggregate_physical_plan constructs
GlobalsIR::aggregate and invokes its physical_plan(), the production planning
path. Its UnboundActionExec marker says scalar dependencies were not executed;
this is plan evidence, complemented by actual execution stacks in:
  /private/tmp/legacy-direct-blocks-matrix_sum_dp-profile.txt
There RepartitionExec::pull_from_input -> SharedCoalescer::finalize ->
BatchCoalescer::finish_buffered_batch -> concat_lists confirms nested-list
coalescing, and AggregateStream -> aggregate_batch -> scalar UDF ->
HailPhysicalExpr::evaluate -> integer_list_sum::evaluate locates reduction.
"""
import vail as hl
from vail.matrix_list_aggregates import reduce_list

path = '/Users/danielking/projects/vail/vail/conformance/runs/20261016-hail-paired/data/profile25.mt'
matrix = hl.MatrixTable.read_legacy(path)
expression = matrix._validate(hl.agg.sum(matrix.DP), 'entry', aggregate=True)
core = matrix._mir.core
row_total = reduce_list(matrix, expression, core, None, direct_entry_sum=True)
print(core._native.aggregate_physical_plan(hl.agg.sum(row_total).to_ir()))
