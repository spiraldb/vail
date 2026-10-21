//! Streaming MatrixTable entry expansion ordered by shared column metadata.
//!
//! Input rows are already ordered by their row key and each row's cell list is
//! already permuted to the same order as the column-record global. The
//! execution operator retains only the current equal-row-key run, then uses
//! Arrow `take` kernels to emit each column across the run before advancing to
//! the next key. This preserves composite ordering without sorting the
//! expanded table.

use crate::plan::PhysicalPlan;
use arrow_array::{
    Array, ArrayRef, LargeListArray, ListArray, RecordBatch, StructArray, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, SortOptions};
use datafusion_common::{DataFusionError, tree_node::TreeNodeRecursion};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::{
    EquivalenceProperties, LexOrdering, PhysicalExpr, PhysicalSortExpr, expressions::Column,
};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
    execution_plan::{Boundedness, EmissionType},
    stream::RecordBatchStreamAdapter,
};
use futures::TryStreamExt;
use std::{fmt, sync::Arc};
use vail_storage::fragment_index::make_typed_key_comparators;

#[derive(Clone, Debug)]
pub(crate) struct Config {
    pub row_struct: usize,
    pub cells: usize,
    pub key_indices: Vec<usize>,
    pub columns_global: String,
    pub column_struct: String,
    pub row_field_names: Vec<String>,
    pub col_field_names: Vec<String>,
    pub value_field: String,
    pub output_schema: SchemaRef,
    pub output_partition_tag: String,
    pub output_key_fields: Vec<String>,
    pub global_schema: SchemaRef,
}

pub(crate) fn lower(
    rows: PhysicalPlan,
    globals: PhysicalPlan,
    config: Config,
) -> datafusion_common::Result<PhysicalPlan> {
    Ok(Arc::new(MatrixEntriesExec::new(rows, globals, config)?))
}

#[derive(Debug)]
struct MatrixEntriesExec {
    rows: PhysicalPlan,
    globals: PhysicalPlan,
    config: Config,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl MatrixEntriesExec {
    fn new(
        rows: PhysicalPlan,
        globals: PhysicalPlan,
        config: Config,
    ) -> datafusion_common::Result<Self> {
        let mut fields = config.output_schema.fields().to_vec();
        let tag_name = config.output_partition_tag.clone();
        fields.push(Arc::new(Field::new(tag_name, DataType::UInt64, false)));
        let schema = Arc::new(Schema::new(fields));
        let mut equivalences = EquivalenceProperties::new(schema.clone());
        let ordering = config
            .output_key_fields
            .iter()
            .map(|name| {
                let index = schema.index_of(name)?;
                Ok(PhysicalSortExpr::new(
                    Arc::new(Column::new(name, index)),
                    SortOptions {
                        descending: false,
                        nulls_first: false,
                    },
                ))
            })
            .collect::<datafusion_common::Result<Vec<_>>>()?;
        if let Some(ordering) = LexOrdering::new(ordering) {
            equivalences.add_ordering(ordering);
        }
        let properties = Arc::new(PlanProperties::new(
            equivalences,
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            rows,
            globals,
            config,
            schema,
            properties,
        })
    }
}

impl DisplayAs for MatrixEntriesExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MatrixEntriesByKeyRunsExec")
    }
}

impl ExecutionPlan for MatrixEntriesExec {
    fn name(&self) -> &'static str {
        "MatrixEntriesByKeyRunsExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&PhysicalPlan> {
        vec![&self.rows, &self.globals]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true, true]
    }

    fn required_input_distribution(&self) -> Vec<datafusion_physical_plan::Distribution> {
        vec![
            datafusion_physical_plan::Distribution::SinglePartition,
            datafusion_physical_plan::Distribution::SinglePartition,
        ]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<PhysicalPlan>,
    ) -> datafusion_common::Result<PhysicalPlan> {
        if children.len() != 2 {
            return Err(DataFusionError::Plan(
                "MatrixEntriesByKeyRunsExec expects row and global inputs".into(),
            ));
        }
        Ok(Arc::new(Self::new(
            children[0].clone(),
            children[1].clone(),
            self.config.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "MatrixEntriesByKeyRunsExec requires partition zero".into(),
            ));
        }
        let rows_plan = self.rows.clone();
        let globals_plan = self.globals.clone();
        let config = self.config.clone();
        let schema = self.schema.clone();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            async_stream::try_stream! {
                let columns = read_columns(globals_plan, &config, context.clone()).await?;
                let mut input = rows_plan.execute(0, context)?;
                let mut run = Vec::<RecordBatch>::new();
                let mut singleton_rows = Vec::<RecordBatch>::new();
                let singleton_limit = (8192 / columns.len().max(1)).max(1);
                let mut previous_key: Option<Vec<ArrayRef>> = None;
                let mut output_batches = Vec::<RecordBatch>::new();
                let mut output_rows = 0usize;
                while let Some(batch) = input.try_next().await? {
                    if batch.num_rows() == 0 { continue; }
                    let key_columns = config.key_indices.iter().map(|&i| batch.column(i).clone()).collect::<Vec<_>>();
                    let options = vec![SortOptions { descending: false, nulls_first: false }; key_columns.len()];
                    let within = if key_columns.is_empty() { vec![] } else {
                        make_typed_key_comparators(&key_columns, &key_columns, &options)
                            .map_err(|e| DataFusionError::Execution(e.to_string()))?
                    };
                    let mut start = 0usize;
                    for row in 0..batch.num_rows() {
                        let comparison = if row > 0 && !within.is_empty() {
                            compare(&within, row - 1, row)
                        } else if row == 0 && !key_columns.is_empty() {
                            if let Some(previous) = &previous_key {
                                let boundary = make_typed_key_comparators(previous, &key_columns, &options)
                                    .map_err(|e| DataFusionError::Execution(e.to_string()))?;
                                compare(&boundary, 0, 0)
                            } else { None }
                        } else { None };
                        if comparison == Some(std::cmp::Ordering::Greater) {
                            Err(DataFusionError::Execution("matrix rows are not ordered by row key".into()))?;
                        }
                        if comparison == Some(std::cmp::Ordering::Less) {
                            if row > start { run.push(batch.slice(start, row - start)); }
                            for output in finish_run(
                                &mut run, &mut singleton_rows, singleton_limit,
                                &columns, &config, &schema,
                            )? {
                                output_rows += output.num_rows();
                                output_batches.push(output);
                                if output_rows >= 8192 {
                                    yield combine_batches(&schema, &mut output_batches)?;
                                    output_rows = 0;
                                }
                            }
                            start = row;
                        }
                    }
                    if start < batch.num_rows() { run.push(batch.slice(start, batch.num_rows() - start)); }
                    if !key_columns.is_empty() {
                        previous_key = Some(key_columns.iter().map(|column| column.slice(batch.num_rows() - 1, 1)).collect());
                    }
                }
                for output in finish_run(
                    &mut run, &mut singleton_rows, singleton_limit,
                    &columns, &config, &schema,
                )? {
                    output_rows += output.num_rows();
                    output_batches.push(output);
                    if output_rows >= 8192 {
                        yield combine_batches(&schema, &mut output_batches)?;
                        output_rows = 0;
                    }
                }
                if !singleton_rows.is_empty() {
                    if let Some(output) = emit_singletons(&mut singleton_rows, &columns, &config, &schema)? {
                        output_batches.push(output);
                    }
                }
                if !output_batches.is_empty() {
                    yield combine_batches(&schema, &mut output_batches)?;
                }
            },
        )))
    }

    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> datafusion_common::Result<TreeNodeRecursion>,
    ) -> datafusion_common::Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
}

fn finish_run(
    run: &mut Vec<RecordBatch>,
    singletons: &mut Vec<RecordBatch>,
    singleton_limit: usize,
    columns: &StructArray,
    config: &Config,
    schema: &SchemaRef,
) -> datafusion_common::Result<Vec<RecordBatch>> {
    if run.is_empty() {
        return Ok(Vec::new());
    }
    let row_count = run.iter().map(RecordBatch::num_rows).sum::<usize>();
    if row_count == 1 {
        singletons.push(run.pop().expect("one-row run has a batch"));
        run.clear();
        if singletons.len() < singleton_limit {
            return Ok(Vec::new());
        }
        return Ok(emit_singletons(singletons, columns, config, schema)?
            .into_iter()
            .collect());
    }
    let mut output = Vec::with_capacity(2);
    if let Some(batch) = emit_singletons(singletons, columns, config, schema)? {
        output.push(batch);
    }
    if let Some(batch) = emit_run(run, columns, config, schema)? {
        output.push(batch);
    }
    run.clear();
    Ok(output)
}

fn combine_batches(
    schema: &SchemaRef,
    batches: &mut Vec<RecordBatch>,
) -> datafusion_common::Result<RecordBatch> {
    if batches.len() == 1 {
        return Ok(batches
            .pop()
            .ok_or_else(|| DataFusionError::Execution("missing matrix entry batch".into()))?);
    }
    let combined = arrow_select::concat::concat_batches(schema, batches.iter())?;
    batches.clear();
    Ok(combined)
}

fn emit_run(
    run: &[RecordBatch],
    columns: &StructArray,
    config: &Config,
    schema: &SchemaRef,
) -> datafusion_common::Result<Option<RecordBatch>> {
    let mut batches = Vec::new();
    for col in 0..columns.len() {
        for chunk in run {
            if let Some(batch) = emit_run_chunk(chunk, col, columns, config, schema)? {
                batches.push(batch);
            }
        }
    }
    if batches.is_empty() {
        Ok(None)
    } else {
        combine_batches(schema, &mut batches).map(Some)
    }
}

fn emit_singletons(
    singletons: &mut Vec<RecordBatch>,
    columns: &StructArray,
    config: &Config,
    schema: &SchemaRef,
) -> datafusion_common::Result<Option<RecordBatch>> {
    if singletons.is_empty() || columns.is_empty() {
        singletons.clear();
        return Ok(None);
    }
    let input_schema = singletons[0].schema();
    let batch = if singletons.len() == 1 {
        singletons.pop().expect("single row batch exists")
    } else {
        let combined = arrow_select::concat::concat_batches(&input_schema, singletons.iter())?;
        singletons.clear();
        combined
    };
    let row = downcast_struct(batch.column(config.row_struct).clone(), "matrix row")?;
    let cells = batch.column(config.cells).clone();
    let row_offsets = offsets(&cells)?;
    let cell_values = downcast_struct(list_child_values(&cells)?, "matrix cells")?;
    let value_index = cell_values
        .fields()
        .iter()
        .position(|field| field.name() == &config.value_field)
        .ok_or_else(|| DataFusionError::Execution("matrix entry value field is absent".into()))?;
    let entry_values = downcast_struct(
        cell_values.column(value_index).clone(),
        "matrix entry values",
    )?;
    let capacity = batch.num_rows().saturating_mul(columns.len());
    let mut row_indices = Vec::<u64>::with_capacity(capacity);
    let mut column_indices = Vec::<u64>::with_capacity(capacity);
    let mut entry_indices = Vec::<u64>::with_capacity(capacity);
    for row_index in 0..batch.num_rows() {
        if cells.is_null(row_index) {
            continue;
        }
        let start = row_offsets[row_index];
        let end = row_offsets[row_index + 1];
        if end - start != columns.len() as i64 {
            return Err(DataFusionError::Execution(format!(
                "matrix row has {} cells for {} columns",
                end - start,
                columns.len()
            )));
        }
        for col in 0..columns.len() {
            let slot = usize::try_from(start + col as i64)
                .map_err(|_| DataFusionError::Execution("negative matrix cell offset".into()))?;
            if cell_values.is_null(slot) || entry_values.is_null(slot) {
                continue;
            }
            row_indices.push(row_index as u64);
            column_indices.push(col as u64);
            entry_indices.push(slot as u64);
        }
    }
    if row_indices.is_empty() {
        return Ok(None);
    }
    let output_len = row_indices.len();
    let row_take = UInt64Array::from(row_indices);
    let column_take = UInt64Array::from(column_indices);
    let entry_take = UInt64Array::from(entry_indices);
    let column_index = columns
        .fields()
        .iter()
        .position(|field| field.name() == &config.column_struct)
        .ok_or_else(|| DataFusionError::Execution("matrix column struct field is absent".into()))?;
    let col_struct = downcast_struct(columns.column(column_index).clone(), "matrix columns")?;
    build_output_batch(
        &row,
        &col_struct,
        &entry_values,
        config,
        schema,
        output_len,
        &row_take,
        &column_take,
        &entry_take,
    )
    .map(Some)
}

fn build_output_batch(
    row: &StructArray,
    col_struct: &StructArray,
    entry_values: &StructArray,
    config: &Config,
    output_schema: &SchemaRef,
    output_len: usize,
    row_take: &UInt64Array,
    column_take: &UInt64Array,
    entry_take: &UInt64Array,
) -> datafusion_common::Result<RecordBatch> {
    let mut arrays = Vec::<ArrayRef>::with_capacity(output_schema.fields().len());
    for name in &config.row_field_names {
        let index = row
            .fields()
            .iter()
            .position(|field| field.name() == name)
            .ok_or_else(|| {
                DataFusionError::Execution(format!("matrix row field {name:?} is absent"))
            })?;
        arrays.push(arrow_select::take::take(row.column(index), row_take, None)?);
    }
    for name in &config.col_field_names {
        let index = col_struct
            .fields()
            .iter()
            .position(|field| field.name() == name)
            .ok_or_else(|| {
                DataFusionError::Execution(format!("matrix column field {name:?} is absent"))
            })?;
        arrays.push(arrow_select::take::take(
            col_struct.column(index),
            column_take,
            None,
        )?);
    }
    for (index, _) in entry_values.fields().iter().enumerate() {
        arrays.push(arrow_select::take::take(
            entry_values.column(index),
            entry_take,
            None,
        )?);
    }
    arrays.push(Arc::new(UInt64Array::from_value(0, output_len)));
    Ok(RecordBatch::try_new(output_schema.clone(), arrays)?)
}

fn compare(
    comparators: &[arrow_ord::ord::DynComparator],
    left: usize,
    right: usize,
) -> Option<std::cmp::Ordering> {
    for comparator in comparators {
        let ordering = comparator(left, right);
        if ordering != std::cmp::Ordering::Equal {
            return Some(ordering);
        }
    }
    Some(std::cmp::Ordering::Equal)
}

async fn read_columns(
    plan: PhysicalPlan,
    config: &Config,
    context: Arc<TaskContext>,
) -> datafusion_common::Result<Arc<StructArray>> {
    let mut stream = plan.execute(0, context)?;
    let batch = stream.try_next().await?.ok_or_else(|| {
        DataFusionError::Execution("matrix column metadata produced no row".into())
    })?;
    if batch.num_rows() != 1 {
        return Err(DataFusionError::Execution(
            "matrix column globals must have exactly one row".into(),
        ));
    }
    let index = config.global_schema.index_of(&config.columns_global)?;
    let list = batch.column(index);
    let values = list_values(list)?;
    downcast_struct(values, "matrix column records")
}

fn list_values(array: &ArrayRef) -> datafusion_common::Result<ArrayRef> {
    if let Some(list) = array.as_any().downcast_ref::<ListArray>() {
        if list.is_null(0) {
            return Ok(arrow_array::new_empty_array(&list.value_type()));
        }
        return Ok(list.value(0));
    }
    if let Some(list) = array.as_any().downcast_ref::<LargeListArray>() {
        if list.is_null(0) {
            return Ok(arrow_array::new_empty_array(&list.value_type()));
        }
        return Ok(list.value(0));
    }
    Err(DataFusionError::Execution(
        "matrix array field must be a list".into(),
    ))
}

fn list_child_values(array: &ArrayRef) -> datafusion_common::Result<ArrayRef> {
    if let Some(list) = array.as_any().downcast_ref::<ListArray>() {
        return Ok(list.values().clone());
    }
    if let Some(list) = array.as_any().downcast_ref::<LargeListArray>() {
        return Ok(list.values().clone());
    }
    Err(DataFusionError::Execution(
        "matrix cell field must be a list".into(),
    ))
}

fn downcast_struct(array: ArrayRef, what: &str) -> datafusion_common::Result<Arc<StructArray>> {
    array
        .as_any()
        .downcast_ref::<StructArray>()
        .map(|value| Arc::new(value.clone()))
        .ok_or_else(|| DataFusionError::Execution(format!("{what} must be a struct array")))
}

fn offsets(array: &ArrayRef) -> datafusion_common::Result<Vec<i64>> {
    if let Some(list) = array.as_any().downcast_ref::<ListArray>() {
        return Ok(list
            .value_offsets()
            .iter()
            .map(|&value| i64::from(value))
            .collect());
    }
    if let Some(list) = array.as_any().downcast_ref::<LargeListArray>() {
        return Ok(list.value_offsets().to_vec());
    }
    Err(DataFusionError::Execution(
        "matrix cell field must be a list".into(),
    ))
}

fn emit_run_chunk(
    batch: &RecordBatch,
    col: usize,
    columns: &StructArray,
    config: &Config,
    output_schema: &SchemaRef,
) -> datafusion_common::Result<Option<RecordBatch>> {
    if columns.is_empty() {
        return Ok(None);
    }
    let column_struct_index = columns
        .fields()
        .iter()
        .position(|field| field.name() == &config.column_struct)
        .ok_or_else(|| DataFusionError::Execution("matrix column struct field is absent".into()))?;
    let col_struct = downcast_struct(
        columns.column(column_struct_index).clone(),
        "matrix columns",
    )?;
    let value_field = config.value_field.as_str();
    let row = downcast_struct(batch.column(config.row_struct).clone(), "matrix row")?;
    let cells = batch.column(config.cells).clone();
    let row_offsets = offsets(&cells)?;
    let cell_values = downcast_struct(list_child_values(&cells)?, "matrix cells")?;
    let value_index = cell_values
        .fields()
        .iter()
        .position(|field| field.name() == value_field)
        .ok_or_else(|| DataFusionError::Execution("matrix entry value field is absent".into()))?;
    let entry_values = downcast_struct(
        cell_values.column(value_index).clone(),
        "matrix entry values",
    )?;
    let mut row_indices = Vec::<u64>::new();
    let mut entry_indices = Vec::<u64>::new();
    let mut single_index = None;
    for row_index in 0..batch.num_rows() {
        if cells.is_null(row_index) {
            continue;
        }
        let start = row_offsets[row_index];
        let end = row_offsets[row_index + 1];
        if end - start != columns.len() as i64 {
            return Err(DataFusionError::Execution(format!(
                "matrix row has {} cells for {} columns",
                end - start,
                columns.len()
            )));
        }
        let slot = start + col as i64;
        if slot >= end {
            return Err(DataFusionError::Execution(
                "matrix cell and column counts differ".into(),
            ));
        }
        let slot = usize::try_from(slot)
            .map_err(|_| DataFusionError::Execution("negative matrix cell offset".into()))?;
        if cell_values.is_null(slot) || entry_values.is_null(slot) {
            continue;
        }
        if batch.num_rows() == 1 {
            single_index = Some((row_index as u64, slot as u64));
        } else {
            row_indices.push(row_index as u64);
            entry_indices.push(slot as u64);
        }
    }
    if single_index.is_none() && row_indices.is_empty() {
        return Ok(None);
    }
    let output_len = if single_index.is_some() {
        1
    } else {
        row_indices.len()
    };
    let (row_take, entry_take) = if let Some((row, entry)) = single_index {
        (
            UInt64Array::from_value(row, 1),
            UInt64Array::from_value(entry, 1),
        )
    } else {
        (
            UInt64Array::from(row_indices),
            UInt64Array::from(entry_indices),
        )
    };
    let column_take = UInt64Array::from_value(col as u64, output_len);
    let mut arrays = Vec::<ArrayRef>::with_capacity(output_schema.fields().len());
    for name in &config.row_field_names {
        let index = row
            .fields()
            .iter()
            .position(|field| field.name() == name)
            .ok_or_else(|| {
                DataFusionError::Execution(format!("matrix row field {name:?} is absent"))
            })?;
        arrays.push(arrow_select::take::take(
            row.column(index),
            &row_take,
            None,
        )?);
    }
    for name in &config.col_field_names {
        let index = col_struct
            .fields()
            .iter()
            .position(|field| field.name() == name)
            .ok_or_else(|| {
                DataFusionError::Execution(format!("matrix column field {name:?} is absent"))
            })?;
        arrays.push(arrow_select::take::take(
            col_struct.column(index),
            &column_take,
            None,
        )?);
    }
    for (index, _) in entry_values.fields().iter().enumerate() {
        arrays.push(arrow_select::take::take(
            entry_values.column(index),
            &entry_take,
            None,
        )?);
    }
    arrays.push(Arc::new(UInt64Array::from_value(0, output_len)));
    Ok(Some(RecordBatch::try_new(output_schema.clone(), arrays)?))
}
