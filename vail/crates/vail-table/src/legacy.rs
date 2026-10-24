//! Native legacy dataset planning. Large row/entry components stay as file
//! descriptors; only the singleton globals and column axis are materialized.
use crate::{Result, TableError, TableIR, TableNode, TableRef, globals::GlobalsIR};
use arrow_array::{Array, ArrayRef, Int64Array, ListArray, RecordBatch, StructArray};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;
use vail_execution::expressions::Type;
use vail_storage::legacy::{Component, Dataset};
use vortex::{VortexSessionDefault, session::VortexSession};
#[derive(Debug)]
pub struct Read {
    // Plugin registries and execution machinery are shared across file scans;
    // cursors, buffers and layout caches remain local to each execution.
    pub(crate) sessions: Arc<SessionCache>,
    pub component: Arc<Component>,
    pub counts: Arc<Option<Vec<u64>>>,
    pub matrix: Option<Arc<Matrix>>,
}
/// Retain one prepared registry set, bound to its execution runtime. Replacing
/// the cache entry never changes sessions held by active scans on another runtime.
#[derive(Debug, Default)]
pub(crate) struct SessionCache(std::sync::Mutex<Option<(tokio::runtime::Id, VortexSession)>>);
impl SessionCache {
    pub(crate) fn current(&self) -> VortexSession {
        let runtime = tokio::runtime::Handle::current().id();
        let mut cached = self.0.lock().unwrap();
        if let Some((id, session)) = cached.as_ref() {
            if *id == runtime {
                return session.clone();
            }
        }
        let session = VortexSession::default();
        *cached = Some((runtime, session.clone()));
        session
    }
}

#[derive(Debug)]
pub struct Matrix {
    pub entries: Arc<Component>,
    pub ncols: usize,
    pub schema: Arc<Schema>,
}
const ROW: &str = "__vail_matrix_row";
const CELLS: &str = "__vail_matrix_cells";
const CID: &str = "__vail_matrix_col_id";
const COLUMNS: &str = "__vail_matrix_columns";
const COL: &str = "__vail_matrix_col";
fn error(e: impl std::fmt::Display) -> TableError {
    TableError::Invalid(e.to_string())
}
fn expect(component: &Component, dtype: &Type, name: &str) -> Result<()> {
    if &component.dtype != dtype {
        return Err(error(format!(
            "Hail {name} schema differs from dataset metadata"
        )));
    }
    Ok(())
}
pub fn read_table(path: &str) -> Result<TableRef> {
    let dataset = Dataset::open(path, "TableSpec").map_err(error)?;
    let relation = dataset.relation().map_err(error)?;
    let rows = dataset.component("rows").map_err(error)?;
    expect(&rows, &relation.types["row"].0, "row")?;
    let globals = dataset.component("globals").map_err(error)?;
    expect(&globals, &relation.types["global"].0, "global")?;
    let globals = GlobalsIR::arrow_literal(globals.collect_small().map_err(error)?)?;
    source(&dataset, rows, relation.keys["key"].clone(), globals, None)
}
fn source(
    dataset: &Dataset,
    rows: Component,
    keys: Vec<String>,
    globals: crate::globals::GlobalsRef,
    matrix: Option<Arc<Matrix>>,
) -> Result<TableRef> {
    let counts = dataset.counts().map_err(error)?;
    if let Some(c) = &counts
        && c.len() != rows.paths.len()
    {
        return Err(error("Hail partition counts differ from part file count"));
    }
    let paths = rows
        .paths
        .iter()
        .map(|p| p.to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    let schema = matrix
        .as_ref()
        .map_or_else(|| rows.schema.clone(), |m| m.schema.clone());
    let descriptor = Arc::new(Read {
        sessions: Arc::new(SessionCache::default()),
        component: Arc::new(rows),
        counts: Arc::new(counts),
        matrix,
    });
    // TextRead is the shared FileSource leaf; it assigns an independent physical
    // source partition to every path and preserves the private partition tag.
    Ok(TableIR::new(
        schema,
        keys,
        globals,
        TableNode::TextRead {
            paths,
            format: "legacy".into(),
            options_json: serde_json::json!({}),
            legacy: Some(descriptor),
        },
    ))
}
pub fn read_matrix(path: &str) -> Result<(TableRef, Vec<String>, Vec<String>, bool)> {
    let dataset = Dataset::open(path, "MatrixTableSpec").map_err(error)?;
    let relation = dataset.relation().map_err(error)?;
    let rows = dataset.component("rows").map_err(error)?;
    // Modern Hail matrices persist their row axis as a nested TableSpec.
    // Its distinctlyKeyed property is a writer-verified global guarantee,
    // including partition boundaries. Older exports omit it and stay conservative.
    let row_key_unique = matrix_rows_distinct(&dataset, &relation)?;
    let cols = dataset.component("cols").map_err(error)?;
    let entries = dataset.component("entries").map_err(error)?;
    let global = dataset.component("globals").map_err(error)?;
    expect(&rows, &relation.types["row"].0, "axis")?;
    expect(&cols, &relation.types["col"].0, "axis")?;
    expect(&global, &relation.types["global"].0, "global")?;
    if entries.schema.fields().len() != 1 {
        return Err(error("Hail entries component must have one array field"));
    }
    let Type::Struct {
        r#struct: entry_fields,
    } = &entries.dtype
    else {
        return Err(error("invalid Hail entry type"));
    };
    if entry_fields[0].1
        != (Type::Array {
            array: Box::new(relation.types["entry"].0.clone()),
        })
    {
        return Err(error("Hail entry schema differs from matrix metadata"));
    }
    if rows.paths.len() != entries.paths.len() {
        return Err(error("Hail row and entry partition counts differ"));
    }
    let cols = cols.collect_small().map_err(error)?;
    let mut globals = global.collect_small().map_err(error)?;
    if globals.num_rows() != 1 {
        return Err(error("Hail globals must contain exactly one record"));
    }
    if globals.schema().index_of(COLUMNS).is_ok() {
        return Err(error(
            "Hail global field __vail_matrix_columns is reserved by Vail matrices",
        ));
    }
    let ncols = cols.num_rows();
    let col_values = Arc::new(StructArray::from(cols)) as ArrayRef;
    let col_item = Arc::new(StructArray::from(vec![
        (
            Arc::new(Field::new(CID, DataType::Int64, true)),
            Arc::new(Int64Array::from_iter_values(0..ncols as i64)) as ArrayRef,
        ),
        (
            Arc::new(Field::new(COL, col_values.data_type().clone(), true)),
            col_values,
        ),
    ])) as ArrayRef;
    let column_array = Arc::new(ListArray::new(
        Arc::new(Field::new("item", col_item.data_type().clone(), true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![
            0,
            i32::try_from(ncols).map_err(error)?,
        ])),
        col_item,
        None,
    )) as ArrayRef;
    let mut fields = globals.schema().fields().to_vec();
    fields.push(Arc::new(Field::new(
        COLUMNS,
        column_array.data_type().clone(),
        true,
    )));
    let mut arrays = globals.columns().to_vec();
    arrays.push(column_array);
    globals = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?;
    let cell_type = DataType::Struct(
        vec![Field::new(
            "value",
            relation.types["entry"].0.to_arrow()?,
            true,
        )]
        .into(),
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new(ROW, rows.dtype.to_arrow()?, true),
        Field::new(
            CELLS,
            DataType::List(Arc::new(Field::new("item", cell_type, true))),
            true,
        ),
    ]));
    let matrix = Arc::new(Matrix {
        entries: Arc::new(entries),
        ncols,
        schema,
    });
    let table = source(
        &dataset,
        rows,
        vec![],
        GlobalsIR::arrow_literal(globals)?,
        Some(matrix),
    )?;
    Ok((
        table,
        relation.keys["row_key"].clone(),
        relation.keys["col_key"].clone(),
        row_key_unique,
    ))
}
fn matrix_rows_distinct(
    dataset: &Dataset,
    relation: &vail_storage::legacy::Relation,
) -> Result<bool> {
    let Some(relative) = dataset.metadata["components"]["rows"]["rel_path"].as_str() else {
        return Ok(false);
    };
    let row_component = dataset.root.join(relative);
    let Some(parent) = row_component.parent() else {
        return Ok(false);
    };
    if !parent.join("metadata.json.gz").exists() {
        return Ok(false);
    }
    let metadata = vail_storage::legacy::metadata(parent).map_err(error)?;
    if metadata["name"] != "TableSpec"
        || metadata["components"]["properties"]["name"] != "PropertiesSpec"
        || metadata["components"]["properties"]["properties"]["distinctlyKeyed"].as_bool()
            != Some(true)
    {
        return Ok(false);
    }
    let Some(table_type) = metadata["table_type"].as_str() else {
        return Ok(false);
    };
    let mut parser = vail_storage::legacy::Parser::new(table_type, &dataset.references);
    let row_relation = parser.relation().map_err(error)?;
    parser.finish().map_err(error)?;
    Ok(row_relation.keys["key"] == relation.keys["row_key"]
        && row_relation.types["row"].0 == relation.types["row"].0)
}

/// Assemble one bounded pair of aligned row and entry slices. Missing entry
/// structs remain missing; shared column metadata determines slot positions.
pub(crate) fn matrix_batch(
    rows: RecordBatch,
    entries: Option<RecordBatch>,
    schema: Arc<Schema>,
    matrix: &Matrix,
) -> vail_storage::legacy::Result<RecordBatch> {
    let n = rows.num_rows();
    let ncols = matrix.ncols;
    let row = Arc::new(StructArray::from(rows)) as ArrayRef;
    let cells = if let Some(entries) = entries {
        let entries = entries
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or("invalid Hail entry array")?;
        let entry_nulls = entries.logical_nulls();
        for r in 0..n {
            if entry_nulls.as_ref().is_some_and(|nulls| !nulls.is_valid(r))
                || entries.value_length(r) as usize != ncols
            {
                return Err("Hail entry array length differs from column count".into());
            }
        }
        let start = entries.offsets()[0] as usize;
        let end = *entries.offsets().last().unwrap() as usize;
        let values = entries.values().slice(start, end - start);
        return matrix_batch_values(row, values, entries, schema, n);
    } else {
        arrow_array::new_null_array(schema.field(1).data_type(), n)
    };
    Ok(RecordBatch::try_new(schema, vec![row, cells])?)
}

fn matrix_batch_values(
    row: ArrayRef,
    values: ArrayRef,
    entries: &ListArray,
    schema: Arc<Schema>,
    n: usize,
) -> vail_storage::legacy::Result<RecordBatch> {
    let value = Arc::new(StructArray::from(vec![(
        Arc::new(Field::new("value", values.data_type().clone(), true)),
        values,
    )])) as ArrayRef;
    let cells = Arc::new(ListArray::new(
        Arc::new(Field::new("item", value.data_type().clone(), true)),
        if entries.offsets()[0] == 0 {
            entries.offsets().clone()
        } else {
            let start = entries.offsets()[0];
            OffsetBuffer::new(ScalarBuffer::from(
                entries
                    .offsets()
                    .iter()
                    .map(|offset| offset - start)
                    .collect::<Vec<_>>(),
            ))
        },
        value,
        None,
    )) as ArrayRef;
    debug_assert_eq!(n, cells.len());
    Ok(RecordBatch::try_new(schema, vec![row, cells])?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int32Array;
    use arrow_buffer::NullBuffer;

    #[test]
    fn positional_slots_preserve_missing_and_all_null_entries() {
        let values = Arc::new(StructArray::new(
            vec![Field::new("x", DataType::Int32, true)].into(),
            vec![Arc::new(Int32Array::from(vec![None, None]))],
            Some(NullBuffer::from(vec![true, false])),
        )) as ArrayRef;
        let entries = ListArray::new(
            Arc::new(Field::new("item", values.data_type().clone(), true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 2])),
            values.clone(),
            None,
        );
        let row = Arc::new(StructArray::new_empty_fields(1, None)) as ArrayRef;
        let slot_type =
            DataType::Struct(vec![Field::new("value", values.data_type().clone(), true)].into());
        let schema = Arc::new(Schema::new(vec![
            Field::new(ROW, row.data_type().clone(), true),
            Field::new(
                CELLS,
                DataType::List(Arc::new(Field::new("item", slot_type, true))),
                true,
            ),
        ]));
        let batch = matrix_batch_values(row, values, &entries, schema, 1).unwrap();
        let cells = batch
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let slots = cells
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(slots.num_columns(), 1);
        assert_eq!(slots.fields()[0].name(), "value");
        let values = slots
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert!(
            values
                .logical_nulls()
                .as_ref()
                .is_none_or(|nulls| nulls.is_valid(0))
        );
        assert!(values.column(0).logical_nulls().unwrap().is_null(0));
        assert!(values.logical_nulls().unwrap().is_null(1));
    }
}
