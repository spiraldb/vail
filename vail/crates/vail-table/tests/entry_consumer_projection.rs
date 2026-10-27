//! Public entry projections preserve logical nulls and independent consumers.
use arrow_array::{Array, ArrayRef, Int32Array, ListArray, RecordBatch, StructArray};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Fields, Schema};
use std::sync::Arc;
use vail_execution::expressions::{Expr, Type};
use vail_table::{TableIR, TableRef, globals::{GlobalBindings, GlobalsIR}};

fn structure(names: &[&str], arrays: Vec<ArrayRef>, nulls: Option<NullBuffer>) -> StructArray {
    let fields: Fields = names.iter().zip(&arrays).map(|(name, array)|
        Arc::new(Field::new(*name, array.data_type().clone(), true))).collect::<Vec<_>>().into();
    StructArray::try_new(fields, arrays, nulls).unwrap()
}

fn list(values: ArrayRef, offsets: Vec<i32>, nulls: Option<NullBuffer>) -> ListArray {
    ListArray::try_new(Arc::new(Field::new("item", values.data_type().clone(), true)),
        OffsetBuffer::new(ScalarBuffer::from(offsets)), values, nulls).unwrap()
}

fn fixture(hidden: usize) -> TableRef {
    let unused = Arc::new(list(Arc::new(Int32Array::from(vec![17; hidden])),
        vec![0, hidden as i32, hidden as i32, hidden as i32, hidden as i32, hidden as i32, hidden as i32],
        Some(NullBuffer::from(vec![false, true, true, true, true, true])))) as ArrayRef;
    let entries = Arc::new(structure(&["value", "unused"], vec![
        Arc::new(Int32Array::from(vec![10, 11, 20, 21, 30, 31])), unused,
    ], Some(NullBuffer::from(vec![true, false, true, true, true, true])))) as ArrayRef;
    let cells = Arc::new(structure(&["entry"], vec![entries], None)) as ArrayRef;
    let cells = Arc::new(list(cells, vec![0, 2, 4, 6],
        Some(NullBuffer::from(vec![true, false, true])))) as ArrayRef;
    let rows = Arc::new(structure(&["r"], vec![Arc::new(Int32Array::from(vec![0, 0, 1]))], None)) as ArrayRef;
    let key = Arc::new(Int32Array::from(vec![0, 0, 1])) as ArrayRef;
    let schema = Arc::new(Schema::new(vec![
        Field::new("row", rows.data_type().clone(), true),
        Field::new("cells", cells.data_type().clone(), true),
        Field::new("key", DataType::Int32, false),
    ]));
    let table = TableIR::parallelize(RecordBatch::try_new(schema, vec![rows, cells, key]).unwrap(), 1).unwrap();
    let cols = Arc::new(structure(&["c"], vec![Arc::new(Int32Array::from(vec![0, 1]))], None)) as ArrayRef;
    let cols = Arc::new(structure(&["column"], vec![cols], None)) as ArrayRef;
    let cols = Arc::new(list(cols, vec![0, 2], None)) as ArrayRef;
    let globals_schema = Arc::new(Schema::new(vec![Field::new("columns", cols.data_type().clone(), true)]));
    let dtype = Type::from_arrow(&DataType::Struct(globals_schema.fields().clone())).unwrap();
    let globals = GlobalsIR::arrow_literal(RecordBatch::try_new(globals_schema, vec![cols]).unwrap()).unwrap();
    table.map_globals_bound(vec![("columns".into(), Expr::GetField {
        value: Arc::new(Expr::ExternalGlobal { reference: "g".into(), dtype }), name: "columns".into(),
    })], GlobalBindings::from([("g".into(), globals)])).unwrap()
        .matrix_entries_by_key_runs("row".into(), "cells".into(), "columns".into(),
            vec!["key".into()], vec!["r".into()], "column".into(), vec!["c".into()], "entry".into()).unwrap()
}

async fn values(table: &TableRef) -> Vec<(i32, i32, i32)> {
    let mut result = Vec::new();
    for partition in table.collect_partitions().await.unwrap() {
        for batch in partition {
            let r = batch.column_by_name("r").unwrap().as_any().downcast_ref::<Int32Array>().unwrap();
            let c = batch.column_by_name("c").unwrap().as_any().downcast_ref::<Int32Array>().unwrap();
            let value = batch.column_by_name("value").unwrap().as_any().downcast_ref::<Int32Array>().unwrap();
            result.extend((0..batch.num_rows()).map(|i| (r.value(i), c.value(i), value.value(i))));
        }
    }
    result
}

#[tokio::test]
async fn selected_values_ignore_missing_ancestors_and_preserve_shared_wide_consumer() {
    let source = fixture(1024);
    let projected = source.map_rows(["r", "c", "value"].into_iter()
        .map(|name| (name.into(), Expr::Column { name: name.into() })).collect()).unwrap();
    let expected = vec![(0, 0, 10), (1, 0, 30), (1, 1, 31)];
    assert_eq!(values(&projected).await, expected);
    assert_eq!(values(&source).await, expected);
    assert_eq!(values(&projected).await, expected);
}
