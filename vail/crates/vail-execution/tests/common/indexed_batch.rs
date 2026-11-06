use arrow_array::{
    Array, ArrayRef, Int32Array, ListArray, RecordBatch, StringArray, StructArray, types::Int32Type,
};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field};
use serde_json::json;
use std::sync::Arc;
use vail_execution::expressions::{Expr, PrimitiveType, Type};

pub(crate) fn array(inner: Type) -> Type {
    Type::Array {
        array: Box::new(inner),
    }
}
pub(crate) fn int() -> Type {
    Type::Primitive(PrimitiveType::Int32)
}
pub(crate) fn field(value: Expr, name: &str) -> Expr {
    Expr::GetField {
        value: Arc::new(value),
        name: name.into(),
    }
}
pub(crate) fn input(axis: &str, name: Option<&str>, dtype: Type) -> Expr {
    Expr::AlignedMapInput {
        axis: axis.into(),
        name: name.map(str::to_owned),
        dtype,
    }
}
pub(crate) fn fixture(
    index: [i32; 2],
    last_present: bool,
    unused_bytes: usize,
) -> (RecordBatch, Expr) {
    fixture_with_hidden(index, last_present, unused_bytes, 0)
}

pub(crate) fn fixture_with_hidden(
    index: [i32; 2],
    last_present: bool,
    unused_bytes: usize,
    hidden: usize,
) -> (RecordBatch, Expr) {
    let children = ListArray::from_iter_primitive::<Int32Type, _, _>([
        Some(vec![Some(1), Some(2)]),
        Some(vec![Some(3), Some(4)]),
        Some(vec![Some(999); hidden + 1]),
        Some(vec![Some(777)]),
        Some(vec![Some(5)]),
        Some(vec![Some(6)]),
    ]);
    let pl = ListArray::new(
        Arc::new(Field::new("item", children.data_type().clone(), true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 3, 4, 6])),
        Arc::new(children),
        Some(NullBuffer::from(vec![true, false, true, true])),
    );
    let nested = StructArray::from(vec![(
        Arc::new(Field::new("b", DataType::Int32, true)),
        Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef,
    )]);
    let payload = "x".repeat(unused_bytes);
    let values = StructArray::new(
        vec![
            Field::new("PL", pl.data_type().clone(), true),
            Field::new("a/b", DataType::Int32, true),
            Field::new("a", nested.data_type().clone(), true),
            Field::new("unused", DataType::Utf8, true),
        ]
        .into(),
        vec![
            Arc::new(pl),
            Arc::new(Int32Array::from(vec![10, 20, 30, 40])),
            Arc::new(nested),
            Arc::new(StringArray::from(vec![payload.as_str(); 4])),
        ],
        Some(NullBuffer::from(vec![true, true, false, last_present])),
    );
    let slots = StructArray::from(vec![(
        Arc::new(Field::new("value", values.data_type().clone(), true)),
        Arc::new(values) as ArrayRef,
    )]);
    let entry_type = Type::from_arrow(slots.data_type()).unwrap();
    let entries = ListArray::new(
        Arc::new(Field::new("item", slots.data_type().clone(), true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 4])),
        Arc::new(slots),
        None,
    );
    let row_fields = StructArray::from(vec![(
        Arc::new(Field::new("index", DataType::Int32, true)),
        Arc::new(Int32Array::from(index.to_vec())) as ArrayRef,
    )]);
    let rows = StructArray::from(vec![(
        Arc::new(Field::new(
            "__vail_aligned_row",
            row_fields.data_type().clone(),
            true,
        )),
        Arc::new(row_fields) as ArrayRef,
    )]);
    let batch = RecordBatch::try_from_iter([
        ("entries", Arc::new(entries) as ArrayRef),
        ("rows", Arc::new(rows) as ArrayRef),
    ])
    .unwrap();
    let entry = field(input("entry", None, entry_type), "value");
    let selected = Expr::ArrayIndex {
        value: Arc::new(field(entry.clone(), "PL")),
        index: Arc::new(input("row", Some("index"), int())),
    };
    let variable = Expr::LambdaVariable {
        name: "selected".into(),
        dtype: Some(array(int())),
    };
    let selected = Expr::Switch {
        value: Arc::new(selected),
        parameter: "selected".into(),
        cases: vec![],
        missing: None,
        default: Arc::new(Expr::Struct {
            fields: vec![
                ("selected".into(), variable.clone()),
                (
                    "quality".into(),
                    Expr::StringFunction {
                        function: "gq_from_pl".into(),
                        args: vec![variable],
                    },
                ),
                ("witness".into(), field(entry, "a/b")),
            ],
        }),
    };
    let result_type = Type::Struct {
        r#struct: vec![
            ("selected".into(), array(int())),
            ("quality".into(), int()),
            ("witness".into(), int()),
        ],
    };
    let body = Expr::IfElse {
        condition: Arc::new(input(
            "entry_present",
            None,
            Type::Primitive(PrimitiveType::Bool),
        )),
        consequent: Arc::new(selected),
        alternate: Arc::new(Expr::Literal {
            dtype: result_type.clone(),
            value: serde_json::Value::Null,
        }),
    };
    (
        batch,
        Expr::AlignedMatrixMap {
            entries: Arc::new(Expr::Column {
                name: "entries".into(),
            }),
            rows: Arc::new(Expr::Column {
                name: "rows".into(),
            }),
            columns: Arc::new(Expr::Literal {
                dtype: array(int()),
                value: json!([0, 1]),
            }),
            globals: Arc::new(Expr::Struct { fields: vec![] }),
            body: Arc::new(body),
            dtype: array(result_type),
            preserve_cell: false,
        },
    )
}
