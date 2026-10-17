use arrow_array::{Array, Int32Array, ListArray};
use serde_json::json;
use vail_execution::expressions::{BinaryOperator, PrimitiveType, Type};
use vail_table::{ScalarIR, TableIR};

#[tokio::test]
async fn row_array_lambda_can_capture_a_global_value() {
    let table = TableIR::range(3, 2)
        .unwrap()
        .map_globals(vec![(
            "offset".into(),
            ScalarIR::Literal {
                dtype: Type::Primitive(PrimitiveType::Int32),
                value: json!(10),
            },
        )])
        .unwrap();
    let expression = ScalarIR::ArrayMap {
        value: Box::new(ScalarIR::Literal {
            dtype: Type::Array {
                array: Box::new(Type::Primitive(PrimitiveType::Int32)),
            },
            value: json!([1, 2]),
        }),
        parameter: "element".into(),
        body: Box::new(ScalarIR::Binary {
            operator: BinaryOperator::Add,
            left: Box::new(ScalarIR::LambdaVariable {
                name: "element".into(),
            }),
            right: Box::new(ScalarIR::Global {
                name: "offset".into(),
            }),
        }),
    };
    let mapped = table
        .map_rows(vec![
            ("idx".into(), ScalarIR::Column { name: "idx".into() }),
            ("values".into(), expression),
        ])
        .unwrap();

    let partitions = mapped.collect_partitions().await.unwrap();
    let mut rows = Vec::new();
    for batch in partitions.into_iter().flatten() {
        let values = batch
            .column_by_name("values")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        for row in 0..values.len() {
            let elements = values.value(row);
            let elements = elements.as_any().downcast_ref::<Int32Array>().unwrap();
            rows.push(elements.values().to_vec());
        }
    }
    assert_eq!(rows, vec![vec![11, 12]; 3]);
}
