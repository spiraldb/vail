use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Int32Array, Int64Array, ListArray, RecordBatch,
    RecordBatchOptions,
};
use arrow_schema::{DataType, Field, Schema};
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr::PhysicalExpr;
use serde_json::json;
use vail_execution::expressions::{Expr, PrimitiveType, Type, lower_with_bindings};

fn int32(value: i32) -> Expr {
    Expr::Literal {
        dtype: Type::Primitive(PrimitiveType::Int32),
        value: json!(value),
    }
}

fn int64(value: i64) -> Expr {
    Expr::Literal {
        dtype: Type::Primitive(PrimitiveType::Int64),
        value: json!(value),
    }
}

fn boolean(value: bool) -> Expr {
    Expr::Literal {
        dtype: Type::Primitive(PrimitiveType::Bool),
        value: json!(value),
    }
}

fn array_i32(value: Expr, count: Expr) -> Expr {
    Expr::NestedFunction {
        function: "array_repeat".into(),
        args: vec![value, count],
        dtype: Type::Array {
            array: Box::new(Type::Primitive(PrimitiveType::Int32)),
        },
    }
}

fn batch(schema: Arc<Schema>, columns: Vec<ArrayRef>, rows: usize) -> RecordBatch {
    RecordBatch::try_new_with_options(
        schema,
        columns,
        &RecordBatchOptions::new().with_row_count(Some(rows)),
    )
    .unwrap()
}

fn evaluate(expr: &dyn PhysicalExpr, batch: &RecordBatch) -> ColumnarValue {
    expr.evaluate(batch).unwrap()
}

fn arrays(value: ColumnarValue, rows: usize) -> Vec<Vec<i32>> {
    let array = value.into_array(rows).unwrap();
    let lists = array.as_any().downcast_ref::<ListArray>().unwrap();
    (0..lists.len())
        .map(|index| {
            let values = lists.value(index);
            values
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect()
}

fn compile(expr: &Expr, schema: &Schema) -> Arc<dyn PhysicalExpr> {
    lower_with_bindings(expr, schema, &Schema::empty(), &BTreeMap::new()).unwrap()
}

#[test]
fn closed_nested_value_is_lazy_and_reused_across_batches() {
    let output = Type::Array {
        array: Box::new(Type::Primitive(PrimitiveType::Int32)),
    };
    let expr = Expr::IfElse {
        condition: Arc::new(boolean(true)),
        consequent: Arc::new(array_i32(int32(7), int64(3))),
        alternate: Arc::new(Expr::Die {
            message: Arc::new(Expr::Literal {
                dtype: Type::Primitive(PrimitiveType::Str),
                value: json!("unselected branch"),
            }),
            dtype: output,
        }),
    };
    let schema = Schema::empty();
    let physical = compile(&expr, &schema);
    let empty = batch(Arc::new(schema.clone()), vec![], 0);
    assert!(arrays(evaluate(physical.as_ref(), &empty), 0).is_empty());
    for rows in [2, 1, 4] {
        let input = batch(Arc::new(schema.clone()), vec![], rows);
        assert_eq!(
            arrays(evaluate(physical.as_ref(), &input), rows),
            vec![vec![7, 7, 7]; rows]
        );
    }
}

#[test]
fn closed_expression_errors_are_not_cached_and_empty_batches_are_lazy() {
    let output = Type::Array {
        array: Box::new(Type::Primitive(PrimitiveType::Int32)),
    };
    let expr = Expr::IfElse {
        condition: Arc::new(boolean(false)),
        consequent: Arc::new(array_i32(int32(1), int64(2))),
        alternate: Arc::new(Expr::Die {
            message: Arc::new(Expr::Literal {
                dtype: Type::Primitive(PrimitiveType::Str),
                value: json!("selected failure"),
            }),
            dtype: output,
        }),
    };
    let schema = Schema::empty();
    let physical = compile(&expr, &schema);
    let empty = batch(Arc::new(schema.clone()), vec![], 0);
    assert!(physical.evaluate(&empty).is_ok());
    let nonempty = batch(Arc::new(schema), vec![], 1);
    assert!(physical.evaluate(&nonempty).is_err());
    assert!(physical.evaluate(&nonempty).is_err());
}

#[test]
fn row_dependent_nested_values_are_evaluated_for_each_batch() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Int32, false),
        Field::new("count", DataType::Int64, false),
    ]));
    let expr = Expr::NestedFunction {
        function: "array_repeat".into(),
        args: vec![
            Expr::Column {
                name: "value".into(),
            },
            Expr::Column {
                name: "count".into(),
            },
        ],
        dtype: Type::Array {
            array: Box::new(Type::Primitive(PrimitiveType::Int32)),
        },
    };
    let physical = compile(&expr, schema.as_ref());
    for value in [11, 29] {
        let input = batch(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![value])) as ArrayRef,
                Arc::new(Int64Array::from(vec![2])) as ArrayRef,
            ],
            1,
        );
        assert_eq!(
            arrays(evaluate(physical.as_ref(), &input), 1),
            vec![vec![value, value]]
        );
    }
}

#[test]
fn errors_in_unselected_expression_branches_remain_unobserved() {
    let output = Type::Array {
        array: Box::new(Type::Primitive(PrimitiveType::Int32)),
    };
    let expr = Expr::IfElse {
        condition: Arc::new(boolean(true)),
        consequent: Arc::new(array_i32(int32(5), int64(1))),
        alternate: Arc::new(Expr::Die {
            message: Arc::new(Expr::Literal {
                dtype: Type::Primitive(PrimitiveType::Str),
                value: json!("must stay unobserved"),
            }),
            dtype: output,
        }),
    };
    let schema = Schema::empty();
    let physical = compile(&expr, &schema);
    let input = batch(Arc::new(schema), vec![], 2);
    assert_eq!(
        arrays(evaluate(physical.as_ref(), &input), 2),
        vec![vec![5], vec![5]]
    );
}

#[test]
fn empty_selection_does_not_evaluate_a_closed_expression() {
    let output = Type::Array {
        array: Box::new(Type::Primitive(PrimitiveType::Int32)),
    };
    let expr = Expr::IfElse {
        condition: Arc::new(boolean(false)),
        consequent: Arc::new(array_i32(int32(5), int64(1))),
        alternate: Arc::new(Expr::Die {
            message: Arc::new(Expr::Literal {
                dtype: Type::Primitive(PrimitiveType::Str),
                value: json!("empty selection must stay lazy"),
            }),
            dtype: output,
        }),
    };
    let schema = Schema::empty();
    let physical = compile(&expr, &schema);
    let input = batch(Arc::new(schema), vec![], 2);
    let selection = BooleanArray::from(vec![false, false]);
    let result = physical
        .evaluate_selection(&input, &selection)
        .unwrap()
        .into_array(0)
        .unwrap();
    assert_eq!(result.len(), 0);
    assert!(physical.evaluate(&input).is_err());
}
