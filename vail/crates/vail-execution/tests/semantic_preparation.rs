use arrow_array::{Array, ArrayRef, Int32Array, ListArray, RecordBatch};
use arrow_array::types::Int32Type;
use arrow_schema::{Field, Schema};
use datafusion_common::{DFSchema, ScalarValue};
use datafusion_expr::{execution_props::ExecutionProps, physical_planning_context::PhysicalPlanningContext};
use datafusion_physical_expr::{PhysicalExpr, create_physical_expr};
use std::sync::{Arc, Barrier};
use vail_execution::{expressions::{BinaryOperator, Expr, PrimitiveType, Type}, logical_expr::lower_logical};

fn program(offset: bool) -> (Arc<dyn PhysicalExpr>, Arc<Schema>) {
    let element = Expr::LambdaVariable {name: "element".into(), dtype: None};
    let increment = if offset {Expr::Column {name: "offset".into()}} else {
        Expr::RuntimeLiteral {value: ScalarValue::Int32(Some(1))}
    };
    let expression = Expr::ArrayMap {
        value: Arc::new(Expr::Column {name: "xs".into()}), parameter: "element".into(),
        body: Arc::new(Expr::Binary {operator: BinaryOperator::Add,
            left: Arc::new(element), right: Arc::new(increment)}),
    };
    let list = Type::Array {array: Box::new(Type::Primitive(PrimitiveType::Int32))}.to_arrow().unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("xs", list, true), Field::new("offset", arrow_schema::DataType::Int32, true),
    ]));
    let logical_schema = DFSchema::try_from(schema.as_ref().clone()).unwrap();
    let logical = lower_logical(&expression, &logical_schema).unwrap();
    let program = create_physical_expr(&logical, &logical_schema, &ExecutionProps::new(), &PhysicalPlanningContext::default()).unwrap();
    (program, schema)
}
fn invoke(program: &Arc<dyn PhysicalExpr>, schema: &Arc<Schema>, increment: i32) -> ArrayRef {
    let xs = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>([
        Some(vec![Some(1), Some(2)]), Some(vec![]), None,
    ])) as ArrayRef;
    let offset = Arc::new(Int32Array::from_value(increment, 3)) as ArrayRef;
    let batch = RecordBatch::try_new(schema.clone(), vec![xs, offset]).unwrap();
    let field = program.return_field(schema).unwrap();
    let output = program.evaluate(&batch).unwrap().into_array(3).unwrap();
    assert_eq!(field.data_type(), output.data_type());
    output
}

fn check(output: &ArrayRef, increment: i32) {
    let lists = output.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(lists.len(), 3);
    assert!(lists.is_valid(0));
    assert!(lists.is_valid(1));
    assert!(lists.is_null(2));
    assert_eq!(lists.value_length(1), 0);
    let first = lists.value(0);
    let first = first.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(first.null_count(), 0);
    assert_eq!(first.values().as_ref(), &[1 + increment, 2 + increment]);
}
#[test]
fn shared_physical_program_is_safe_on_concurrent_first_use() {
    let (function, names) = program(false);
    let start = Arc::new(Barrier::new(4));
    let threads = (0..4).map(|_| {
        let function = function.clone(); let names = names.clone(); let start = start.clone();
        std::thread::spawn(move || {start.wait(); check(&invoke(&function, &names, 1), 1);})
    }).collect::<Vec<_>>();
    for thread in threads {thread.join().unwrap();}
    check(&invoke(&function, &names, 1), 1);
}
#[test]
fn repeated_batch_bindings_do_not_reuse_an_earlier_value() {
    let (function, names) = program(true);
    for increment in [1, 10, -3, 1] {check(&invoke(&function, &names, increment), increment);}
}
