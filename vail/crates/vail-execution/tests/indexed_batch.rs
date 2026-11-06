use arrow_array::{Array, Int32Array, ListArray, StructArray};
use std::sync::Arc;
use vail_execution::expressions::{ArrayReduction, Expr, PreparationOptions, Type, lower, lower_with_options, project_batch};

#[path = "common/indexed_batch.rs"]
mod fixture;
use fixture::*;

#[test]
fn nested_selection_preserves_missing_entries_lists_and_callback_captures() {
    let (batch, expression) = fixture([1, -1], true, 8);
    let output = project_batch(&batch, &[("result".into(), expression.clone())]).unwrap();
    let lists = output
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let values = lists
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert_eq!(
        values
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap(),
        &Int32Array::from(vec![Some(1), None, None, Some(93)])
    );
    assert_eq!(
        values
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap(),
        &Int32Array::from(vec![Some(10), Some(20), None, Some(40)])
    );
    let selected = values
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(
        selected
            .iter()
            .map(|list| list.map(|list| list
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()))
            .collect::<Vec<_>>(),
        vec![Some(vec![3, 4]), None, None, Some(vec![6])]
    );
    let sliced =
        project_batch(&batch.slice(1, 1), &[("result".into(), expression.clone())]).unwrap();
    assert_eq!(sliced.column(0).as_ref(), &lists.slice(1, 1) as &dyn Array);
    let empty = project_batch(&batch.slice(0, 0), &[("result".into(), expression)]).unwrap();
    assert_eq!(empty.num_rows(), 0);
}

#[test]
fn list_bounds_errors_only_observe_present_entries() {
    let (batch, expression) = fixture([1, 99], false, 8);
    let result = project_batch(&batch, &[("result".into(), expression)]).unwrap();
    assert_eq!(result.num_rows(), 2);
    let (batch, expression) = fixture([1, 99], true, 8);
    let error = project_batch(&batch, &[("result".into(), expression)]).unwrap_err();
    assert!(
        error.to_string().contains("array index out of bounds"),
        "{error}"
    );
}

#[test]
fn literal_slash_field_and_nested_field_are_distinct() {
    let (batch, mut expression) = fixture([1, -1], true, 8);
    let Expr::AlignedMatrixMap { body, dtype, .. } = &mut expression else {
        unreachable!()
    };
    let entry_type = Type::from_arrow(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .values()
            .data_type(),
    )
    .unwrap();
    let entry = field(input("entry", None, entry_type), "value");
    *body = Arc::new(Expr::ArrayReduce {
        operation: ArrayReduction::Sum,
        filter_missing: true,
        value: Arc::new(Expr::Array {
            elements: vec![field(entry.clone(), "a/b"), field(field(entry, "a"), "b")],
            element_type: int(),
        }),
    });
    *dtype = array(int());
    let output = lower(&expression, batch.schema().as_ref())
        .unwrap()
        .evaluate(&batch)
        .unwrap()
        .into_array(2)
        .unwrap();
    assert_eq!(
        output
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap(),
        &Int32Array::from(vec![Some(11), Some(22), None, Some(44)])
    );
}

#[test]
fn aligned_row_invariants_preserve_missing_parent_semantics() {
    use arrow_array::{ArrayRef,BooleanArray,RecordBatch};
    use arrow_buffer::NullBuffer;
    use vail_execution::expressions::{BinaryOperator,PrimitiveType};
    for outer_null in [false,true] {
        let (original,mut expression)=fixture([1,2],true,0);
        let original_rows=original.column(1).as_any().downcast_ref::<StructArray>().unwrap();
        let row=original_rows.column(0).as_any().downcast_ref::<StructArray>().unwrap();
        let row=Arc::new(StructArray::new(row.fields().clone(),row.columns().to_vec(),
            (!outer_null).then(||NullBuffer::from(vec![true,false])))) as ArrayRef;
        let rows=Arc::new(StructArray::new(original_rows.fields().clone(),vec![row],
            outer_null.then(||NullBuffer::from(vec![true,false])))) as ArrayRef;
        let mut schema_fields=original.schema().fields().to_vec();
        schema_fields[1]=Arc::new(schema_fields[1].as_ref().clone().with_nullable(true));
        let schema=Arc::new(arrow_schema::Schema::new_with_metadata(schema_fields,original.schema().metadata().clone()));
        let batch=RecordBatch::try_new(schema,vec![original.column(0).clone(),rows]).unwrap();
        let index=input("row",Some("index"),int());
        let missing=Expr::IsMissing {value:Arc::new(index.clone())};
        let fields=vec![
            ("offset".into(),Expr::Binary {operator:BinaryOperator::Add,left:Arc::new(index.clone()),right:Arc::new(Expr::Literal{dtype:int(),value:serde_json::json!(1)})}),
            ("missing".into(),missing.clone()),
            ("fallback".into(),Expr::IfElse {condition:Arc::new(missing),consequent:Arc::new(Expr::Literal{dtype:int(),value:serde_json::json!(0)}),alternate:Arc::new(index.clone())}),
            ("length".into(),Expr::ArrayLength {value:Arc::new(Expr::Array{element_type:int(),elements:vec![index]})}),
        ];
        let result=Type::Struct {r#struct:vec![("offset".into(),int()),("missing".into(),Type::Primitive(PrimitiveType::Bool)),("fallback".into(),int()),("length".into(),int())]};
        let Expr::AlignedMatrixMap {body,dtype,..}=&mut expression else {unreachable!()};
        *body=Arc::new(Expr::IfElse {condition:Arc::new(input("entry_present",None,Type::Primitive(PrimitiveType::Bool))),consequent:Arc::new(Expr::Struct{fields}),alternate:Arc::new(Expr::Literal{dtype:result.clone(),value:serde_json::Value::Null})});
        *dtype=array(result);
        let options=PreparationOptions::default().for_selected_region();
        let compiled=lower_with_options(&expression,batch.schema().as_ref(),&options).unwrap();
        eprintln!("row-parent control compiled children: {:?}",compiled.children().iter().map(|child|child.to_string()).collect::<Vec<_>>());
        let output=compiled.evaluate(&batch).unwrap().into_array(2).unwrap();
        let lists=output.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!((0..lists.len()).map(|i|lists.is_valid(i)).collect::<Vec<_>>(),vec![true,true]);
        assert_eq!(lists.value_offsets(),&[0,2,4]);
        let output=lists.values().as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!((0..output.len()).map(|i|output.is_valid(i)).collect::<Vec<_>>(),vec![true,true,false,true]);
        assert_eq!(output.column(0).as_any().downcast_ref::<Int32Array>().unwrap().iter().collect::<Vec<_>>(),vec![Some(2),Some(2),None,None]);
        assert_eq!(output.column(1).as_any().downcast_ref::<BooleanArray>().unwrap().iter().collect::<Vec<_>>(),vec![Some(false),Some(false),None,Some(true)]);
        assert_eq!(output.column(2).as_any().downcast_ref::<Int32Array>().unwrap().iter().collect::<Vec<_>>(),vec![Some(1),Some(1),None,Some(0)]);
        assert_eq!(output.column(3).as_any().downcast_ref::<Int32Array>().unwrap().iter().collect::<Vec<_>>(),vec![Some(1),Some(1),None,Some(1)]);
    }
}
