//! Public expression semantics and allocation bounds for composed list pipelines.
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, ListArray,
    RecordBatch,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field, Schema};
use datafusion_physical_expr::PhysicalExpr;
use serde_json::{Value, json};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::Arc;
use vail_execution::expressions::{Expr, lower};

thread_local! { static BYTES: Cell<Option<usize>> = const { Cell::new(None) }; }
struct Allocator;
fn charge(bytes: usize) {
    let _ = BYTES.try_with(|sum| {
        if let Some(old) = sum.get() {
            sum.set(Some(old + bytes));
        }
    });
}
unsafe impl GlobalAlloc for Allocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        charge(layout.size());
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        charge(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, old: Layout, bytes: usize) -> *mut u8 {
        charge(bytes);
        unsafe { System.realloc(ptr, old, bytes) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}
#[global_allocator]
static ALLOCATOR: Allocator = Allocator;

fn column() -> Value {
    json!({"op":"column","name":"xs"})
}
fn variable(name: &str) -> Value {
    json!({"op":"lambda_variable","name":name})
}
fn number(value: i32) -> Value {
    json!({"op":"literal","dtype":"int32","value":value})
}
fn binary(op: &str, a: Value, b: Value) -> Value {
    json!({"op":"binary","operator":op,"left":a,"right":b})
}
fn map(value: Value, name: &str, body: Value) -> Value {
    json!({"op":"array_map","value":value,"parameter":name,"body":body})
}
fn filter(value: Value, name: &str, predicate: Value) -> Value {
    json!({"op":"array_filter","value":value,"parameter":name,"predicate":predicate})
}
fn array(elements: Vec<Value>) -> Value {
    json!({"op":"array","element_type":"int32","elements":elements})
}
fn flatmap(value: Value, name: &str, body: Value) -> Value {
    json!({"op":"array_flatten","value":map(value,name,body)})
}
fn input() -> RecordBatch {
    let values: ArrayRef = Arc::new(Int32Array::from(vec![
        Some(99),
        Some(0),
        None,
        Some(2),
        Some(3),
        Some(4),
        Some(99),
    ]));
    let lists = ListArray::new(
        Arc::new(Field::new_list_field(DataType::Int32, true)),
        OffsetBuffer::new(vec![0i32, 1, 4, 5, 5, 6, 7].into()),
        values,
        Some(NullBuffer::from(vec![true, true, false, true, true, true])),
    )
    .slice(1, 4);
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "xs",
            lists.data_type().clone(),
            true,
        )])),
        vec![Arc::new(lists)],
    )
    .unwrap()
}
fn evaluate(expression: Value, batch: &RecordBatch) -> ArrayRef {
    let expression: Expr = serde_json::from_value(expression).unwrap();
    let output = lower(&expression, batch.schema_ref())
        .unwrap()
        .evaluate(batch)
        .unwrap()
        .into_array(batch.num_rows())
        .unwrap();
    let actual = planned(&expression, batch)
        .evaluate(batch)
        .unwrap()
        .into_array(batch.num_rows())
        .unwrap();
    assert_eq!(actual.as_ref(), output.as_ref());
    output
}
fn planned(expression: &Expr, batch: &RecordBatch) -> Arc<dyn PhysicalExpr> {
    use datafusion_common::DFSchema;
    use datafusion_expr::{
        execution_props::ExecutionProps, physical_planning_context::PhysicalPlanningContext,
    };
    use datafusion_physical_expr::create_physical_expr;
    let schema = DFSchema::try_from(batch.schema().as_ref().clone()).unwrap();
    let logical = vail_execution::logical_expr::lower_logical(expression, &schema).unwrap();
    create_physical_expr(
        &logical,
        &schema,
        &ExecutionProps::new(),
        &PhysicalPlanningContext::default(),
    )
    .unwrap()
}
fn rows(array: &ArrayRef) -> Vec<Option<Vec<Option<i32>>>> {
    let lists = array.as_any().downcast_ref::<ListArray>().unwrap();
    (0..lists.len())
        .map(|row| {
            lists.is_valid(row).then(|| {
                let values = lists.value(row);
                values
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .iter()
                    .collect()
            })
        })
        .collect()
}

#[test]
fn maps_and_filter_keep_null_empty_and_sliced_rows() {
    let expression = map(
        filter(
            map(column(), "x", binary("add", variable("x"), number(1))),
            "y",
            binary("gt", variable("y"), number(2)),
        ),
        "z",
        binary("mul", variable("z"), number(2)),
    );
    assert_eq!(
        rows(&evaluate(expression, &input())),
        vec![
            Some(vec![Some(6)]),
            None,
            Some(vec![]),
            Some(vec![Some(10)])
        ]
    );
}

#[test]
fn flatmaps_compose_in_encounter_order_and_keep_null_elements() {
    let first = flatmap(
        column(),
        "x",
        array(vec![
            variable("x"),
            binary("add", variable("x"), number(10)),
        ]),
    );
    let second = flatmap(
        first,
        "y",
        array(vec![variable("y"), binary("mul", variable("y"), number(2))]),
    );
    let out = rows(&evaluate(second, &input()));
    assert_eq!(
        out,
        vec![
            Some(vec![
                Some(0),
                Some(0),
                Some(10),
                Some(20),
                None,
                None,
                None,
                None,
                Some(2),
                Some(4),
                Some(12),
                Some(24)
            ]),
            None,
            Some(vec![]),
            Some(vec![Some(4), Some(8), Some(14), Some(28)])
        ]
    );
}

#[test]
fn conditional_empty_and_missing_inner_arrays_are_skipped() {
    let body = json!({"op":"if_else","condition":binary("gt",variable("x"),number(1)),
        "consequent":array(vec![variable("x")]),"alternate":array(vec![])});
    let expression = map(
        flatmap(column(), "x", body),
        "v",
        binary("add", variable("v"), number(1)),
    );
    assert_eq!(
        rows(&evaluate(expression, &input())),
        vec![Some(vec![Some(3)]), None, Some(vec![]), Some(vec![Some(5)])]
    );
}

#[test]
fn missing_inner_lists_and_missing_predicates_do_not_emit_elements() {
    let body = json!({"op":"if_else","condition":binary("gt",variable("x"),number(1)),
        "consequent":array(vec![variable("x")]),
        "alternate":{"op":"literal","dtype":{"array":"int32"},"value":null}});
    let expression = flatmap(column(), "x", body);
    assert_eq!(
        rows(&evaluate(expression, &input())),
        vec![Some(vec![Some(2)]), None, Some(vec![]), Some(vec![Some(4)])]
    );
}

#[test]
fn integer_arithmetic_wraps_in_fused_maps() {
    let list = ListArray::new(
        Arc::new(Field::new_list_field(DataType::Int32, true)),
        OffsetBuffer::from_lengths([2]),
        Arc::new(Int32Array::from(vec![i32::MAX, i32::MIN])),
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "xs",
            list.data_type().clone(),
            true,
        )])),
        vec![Arc::new(list)],
    )
    .unwrap();
    let expression = map(
        map(column(), "x", binary("add", variable("x"), number(1))),
        "x",
        binary("mul", variable("x"), number(2)),
    );
    assert_eq!(
        rows(&evaluate(
            flatmap(expression, "out", array(vec![variable("out")])),
            &batch
        )),
        vec![Some(vec![Some(0), Some(2)])]
    );
}

#[test]
fn filter_does_not_hide_an_error_from_an_earlier_map() {
    let expression: Expr = serde_json::from_value(filter(
        flatmap(
            map(
                column(),
                "x",
                json!({"op":"die","message":
        {"op":"literal","dtype":"str","value":"map was demanded"},"dtype":"int32"}),
            ),
            "v",
            array(vec![variable("v")]),
        ),
        "x",
        json!({"op":"literal","dtype":"bool","value":false}),
    ))
    .unwrap();
    let batch = input();
    let compiled = lower(&expression, batch.schema_ref()).unwrap();
    assert!(
        compiled
            .evaluate(&batch)
            .unwrap_err()
            .to_string()
            .contains("map was demanded")
    );
}

fn one_list(values: ArrayRef) -> RecordBatch {
    let list = ListArray::new(
        Arc::new(Field::new_list_field(values.data_type().clone(), true)),
        OffsetBuffer::from_lengths([values.len()]),
        values,
        None,
    );
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "xs",
            list.data_type().clone(),
            true,
        )])),
        vec![Arc::new(list)],
    )
    .unwrap()
}

#[test]
fn int64_overflow_and_negation_preserve_wrapping() {
    let batch = one_list(Arc::new(Int64Array::from(vec![
        Some(i64::MAX),
        Some(i64::MIN),
        None,
    ])));
    let one = json!({"op":"literal","dtype":"int64","value":1});
    let expression = map(
        map(column(), "x", binary("add", variable("x"), one)),
        "v",
        json!({"op":"unary","operator":"neg","value":variable("v")}),
    );
    let out = evaluate(
        flatmap(
            expression,
            "out",
            json!({"op":"array","element_type":"int64","elements":[variable("out")]}),
        ),
        &batch,
    );
    let list = out.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(
        list.values().as_ref(),
        &Int64Array::from(vec![Some(i64::MIN), Some(i64::MAX), None])
    );
}

#[test]
fn boolean_flatmap_preserves_three_valued_logic() {
    let batch = one_list(Arc::new(BooleanArray::from(vec![
        Some(true),
        Some(false),
        None,
    ])));
    let boolean = |value| json!({"op":"literal","dtype":"bool","value":value});
    let body = json!({"op":"array","element_type":"bool","elements":[
        binary("and",variable("x"),boolean(false)),binary("or",variable("x"),boolean(true)),
        {"op":"unary","operator":"not","value":variable("x")}]});
    let out = evaluate(flatmap(column(), "x", body), &batch);
    let list = out.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(
        list.values().as_ref(),
        &BooleanArray::from(vec![
            Some(false),
            Some(true),
            Some(false),
            Some(false),
            Some(true),
            Some(true),
            Some(false),
            Some(true),
            None
        ])
    );
}

#[test]
fn float_maps_preserve_signed_zero_nan_infinity_and_nulls() {
    for (dtype, values) in [
        (
            "float32",
            Arc::new(Float32Array::from(vec![
                Some(0.0),
                Some(-0.0),
                Some(f32::NAN),
                Some(f32::INFINITY),
                None,
            ])) as ArrayRef,
        ),
        (
            "float64",
            Arc::new(Float64Array::from(vec![
                Some(0.0),
                Some(-0.0),
                Some(f64::NAN),
                Some(f64::INFINITY),
                None,
            ])) as ArrayRef,
        ),
    ] {
        let batch = one_list(values);
        let neg = map(
            column(),
            "x",
            json!({"op":"unary","operator":"neg","value":variable("x")}),
        );
        let expression = map(
            neg,
            "x",
            binary(
                "mul",
                variable("x"),
                json!({"op":"literal","dtype":dtype,"value":1.0}),
            ),
        );
        let out = evaluate(
            flatmap(
                expression,
                "out",
                json!({"op":"array","element_type":dtype,"elements":[variable("out")]}),
            ),
            &batch,
        );
        let list = out.as_any().downcast_ref::<ListArray>().unwrap();
        if dtype == "float32" {
            let values = list
                .values()
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap();
            assert_eq!(values.value(0).to_bits(), (-0.0f32).to_bits());
            assert_eq!(values.value(1).to_bits(), 0.0f32.to_bits());
            assert!(values.value(2).is_nan());
            assert_eq!(values.value(3), f32::NEG_INFINITY);
            assert!(values.is_null(4));
        } else {
            let values = list
                .values()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            assert_eq!(values.value(0).to_bits(), (-0.0f64).to_bits());
            assert_eq!(values.value(1).to_bits(), 0.0f64.to_bits());
            assert!(values.value(2).is_nan());
            assert_eq!(values.value(3), f64::NEG_INFINITY);
            assert!(values.is_null(4));
        }
    }
}

#[test]
fn filters_on_inner_list_length_run_before_flatmap() {
    let inner = ListArray::new(
        Arc::new(Field::new_list_field(DataType::Int32, true)),
        OffsetBuffer::new(vec![0i32, 2, 2, 3, 5].into()),
        Arc::new(Int32Array::from(vec![1, 2, 99, 3, 4])),
        Some(NullBuffer::from(vec![true, true, false, true])),
    );
    let batch = one_list(Arc::new(inner));
    let selected = filter(
        column(),
        "list",
        binary(
            "gt",
            json!({"op":"array_length","value":variable("list")}),
            number(1),
        ),
    );
    let expression = flatmap(selected, "list", variable("list"));
    assert_eq!(
        rows(&evaluate(expression, &batch)),
        vec![Some(vec![Some(1), Some(2), Some(3), Some(4)])]
    );
}

#[test]
fn missing_if_else_conditions_produce_missing_elements() {
    let condition = json!({"op":"literal","dtype":"bool","value":null});
    let expression = map(
        map(column(), "x", binary("add", variable("x"), number(1))),
        "v",
        json!({"op":"if_else","condition":condition,"consequent":variable("v"),"alternate":number(7)}),
    );
    assert_eq!(
        rows(&evaluate(expression, &input())),
        vec![
            Some(vec![None, None, None]),
            None,
            Some(vec![]),
            Some(vec![None])
        ]
    );
}

#[test]
fn captured_columns_and_nested_parameters_are_bound_to_their_own_scopes() {
    let batch = input();
    let schema = Arc::new(Schema::new(vec![
        batch.schema().field(0).clone(),
        Field::new("bias", DataType::Int32, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            batch.column(0).clone(),
            Arc::new(Int32Array::from(vec![Some(7), Some(8), None, Some(10)])),
        ],
    )
    .unwrap();
    let expression = flatmap(
        column(),
        "x",
        map(
            array(vec![variable("x"), number(3)]),
            "y",
            binary("add", variable("y"), json!({"op":"column","name":"bias"})),
        ),
    );
    assert_eq!(
        rows(&evaluate(expression, &batch)),
        vec![
            Some(vec![Some(7), Some(10), None, Some(10), Some(9), Some(10)]),
            None,
            Some(vec![]),
            Some(vec![Some(14), Some(13)])
        ]
    );
}

#[test]
fn flatten_reads_nested_list_children_without_gathering_them() {
    let inner = ListArray::new(
        Arc::new(Field::new_list_field(DataType::Int32, true)),
        OffsetBuffer::new(vec![0i32, 2, 3, 3, 5].into()),
        Arc::new(Int32Array::from(vec![1, 2, 99, 3, 4])),
        Some(NullBuffer::from(vec![true, false, true, true])),
    );
    let outer = ListArray::new(
        Arc::new(Field::new_list_field(inner.data_type().clone(), true)),
        OffsetBuffer::new(vec![0i32, 3, 4].into()),
        Arc::new(inner),
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "xs",
            outer.data_type().clone(),
            true,
        )])),
        vec![Arc::new(outer)],
    )
    .unwrap();
    let expression = map(
        flatmap(column(), "list", variable("list")),
        "x",
        binary("add", variable("x"), number(1)),
    );
    assert_eq!(
        rows(&evaluate(expression, &batch)),
        vec![Some(vec![Some(2), Some(3)]), Some(vec![Some(4), Some(5)])]
    );
}

#[test]
fn filters_do_not_materialize_large_mapped_or_flatmapped_intermediates() {
    fn allocated(length: usize, nested: bool) -> usize {
        let values = Arc::new(Int32Array::from_iter_values((0..length).map(|x| x as i32)));
        let list = ListArray::new(
            Arc::new(Field::new_list_field(DataType::Int32, true)),
            OffsetBuffer::from_lengths([length]),
            values,
            None,
        );
        let list = if nested {
            let inner = ListArray::new(
                Arc::new(Field::new_list_field(DataType::Int32, true)),
                OffsetBuffer::from_lengths(std::iter::repeat_n(8, length / 8)),
                list.values().clone(),
                None,
            );
            ListArray::new(
                Arc::new(Field::new_list_field(inner.data_type().clone(), true)),
                OffsetBuffer::from_lengths([length / 8]),
                Arc::new(inner),
                None,
            )
        } else {
            list
        };
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "xs",
                list.data_type().clone(),
                true,
            )])),
            vec![Arc::new(list)],
        )
        .unwrap();
        let mapped = if nested {
            flatmap(
                column(),
                "list",
                map(
                    variable("list"),
                    "item",
                    binary("add", variable("item"), number(1)),
                ),
            )
        } else {
            map(column(), "x", binary("add", variable("x"), number(1)))
        };
        let expanded = flatmap(
            mapped,
            "x",
            array(vec![variable("x"), binary("add", variable("x"), number(2))]),
        );
        let expression: Expr = serde_json::from_value(filter(
            expanded,
            "x",
            binary("lt", variable("x"), number(0)),
        ))
        .unwrap();
        let compiled = lower(&expression, batch.schema_ref()).unwrap();
        BYTES.with(|sum| sum.set(Some(0)));
        let out = compiled.evaluate(&batch).unwrap().into_array(1).unwrap();
        let bytes = BYTES.with(|sum| sum.replace(None).unwrap());
        assert_eq!(rows(&out), vec![Some(vec![])]);
        bytes
    }
    for nested in [false, true] {
        let small = allocated(4096, nested);
        let large = allocated(1_048_576, nested);
        eprintln!("fused list workspace bytes: nested={nested}, small={small}, large={large}");
        assert!(
            large < small + 1024 * 1024,
            "workspace grew with input: {small} -> {large}"
        );
    }
}

#[test]
fn filtered_fixed_fanout_keeps_original_nulls_and_row_offsets_across_blocks() {
    let values: Vec<Option<i32>> = (0..20_000)
        .map(|i| (i % 17 != 0).then_some(i % 13))
        .collect();
    let offsets = [0, 0, 9000, 10000, 20000];
    let validity = [true, true, false, true];
    let lists = ListArray::new(
        Arc::new(Field::new_list_field(DataType::Int32, true)),
        OffsetBuffer::new(offsets.to_vec().into()),
        Arc::new(Int32Array::from(values.clone())),
        Some(NullBuffer::from(validity.to_vec())),
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "xs",
            lists.data_type().clone(),
            true,
        )])),
        vec![Arc::new(lists)],
    )
    .unwrap();
    let expression = flatmap(
        filter(column(), "x", binary("gt", variable("x"), number(3))),
        "v",
        array(vec![variable("v"), binary("add", variable("v"), number(1))]),
    );
    let expected = (0..4)
        .map(|row| {
            validity[row].then(|| {
                values[offsets[row] as usize..offsets[row + 1] as usize]
                    .iter()
                    .filter_map(|&x| x.filter(|&x| x > 3))
                    .flat_map(|x| [Some(x), Some(x + 1)])
                    .collect()
            })
        })
        .collect::<Vec<_>>();
    assert_eq!(rows(&evaluate(expression, &batch)), expected);
}

#[test]
fn flatmap_preserves_null_struct_ancestors_in_selected_original_entries() {
    use arrow_array::StructArray;
    let entries = StructArray::new(
        vec![
            Arc::new(Field::new("value", DataType::Int32, true)),
            Arc::new(Field::new("unused", DataType::Int32, false)),
        ]
        .into(),
        vec![
            Arc::new(Int32Array::from(vec![Some(1), Some(999), None, Some(4)])),
            Arc::new(Int32Array::from(vec![8, 8, 8, 8])),
        ],
        Some(NullBuffer::from(vec![true, false, true, true])),
    );
    let batch = one_list(Arc::new(entries));
    let value = json!({"op":"get_field","value":variable("entry"),"name":"value"});
    let selected = filter(
        column(),
        "e",
        json!({"op":"unary","operator":"not","value":{"op":"is_missing","value":variable("e")}}),
    );
    let expression = flatmap(
        selected,
        "entry",
        array(vec![value.clone(), binary("add", value, number(1))]),
    );
    assert_eq!(
        rows(&evaluate(expression, &batch)),
        vec![Some(vec![Some(1), Some(2), None, None, Some(4), Some(5)])]
    );
}

#[test]
fn reused_and_concurrent_programs_do_not_reuse_input_results() {
    let expression: Expr = serde_json::from_value(filter(
        flatmap(
            column(),
            "x",
            array(vec![variable("x"), binary("add", variable("x"), number(1))]),
        ),
        "v",
        binary("ge", variable("v"), number(0)),
    ))
    .unwrap();
    let schema = one_list(Arc::new(Int32Array::from(vec![1]))).schema();
    let program = lower(&expression, &schema).unwrap();
    let threads = (0..4)
        .map(|thread| {
            let program = program.clone();
            std::thread::spawn(move || {
                for (step, len) in [0, 3, 13000, 1, 5000, 3].into_iter().enumerate() {
                    let source: Vec<Option<i32>> = (0..len)
                        .map(|i| (i % 11 != step).then_some((i % 17) as i32 - thread - step as i32))
                        .collect();
                    let expected: Vec<Option<i32>> = source
                        .iter()
                        .flat_map(|&x| [x, x.map(|x| x + 1)])
                        .filter(|x| x.is_some_and(|x| x >= 0))
                        .collect();
                    let input = one_list(Arc::new(Int32Array::from(source)));
                    let actual = program.evaluate(&input).unwrap().into_array(1).unwrap();
                    assert_eq!(rows(&actual), vec![Some(expected)]);
                }
            })
        })
        .collect::<Vec<_>>();
    for thread in threads {
        thread.join().unwrap();
    }
}

#[test]
fn three_level_flatmap_preserves_slices_empty_lists_and_null_owners() {
    use arrow_array::StructArray;
    for nullable_owners in [false, true] {
        let source: Vec<Option<i32>> = (0..13_000)
            .map(|i| (i % 17 != 0).then_some(i % 9))
            .collect();
        let bottom = ListArray::new(
            Arc::new(Field::new_list_field(DataType::Int32, true)),
            OffsetBuffer::new(vec![0, 1, 1, 7000, 7000, 13000].into()),
            Arc::new(Int32Array::from(source.clone())),
            nullable_owners.then(|| NullBuffer::from(vec![true, true, false, true, true])),
        );
        let entries = StructArray::new(
            vec![Arc::new(Field::new(
                "values",
                bottom.data_type().clone(),
                true,
            ))]
            .into(),
            vec![Arc::new(bottom)],
            nullable_owners.then(|| NullBuffer::from(vec![true, true, true, true, false])),
        );
        let middle = ListArray::new(
            Arc::new(Field::new_list_field(entries.data_type().clone(), true)),
            OffsetBuffer::new(vec![0, 0, 2, 5].into()),
            Arc::new(entries),
            None,
        );
        let outer = ListArray::new(
            Arc::new(Field::new_list_field(middle.data_type().clone(), true)),
            OffsetBuffer::new(vec![0, 1, 1, 3].into()),
            Arc::new(middle),
            None,
        )
        .slice(1, 2);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "xs",
                outer.data_type().clone(),
                true,
            )])),
            vec![Arc::new(outer)],
        )
        .unwrap();
        let inner_map = flatmap(
            json!({"op":"get_field","value":variable("entry"),"name":"values"}),
            "x",
            array(vec![
                binary("add", variable("x"), number(1)),
                binary("mul", variable("x"), number(2)),
            ]),
        );
        let expression = flatmap(
            column(),
            "groups",
            flatmap(variable("groups"), "entry", inner_map),
        );
        let expected = if nullable_owners {
            source[..1]
                .iter()
                .flat_map(|x| [x.map(|x| x + 1), x.map(|x| x * 2)])
                .collect()
        } else {
            source
                .iter()
                .flat_map(|x| [x.map(|x| x + 1), x.map(|x| x * 2)])
                .collect()
        };
        assert_eq!(
            rows(&evaluate(expression, &batch)),
            vec![Some(vec![]), Some(expected)]
        );
    }
}

#[test]
fn replacing_a_source_child_preserves_the_new_child_values() {
    use datafusion_physical_expr::expressions::Column;
    let nested = |values| {
        one_list(Arc::new(ListArray::new(
            Arc::new(Field::new_list_field(DataType::Int32, true)),
            OffsetBuffer::new(vec![0, 1, 2].into()),
            Arc::new(Int32Array::from(values)),
            Some(NullBuffer::from(vec![false, true])),
        )))
    };
    let original = nested(vec![1, 2]);
    let alternate = nested(vec![7, 8]);
    let schema = Arc::new(Schema::new(vec![
        original.schema().field(0).clone(),
        Field::new("alternate", alternate.column(0).data_type().clone(), true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![original.column(0).clone(), alternate.column(0).clone()],
    )
    .unwrap();
    let expression: Expr = serde_json::from_value(flatmap(
        column(),
        "list",
        flatmap(
            variable("list"),
            "x",
            array(vec![
                variable("x"),
                binary("add", variable("x"), number(10)),
            ]),
        ),
    ))
    .unwrap();
    let program = lower(&expression, &schema).unwrap();
    let children = program
        .children()
        .iter()
        .map(|child| {
            if child
                .downcast_ref::<Column>()
                .is_some_and(|column| column.index() == 0)
            {
                Arc::new(Column::new("alternate", 1)) as _
            } else {
                Arc::clone(child)
            }
        })
        .collect();
    let replaced = program.with_new_children(children).unwrap();
    let actual = replaced.evaluate(&batch).unwrap().into_array(1).unwrap();
    assert_eq!(rows(&actual), vec![Some(vec![Some(8), Some(18)])]);
}

#[test]
fn internal_integer_addition_matches_the_ordinary_wrapping_kernel() {
    use vail_execution::expressions::{PreparationOptions, lower_with_options};
    let batch = one_list(Arc::new(Int64Array::from(vec![i64::MAX])));
    let add = json!({"op":"integer_binary","operator":"add","left":variable("x"),"right":{"op":"literal","dtype":"int64","value":1}});
    let expanded = flatmap(
        column(),
        "x",
        json!({"op":"array","element_type":"int64","elements":[add]}),
    );
    let actual = evaluate(expanded.clone(), &batch);
    let expression: Expr = serde_json::from_value(expanded.clone()).unwrap();
    let options = PreparationOptions::default().without_list_flatmap();
    let ordinary = lower_with_options(&expression, batch.schema_ref(), &options)
        .unwrap()
        .evaluate(&batch)
        .unwrap()
        .into_array(1)
        .unwrap();
    assert_eq!(actual.as_ref(), ordinary.as_ref());
    let values = actual
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .value(0);
    assert_eq!(values.as_ref(), &Int64Array::from(vec![i64::MIN]));
    let filtered = evaluate(
        filter(
            expanded,
            "v",
            json!({"op":"literal","dtype":"bool","value":false}),
        ),
        &batch,
    );
    assert_eq!(
        filtered
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .value_length(0),
        0
    );
}

#[test]
fn array_and_capture_fields_can_share_one_nullable_row_struct() {
    use arrow_array::StructArray;
    let source = input();
    let row = StructArray::new(
        vec![
            Arc::new(Field::new(
                "values",
                source.column(0).data_type().clone(),
                true,
            )),
            Arc::new(Field::new("bias", DataType::Int32, true)),
        ]
        .into(),
        vec![
            source.column(0).clone(),
            Arc::new(Int32Array::from(vec![Some(10), Some(20), Some(30), None])),
        ],
        Some(NullBuffer::from(vec![true, true, false, true])),
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "row",
            row.data_type().clone(),
            true,
        )])),
        vec![Arc::new(row)],
    )
    .unwrap();
    let row = json!({"op":"column","name":"row"});
    let values = json!({"op":"get_field","value":row.clone(),"name":"values"});
    let bias = json!({"op":"get_field","value":row,"name":"bias"});
    let expression = flatmap(
        values,
        "x",
        array(vec![variable("x"), binary("add", variable("x"), bias)]),
    );
    assert_eq!(
        rows(&evaluate(expression, &batch)),
        vec![
            Some(vec![Some(0), Some(10), None, None, Some(2), Some(12)]),
            None,
            None,
            Some(vec![Some(4), None])
        ]
    );
}

#[test]
fn mixed_row_array_and_scalar_global_captures_keep_their_domains() {
    use vail_execution::expressions::lower_with_globals;
    let source = input();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            source.schema().field(0).clone(),
            Field::new("bias", DataType::Int32, true),
        ])),
        vec![
            source.column(0).clone(),
            Arc::new(Int32Array::from(vec![Some(10), Some(20), Some(30), None])),
        ],
    )
    .unwrap();
    let globals = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("g", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![100]))],
    )
    .unwrap();
    let sum = binary(
        "add",
        binary("add", variable("x"), json!({"op":"column","name":"bias"})),
        json!({"op":"global","name":"g"}),
    );
    let expression: Expr =
        serde_json::from_value(flatmap(column(), "x", array(vec![sum, variable("x")]))).unwrap();
    let result = lower_with_globals(&expression, batch.schema_ref(), &globals)
        .unwrap()
        .evaluate(&batch)
        .unwrap()
        .into_array(4)
        .unwrap();
    assert_eq!(
        rows(&result),
        vec![
            Some(vec![Some(110), Some(0), None, None, Some(112), Some(2)]),
            None,
            Some(vec![]),
            Some(vec![None, Some(4)])
        ]
    );
}

fn field(value: Value, name: &str) -> Value {
    json!({"op":"get_field","value":value,"name":name})
}
fn record(x: Value) -> Value {
    json!({"op":"struct","fields":[["value",x.clone()],["nested",{"op":"struct","fields":[["twice",binary("mul",x,number(2))]]}]]})
}
fn struct_array(elements: Vec<Value>) -> Value {
    json!({"op":"array","element_type":{"struct":[["value","int32"],["nested",{"struct":[["twice","int32"]]}]]},"elements":elements})
}
fn compare_ordinary(expression: Value, batch: &RecordBatch) -> ArrayRef {
    use vail_execution::expressions::{PreparationOptions, lower_with_options};
    let expr: Expr = serde_json::from_value(expression.clone()).unwrap();
    let ordinary = lower_with_options(
        &expr,
        batch.schema_ref(),
        &PreparationOptions::default().without_list_flatmap(),
    )
    .unwrap()
    .evaluate(batch)
    .unwrap()
    .into_array(batch.num_rows())
    .unwrap();
    let actual = evaluate(expression, batch);
    assert_eq!(actual.as_ref(), ordinary.as_ref());
    actual
}
#[test]
fn struct_flatmap_filters_constructor_fields_and_writes_nested_columns() {
    let expanded = flatmap(
        column(),
        "x",
        struct_array(vec![
            record(variable("x")),
            record(binary("add", variable("x"), number(10))),
        ]),
    );
    let expr = filter(
        expanded,
        "s",
        binary("gt", field(variable("s"), "value"), number(5)),
    );
    let out = compare_ordinary(expr, &input());
    let lists = out.as_any().downcast_ref::<ListArray>().unwrap();
    let first = lists.value(0);
    let first = first
        .as_any()
        .downcast_ref::<arrow_array::StructArray>()
        .unwrap();
    assert_eq!(
        first.column(0).as_ref(),
        &Int32Array::from(vec![Some(10), Some(12)])
    );
    let nested = first
        .column(1)
        .as_any()
        .downcast_ref::<arrow_array::StructArray>()
        .unwrap();
    assert_eq!(
        nested.column(0).as_ref(),
        &Int32Array::from(vec![Some(20), Some(24)])
    );
}
#[test]
fn conditional_struct_nulls_differ_from_defined_null_fields() {
    let ty = json!({"struct":[["value","int32"],["nested",{"struct":[["twice","int32"]]}]]});
    let expr = flatmap(
        column(),
        "x",
        struct_array(vec![
            json!({"op":"if_else",
        "condition":binary("gt",variable("x"),number(1)),"consequent":record(variable("x")),
        "alternate":{"op":"literal","dtype":ty,"value":null}}),
            record(json!({"op":"literal","dtype":"int32","value":null})),
        ]),
    );
    let out = compare_ordinary(expr, &input());
    let first = out.as_any().downcast_ref::<ListArray>().unwrap().value(0);
    let first = first
        .as_any()
        .downcast_ref::<arrow_array::StructArray>()
        .unwrap();
    assert_eq!(
        (0..first.len())
            .map(|i| first.is_valid(i))
            .collect::<Vec<_>>(),
        vec![false, true, false, true, true, true]
    );
}
#[test]
fn maps_fuse_struct_constructor_projection_and_filter() {
    let expr = map(
        filter(
            map(column(), "x", record(variable("x"))),
            "s",
            binary("gt", field(variable("s"), "value"), number(1)),
        ),
        "s",
        record(field(field(variable("s"), "nested"), "twice")),
    );
    compare_ordinary(expr, &input());
}

fn fold(value: Value, zero: Value, operator: &str) -> Value {
    json!({"op":"array_fold","value":value,"zero":zero,"accumulator":"acc","parameter":"v","body":binary(operator,variable("acc"),variable("v")),"scan":false})
}
#[test]
fn fused_fold_keeps_typed_seed_wrapping_nulls_and_list_boundaries() {
    for operator in ["add", "mul"] {
        for seed in [
            number(7),
            number(i32::MAX),
            json!({"op":"literal","dtype":"int32","value":null}),
        ] {
            let value = map(column(), "x", binary("add", variable("x"), number(1)));
            let result = compare_ordinary(fold(value, seed, operator), &input());
            assert!(result.as_any().is::<Int32Array>());
        }
    }
    let lists = ListArray::new(
        Arc::new(Field::new_list_field(DataType::Int32, true)),
        OffsetBuffer::from_lengths([0, 20001, 0, 1]),
        Arc::new(Int32Array::from(vec![Some(1); 20002])),
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "xs",
            lists.data_type().clone(),
            true,
        )])),
        vec![Arc::new(lists)],
    )
    .unwrap();
    let value = flatmap(column(), "x", array(vec![variable("x"), number(2)]));
    let actual = compare_ordinary(fold(value, number(7), "add"), &batch);
    assert_eq!(actual.as_ref(), &Int32Array::from(vec![7, 60010, 7, 10]));
}
#[test]
fn fused_fold_does_not_admit_nonassociative_or_nonliteral_seed_bodies() {
    let value = flatmap(column(), "x", array(vec![variable("x"), number(2)]));
    compare_ordinary(fold(value.clone(), number(7), "sub"), &input());
    compare_ordinary(
        fold(value, binary("add", number(1), number(2)), "add"),
        &input(),
    );
}
#[test]
fn empty_struct_outputs_have_length_even_without_children() {
    let empty = json!({"op":"struct","fields":[]});
    let value = json!({"op":"array_flatten","value":map(column(),"x",json!({"op":"array","element_type":{"struct":[]},"elements":[empty.clone(),empty]}))});
    compare_ordinary(value, &input());
}

#[test]
fn struct_output_copies_only_selected_list_and_string_spans() {
    use arrow_array::{StringArray, StructArray};
    let children = ListArray::new(
        Arc::new(Field::new_list_field(DataType::Int32, true)),
        OffsetBuffer::new(vec![0, 1, 4, 4, 6].into()),
        Arc::new(Int32Array::from(vec![
            Some(1),
            Some(999),
            None,
            Some(999),
            None,
            Some(6),
        ])),
        Some(NullBuffer::from(vec![true, false, true, true])),
    );
    let entries = StructArray::new(
        vec![
            Arc::new(Field::new("list", children.data_type().clone(), true)),
            Arc::new(Field::new("text", DataType::Utf8, true)),
        ]
        .into(),
        vec![
            Arc::new(children),
            Arc::new(StringArray::from(vec![
                Some("one"),
                Some("hidden"),
                None,
                Some("four"),
            ])),
        ],
        Some(NullBuffer::from(vec![true, false, true, true])),
    )
    .slice(1, 3);
    let batch = one_list(Arc::new(entries));
    let ty = json!({"struct":[["list",{"array":"int32"}],["text","str"]]});
    let expression = flatmap(
        column(),
        "x",
        json!({"op":"array","element_type":ty,"elements":[variable("x"),variable("x")]}),
    );
    let actual = compare_ordinary(expression, &batch);
    drop(batch);
    let list = actual
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .value(0);
    let entries = list.as_any().downcast_ref::<StructArray>().unwrap();
    assert_eq!(entries.null_count(), 2);
    let copied = entries
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(
        copied.values().as_ref(),
        &Int32Array::from(vec![None, Some(6), None, Some(6)])
    );
    let text = entries
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        text.iter().collect::<Vec<_>>(),
        vec![None, None, None, None, Some("four"), Some("four")]
    );
}
#[test]
fn struct_filter_empty_output_allocation_does_not_grow_with_input() {
    fn allocated(n: usize) -> usize {
        let batch = one_list(Arc::new(Int32Array::from(vec![1; n])));
        let expression: Expr = serde_json::from_value(filter(
            flatmap(
                column(),
                "x",
                struct_array(vec![
                    record(variable("x")),
                    record(binary("add", variable("x"), number(1))),
                ]),
            ),
            "s",
            binary("lt", field(variable("s"), "value"), number(0)),
        ))
        .unwrap();
        let program = lower(&expression, batch.schema_ref()).unwrap();
        BYTES.with(|sum| sum.set(Some(0)));
        let output = program.evaluate(&batch).unwrap().into_array(1).unwrap();
        let bytes = BYTES.with(|sum| sum.replace(None).unwrap());
        assert_eq!(
            output
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .value_length(0),
            0
        );
        bytes
    }
    let small = allocated(4096);
    let large = allocated(1_048_576);
    eprintln!("struct empty-output evaluation allocations: {small} -> {large}");
    assert!(large < small + 1024 * 1024);
}

#[test]
fn constructor_projection_does_not_drop_an_unused_error() {
    let body = json!({"op":"struct","fields":[["value",variable("x")],["error",{"op":"die","message":{"op":"literal","dtype":"str","value":"keep this error"},"dtype":"int32"}]]});
    let expanded = json!({"op":"array_flatten","value":map(map(column(),"x",body),"s",array(vec![field(variable("s"),"value")]))});
    let expr: Expr = serde_json::from_value(expanded).unwrap();
    assert!(
        lower(&expr, input().schema_ref())
            .unwrap()
            .evaluate(&input())
            .is_err()
    );
}
