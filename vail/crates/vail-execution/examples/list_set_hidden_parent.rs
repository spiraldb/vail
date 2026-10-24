//! Public expression/Arrow boundary, retaining null-parent hidden ranges.
//! Example arguments: hidden-child-count iterations [logical|physical].
//! Input allocation and query preparation are outside reported query windows.
//! Use the operating system's time utility for whole-process CPU/peak RSS;
//! those include producer allocation and are not kernel allocation counters.
use arrow_array::{Array, Int32Array, ListArray, RecordBatch};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field, Schema};
use std::{collections::BTreeSet, sync::Arc, time::Instant};
use vail_execution::expressions::{Expr, PrimitiveType, Type};

fn main() {
    let args: Vec<_> = std::env::args().collect();
    let hidden: usize = args[1].parse().unwrap();
    let iterations: usize = args[2].parse().unwrap();
    let owners: usize = args.get(4).map(|x| x.parse().unwrap()).unwrap_or(1);
    let logical = args.get(3).map(String::as_str).unwrap_or("physical") == "logical";
    let mut values = vec![7; hidden.checked_add(4).unwrap()];
    values[hidden + 2] = 9;
    values[hidden + 3] = 9;
    let input = ListArray::new(
        Arc::new(Field::new_list_field(DataType::Int32, true)),
        OffsetBuffer::new(
            vec![
                0,
                2,
                i32::try_from(hidden + 2).unwrap(),
                i32::try_from(hidden + 4).unwrap(),
            ]
            .into(),
        ),
        Arc::new(Int32Array::from(values)),
        Some(NullBuffer::from(vec![true, false, true])),
    );
    let schema = Schema::new(vec![Field::new("items", input.data_type().clone(), true)]);
    let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(input)]).unwrap();
    let retained_owners = (1..owners).map(|_| batch.clone()).collect::<Vec<_>>();
    let expr = Expr::NestedFunction {
        function: "__vail_list_reduce_collect_as_set".into(),
        args: vec![Expr::Column {
            name: "items".into(),
        }],
        dtype: Type::Set {
            set: Box::new(Type::Primitive(PrimitiveType::Int32)),
        },
    };
    let physical = if logical {
        let df_schema = datafusion_common::DFSchema::try_from(schema.clone()).unwrap();
        let expr = vail_execution::logical_expr::lower_logical(&expr, &df_schema).unwrap();
        datafusion_physical_expr::planner::create_physical_expr(
            &expr,
            &df_schema,
            &datafusion_physical_expr::execution_props::ExecutionProps::new(),
            &datafusion_expr::physical_planning_context::PhysicalPlanningContext::default(),
        )
        .unwrap()
    } else {
        vail_execution::expressions::lower(&expr, &schema).unwrap()
    };
    println!(
        "{}",
        serde_json::json!({"phase":"ready","hidden":hidden,"visible_children":4,"logical":logical,"owners":owners})
    );
    for iteration in 0..iterations {
        let start = Instant::now();
        let result = physical.evaluate(&batch).unwrap().into_array(3).unwrap();
        let elapsed = start.elapsed().as_secs_f64();
        let lists = result.as_any().downcast_ref::<ListArray>().unwrap();
        assert!(lists.is_null(1));
        for (row, expected) in [(0, 7), (2, 9)] {
            let value = lists.value(row);
            let values = value.as_any().downcast_ref::<Int32Array>().unwrap();
            assert_eq!(
                values.iter().collect::<BTreeSet<_>>(),
                BTreeSet::from([Some(expected)])
            );
            assert_eq!(values.len(), 1);
        }
        println!(
            "{}",
            serde_json::json!({"iteration":iteration,"wall_seconds":elapsed,"result":[[7],null,[9]]})
        );
    }
    std::hint::black_box(&retained_owners);
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .value_length(1),
        i32::try_from(hidden).unwrap()
    );
}
