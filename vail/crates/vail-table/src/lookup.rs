//! Native lowering for keyed table lookup.
//!
//! The right side is reduced to one representative per present canonical key,
//! then each left partition is joined with DataFusion's sort-merge join. All
//! plans remain deferred until the caller executes an action.
use crate::globals::GlobalBindings;
use crate::partitioned::PreparedTable;
use crate::plan::{self, PhysicalPlan, column};
use crate::{Result, ScalarIR, TableError, TableRef};
use arrow_schema::SortOptions;
use datafusion_common::config::ConfigOptions;
use datafusion_common::{DataFusionError, ScalarValue, tree_node::TreeNodeRecursion};
use datafusion_execution::TaskContext;
use datafusion_expr::{JoinType, Operator, WindowFunctionDefinition, window_frame::WindowFrame};
use datafusion_physical_expr::ScalarFunctionExpr;
use datafusion_physical_expr::aggregate::AggregateExprBuilder;
use datafusion_physical_expr::{
    LexOrdering, PhysicalExpr, PhysicalSortExpr,
    expressions::{BinaryExpr, CaseExpr, IsNullExpr, Literal, NotExpr},
};
use datafusion_physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, InputOrderMode, PlanProperties,
    SendableRecordBatchStream,
    filter::FilterExec,
    joins::{NestedLoopJoinExec, SortMergeJoinExec, utils::JoinFilter},
    projection::ProjectionExec,
    sorts::sort::SortExec,
    windows::{BoundedWindowAggExec, create_window_expr},
};
use std::fmt;
use std::sync::Arc;
use vail_execution::expressions::{Expr, Type, lower_with_bindings};

fn private_name(base: &str, schema: &arrow_schema::Schema) -> String {
    let mut name = base.to_owned();
    while schema.index_of(&name).is_ok() {
        name.push('_');
    }
    name
}

fn empty_payload_rows(name: &str) -> Expr {
    Expr::ArrayMap {
        value: Arc::new(Expr::Column {
            name: name.to_owned(),
        }),
        parameter: "__vail_empty_payload_item".into(),
        body: Arc::new(Expr::Struct { fields: vec![] }),
    }
}

fn sort_by(input: PhysicalPlan, fields: &[(usize, SortOptions)]) -> Result<PhysicalPlan> {
    let ordering = datafusion_physical_expr::LexOrdering::new(
        fields
            .iter()
            .map(|(index, options)| PhysicalSortExpr::new(column(&input, *index), *options))
            .collect::<Vec<_>>(),
    )
    .ok_or_else(|| TableError::Invalid("lookup sort requires at least one key".into()))?;
    Ok(Arc::new(SortExec::new(ordering, input)))
}

/// Tell DataFusion about ordering inherited from a keyed source after a
/// projection computes its canonical key. The projection preserves row order;
/// the source's right-key ordering is equivalent to the encoded tuple order.
#[derive(Debug)]
struct DeclaredOrderingExec {
    input: PhysicalPlan,
    key: String,
    ordinal: String,
    cache: Arc<PlanProperties>,
}
impl DeclaredOrderingExec {
    fn new(input: PhysicalPlan, key: &str, ordinal: &str) -> Result<PhysicalPlan> {
        let schema = input.schema();
        let ordering = LexOrdering::new(vec![
            PhysicalSortExpr::new(
                column(&input, schema.index_of(key)?),
                SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            ),
            PhysicalSortExpr::new(
                column(&input, schema.index_of(ordinal)?),
                SortOptions::default(),
            ),
        ])
        .ok_or_else(|| TableError::Invalid("lookup source ordering is empty".into()))?;
        let mut cache = (**input.properties()).clone();
        cache.eq_properties.add_ordering(ordering);
        cache.set_eq_properties(cache.eq_properties.clone());
        Ok(Arc::new(Self {
            input,
            key: key.to_owned(),
            ordinal: ordinal.to_owned(),
            cache: Arc::new(cache),
        }))
    }
}
impl DisplayAs for DeclaredOrderingExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LookupSourceOrderingExec")
    }
}
impl ExecutionPlan for DeclaredOrderingExec {
    fn name(&self) -> &'static str {
        "LookupSourceOrderingExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }
    fn children(&self) -> Vec<&PhysicalPlan> {
        vec![&self.input]
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> datafusion_common::Result<TreeNodeRecursion>,
    ) -> datafusion_common::Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<PhysicalPlan>,
    ) -> datafusion_common::Result<PhysicalPlan> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "lookup ordering adapter requires one child".into(),
            ));
        }
        Self::new(children[0].clone(), &self.key, &self.ordinal)
            .map_err(|e| DataFusionError::Plan(e.to_string()))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        self.input.execute(partition, context)
    }
}

/// Project canonical lookup keys and payload columns from a table input. Local
/// and captured globals are joined as singleton relations before expression
/// lowering, matching the expression binding used by other native operators.
fn keyed_input(
    mut input: PhysicalPlan,
    table: &TableRef,
    keys: &[ScalarIR],
    bindings: &GlobalBindings,
    side: &str,
    right_keys: Option<&[String]>,
    raw_key: bool,
) -> Result<(PhysicalPlan, String, String, Vec<String>, String)> {
    input = plan::ordinal(input);
    let ordinal_original_index = input.schema().fields().len() - 1;
    let local_global_count = table.globals().schema().fields().len();
    input = plan::bind_globals(input, table)?;
    let (bound, external) = plan::bind_external(input, bindings)?;
    let data_offset = local_global_count + bindings.len();
    let physical = plan::physical_schema(table);
    let ordinal_name = bound
        .schema()
        .field(data_offset + ordinal_original_index)
        .name()
        .clone();
    let used = bound.schema().as_ref().clone();
    let canonical_name = private_name(&format!("__vail_lookup_{side}_key"), &used);
    let ordinal_out = private_name(&format!("__vail_lookup_{side}_ordinal"), &used);
    let marker_name = private_name(&format!("__vail_lookup_{side}_present"), &used);
    let mut projections: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::new();

    let key_exprs = if let Some(right_keys) = right_keys {
        right_keys
            .iter()
            .map(|name| {
                lower_with_bindings(
                    &Expr::Column { name: name.clone() },
                    &bound.schema(),
                    table.globals().schema(),
                    &external,
                )
            })
            .collect::<std::result::Result<Vec<_>, _>>()?
    } else {
        keys.iter()
            .map(|key| {
                lower_with_bindings(key, &bound.schema(), table.globals().schema(), &external)
            })
            .collect::<std::result::Result<Vec<_>, _>>()?
    };
    let key_struct = if key_exprs.is_empty() {
        Arc::new(Literal::new(ScalarValue::Boolean(Some(true)))) as Arc<dyn PhysicalExpr>
    } else {
        let expr = if let Some(right_keys) = right_keys {
            Expr::Struct {
                fields: right_keys
                    .iter()
                    .zip(key_exprs.iter())
                    .enumerate()
                    .map(|(index, (name, _))| {
                        (
                            format!("__key_{index}_{name}"),
                            Expr::Column { name: name.clone() },
                        )
                    })
                    .collect(),
            }
        } else {
            Expr::Struct {
                fields: keys
                    .iter()
                    .enumerate()
                    .map(|(index, key)| (format!("__key_{index}"), key.clone()))
                    .collect(),
            }
        };
        lower_with_bindings(&expr, &bound.schema(), table.globals().schema(), &external)?
    };
    let mut missing: Option<Arc<dyn PhysicalExpr>> = None;
    for key in &key_exprs {
        let component_missing: Arc<dyn PhysicalExpr> = Arc::new(IsNullExpr::new(key.clone()));
        missing = Some(match missing {
            Some(previous) => Arc::new(BinaryExpr::new(previous, Operator::Or, component_missing)),
            None => component_missing,
        });
    }
    let canonical: Arc<dyn PhysicalExpr> = if raw_key {
        key_exprs
            .first()
            .ok_or_else(|| TableError::Invalid("interval lookup requires one key".into()))?
            .clone()
    } else if let Some(missing) = missing {
        let key = key_struct;
        let null = ScalarValue::try_from(key.data_type(bound.schema().as_ref())?)?;
        Arc::new(CaseExpr::try_new(
            None,
            vec![(missing, Arc::new(Literal::new(null)))],
            Some(key),
        )?)
    } else {
        key_struct
    };
    projections.push((canonical, canonical_name.clone()));
    let ordinal_index = bound.schema().index_of(&ordinal_name)?;
    projections.push((column(&bound, ordinal_index), ordinal_out.clone()));
    projections.push((
        Arc::new(Literal::new(ScalarValue::Boolean(Some(true)))),
        marker_name.clone(),
    ));

    let mut original_names = Vec::with_capacity(physical.fields().len());
    for i in 0..physical.fields().len() {
        let name = private_name(&format!("__vail_lookup_{side}_value_{i}"), &used);
        original_names.push(name.clone());
        projections.push((column(&bound, data_offset + i), name));
    }
    let projected: PhysicalPlan = Arc::new(ProjectionExec::try_new(projections, bound)?);
    // Keep enough metadata in the synthetic schema names to restore source
    // fields and the physical partition tag after the join.
    let _ = physical;
    Ok((
        projected,
        canonical_name,
        ordinal_out,
        original_names,
        marker_name,
    ))
}

fn reduce_right(input: PhysicalPlan, key_name: &str, ordinal_name: &str) -> Result<PhysicalPlan> {
    let schema = input.schema();
    let key_index = schema.index_of(key_name)?;
    schema.index_of(ordinal_name)?;
    // Top-level missing key components never match. Nested missing values are
    // encoded inside the canonical key and remain matchable.
    let missing = Arc::new(IsNullExpr::new(column(&input, key_index)));
    let present: PhysicalPlan = Arc::new(FilterExec::try_new(
        Arc::new(datafusion_physical_expr::expressions::NotExpr::new(missing)),
        input,
    )?);
    // Missing top-level keys can interrupt the target's natural key order
    // after they are collapsed to one null probe value. Once removed, the
    // remaining present canonical tuples retain the source's key ordering.
    let sorted = DeclaredOrderingExec::new(present, key_name, ordinal_name)?;
    let key_index = sorted.schema().index_of(key_name)?;
    let ordinal_index = sorted.schema().index_of(ordinal_name)?;
    let number_name = private_name("__vail_lookup_row_number", &sorted.schema());
    let window = create_window_expr(
        &WindowFunctionDefinition::WindowUDF(
            datafusion_functions_window::row_number::row_number_udwf(),
        ),
        number_name.clone(),
        &[],
        &[column(&sorted, key_index)],
        &[PhysicalSortExpr::new(
            column(&sorted, ordinal_index),
            SortOptions::default(),
        )],
        Arc::new(WindowFrame::new(Some(true))),
        sorted.schema(),
        false,
        false,
        None,
    )?;
    let windowed: PhysicalPlan = Arc::new(BoundedWindowAggExec::try_new(
        vec![window],
        sorted,
        InputOrderMode::Sorted,
        false,
    )?);
    let row_number = windowed.schema().index_of(&number_name)?;
    let keep_first: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
        column(&windowed, row_number),
        Operator::Eq,
        Arc::new(Literal::new(ScalarValue::UInt64(Some(1)))),
    ));
    Ok(Arc::new(FilterExec::try_new(keep_first, windowed)?))
}

/// Retain every right row in source order, grouped by canonical key. The caller
/// supplies the single, ordered `prepared_ordered` stream; native array_agg's
/// grouped accumulator records rows in their input batch/row order, so this
/// does not need a per-value ORDER BY accumulator.
fn collect_right(
    input: PhysicalPlan,
    key_name: &str,
    payload: &[(String, String)],
) -> Result<(PhysicalPlan, String, String)> {
    let schema = input.schema();
    let key = column(&input, schema.index_of(key_name)?);
    let mut args: Vec<Arc<dyn PhysicalExpr>> = Vec::with_capacity(payload.len().max(1) * 2);
    for (field, name) in payload {
        args.push(Arc::new(Literal::new(ScalarValue::Utf8(Some(
            field.clone(),
        )))));
        args.push(column(&input, schema.index_of(name)?));
    }
    if args.is_empty() {
        args.push(Arc::new(Literal::new(ScalarValue::Utf8(Some(
            "__vail_empty_payload".into(),
        )))));
        args.push(Arc::new(Literal::new(ScalarValue::Boolean(Some(true)))));
    }
    let row: Arc<dyn PhysicalExpr> = {
        Arc::new(ScalarFunctionExpr::try_new(
            datafusion_functions::core::named_struct(),
            args,
            &schema,
            Arc::new(ConfigOptions::default()),
        )?)
    };
    let rows_name = private_name("__vail_lookup_matches", &schema);
    let rows = AggregateExprBuilder::new(
        datafusion_functions_aggregate::array_agg::array_agg_udaf(),
        vec![row],
    )
    .schema(schema.clone())
    .alias(rows_name.clone())
    .build()?;
    let grouped: PhysicalPlan = Arc::new(AggregateExec::try_new(
        AggregateMode::Single,
        PhysicalGroupBy::new_single(vec![(key, key_name.to_owned())]),
        vec![Arc::new(rows)],
        vec![None],
        input,
        schema,
    )?);
    // Group columns are encoded for Hail equality. Re-expose the canonical
    // key as the join key; the input key is already canonical binary.
    let marker_name = private_name("__vail_lookup_right_present", &grouped.schema());
    let grouped: PhysicalPlan = Arc::new(ProjectionExec::try_new(
        vec![
            (
                column(&grouped, grouped.schema().index_of(key_name)?),
                key_name.to_owned(),
            ),
            (
                column(&grouped, grouped.schema().index_of(&rows_name)?),
                rows_name.clone(),
            ),
            (
                Arc::new(Literal::new(ScalarValue::Boolean(Some(true)))),
                marker_name.clone(),
            ),
        ],
        grouped,
    )?);
    Ok((grouped, rows_name, marker_name))
}

/// Lower lookup for one left partition and the shared, reduced right input.
fn join_partition(
    left: PhysicalPlan,
    right: PhysicalPlan,
    left_key: &str,
    right_key: &str,
    left_ordinal: &str,
    right_ordinal: &str,
    right_marker: &str,
    right_payload: &[(String, String)],
    left_value_names: &[String],
    left_table: &TableRef,
    field: &str,
    output_schema: arrow_schema::SchemaRef,
    all_matches: bool,
    matches_field: Option<&str>,
    interval_mode: bool,
    query_interval_mode: bool,
) -> Result<PhysicalPlan> {
    let left_key_index = left.schema().index_of(left_key)?;
    let left_ordinal_index = left.schema().index_of(left_ordinal)?;
    let sorted_left = sort_by(
        left,
        &[
            (
                left_key_index,
                SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            ),
            (left_ordinal_index, SortOptions::default()),
        ],
    )?;
    let sorted_right = right;
    let sorted_left_key = sorted_left.schema().index_of(left_key)?;
    let sorted_right_key = sorted_right.schema().index_of(right_key)?;
    let joined: PhysicalPlan = if interval_mode || query_interval_mode {
        let left_field = sorted_left.schema().field(sorted_left_key).clone();
        let right_field = sorted_right.schema().field(sorted_right_key).clone();
        let filter_schema = Arc::new(arrow_schema::Schema::new(vec![left_field, right_field]));
        let filter_expr = if query_interval_mode {
            let Type::Interval {
                interval: point_type,
            } = Type::from_arrow(filter_schema.field(0).data_type())?
            else {
                return Err(TableError::Invalid(
                    "query interval key type expected".into(),
                ));
            };
            Expr::IntervalContains {
                interval: Arc::new(crate::interval_read::ordered_key_interval(
                    Expr::Column {
                        name: left_key.to_owned(),
                    },
                    &point_type,
                )),
                point: Arc::new(crate::interval_read::ordered_key_point(
                    Expr::Column {
                        name: right_key.to_owned(),
                    },
                    &point_type,
                )),
            }
        } else {
            Expr::IntervalContains {
                interval: Arc::new(Expr::Column {
                    name: right_key.to_owned(),
                }),
                point: Arc::new(Expr::Column {
                    name: left_key.to_owned(),
                }),
            }
        };
        let filter_expr = lower_with_bindings(
            &filter_expr,
            &filter_schema,
            &arrow_schema::Schema::empty(),
            &Default::default(),
        )?;
        let filter = JoinFilter::new(
            filter_expr,
            JoinFilter::build_column_indices(vec![sorted_left_key], vec![sorted_right_key]),
            filter_schema,
        );
        Arc::new(NestedLoopJoinExec::try_new(
            sorted_left.clone(),
            sorted_right.clone(),
            Some(filter),
            &JoinType::Left,
            None,
        )?)
    } else {
        Arc::new(SortMergeJoinExec::try_new(
            sorted_left.clone(),
            sorted_right.clone(),
            vec![(
                column(&sorted_left, sorted_left_key),
                column(&sorted_right, sorted_right_key),
            )],
            None,
            JoinType::Left,
            vec![SortOptions {
                descending: false,
                nulls_first: false,
            }],
            datafusion_common::NullEquality::NullEqualsNothing,
        )?)
    };
    let joined: PhysicalPlan = if (interval_mode || query_interval_mode) && !all_matches {
        let schema = joined.schema();
        let left_ordinal_idx = schema.index_of(left_ordinal)?;
        let right_ordinal_idx = schema.index_of(right_ordinal)?;
        let row_number_name = private_name("__vail_interval_lookup_row_number", &schema);
        let window = create_window_expr(
            &WindowFunctionDefinition::WindowUDF(
                datafusion_functions_window::row_number::row_number_udwf(),
            ),
            row_number_name.clone(),
            &[],
            &[column(&joined, left_ordinal_idx)],
            &[PhysicalSortExpr::new(
                column(&joined, right_ordinal_idx),
                SortOptions::default(),
            )],
            Arc::new(WindowFrame::new(Some(true))),
            schema,
            false,
            false,
            None,
        )?;
        let windowed: PhysicalPlan = Arc::new(BoundedWindowAggExec::try_new(
            vec![window],
            joined,
            InputOrderMode::Sorted,
            false,
        )?);
        let predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            column(&windowed, windowed.schema().index_of(&row_number_name)?),
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::UInt64(Some(1)))),
        ));
        Arc::new(FilterExec::try_new(predicate, windowed)?)
    } else {
        joined
    };
    if (interval_mode || query_interval_mode) && all_matches {
        let input_schema = joined.schema();
        let left_ord_idx = input_schema.index_of(left_ordinal)?;
        let right_ord_idx = input_schema.index_of(right_ordinal)?;
        let row_name = private_name("__vail_interval_matches", &input_schema);
        let mut args: Vec<Arc<dyn PhysicalExpr>> = Vec::with_capacity(right_payload.len() * 2);
        for (name, source) in right_payload {
            args.push(Arc::new(Literal::new(ScalarValue::Utf8(Some(
                name.clone(),
            )))));
            args.push(column(&joined, input_schema.index_of(source)?));
        }
        if args.is_empty() {
            args.push(Arc::new(Literal::new(ScalarValue::Utf8(Some(
                "__vail_empty_payload".into(),
            )))));
            args.push(Arc::new(Literal::new(ScalarValue::Boolean(Some(true)))));
        }
        let row: Arc<dyn PhysicalExpr> = {
            Arc::new(ScalarFunctionExpr::try_new(
                datafusion_functions::core::named_struct(),
                args,
                &input_schema,
                Arc::new(ConfigOptions::default()),
            )?)
        };
        let collected = AggregateExprBuilder::new(
            datafusion_functions_aggregate::array_agg::array_agg_udaf(),
            vec![row],
        )
        // NestedLoopJoinExec does not preserve either input's ordering, while
        // interval all_matches exposes right-table source order.
        .order_by(vec![PhysicalSortExpr::new(
            column(&joined, right_ord_idx),
            SortOptions::default(),
        )])
        .schema(input_schema.clone())
        .alias(row_name.clone())
        .build()?;
        let mut aggregates = Vec::new();
        for (i, source) in left_value_names.iter().enumerate() {
            let value = column(&joined, input_schema.index_of(source)?);
            // All matches repeat the same left value. Keep one Arrow value
            // without first_value's scalar compaction of nested empty structs.
            let aggregate = AggregateExprBuilder::new(
                vail_execution::advanced_collection_aggregates::make_udaf(
                    "take",
                    &value.data_type(&input_schema)?,
                    &serde_json::json!({"n": 1}),
                )?
                .ok_or_else(|| TableError::Invalid("take aggregate unavailable".into()))?,
                vec![value],
            )
            .schema(input_schema.clone())
            .alias(format!("__vail_left_value_{i}"))
            .build()?;
            aggregates.push(Arc::new(aggregate));
        }
        aggregates.push(Arc::new(collected));
        let marker_idx = input_schema.index_of(right_marker)?;
        let marker_null: Arc<dyn PhysicalExpr> =
            Arc::new(IsNullExpr::new(column(&joined, marker_idx)));
        let filters = (0..aggregates.len() - 1)
            .map(|_| None)
            .chain(std::iter::once(Some(
                Arc::new(NotExpr::new(marker_null)) as Arc<dyn PhysicalExpr>
            )))
            .collect();
        let grouped: PhysicalPlan = Arc::new(AggregateExec::try_new(
            AggregateMode::Single,
            PhysicalGroupBy::new_single(vec![(
                column(&joined, left_ord_idx),
                left_ordinal.to_owned(),
            )]),
            aggregates,
            filters,
            joined,
            input_schema,
        )?);
        let grouped_ordinal = grouped.schema().index_of(left_ordinal)?;
        let grouped = sort_by(grouped, &[(grouped_ordinal, SortOptions::default())])?;
        let schema = grouped.schema();
        let mut projection: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::new();
        for (i, field_name) in left_table
            .schema()
            .fields()
            .iter()
            .map(|f| f.name())
            .enumerate()
        {
            projection.push((
                vail_execution::expressions::lower(
                    &Expr::ArrayIndex {
                        value: Arc::new(Expr::Column {
                            name: format!("__vail_left_value_{i}"),
                        }),
                        index: Arc::new(Expr::Literal {
                            value: serde_json::json!(0),
                            dtype: Type::Primitive(
                                vail_execution::expressions::PrimitiveType::Int32,
                            ),
                        }),
                    },
                    &schema,
                )?,
                field_name.clone(),
            ));
        }
        let item_type = match output_schema.field_with_name(field)?.data_type() {
            arrow_schema::DataType::List(item) => Type::from_arrow(item.data_type())?,
            other => {
                return Err(TableError::Invalid(format!(
                    "all_matches output is not a list: {other:?}"
                )));
            }
        };
        let collected_expr = Expr::IfElse {
            condition: Arc::new(Expr::IsMissing {
                value: Arc::new(Expr::Column {
                    name: row_name.clone(),
                }),
            }),
            consequent: Arc::new(Expr::Array {
                element_type: item_type,
                elements: vec![],
            }),
            alternate: Arc::new(if right_payload.is_empty() {
                empty_payload_rows(&row_name)
            } else {
                Expr::Column { name: row_name }
            }),
        };
        projection.push((
            lower_with_bindings(
                &collected_expr,
                &schema,
                &arrow_schema::Schema::empty(),
                &Default::default(),
            )?,
            field.to_owned(),
        ));
        let partition_idx =
            schema.index_of(&format!("__vail_left_value_{}", left_value_names.len() - 1))?;
        let partition_name = output_schema
            .field(output_schema.fields().len() - 1)
            .name()
            .clone();
        projection.push((column(&grouped, partition_idx), partition_name));
        let protected: PhysicalPlan = Arc::new(crate::partitioned::ProtectedExec::new(
            grouped,
            "VailIntervalLookupOrderExec",
        ));
        return Ok(Arc::new(ProjectionExec::try_new(projection, protected)?));
    }
    // SortMergeJoin emits key order. Restore the left partition's original
    // sequence before removing the private ordinal.
    let joined_ordinal = joined.schema().index_of(left_ordinal)?;
    let joined = sort_by(joined, &[(joined_ordinal, SortOptions::default())])?;
    let joined: PhysicalPlan = Arc::new(crate::partitioned::ProtectedExec::new(
        joined,
        "VailLookupOrderExec",
    ));
    let joined_schema = joined.schema();
    let mut output: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::new();
    for (index, name) in left_value_names
        .iter()
        .enumerate()
        .take(left_table.schema().fields().len())
    {
        output.push((
            column(&joined, joined_schema.index_of(name)?),
            left_table.schema().field(index).name().clone(),
        ));
    }
    let payload_type = output_schema.field_with_name(field)?.data_type().clone();
    let payload_expr =
        if all_matches {
            let item_type = match output_schema.field_with_name(field)?.data_type() {
                arrow_schema::DataType::List(item) => Type::from_arrow(item.data_type())?,
                other => {
                    return Err(TableError::Invalid(format!(
                        "all_matches output is not a list: {other:?}"
                    )));
                }
            };
            Expr::IfElse {
                condition: Arc::new(Expr::IsMissing {
                    value: Arc::new(Expr::Column {
                        name: right_marker.to_owned(),
                    }),
                }),
                consequent: Arc::new(Expr::Array {
                    element_type: item_type,
                    elements: vec![],
                }),
                alternate: Arc::new(if right_payload.is_empty() {
                    empty_payload_rows(matches_field.ok_or_else(|| {
                        TableError::Invalid("missing collected lookup values".into())
                    })?)
                } else {
                    Expr::Column {
                        name: matches_field
                            .ok_or_else(|| {
                                TableError::Invalid("missing collected lookup values".into())
                            })?
                            .to_owned(),
                    }
                }),
            }
        } else {
            Expr::IfElse {
                condition: Arc::new(Expr::IsMissing {
                    value: Arc::new(Expr::Column {
                        name: right_marker.to_owned(),
                    }),
                }),
                consequent: Arc::new(Expr::RuntimeLiteral {
                    value: ScalarValue::try_from(payload_type)?,
                }),
                alternate: Arc::new(Expr::Struct {
                    fields: right_payload
                        .iter()
                        .map(|(field, name)| (field.clone(), Expr::Column { name: name.clone() }))
                        .collect(),
                }),
            }
        };
    let payload = lower_with_bindings(
        &payload_expr,
        &joined_schema,
        &arrow_schema::Schema::empty(),
        &Default::default(),
    )?;
    output.push((payload, field.to_owned()));
    // The left physical partition tag is part of left_value_names' final item.
    let partition_name = output_schema.field(output_schema.fields().len() - 1).name();
    let partition_source = left_value_names
        .last()
        .ok_or_else(|| TableError::Invalid("lookup left input has no partition tag".into()))?;
    output.push((
        column(&joined, joined_schema.index_of(partition_source)?),
        partition_name.clone(),
    ));
    Ok(Arc::new(ProjectionExec::try_new(output, joined)?))
}

/// Prepare a lookup output while preserving every left partition, its row
/// count, and its input order.
pub(crate) async fn prepare(
    output: &TableRef,
    left: &TableRef,
    right: &TableRef,
    keys: &[ScalarIR],
    field: &str,
    bindings: &GlobalBindings,
    all_matches: bool,
    interval_mode: bool,
    query_interval_mode: bool,
) -> Result<PreparedTable> {
    if keys.len() != right.keys().len() {
        return Err(TableError::Invalid(
            "lookup key arity differs from the right table key".into(),
        ));
    }
    let output_schema = plan::physical_schema(output);
    let left_prepared = crate::partitioned::prepared_input(left)?;
    let right_rows = crate::partitioned::prepared_ordered(right)?.rows;
    let no_external_bindings = GlobalBindings::new();
    let (right_projected, right_key, right_ordinal, right_values, right_marker) = keyed_input(
        right_rows,
        right,
        keys,
        &no_external_bindings,
        "right",
        Some(right.keys()),
        interval_mode || query_interval_mode,
    )?;
    let right_payload_indices = right
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| !right.keys().contains(field.name()))
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    let right_payload = right_payload_indices
        .iter()
        .map(|index| {
            (
                right.schema().field(*index).name().clone(),
                right_values[*index].clone(),
            )
        })
        .collect::<Vec<_>>();
    let (right_reduced, matches_field, join_marker) = if all_matches {
        if interval_mode || query_interval_mode {
            (right_projected, None, right_marker)
        } else {
            let (grouped, name, marker) =
                collect_right(right_projected, &right_key, &right_payload)?;
            (grouped, Some(name), marker)
        }
    } else {
        (
            if interval_mode || query_interval_mode {
                right_projected
            } else {
                reduce_right(right_projected, &right_key, &right_ordinal)?
            },
            None,
            right_marker,
        )
    };

    let range_index = if query_interval_mode && all_matches {
        Some(crate::range_lookup::RangeLookupExec::indexed_right(
            right_reduced.clone(),
            &right_key,
            &right_payload,
            &output_schema,
            field,
        )?)
    } else {
        None
    };
    let mut parts = Vec::with_capacity(left_prepared.parts.len());
    for left_rows in left_prepared.parts {
        let (left_projected, left_key, left_ordinal, left_values, _left_marker) = keyed_input(
            left_rows,
            left,
            keys,
            bindings,
            "left",
            None,
            interval_mode || query_interval_mode,
        )?;
        if query_interval_mode && all_matches {
            parts.push(crate::range_lookup::RangeLookupExec::new(
                left_projected,
                range_index
                    .as_ref()
                    .ok_or_else(|| TableError::Invalid("missing query range index".into()))?
                    .clone(),
                &left_key,
                &left_values,
                output_schema.clone(),
                field,
            )?);
            continue;
        }
        parts.push(join_partition(
            left_projected,
            // Each join has its own right-side consumer. Sharing the Arc
            // alone would make independent joins compete for exchange output.
            crate::partitioned::reset_plan_states(right_reduced.clone())?,
            &left_key,
            &right_key,
            &left_ordinal,
            &right_ordinal,
            &join_marker,
            &right_payload,
            &left_values,
            left,
            field,
            output_schema.clone(),
            all_matches,
            matches_field.as_deref(),
            interval_mode,
            query_interval_mode,
        )?);
    }
    Ok(PreparedTable {
        parts,
        counts: left_prepared.counts,
        schema: output_schema,
    })
}

impl crate::TableIR {
    /// Pack an unchanged leading key prefix without sorting or moving rows.
    /// The key is a present Struct of original key columns in original order,
    /// so lexicographic nulls-last key order is preserved by construction.
    pub fn query_prefix(
        self: &Arc<Self>,
        count: usize,
        key: String,
        payload: String,
    ) -> Result<TableRef> {
        if count == 0 || count > self.keys().len() || key == payload {
            return Err(TableError::Invalid(
                "invalid query key prefix projection".into(),
            ));
        }
        let prefix = Expr::Struct {
            fields: self.keys()[..count]
                .iter()
                .map(|name| (name.clone(), Expr::Column { name: name.clone() }))
                .collect(),
        };
        let row = Expr::Struct {
            fields: self
                .schema()
                .fields()
                .iter()
                .map(|field| {
                    (
                        field.name().clone(),
                        Expr::Column {
                            name: field.name().clone(),
                        },
                    )
                })
                .collect(),
        };
        self.key_by(vec![])?
            .map_rows(vec![(key.clone(), prefix), (payload, row)])?
            .with_keys(vec![key])
    }
}
