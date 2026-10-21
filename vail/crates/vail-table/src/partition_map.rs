//! A partition stream is packed by DataFusion, transformed by expression IR,
//! and unnested by DataFusion. No Python callback runs during execution.
use crate::globals::GlobalBindings;
use crate::plan::{PhysicalPlan, TablePlan, bind_external, bind_globals, column};
use crate::{Result, ScalarIR, TableError, TableRef};
use arrow_schema::{DataType, Field, Schema};
use datafusion_common::{NullHandling, ScalarValue, UnnestOptions, config::ConfigOptions};
use datafusion_physical_expr::ScalarFunctionExpr;
use datafusion_physical_expr::{
    PhysicalExpr, aggregate::AggregateExprBuilder, expressions::Literal,
};
use datafusion_physical_plan::{
    aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy},
    projection::ProjectionExec,
    unnest::{ListUnnest, UnnestExec},
};
use std::sync::Arc;
use vail_execution::expressions::{Type, lower_with_bindings};

/// Conservatively prove a forward traversal that leaves each key unchanged.
/// Unknown expressions intentionally lose ordering metadata without sorting.
pub(crate) fn preserves_keys(body: &ScalarIR, rows_name: &str, keys: &[String]) -> bool {
    match body {
        ScalarIR::Column { name } => name == rows_name,
        ScalarIR::ArrayFilter { value, .. } => preserves_keys(value, rows_name, keys),
        ScalarIR::ArrayMap {
            value,
            parameter,
            body,
        } => {
            if !preserves_keys(value, rows_name, keys) {
                return false;
            }
            match body.as_ref() {
                ScalarIR::LambdaVariable { name, .. } => name == parameter,
                ScalarIR::Struct { fields } => {
                    let forwarded: std::collections::HashSet<&str> = fields
                        .iter()
                        .filter_map(|(name, expr)| match expr {
                            ScalarIR::GetField { value, name: field } if field == name => {
                                match value.as_ref() {
                                    ScalarIR::LambdaVariable { name: variable, .. }
                                        if variable == parameter =>
                                    {
                                        Some(name.as_str())
                                    }
                                    _ => None,
                                }
                            }
                            _ => None,
                        })
                        .collect();
                    keys.iter().all(|key| forwarded.contains(key.as_str()))
                }
                _ => false,
            }
        }
        _ => false,
    }
}

pub(crate) fn lower(
    input: TablePlan,
    child: &TableRef,
    rows_name: &str,
    body: &ScalarIR,
    bindings: &GlobalBindings,
) -> Result<TablePlan> {
    // This callback is planned against one prepared source partition and the
    // AggregateExec below is single-pass/non-partial. Native array_agg keeps
    // input batch and row order, so no ordinal column or per-row sort is needed.
    let numbered = input.rows;
    let schema = numbered.schema();
    let mut args = Vec::<Arc<dyn PhysicalExpr>>::new();
    for field in child.schema().fields() {
        args.push(Arc::new(Literal::new(ScalarValue::Utf8(Some(
            field.name().clone(),
        )))));
        args.push(column(&numbered, schema.index_of(field.name())?));
    }
    let row = Arc::new(ScalarFunctionExpr::try_new(
        datafusion_functions::core::named_struct(),
        args,
        &schema,
        Arc::new(ConfigOptions::default()),
    )?);
    let collected = AggregateExprBuilder::new(
        datafusion_functions_aggregate::array_agg::array_agg_udaf(),
        vec![row],
    )
    .schema(schema.clone())
    .alias(rows_name)
    .build()?;
    let packed: PhysicalPlan = Arc::new(AggregateExec::try_new(
        AggregateMode::Single,
        PhysicalGroupBy::new_single(vec![]),
        vec![Arc::new(collected)],
        vec![None],
        numbered,
        schema,
    )?);
    // Aggregating an empty partition returns a missing array. Stream input is
    // empty, never missing, so replace it with a typed empty Arrow array.
    let array_type = Type::Array {
        array: Box::new(Type::Struct {
            r#struct: child
                .schema()
                .fields()
                .iter()
                .map(|f| Ok((f.name().clone(), Type::from_arrow(f.data_type())?)))
                .collect::<std::result::Result<Vec<_>, vail_execution::expressions::ExprError>>()?,
        }),
    };
    let empty = vail_execution::expressions::lower_with_global_columns(
        &ScalarIR::Literal {
            dtype: array_type.clone(),
            value: serde_json::json!([]),
        },
        &packed.schema(),
        &Schema::empty(),
    )?;
    let original = column(&packed, 0);
    let missing: Arc<dyn PhysicalExpr> = Arc::new(
        datafusion_physical_expr::expressions::IsNullExpr::new(original.clone()),
    );
    let nonnull: Arc<dyn PhysicalExpr> =
        Arc::new(datafusion_physical_expr::expressions::CaseExpr::try_new(
            None,
            vec![(missing, empty)],
            Some(original),
        )?);
    let packed: PhysicalPlan = Arc::new(ProjectionExec::try_new(
        vec![
            (nonnull as Arc<dyn PhysicalExpr>, rows_name.into()),
            (
                Arc::new(Literal::new(ScalarValue::UInt64(Some(0)))) as Arc<dyn PhysicalExpr>,
                "__vail_partition".into(),
            ),
        ],
        packed,
    )?);
    let (bound, indices) = bind_external(bind_globals(packed, child)?, bindings)?;
    let mapped = lower_with_bindings(body, &bound.schema(), child.globals().schema(), &indices)?;
    let dtype = mapped.data_type(&bound.schema())?;
    let DataType::List(item) = dtype else {
        return Err(TableError::Invalid(
            "partition map output must be an array of structs".into(),
        ));
    };
    let DataType::Struct(row_fields) = item.data_type() else {
        return Err(TableError::Invalid(
            "partition map output must be row structs".into(),
        ));
    };
    let row_fields = row_fields.clone();
    let projected: PhysicalPlan = Arc::new(ProjectionExec::try_new(
        vec![
            (mapped, "__vail_partition_rows".into()),
            (
                Arc::new(Literal::new(ScalarValue::UInt64(Some(0)))) as Arc<dyn PhysicalExpr>,
                "__vail_partition".into(),
            ),
        ],
        bound,
    )?);
    let unnested: PhysicalPlan = Arc::new(UnnestExec::new(
        projected,
        vec![ListUnnest {
            index_in_input_schema: 0,
            depth: 1,
        }],
        vec![],
        Arc::new(Schema::new(vec![
            Field::new(
                "__vail_partition_rows",
                DataType::Struct(row_fields.clone()),
                true,
            ),
            Field::new("__vail_partition", DataType::UInt64, false),
        ])),
        UnnestOptions::new().with_null_handling(NullHandling::Drop),
    )?);
    let mut fields = Vec::<(Arc<dyn PhysicalExpr>, String)>::new();
    for field in &row_fields {
        fields.push((
            Arc::new(ScalarFunctionExpr::try_new(
                datafusion_functions::core::get_field(),
                vec![
                    column(&unnested, 0),
                    Arc::new(Literal::new(ScalarValue::Utf8(Some(field.name().clone())))),
                ],
                &unnested.schema(),
                Arc::new(ConfigOptions::default()),
            )?),
            field.name().clone(),
        ));
    }
    fields.push((column(&unnested, 1), "__vail_partition".into()));
    Ok(TablePlan {
        rows: Arc::new(ProjectionExec::try_new(fields, unnested)?),
        layout: input.layout,
    })
}

/// Map/filter/flatmap over partition rows are ordinary relational operators.
/// Packing an entire partition is needed only for operations such as to_array.
pub(crate) fn lower_stream(
    child: &TableRef,
    rows_name: &str,
    body: &ScalarIR,
    bindings: &GlobalBindings,
) -> Result<Option<TableRef>> {
    fn rewrite(
        expr: &ScalarIR,
        parameter: &str,
        row: &ScalarIR,
    ) -> std::result::Result<ScalarIR, vail_execution::expressions::ExprError> {
        if matches!(expr,ScalarIR::LambdaVariable {name, .. } if name==parameter) {
            return Ok(row.clone());
        }
        expr.try_map_children(|child| rewrite(child, parameter, row))
    }
    fn row(table: &TableRef) -> ScalarIR {
        ScalarIR::Struct {
            fields: table
                .schema()
                .fields()
                .iter()
                .map(|f| {
                    (
                        f.name().clone(),
                        ScalarIR::Column {
                            name: f.name().clone(),
                        },
                    )
                })
                .collect(),
        }
    }
    fn lower(
        child: &TableRef,
        body: &ScalarIR,
        rows_name: &str,
        bindings: &GlobalBindings,
    ) -> Result<Option<TableRef>> {
        match body {
            ScalarIR::Column { name } if name == rows_name => Ok(Some(child.clone())),
            ScalarIR::ArrayMap {
                value,
                parameter,
                body,
            } => {
                let Some(input) = lower(child, value, rows_name, bindings)? else {
                    return Ok(None);
                };
                let expression = rewrite(body, parameter, &row(&input))?;
                let Type::Struct { r#struct: fields } =
                    expression.dtype_with_globals(input.schema(), input.globals().schema())?
                else {
                    return Ok(None);
                };
                let projections = match expression {
                    ScalarIR::Struct { fields } => fields,
                    expression => {
                        let value = Arc::new(expression);
                        fields
                            .into_iter()
                            .map(|(name, _)| {
                                let field = ScalarIR::GetField {
                                    value: value.clone(),
                                    name: name.clone(),
                                };
                                (name, field)
                            })
                            .collect()
                    }
                };
                Ok(Some(
                    input
                        .key_by(vec![])?
                        .map_rows_bound(projections, bindings.clone())?,
                ))
            }
            ScalarIR::ArrayFilter {
                value,
                parameter,
                predicate,
            } => {
                let Some(input) = lower(child, value, rows_name, bindings)? else {
                    return Ok(None);
                };
                let predicate = rewrite(predicate, parameter, &row(&input))?;
                Ok(Some(input.filter_bound(
                    predicate,
                    true,
                    bindings.clone(),
                )?))
            }
            ScalarIR::ArrayFlatten { value } => {
                let ScalarIR::ArrayMap {
                    value,
                    parameter,
                    body,
                } = value.as_ref()
                else {
                    return Ok(None);
                };
                let Some(input) = lower(child, value, rows_name, bindings)? else {
                    return Ok(None);
                };
                let expression = rewrite(body, parameter, &row(&input))?;
                let Type::Array { array } =
                    expression.dtype_with_globals(input.schema(), input.globals().schema())?
                else {
                    return Ok(None);
                };
                let Type::Struct { r#struct: fields } = *array else {
                    return Ok(None);
                };
                let mut name = "__vail_flatmap_rows".to_string();
                while input.schema().index_of(&name).is_ok()
                    || input.globals().schema().index_of(&name).is_ok()
                {
                    name.push('_');
                }
                let mapped = input
                    .key_by(vec![])?
                    .map_rows_bound(vec![(name.clone(), expression)], bindings.clone())?
                    .explode(vec![name.clone()])?;
                let projections = fields
                    .into_iter()
                    .map(|(field, _)| {
                        (
                            field.clone(),
                            ScalarIR::GetField {
                                value: Arc::new(ScalarIR::Column { name: name.clone() }),
                                name: field,
                            },
                        )
                    })
                    .collect();
                Ok(Some(mapped.map_rows(projections)?))
            }
            _ => Ok(None),
        }
    }
    lower(child, body, rows_name, bindings)
}
