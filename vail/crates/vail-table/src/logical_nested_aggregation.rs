//! Nested aggregate scopes as ordinary DataFusion logical relations.
use crate::aggregation::resolve;
use crate::logical_aggregation::{body_plan, col, columns, join_scope, unique};
use crate::{Result, TableError, TableRef};
use arrow_schema::{DataType, Field};
use datafusion_common::{NullHandling, ScalarValue, UnnestOptions};
use datafusion_expr::{
    Expr as DFExpr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, expr_fn::ExprFunctionExt, lit,
};
use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};
use vail_execution::expressions::{Expr, Type};
use vail_execution::logical_expr::{
    lower_logical_with_bindings, restore_arrow_array_type, restore_type,
};

fn presence() -> DFExpr {
    datafusion_functions_aggregate::count::count_udaf().call(vec![lit(1i64)])
}

fn collect(
    input: LogicalPlan,
    keys: &[String],
    value: DFExpr,
    dtype: DataType,
    name: &str,
    order: Vec<datafusion_expr::expr::Sort>,
    used: &mut HashSet<String>,
) -> Result<(LogicalPlan, ScalarValue, String)> {
    let physical_type = value.get_type(input.schema())?;
    // DataFusion's ordered array_agg carries sort keys through partial states.
    // A plain collect accumulator cannot merge independently sorted partitions.
    let udf = if order.is_empty() {
        vail_execution::collect_aggregate::collect_udf(&physical_type)
    } else {
        datafusion_functions_aggregate::array_agg::array_agg_udaf()
    };
    let empty = ScalarValue::List(ScalarValue::new_list(&[], &physical_type, true));
    let empty = restore_arrow_array_type(&empty.to_array_of_size(1)?, &dtype)?;
    let empty = vail_execution::expressions::scalar_at(&empty, 0)?;
    let p = unique(used, "__vail_nested_presence");
    let collected = udf.call(vec![value]).order_by(order).build()?.alias(name);
    let result = LogicalPlanBuilder::from(input)
        .aggregate(
            keys.iter().map(col).collect::<Vec<_>>(),
            vec![collected, presence().alias(&p)],
        )?
        .build()?;
    let mut fields = keys.iter().map(col).collect::<Vec<_>>();
    fields.push(restore_type(col(name), dtype).alias(name));
    fields.push(col(&p));
    Ok((
        LogicalPlanBuilder::from(result).project(fields)?.build()?,
        empty,
        p,
    ))
}

pub(crate) fn lower(
    input: LogicalPlan,
    table: &TableRef,
    key_names: &[String],
    group_names: &[String],
    scope: &BTreeMap<String, String>,
    used: &mut HashSet<String>,
    bindings: &BTreeMap<String, usize>,
    expression: &Expr,
    name: &str,
    external: &crate::globals::GlobalBindings,
) -> Result<(LogicalPlan, ScalarValue, String)> {
    match expression {
        Expr::AggGroupBy { key, body } => {
            let key = resolve(key, scope)?;
            let dtype =
                key.dtype_with_globals(input.schema().as_arrow(), table.globals().schema())?;
            dtype.ensure_keyable()?;
            if !dtype.native_key() {
                return Err(TableError::Invalid(
                    "nested grouping requires a native DataFusion key type".into(),
                ));
            }
            let value_name = unique(used, "__vail_subgroup_value");
            let key_name = unique(used, "__vail_subgroup_key");
            let group_name = unique(used, "__vail_subgroup_encoded");
            let mut fields = columns(&input);
            fields.push(
                lower_logical_with_bindings(
                    &key,
                    input.schema(),
                    table.globals().schema(),
                    bindings,
                )?
                .alias(&key_name),
            );
            let input = LogicalPlanBuilder::from(input).project(fields)?.build()?;
            let mut fields = columns(&input);
            fields.push(
                crate::aggregation::logical_group_key(col(&key_name), &dtype.to_arrow()?)?
                    .alias(&group_name),
            );
            let input = LogicalPlanBuilder::from(input).project(fields)?.build()?;
            let mut nested_keys = key_names.to_vec();
            nested_keys.push(key_name.clone());
            let mut nested_groups = group_names.to_vec();
            nested_groups.push(group_name);
            let body = body_plan(
                input,
                table,
                &nested_keys,
                &nested_groups,
                scope,
                used,
                bindings,
                body,
                &value_name,
                external,
            )?;
            let ty = Type::from_arrow(
                body.schema()
                    .field_with_unqualified_name(&value_name)?
                    .data_type(),
            )?;
            let entry = Expr::Struct {
                fields: vec![
                    ("key".into(), Expr::Column { name: key_name }),
                    ("value".into(), Expr::Column { name: value_name }),
                ],
            };
            let entry = lower_logical_with_bindings(
                &entry,
                body.schema(),
                table.globals().schema(),
                &BTreeMap::new(),
            )?;
            collect(
                body,
                group_names,
                entry,
                Type::Dict {
                    dict: Box::new((dtype, ty)),
                }
                .to_arrow()?,
                name,
                vec![],
                used,
            )
        }
        Expr::AggArray {
            value,
            parameter,
            body,
        } => {
            let value = resolve(value, scope)?;
            let dtype = value
                .dtype_with_globals(input.schema().as_arrow(), table.globals().schema())?
                .to_arrow()?;
            let DataType::List(_) = &dtype else {
                return Err(TableError::Invalid("array_agg requires an array".into()));
            };
            let value = lower_logical_with_bindings(
                &value,
                input.schema(),
                table.globals().schema(),
                bindings,
            )?;
            let length_name = unique(used, "__vail_array_length");
            let index_name = unique(used, "__vail_array_index");
            let index_group = unique(used, "__vail_array_index_group");
            let element_name = unique(used, "__vail_array_element");
            let body_name = unique(used, "__vail_array_result");
            let length_presence = unique(used, "__vail_array_presence");
            let lengths = LogicalPlanBuilder::from(input.clone())
                .aggregate(
                    group_names.iter().map(col).collect::<Vec<_>>(),
                    vec![
                        vail_execution::array_length_aggregate::length_udaf(&dtype)
                            .call(vec![value.clone()])
                            .alias(&length_name),
                        presence().alias(&length_presence),
                    ],
                )?
                .build()?;
            let positions = datafusion_functions_nested::range::range_udf().call(vec![
                lit(0i64),
                datafusion_functions_nested::length::array_length_udf()
                    .call(vec![value.clone()])
                    .cast_to(&DataType::Int64, input.schema())?,
            ]);
            let mut fields = columns(&input);
            fields.push(value.alias(&element_name));
            fields.push(positions.alias(&index_name));
            let input = LogicalPlanBuilder::from(input)
                .project(fields)?
                .unnest_columns_with_options(
                    vec![element_name.as_str().into(), index_name.as_str().into()],
                    UnnestOptions::new().with_null_handling(NullHandling::Drop),
                )?
                .build()?;
            let mut fields = columns(&input);
            fields.push(col(&index_name).alias(&index_group));
            let input = LogicalPlanBuilder::from(input).project(fields)?.build()?;
            let mut nested_keys = key_names.to_vec();
            nested_keys.push(index_name.clone());
            let mut nested_groups = group_names.to_vec();
            nested_groups.push(index_group);
            let mut scope = scope.clone();
            scope.insert(parameter.clone(), element_name);
            let body = body_plan(
                input,
                table,
                &nested_keys,
                &nested_groups,
                &scope,
                used,
                bindings,
                body,
                &body_name,
                external,
            )?;
            let ty = body
                .schema()
                .field_with_unqualified_name(&body_name)?
                .data_type()
                .clone();
            let dtype = DataType::List(Arc::new(Field::new("item", ty, true)));
            let (values, empty, p) = collect(
                body,
                group_names,
                col(&body_name),
                dtype.clone(),
                name,
                vec![col(&index_name).sort(true, true)],
                used,
            )?;
            let joined = join_scope(
                lengths,
                values,
                group_names,
                &[(name.into(), empty)],
                &p,
                used,
            )?;
            let null = ScalarValue::try_from(&dtype)?;
            let output =
                datafusion_expr::expr_fn::when(col(&length_name).is_null(), lit(null.clone()))
                    .otherwise(col(name))?;
            let mut fields = group_names.iter().map(col).collect::<Vec<_>>();
            fields.push(output.alias(name));
            fields.push(col(&length_presence));
            Ok((
                LogicalPlanBuilder::from(joined).project(fields)?.build()?,
                null,
                length_presence,
            ))
        }
        _ => Err(TableError::Invalid(
            "unsupported nested aggregate expression".into(),
        )),
    }
}
