//! Lower Vail scalar IR into DataFusion's optimizable logical expression tree.
//!
//! Ordinary scalar operations remain visible as columns, literals, casts,
//! binary expressions, CASE expressions, and nested field access. This is the
//! expression boundary used by Vail logical plans; physical-only kernels stay
//! behind explicit errors until they have a logical UDF contract.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_schema::Schema;
use datafusion_common::{DFSchema, ScalarValue};
use datafusion_expr::{Expr as LogicalExpr, ExprSchemable, Operator, Signature, Volatility, lit};

use crate::expressions::{BinaryOperator, Expr, ExprError, PrimitiveType, Type, UnaryOperator};

/// Lower a Vail expression against a logical input schema and global bindings.
///
/// Binding indices for external globals address `schema` (the joined logical
/// input), so generated references are ordinary DataFusion columns and remain
/// visible to optimizer rules.
pub fn lower_logical_with_bindings(
    expr: &Expr,
    schema: &DFSchema,
    globals: &Schema,
    bindings: &BTreeMap<String, usize>,
) -> Result<LogicalExpr, ExprError> {
    lower(expr, schema, globals, bindings, &[])
}

/// Convenience form for expressions without globals.
pub fn lower_logical(expr: &Expr, schema: &DFSchema) -> Result<LogicalExpr, ExprError> {
    lower_logical_with_bindings(expr, schema, &Schema::empty(), &BTreeMap::new())
}

/// Change compatible Arrow descriptors without casting or copying value buffers.
pub fn restore_arrow_array_type(
    array: &arrow_array::ArrayRef,
    dtype: &arrow_schema::DataType,
) -> datafusion_common::Result<arrow_array::ArrayRef> {
    if array.data_type() == dtype {
        return Ok(array.clone());
    }
    if vail_ordering::same_layout(array.data_type(), dtype) {
        fn retag_data(
            data: arrow_data::ArrayData,
            dtype: &arrow_schema::DataType,
        ) -> datafusion_common::Result<arrow_data::ArrayData> {
            let child_types = match dtype {
                arrow_schema::DataType::List(field)
                | arrow_schema::DataType::LargeList(field)
                | arrow_schema::DataType::FixedSizeList(field, _) => vec![field.data_type()],
                arrow_schema::DataType::Struct(fields) => {
                    fields.iter().map(|field| field.data_type()).collect()
                }
                _ => Vec::new(),
            };
            let children = data
                .child_data()
                .iter()
                .zip(child_types)
                .map(|(child, child_type)| retag_data(child.clone(), child_type))
                .collect::<datafusion_common::Result<Vec<_>>>()?;
            Ok(data
                .into_builder()
                .data_type(dtype.clone())
                .child_data(children)
                .build()?)
        }
        return Ok(arrow_array::make_array(retag_data(array.to_data(), dtype)?));
    }
    Ok(arrow_cast::cast(array.as_ref(), dtype)?)
}

/// Restore an IR-declared type after a library operator constructs new Arrow
/// fields. Volatile prevents constant folding from stripping list metadata.
pub fn restore_type(value: LogicalExpr, dtype: arrow_schema::DataType) -> LogicalExpr {
    let output = dtype.clone();
    let udf = datafusion_expr::ScalarUDF::from(
        datafusion_expr::expr_fn::SimpleScalarUDF::new_with_signature(
            "vail_restore_arrow_type",
            Signature::any(1, Volatility::Volatile),
            dtype,
            Arc::new(move |args| {
                let scalar = matches!(&args[0], datafusion_expr::ColumnarValue::Scalar(_));
                let array = args[0].clone().into_array(match &args[0] {
                    datafusion_expr::ColumnarValue::Array(a) => a.len(),
                    _ => 1,
                })?;
                let array = restore_arrow_array_type(&array, &output)?;
                if scalar {
                    Ok(datafusion_expr::ColumnarValue::Scalar(
                        if let Some(list) = array.as_any().downcast_ref::<arrow_array::ListArray>()
                        {
                            ScalarValue::List(Arc::new(list.slice(0, 1)))
                        } else if let Some(record) =
                            array.as_any().downcast_ref::<arrow_array::StructArray>()
                        {
                            ScalarValue::Struct(Arc::new(record.slice(0, 1)))
                        } else {
                            ScalarValue::try_from_array(&array, 0)?
                        },
                    ))
                } else {
                    Ok(datafusion_expr::ColumnarValue::Array(array))
                }
            }),
        ),
    );
    udf.call(vec![value])
}

fn coerce_type(
    value: LogicalExpr,
    dtype: arrow_schema::DataType,
    schema: &DFSchema,
) -> Result<LogicalExpr, ExprError> {
    let actual = value.get_type(schema)?;
    if actual == dtype {
        return Ok(value);
    }
    if vail_ordering::same_layout(&actual, &dtype) {
        return Ok(restore_type(value, dtype));
    }
    Ok(value.cast_to(&dtype, schema)?)
}

fn lower(
    expr: &Expr,
    schema: &DFSchema,
    globals: &Schema,
    bindings: &BTreeMap<String, usize>,
    scope: &[String],
) -> Result<LogicalExpr, ExprError> {
    let recurse = |e: &Expr| lower(e, schema, globals, bindings, scope);
    Ok(match expr {
        Expr::MatrixScope { .. }
        | Expr::TableLookup { .. }
        | Expr::MatrixField { .. }
        | Expr::MatrixNumCols { .. }
        | Expr::MatrixColumnPosition { .. } => {
            return Err(ExprError::Invalid(
                "unresolved frontend expression scope".into(),
            ));
        }
        Expr::RuntimeLiteral { value } => lit(value.clone()),
        Expr::Literal { dtype, value } => {
            if !matches!(dtype, Type::Primitive(_)) {
                return logical_kernel(expr, schema, globals, bindings, scope);
            }
            lit(literal_scalar(value, dtype)?)
        }
        Expr::Column { name } => {
            let start = globals.fields().len();
            let matches = (start..schema.fields().len())
                .filter(|&i| schema.field(i).name() == name)
                .collect::<Vec<_>>();
            let index = if matches.len() == 1 {
                Some(matches[0])
            } else {
                None
            }
            .ok_or_else(|| {
                ExprError::Invalid(format!("source column {name:?} is absent or ambiguous"))
            })?;
            let (qualifier, field) = schema.qualified_field(index);
            LogicalExpr::Column(datafusion_common::Column::new(
                qualifier.cloned(),
                field.name().clone(),
            ))
        }
        Expr::Global { name } => {
            let index = globals
                .index_of(name)
                .map_err(|_| ExprError::Invalid(format!("global column {name:?} is absent")))?;
            let field = globals.field(index);
            let logical_index = index;
            if logical_index >= schema.fields().len() {
                return Err(ExprError::Invalid(format!(
                    "global column {name:?} is not bound in input"
                )));
            }
            let (qualifier, actual) = schema.qualified_field(logical_index);
            if actual.data_type() != field.data_type() {
                return Err(ExprError::Invalid(format!(
                    "global column {name:?} type differs from its declaration"
                )));
            }
            LogicalExpr::Column(datafusion_common::Column::new(
                qualifier.cloned(),
                actual.name().clone(),
            ))
        }
        Expr::ExternalGlobal { reference, dtype } => {
            let index = *bindings.get(reference).ok_or_else(|| {
                ExprError::Invalid(format!("unbound external global {reference:?}"))
            })?;
            if index >= schema.fields().len() {
                return Err(ExprError::Invalid(format!(
                    "external global column {reference:?} is absent"
                )));
            }
            let (qualifier, field) = schema.qualified_field(index);
            let actual = Type::from_arrow(field.data_type())?;
            if actual != *dtype {
                return Err(ExprError::Invalid(format!(
                    "external global type mismatch: expected {dtype:?}, found {actual:?}"
                )));
            }
            LogicalExpr::Column(datafusion_common::Column::new(
                qualifier.cloned(),
                field.name().clone(),
            ))
        }
        Expr::LambdaVariable { name } => {
            if !scope.iter().any(|bound| bound == name) {
                return Err(ExprError::Invalid(format!(
                    "unbound lambda variable {name:?}"
                )));
            }
            return Err(ExprError::Invalid(
                "lambda variables require a DataFusion higher-order expression".into(),
            ));
        }
        Expr::Binary {
            operator,
            left,
            right,
        } => {
            if matches!(
                operator,
                BinaryOperator::Eq
                    | BinaryOperator::Ne
                    | BinaryOperator::Lt
                    | BinaryOperator::Le
                    | BinaryOperator::Gt
                    | BinaryOperator::Ge
            ) {
                let left_type = left.dtype_with_globals(&schema.as_arrow(), globals)?;
                let right_type = right.dtype_with_globals(&schema.as_arrow(), globals)?;
                let common = logical_promote(&left_type, &right_type)?;
                if matches!(operator, BinaryOperator::Eq | BinaryOperator::Ne)
                    && common.contains_unordered()
                {
                    return Err(ExprError::Unsupported(
                        "Set/Dictionary equality requires logical collection operators".into(),
                    ));
                }
                if !matches!(operator, BinaryOperator::Eq | BinaryOperator::Ne) {
                    common.ensure_keyable()?;
                    if !common.native_key() {
                        return Err(ExprError::Unsupported(format!(
                            "ordering comparison for {common:?}"
                        )));
                    }
                }
                let common_arrow = common.to_arrow()?;
                let left = recurse(left)?.cast_to(&common_arrow, schema)?;
                let right = recurse(right)?.cast_to(&common_arrow, schema)?;
                datafusion_expr::expr_fn::binary_expr(left, binary_operator(*operator)?, right)
            } else if *operator == BinaryOperator::Pow {
                let dtype = expr
                    .dtype_with_globals(&schema.as_arrow(), globals)?
                    .to_arrow()?;
                datafusion_functions::math::power().call(vec![
                    recurse(left)?.cast_to(&dtype, schema)?,
                    recurse(right)?.cast_to(&dtype, schema)?,
                ])
            } else if *operator == BinaryOperator::Floordiv
                && matches!(
                    expr.dtype_with_globals(&schema.as_arrow(), globals)?,
                    Type::Primitive(PrimitiveType::Float32 | PrimitiveType::Float64)
                )
            {
                let dtype = expr
                    .dtype_with_globals(&schema.as_arrow(), globals)?
                    .to_arrow()?;
                let divided = datafusion_expr::expr_fn::binary_expr(
                    recurse(left)?.cast_to(&arrow_schema::DataType::Float64, schema)?,
                    Operator::Divide,
                    recurse(right)?.cast_to(&arrow_schema::DataType::Float64, schema)?,
                );
                datafusion_functions::math::floor()
                    .call(vec![divided])
                    .cast_to(&dtype, schema)?
            } else if matches!(
                operator,
                BinaryOperator::Floordiv | BinaryOperator::Mod | BinaryOperator::Pow
            ) {
                return logical_kernel(expr, schema, globals, bindings, scope);
            } else {
                let result_type = expr
                    .dtype_with_globals(&schema.as_arrow(), globals)?
                    .to_arrow()?;
                let mut left_expr = recurse(left)?;
                let mut right_expr = recurse(right)?;
                if *operator == BinaryOperator::Div {
                    left_expr = left_expr.cast_to(&result_type, schema)?;
                    right_expr = right_expr.cast_to(&result_type, schema)?;
                } else if matches!(
                    operator,
                    BinaryOperator::Add | BinaryOperator::Sub | BinaryOperator::Mul
                ) {
                    left_expr = left_expr.cast_to(&result_type, schema)?;
                    right_expr = right_expr.cast_to(&result_type, schema)?;
                }
                datafusion_expr::expr_fn::binary_expr(
                    left_expr,
                    binary_operator(*operator)?,
                    right_expr,
                )
            }
        }
        Expr::IntegerBinary {
            operator,
            left,
            right,
        } => datafusion_expr::expr_fn::binary_expr(
            recurse(left)?,
            binary_operator(*operator)?,
            recurse(right)?,
        ),
        Expr::Unary { operator, value } => {
            let value = recurse(value)?;
            match operator {
                UnaryOperator::Neg => LogicalExpr::Negative(Box::new(value)),
                UnaryOperator::Pos => value,
                UnaryOperator::Not => LogicalExpr::Not(Box::new(value)),
            }
        }
        Expr::Cast {
            dtype: Type::Primitive(PrimitiveType::Call),
            ..
        } => return logical_kernel(expr, schema, globals, bindings, scope),
        Expr::Cast { dtype, value } => {
            let source = value.dtype_with_globals(&schema.as_arrow(), globals)?;
            let fallible = matches!(source, Type::Primitive(PrimitiveType::Str))
                && source != *dtype
                || matches!(
                    source,
                    Type::Primitive(PrimitiveType::Float32 | PrimitiveType::Float64)
                ) && matches!(
                    dtype,
                    Type::Primitive(PrimitiveType::Int32 | PrimitiveType::Int64)
                )
                || matches!(source, Type::Primitive(PrimitiveType::Int64))
                    && matches!(dtype, Type::Primitive(PrimitiveType::Int32));
            // A checked cast must not move before a preceding row filter:
            // discarded headers and other excluded values need not parse.
            // The semantic kernel still executes DataFusion's native cast.
            if fallible {
                return logical_kernel(expr, schema, globals, bindings, scope);
            }
            coerce_type(recurse(value)?, dtype.to_arrow()?, schema)?
        }
        Expr::IsMissing { value } => recurse(value)?.is_null(),
        Expr::IfElse {
            condition,
            consequent,
            alternate,
        } => {
            let dtype = expr
                .dtype_with_globals(&schema.as_arrow(), globals)?
                .to_arrow()?;
            let condition = recurse(condition)?;
            let consequent = coerce_type(recurse(consequent)?, dtype.clone(), schema)?;
            let alternate = coerce_type(recurse(alternate)?, dtype.clone(), schema)?;
            let null_result = lit(ScalarValue::try_from(&dtype)?);
            datafusion_expr::expr_fn::when(condition.clone().is_null(), null_result)
                .when(condition, consequent)
                .otherwise(alternate)?
        }
        Expr::Struct { fields } => {
            if fields.is_empty() {
                return logical_kernel(expr, schema, globals, bindings, scope);
            }
            let mut args = Vec::with_capacity(fields.len() * 2);
            for (name, value) in fields {
                args.push(lit(ScalarValue::Utf8(Some(name.clone()))));
                args.push(lower(value, schema, globals, bindings, scope)?);
            }
            datafusion_functions::core::named_struct().call(args)
        }
        Expr::Tuple { elements } => {
            let mut args = Vec::with_capacity(elements.len() * 2);
            for (i, value) in elements.iter().enumerate() {
                args.push(lit(ScalarValue::Utf8(Some(format!("__vail_tuple_{i}")))));
                args.push(recurse(value)?);
            }
            if elements.is_empty() {
                args.extend([
                    lit(ScalarValue::Utf8(Some("__vail_tuple_empty".into()))),
                    lit(false),
                ]);
            }
            let dtype = expr
                .dtype_with_globals(schema.as_arrow(), globals)?
                .to_arrow()?;
            coerce_type(
                datafusion_functions::core::named_struct().call(args),
                dtype,
                schema,
            )?
        }
        Expr::GetField { .. } => return logical_kernel(expr, schema, globals, bindings, scope),
        Expr::NestedFunction { function, args, .. } => {
            if function.starts_with("random_") {
                return logical_kernel(expr, schema, globals, bindings, scope);
            }
            let udf = crate::expressions::nested_function(function)?;
            let arguments = args
                .iter()
                .map(|arg| {
                    let ty = arg
                        .dtype_with_globals(schema.as_arrow(), globals)?
                        .to_arrow()?;
                    coerce_type(
                        recurse(arg)?,
                        vail_ordering::without_type_metadata(&ty),
                        schema,
                    )
                })
                .collect::<Result<Vec<_>, ExprError>>()?;
            let dtype = expr
                .dtype_with_globals(schema.as_arrow(), globals)?
                .to_arrow()?;
            restore_type(udf.call(arguments), dtype)
        }
        Expr::StringFunction { function, args } => {
            // Regex compilation and checked Unicode adapters can fail. Preserve
            // the existing row-evaluation barrier across optimizer rewrites.
            if matches!(
                function.as_str(),
                "regexp_like"
                    | "regexp_replace"
                    | "regex_split"
                    | "first_match_in"
                    | "unicode_slice"
                    | "unicode_translate"
            ) {
                return logical_kernel(expr, schema, globals, bindings, scope);
            }
            let (udf, _, _) = crate::expressions::string_function(function)?;
            udf.call(args.iter().map(recurse).collect::<Result<Vec<_>, _>>()?)
        }
        Expr::CollectionCast { value, dtype } => restore_type(recurse(value)?, dtype.to_arrow()?),
        Expr::ToString { value } => {
            let ty = value.dtype_with_globals(&schema.as_arrow(), globals)?;
            let value = recurse(value)?;
            if matches!(ty, Type::Primitive(PrimitiveType::Str)) {
                value
            } else if matches!(
                ty,
                Type::Primitive(PrimitiveType::Int32 | PrimitiveType::Int64 | PrimitiveType::Bool)
            ) {
                value.cast_to(&arrow_schema::DataType::Utf8, schema)?
            } else {
                return logical_kernel(expr, schema, globals, bindings, scope);
            }
        }
        Expr::Array { elements, .. } => {
            let args = elements
                .iter()
                .map(|element| {
                    let ty = element
                        .dtype_with_globals(schema.as_arrow(), globals)?
                        .to_arrow()?;
                    coerce_type(
                        recurse(element)?,
                        vail_ordering::without_type_metadata(&ty),
                        schema,
                    )
                })
                .collect::<Result<Vec<_>, ExprError>>()?;
            let value = datafusion_functions_nested::make_array::make_array_udf().call(args);
            restore_type(
                value,
                expr.dtype_with_globals(schema.as_arrow(), globals)?
                    .to_arrow()?,
            )
        }
        Expr::ArrayLength { value } => datafusion_functions_nested::length::array_length_udf()
            .call(vec![recurse(value)?])
            .cast_to(
                &expr
                    .dtype_with_globals(&schema.as_arrow(), globals)?
                    .to_arrow()?,
                schema,
            )?,
        Expr::ArrayFlatten { value } => {
            datafusion_functions_nested::flatten::flatten_udf().call(vec![recurse(value)?])
        }
        Expr::StreamRange { start, stop, step } => {
            let int64 = arrow_schema::DataType::Int64;
            let start = recurse(start)?.cast_to(&int64, schema)?;
            let stop = recurse(stop)?.cast_to(&int64, schema)?;
            let step = recurse(step)?.cast_to(&int64, schema)?;
            datafusion_functions_nested::range::range_udf()
                .call(vec![start, stop, step])
                .cast_to(
                    &expr
                        .dtype_with_globals(&schema.as_arrow(), globals)?
                        .to_arrow()?,
                    schema,
                )?
        }
        Expr::Aggregate { .. }
        | Expr::AggregateFold { .. }
        | Expr::AggGroupBy { .. }
        | Expr::AggArray { .. }
        | Expr::AggFilter { .. }
        | Expr::AggExplode { .. } => {
            return Err(ExprError::Invalid(
                "aggregation expressions require the DataFusion aggregate planner".into(),
            ));
        }
        Expr::Die { .. }
        | Expr::ArrayFold { .. }
        | Expr::ArrayMap { .. }
        | Expr::ArrayPredicate { .. }
        | Expr::ArrayFilter { .. }
        | Expr::ArrayGrouped { .. }
        | Expr::Interval { .. }
        | Expr::IntervalContains { .. }
        | Expr::IntervalOverlaps { .. }
        | Expr::ArrayIndex { .. }
        | Expr::ArraySlice { .. }
        | Expr::ArrayReduce { .. }
        | Expr::Locus { .. }
        | Expr::LocusOp { .. }
        | Expr::LocusValid { .. }
        | Expr::LocusFromGlobalPosition { .. }
        | Expr::ParseLocus { .. }
        | Expr::ParseCsvLine { .. } => {
            return logical_kernel(expr, schema, globals, bindings, scope);
        }
    })
}

/// Keep uncommon Hail semantics in an explicit scalar UDF while binding every
/// source value as a UDF argument. Ordinary parent operators therefore remain
/// optimizer-visible, and column dependencies remain visible beneath the call.
fn logical_kernel(
    expr: &Expr,
    schema: &DFSchema,
    globals: &Schema,
    bindings: &BTreeMap<String, usize>,
    scope: &[String],
) -> Result<LogicalExpr, ExprError> {
    use std::collections::BTreeMap as Map;
    let arrow_schema = schema.as_arrow();
    let mut refs: Map<String, (Expr, Type, arrow_schema::DataType)> = Map::new();
    expr.visit(&mut |node| {
        let item = match node {
            Expr::Column { name } => {
                let start = globals.fields().len();
                let matches = (start..schema.fields().len())
                    .filter(|&i| schema.field(i).name() == name)
                    .collect::<Vec<_>>();
                let Some(index) = (matches.len() == 1).then(|| matches[0]) else {
                    return;
                };
                let (qualifier, field) = schema.qualified_field(index);
                (
                    format!("column:{qualifier:?}:{}", field.name()),
                    node.clone(),
                    Type::from_arrow(field.data_type()),
                    field.data_type().clone(),
                )
            }
            Expr::Global { name } => {
                let Ok(index) = globals.index_of(name) else {
                    return;
                };
                if index >= schema.fields().len() {
                    return;
                }
                let input_index = index;
                let (qualifier, field) = schema.qualified_field(input_index);
                let Ok(ty) = Type::from_arrow(field.data_type()) else {
                    return;
                };
                (
                    format!("global:{qualifier:?}:{}", field.name()),
                    node.clone(),
                    Ok(ty),
                    field.data_type().clone(),
                )
            }
            Expr::ExternalGlobal { reference, .. } => {
                let Some(index) = bindings.get(reference).copied() else {
                    return;
                };
                if index >= schema.fields().len() {
                    return;
                }
                let (_, field) = schema.qualified_field(index);
                let Ok(ty) = Type::from_arrow(field.data_type()) else {
                    return;
                };
                (
                    format!("external:{reference}"),
                    node.clone(),
                    Ok(ty),
                    field.data_type().clone(),
                )
            }
            _ => return,
        };
        if let Ok(dtype) = item.2 {
            refs.entry(item.0).or_insert((item.1, dtype, item.3));
        }
    });
    let mut fields = Vec::with_capacity(refs.len());
    let mut logical_args = Vec::with_capacity(refs.len());
    let mut slots = Map::new();
    for (i, (key, (reference, _ty, actual_type))) in refs.iter().enumerate() {
        let slot = format!("__vail_arg_{i}");
        slots.insert(key.clone(), slot.clone());
        fields.push(arrow_schema::Field::new(&slot, actual_type.clone(), true));
        logical_args.push(lower(reference, schema, globals, bindings, scope)?);
    }
    fn replace_refs(
        expr: &Expr,
        slots: &Map<String, String>,
        schema: &DFSchema,
        globals: &Schema,
        bindings: &BTreeMap<String, usize>,
    ) -> Result<Expr, ExprError> {
        let key = match expr {
            Expr::Column { name } => {
                let start = globals.fields().len();
                let matches = (start..schema.fields().len())
                    .filter(|&i| schema.field(i).name() == name)
                    .collect::<Vec<_>>();
                let index = if matches.len() == 1 {
                    Some(matches[0])
                } else {
                    None
                }
                .ok_or_else(|| {
                    ExprError::Invalid(format!("source column {name:?} is absent or ambiguous"))
                })?;
                let (q, field) = schema.qualified_field(index);
                format!("column:{q:?}:{}", field.name())
            }
            Expr::Global { name } => {
                let index = globals
                    .index_of(name)
                    .map_err(|_| ExprError::Invalid(format!("global {name:?} is absent")))?;
                let input = index;
                if input >= schema.fields().len() {
                    return Err(ExprError::Invalid(format!(
                        "global {name:?} is not bound in input"
                    )));
                }
                let (q, field) = schema.qualified_field(input);
                format!("global:{q:?}:{}", field.name())
            }
            Expr::ExternalGlobal { reference, .. } => {
                let index = *bindings.get(reference).ok_or_else(|| {
                    ExprError::Invalid(format!("unbound external global {reference:?}"))
                })?;
                if index >= schema.fields().len() {
                    return Err(ExprError::Invalid(format!(
                        "external global {reference:?} is absent"
                    )));
                }
                let _ = schema.qualified_field(index);
                format!("external:{reference}")
            }
            _ => {
                return expr.try_map_children(|child| {
                    replace_refs(child, slots, schema, globals, bindings)
                });
            }
        };
        let slot = slots
            .get(&key)
            .ok_or_else(|| ExprError::Invalid(format!("missing logical kernel binding {key}")))?;
        Ok(Expr::Column { name: slot.clone() })
    }

    let rewritten = replace_refs(expr, &slots, schema, globals, bindings)?;
    let kernel_schema = Schema::new(fields);
    let kernel = crate::expressions::lower(&rewritten, &kernel_schema)?;
    let output_type = expr
        .dtype_with_globals(&arrow_schema, globals)?
        .to_arrow()?;
    let kernel_schema = Arc::new(kernel_schema);
    static NEXT_KERNEL_ID: AtomicU64 = AtomicU64::new(0);
    let udf_name = format!(
        "vail_semantic_kernel_{}_{}",
        kernel_label(expr),
        NEXT_KERNEL_ID.fetch_add(1, Ordering::Relaxed)
    );
    let expected_output_type = output_type.clone();
    let signature = if refs.is_empty() {
        Signature::nullary(Volatility::Volatile)
    } else {
        Signature::any(refs.len(), Volatility::Volatile)
    };
    let udf = datafusion_expr::ScalarUDF::from(
        datafusion_expr::expr_fn::SimpleScalarUDF::new_with_signature(
            &udf_name,
            signature,
            output_type,
            Arc::new(move |args| {
                let row_count = args
                    .iter()
                    .find_map(|value| match value {
                        datafusion_expr::ColumnarValue::Array(array) => Some(array.len()),
                        datafusion_expr::ColumnarValue::Scalar(_) => None,
                    })
                    .unwrap_or(1);
                let arrays = args
                    .iter()
                    .zip(kernel_schema.fields())
                    .map(|(value, field)| {
                        let array = value.clone().into_array(row_count)?;
                        Ok(arrow_cast::cast(&array, field.data_type())?)
                    })
                    .collect::<datafusion_common::Result<Vec<_>>>()?;
                let batch = arrow_array::RecordBatch::try_new_with_options(
                    Arc::clone(&kernel_schema),
                    arrays,
                    &arrow_array::RecordBatchOptions::new().with_row_count(Some(row_count)),
                )?;
                let output = kernel.evaluate(&batch)?.into_array(row_count)?;
                let output = arrow_cast::cast(output.as_ref(), &expected_output_type)?;
                if args.is_empty() && row_count > 0 {
                    Ok(datafusion_expr::ColumnarValue::Scalar(
                        crate::expressions::scalar_at(&output, 0).map_err(|e| {
                            datafusion_common::DataFusionError::Execution(e.to_string())
                        })?,
                    ))
                } else {
                    Ok(datafusion_expr::ColumnarValue::Array(Arc::new(output)))
                }
            }),
        ),
    );
    Ok(udf.call(logical_args))
}

fn kernel_label(expr: &Expr) -> &'static str {
    match expr {
        Expr::Die { .. } => "die",
        Expr::ArrayFold { scan: true, .. } => "array_scan",
        Expr::ArrayFold { scan: false, .. } => "array_fold",
        Expr::ArrayMap { .. } => "array_transform",
        Expr::ArrayPredicate { first: true, .. } => "array_find",
        Expr::ArrayPredicate { first: false, .. } => "array_any",
        Expr::ArrayFilter { .. } => "array_filter",
        Expr::ArrayGrouped { .. } => "array_grouped",
        Expr::Interval { .. } => "interval",
        Expr::IntervalContains { .. } => "interval_contains",
        Expr::IntervalOverlaps { .. } => "interval_overlaps",
        Expr::ArrayIndex { .. } => "array_index",
        Expr::ArraySlice { .. } => "array_slice",
        Expr::ArrayReduce { .. } => "array_reduce",
        Expr::Locus { .. } => "locus",
        Expr::LocusOp { operation, .. } => match operation.as_str() {
            "contig" => "locus_contig",
            "position" => "locus_position",
            "global_position" => "locus_global_position",
            _ => "locus_operation",
        },
        Expr::LocusValid { .. } => "locus_valid",
        Expr::LocusFromGlobalPosition { .. } => "locus_from_global_position",
        Expr::ParseLocus { .. } => "parse_locus",
        Expr::ParseCsvLine { .. } => "parse_csv_line",
        Expr::ToString { .. } => "hail_to_string",
        Expr::Struct { fields } if fields.is_empty() => "empty_struct",
        Expr::GetField { .. } => "get_field",
        Expr::Binary { operator, .. } => match operator {
            BinaryOperator::Floordiv => "floor_divide",
            BinaryOperator::Mod => "modulo",
            BinaryOperator::Pow => "power",
            BinaryOperator::Add => "integer_add",
            BinaryOperator::Sub => "integer_subtract",
            BinaryOperator::Mul => "integer_multiply",
            _ => "binary_operation",
        },
        _ => "hail_operation",
    }
}

fn binary_operator(op: BinaryOperator) -> Result<Operator, ExprError> {
    Ok(match op {
        BinaryOperator::Add => Operator::Plus,
        BinaryOperator::Sub => Operator::Minus,
        BinaryOperator::Mul => Operator::Multiply,
        BinaryOperator::Div => Operator::Divide,
        BinaryOperator::Eq => Operator::Eq,
        BinaryOperator::Ne => Operator::NotEq,
        BinaryOperator::Lt => Operator::Lt,
        BinaryOperator::Le => Operator::LtEq,
        BinaryOperator::Gt => Operator::Gt,
        BinaryOperator::Ge => Operator::GtEq,
        BinaryOperator::And => Operator::And,
        BinaryOperator::Or => Operator::Or,
        BinaryOperator::Floordiv | BinaryOperator::Mod | BinaryOperator::Pow => {
            return Err(ExprError::Invalid(format!(
                "operator {op:?} needs a Vail logical kernel"
            )));
        }
    })
}

fn logical_promote(left: &Type, right: &Type) -> Result<Type, ExprError> {
    if left == right {
        return Ok(left.clone());
    }
    let rank = |ty: &Type| match ty {
        Type::Primitive(PrimitiveType::Int32) => Some(0),
        Type::Primitive(PrimitiveType::Int64) => Some(1),
        Type::Primitive(PrimitiveType::Float32) => Some(2),
        Type::Primitive(PrimitiveType::Float64) => Some(3),
        _ => None,
    };
    match (rank(left), rank(right)) {
        (Some(a), Some(b)) => Ok(match a.max(b) {
            0 => Type::Primitive(PrimitiveType::Int32),
            1 => Type::Primitive(PrimitiveType::Int64),
            2 => Type::Primitive(PrimitiveType::Float32),
            _ => Type::Primitive(PrimitiveType::Float64),
        }),
        _ => Err(ExprError::Invalid(format!(
            "incompatible logical comparison types {left:?} and {right:?}"
        ))),
    }
}

fn literal_scalar(value: &serde_json::Value, dtype: &Type) -> Result<ScalarValue, ExprError> {
    let arr = crate::expressions::literal_array(value, dtype, 1)?;
    crate::expressions::scalar_at(&arr, 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, BooleanArray, Float64Array, Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
    use serde_json::json;

    fn schema() -> DFSchema {
        DFSchema::try_from(Schema::new(vec![
            Field::new("x", DataType::Float64, true),
            Field::new("y", DataType::Int64, true),
        ]))
        .unwrap()
    }

    #[test]
    fn ordinary_binary_expression_stays_visible_to_optimizer() {
        let expr = Expr::Binary {
            operator: BinaryOperator::Add,
            left: Arc::new(Expr::Column { name: "x".into() }),
            right: Arc::new(Expr::Literal {
                dtype: Type::Primitive(PrimitiveType::Float64),
                value: json!(4.0),
            }),
        };
        let lowered = lower_logical(&expr, &schema()).unwrap();
        let mut saw_column = false;
        let mut saw_literal = false;
        lowered
            .apply(|node| {
                match node {
                    LogicalExpr::Column(_) => saw_column = true,
                    LogicalExpr::Literal(_, _) => saw_literal = true,
                    _ => {}
                }
                Ok(TreeNodeRecursion::Continue)
            })
            .unwrap();
        assert!(matches!(lowered, LogicalExpr::BinaryExpr(_)));
        assert!(saw_column && saw_literal);
    }

    #[test]
    fn equality_remains_a_native_binary_expression() {
        let schema = schema();
        let expr = Expr::Binary {
            operator: BinaryOperator::Eq,
            left: Arc::new(Expr::Column { name: "x".into() }),
            right: Arc::new(Expr::Literal {
                dtype: Type::Primitive(PrimitiveType::Float64),
                value: serde_json::json!(1.0),
            }),
        };
        let lowered = lower_logical(&expr, &schema).unwrap();
        assert!(matches!(lowered, LogicalExpr::BinaryExpr(_)));
    }

    #[test]
    fn set_equality_reports_missing_logical_collection_support() {
        let schema = DFSchema::empty();
        let set = Expr::Literal {
            dtype: Type::Set {
                set: Box::new(Type::Primitive(PrimitiveType::Int32)),
            },
            value: json!([1]),
        };
        let expr = Expr::Binary {
            operator: BinaryOperator::Eq,
            left: Arc::new(set.clone()),
            right: Arc::new(set),
        };
        assert!(matches!(
            lower_logical(&expr, &schema),
            Err(ExprError::Unsupported(message)) if message.contains("Set/Dictionary equality")
        ));
    }

    #[test]
    fn distinct_physical_kernels_have_distinct_optimizer_identities() {
        let schema = DFSchema::empty();
        let make_die = |message| Expr::Die {
            message: Arc::new(Expr::Literal {
                dtype: Type::Primitive(PrimitiveType::Str),
                value: json!(message),
            }),
            dtype: Type::Primitive(PrimitiveType::Int64),
        };
        let first = lower_logical(&make_die("first"), &schema).unwrap();
        let second = lower_logical(&make_die("second"), &schema).unwrap();
        let (LogicalExpr::ScalarFunction(first), LogicalExpr::ScalarFunction(second)) =
            (first, second)
        else {
            panic!("expected explicit semantic-kernel UDFs")
        };
        assert_ne!(first.func.name(), second.func.name());
    }

    #[test]
    fn array_map_fallback_is_named_for_the_semantic_operation() {
        let schema = DFSchema::try_from(Schema::new(vec![Field::new(
            "xs",
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            true,
        )]))
        .unwrap();
        let expr = Expr::ArrayMap {
            value: Arc::new(Expr::Column { name: "xs".into() }),
            parameter: "x".into(),
            body: Arc::new(Expr::Binary {
                operator: BinaryOperator::Add,
                left: Arc::new(Expr::LambdaVariable { name: "x".into() }),
                right: Arc::new(Expr::Literal {
                    dtype: Type::Primitive(PrimitiveType::Int64),
                    value: json!(1),
                }),
            }),
        };
        let LogicalExpr::ScalarFunction(call) = lower_logical(&expr, &schema).unwrap() else {
            panic!("expected explicit array-map kernel")
        };
        assert!(
            call.func
                .name()
                .starts_with("vail_semantic_kernel_array_transform_")
        );
    }

    #[test]
    fn if_else_keeps_null_condition_branch_explicit() {
        let expr = Expr::IfElse {
            condition: Arc::new(Expr::Column {
                name: "flag".into(),
            }),
            consequent: Arc::new(Expr::Literal {
                dtype: Type::Primitive(PrimitiveType::Int64),
                value: json!(1),
            }),
            alternate: Arc::new(Expr::Literal {
                dtype: Type::Primitive(PrimitiveType::Int64),
                value: json!(0),
            }),
        };
        // A separate Boolean source is required by the typed IR.
        let schema = DFSchema::try_from(Schema::new(vec![Field::new(
            "flag",
            DataType::Boolean,
            true,
        )]))
        .unwrap();
        let lowered = lower_logical(&expr, &schema).unwrap();
        let LogicalExpr::Case(case) = lowered else {
            panic!("expected a CASE expression")
        };
        assert_eq!(case.when_then_expr.len(), 2);
    }

    fn evaluate_logical(
        expr: LogicalExpr,
        schema: &DFSchema,
        batch: &RecordBatch,
    ) -> arrow_array::ArrayRef {
        let physical = datafusion_physical_expr::planner::create_physical_expr(
            &expr,
            schema,
            &datafusion_physical_expr::execution_props::ExecutionProps::new(),
            &datafusion_expr::physical_planning_context::PhysicalPlanningContext::default(),
        )
        .unwrap();
        physical
            .evaluate(batch)
            .unwrap()
            .into_array(batch.num_rows())
            .unwrap()
    }

    #[test]
    fn independent_annotations_execute_with_distinct_values() {
        let schema =
            DFSchema::try_from(Schema::new(vec![Field::new("x", DataType::Int64, false)])).unwrap();
        let batch =
            RecordBatch::try_from_iter(vec![("x", Arc::new(Int64Array::from(vec![1, 2])) as _)])
                .unwrap();
        let annotate = |n| Expr::Binary {
            operator: BinaryOperator::Add,
            left: Arc::new(Expr::Column { name: "x".into() }),
            right: Arc::new(Expr::Literal {
                dtype: Type::Primitive(PrimitiveType::Int64),
                value: json!(n),
            }),
        };
        let first = evaluate_logical(
            lower_logical(&annotate(1), &schema).unwrap(),
            &schema,
            &batch,
        );
        let second = evaluate_logical(
            lower_logical(&annotate(2), &schema).unwrap(),
            &schema,
            &batch,
        );
        assert_eq!(
            first
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[2, 3]
        );
        assert_eq!(
            second
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[3, 4]
        );
    }

    #[test]
    fn integer_division_and_wrapping_keep_vail_semantics() {
        let schema =
            DFSchema::try_from(Schema::new(vec![Field::new("x", DataType::Int64, false)])).unwrap();
        let batch = RecordBatch::try_from_iter(vec![(
            "x",
            Arc::new(Int64Array::from(vec![3, i64::MAX])) as _,
        )])
        .unwrap();
        let division = Expr::Binary {
            operator: BinaryOperator::Div,
            left: Arc::new(Expr::Column { name: "x".into() }),
            right: Arc::new(Expr::Literal {
                dtype: Type::Primitive(PrimitiveType::Int64),
                value: json!(2),
            }),
        };
        let divided = evaluate_logical(lower_logical(&division, &schema).unwrap(), &schema, &batch);
        let divided = divided.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(divided.value(0), 1.5);
        assert_eq!(divided.value(1), i64::MAX as f64 / 2.0);

        let addition = Expr::Binary {
            operator: BinaryOperator::Add,
            left: Arc::new(Expr::Column { name: "x".into() }),
            right: Arc::new(Expr::Literal {
                dtype: Type::Primitive(PrimitiveType::Int64),
                value: json!(1),
            }),
        };
        let added = evaluate_logical(lower_logical(&addition, &schema).unwrap(), &schema, &batch);
        assert_eq!(
            added
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(1),
            i64::MIN
        );
    }

    #[test]
    fn missing_if_condition_produces_missing_and_dead_die_branch_is_not_evaluated() {
        let schema = DFSchema::try_from(Schema::new(vec![Field::new(
            "flag",
            DataType::Boolean,
            true,
        )]))
        .unwrap();
        let batch = RecordBatch::try_from_iter(vec![(
            "flag",
            Arc::new(BooleanArray::from(vec![Some(false), None])) as _,
        )])
        .unwrap();
        let die = |message| Expr::Die {
            message: Arc::new(Expr::Literal {
                dtype: Type::Primitive(PrimitiveType::Str),
                value: json!(message),
            }),
            dtype: Type::Primitive(PrimitiveType::Int64),
        };
        let expr = Expr::IfElse {
            condition: Arc::new(Expr::Column {
                name: "flag".into(),
            }),
            consequent: Arc::new(die("unselected")),
            alternate: Arc::new(Expr::Literal {
                dtype: Type::Primitive(PrimitiveType::Int64),
                value: json!(9),
            }),
        };
        let out = evaluate_logical(lower_logical(&expr, &schema).unwrap(), &schema, &batch);
        let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(out.value(0), 9);
        assert!(out.is_null(1));
    }
}
