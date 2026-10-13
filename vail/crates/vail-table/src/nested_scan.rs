//! Compile nested Hail scan scopes into DataFusion UDAFs and ordinary argument
//! expressions. Group assignment and group accumulation are delegated to DF.
use crate::{Result, TableError};
use arrow_schema::{DataType, Field, Schema};
use datafusion_common::DFSchema;
use datafusion_expr::AggregateUDF;
use std::{collections::BTreeMap, sync::Arc};
use vail_execution::{
    expressions::{Expr, PrimitiveType, Type},
    scan_state::ScanState,
};

pub(crate) struct Built {
    pub udf: Arc<AggregateUDF>,
    pub argument: Expr,
    pub input: DataType,
    parameter: String,
    finish: Expr,
}
fn symbol() -> String {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    format!(
        "__vail_scan_result_{}",
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    )
}
fn substitute(expr: &Expr, name: &str, value: &Expr) -> Result<Expr> {
    if matches!(expr, Expr::LambdaVariable { name: parameter } if parameter == name) {
        return Ok(value.clone());
    }
    expr.try_map_children(&mut |child: &Expr| substitute(child, name, value))
        .map_err(Into::into)
}
impl Built {
    fn identity(udf: Arc<AggregateUDF>, argument: Expr, input: DataType) -> Self {
        let parameter = symbol();
        let finish = Expr::LambdaVariable {
            name: parameter.clone(),
        };
        Self {
            udf,
            argument,
            input,
            parameter,
            finish,
        }
    }
    pub(crate) fn finish(&self, value: Expr) -> Result<Expr> {
        substitute(&self.finish, &self.parameter, &value)
    }
}

struct Context<'a> {
    schema: &'a DFSchema,
    globals: &'a Schema,
    bindings: &'a BTreeMap<String, usize>,
    variables: Vec<(String, Type)>,
}
impl Context<'_> {
    fn dtype(&self, value: &Expr) -> Result<Type> {
        Ok(value.dtype_with_lexical(self.schema.as_arrow(), self.globals, &self.variables)?)
    }
}
fn scope(expr: &Expr) -> bool {
    expr.contains_scan()
        && matches!(
            expr,
            Expr::Aggregate { .. }
                | Expr::AggFilter { .. }
                | Expr::AggExplode { .. }
                | Expr::AggGroupBy { .. }
                | Expr::AggArray { .. }
        )
}
pub(crate) fn build(
    expr: &Expr,
    schema: &DFSchema,
    globals: &Schema,
    bindings: &BTreeMap<String, usize>,
) -> Result<Built> {
    node(
        expr,
        &Context {
            schema,
            globals,
            bindings,
            variables: vec![],
        },
    )
}
fn descriptor(built: &Built) -> Result<ScanState> {
    Ok(ScanState::new(
        built.udf.clone(),
        vec![built.input.clone()],
    )?)
}
fn compose(expr: &Expr, context: &Context<'_>) -> Result<Built> {
    let mut children = Vec::new();
    let mut arguments = Vec::new();
    let mut fields = Vec::new();
    fn extract(
        expr: &Expr,
        context: &Context<'_>,
        children: &mut Vec<ScanState>,
        arguments: &mut Vec<(String, Expr)>,
        fields: &mut Vec<Field>,
    ) -> Result<Expr> {
        if scope(expr) {
            let built = node(expr, context)?;
            let child = descriptor(&built)?;
            let mut name = symbol();
            while context
                .schema
                .fields()
                .iter()
                .any(|field| field.name() == &name)
            {
                name = symbol();
            }
            fields.push(Field::new(&name, child.output_type(), true));
            let output = built.finish(Expr::Column { name: name.clone() })?;
            arguments.push((name.clone(), built.argument));
            children.push(child);
            return Ok(output);
        }
        expr.try_map_children(&mut |child: &Expr| {
            extract(child, context, children, arguments, fields)
        })
        .map_err(Into::into)
    }
    let projection = extract(expr, context, &mut children, &mut arguments, &mut fields)?;
    if children.is_empty() {
        return Err(TableError::Invalid(
            "nested scan body must contain an aggregation".into(),
        ));
    }
    let raw_fields = fields
        .iter()
        .map(|field| {
            (
                field.name().clone(),
                Expr::Column {
                    name: field.name().clone(),
                },
            )
        })
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(fields));
    let raw = vail_execution::expressions::lower(&Expr::Struct { fields: raw_fields }, &schema)?;
    let argument = Expr::Struct { fields: arguments };
    let input = context.dtype(&argument)?.to_arrow()?;
    let udf = vail_execution::scan_scopes::composed(
        vail_execution::scan_scopes::Composition {
            children,
            schema: schema.clone(),
            projection: raw,
        },
        input.clone(),
    )?;
    let mut built = Built::identity(udf, argument, input);
    fn replace(expr: &Expr, names: &Schema, parameter: &str) -> Result<Expr> {
        if let Expr::Column { name } = expr {
            if names.index_of(name).is_ok() {
                return Ok(Expr::GetField {
                    value: Box::new(Expr::LambdaVariable {
                        name: parameter.into(),
                    }),
                    name: name.clone(),
                });
            }
        }
        expr.try_map_children(&mut |child: &Expr| replace(child, names, parameter))
            .map_err(Into::into)
    }
    built.finish = replace(&projection, &schema, &built.parameter)?;
    Ok(built)
}
fn node(expr: &Expr, context: &Context<'_>) -> Result<Built> {
    match expr {
        Expr::Aggregate {
            function,
            value,
            options,
            ..
        } => {
            if function == "__scan_fold" {
                let mut schema = context.schema.as_arrow().clone();
                let mut aliases = BTreeMap::new();
                for (i, (name, ty)) in context.variables.iter().enumerate() {
                    let mut column = format!("__vail_scan_variable_{i}");
                    while schema.fields().iter().any(|field| field.name() == &column) {
                        column.push('_');
                    }
                    aliases.insert(name.clone(), column.clone());
                    let mut fields = schema.fields().to_vec();
                    fields.push(Arc::new(Field::new(column, ty.to_arrow()?, true)));
                    schema = Schema::new(fields);
                }
                let schema = Arc::new(schema);
                let dummy: crate::plan::PhysicalPlan =
                    datafusion_datasource::memory::MemorySourceConfig::try_new_exec(
                        &[vec![]],
                        schema.clone(),
                        None,
                    )?;
                let fold = crate::aggregation::resolve(value.as_ref().unwrap(), &aliases)?;
                let (udf, packed) = crate::aggregation::lower_fold(
                    &fold,
                    &dummy,
                    &schema,
                    context.globals,
                    context.bindings,
                    &BTreeMap::new(),
                )?;
                let mut fields = Vec::new();
                for (i, field) in schema.fields().iter().enumerate() {
                    if *field.data_type() == DataType::UInt64 {
                        continue;
                    }
                    let value = match aliases.iter().find(|(_, column)| *column == field.name()) {
                        Some((name, _)) => Expr::LambdaVariable { name: name.clone() },
                        None if i < context.globals.fields().len() => Expr::Global {
                            name: field.name().clone(),
                        },
                        None => Expr::Column {
                            name: field.name().clone(),
                        },
                    };
                    fields.push((field.name().clone(), value));
                }
                return Ok(Built::identity(
                    udf,
                    Expr::Struct { fields },
                    packed.data_type(&schema)?,
                ));
            }
            let argument = value.as_deref().cloned().unwrap_or(Expr::Literal {
                dtype: Type::Primitive(PrimitiveType::Int64),
                value: serde_json::json!(1),
            });
            let mut options = options.clone();
            options
                .as_object_mut()
                .ok_or_else(|| TableError::Invalid("scan options must be an object".into()))?
                .remove("__vail_scan");
            let exploded = options
                .get("__vail_exploded")
                .and_then(serde_json::Value::as_bool)
                == Some(true);
            let mut input = context.dtype(&argument)?.to_arrow()?;
            if exploded {
                let DataType::List(element) = &input else {
                    return Err(TableError::Invalid(
                        "exploded scan input must be a list".into(),
                    ));
                };
                input = element.data_type().clone();
            }
            let reducer = crate::aggregation::Reducer {
                name: "scan".into(),
                function: function.clone(),
                value: if function == "count" {
                    None
                } else {
                    value.as_deref().cloned()
                },
                options,
            };
            let udf = crate::logical_aggregation::factory(&reducer, Some(&input))?;
            if exploded {
                let child = ScanState::new(udf, vec![input])?;
                let input = context.dtype(&argument)?.to_arrow()?;
                let udf = vail_execution::scan_scopes::transformed(
                    child,
                    input.clone(),
                    vail_execution::scan_scopes::Transform::Explode,
                );
                Ok(Built::identity(udf, argument, input))
            } else {
                Ok(Built::identity(udf, argument, input))
            }
        }
        Expr::AggFilter { predicate, body } => {
            let child = compose(body, context)?;
            let guarded = Expr::IfElse {
                condition: predicate.clone(),
                consequent: Box::new(child.argument.clone()),
                alternate: Box::new(Expr::Literal {
                    dtype: Type::from_arrow(&child.input)?,
                    value: serde_json::Value::Null,
                }),
            };
            let argument = Expr::Struct {
                fields: vec![
                    ("predicate".into(), (**predicate).clone()),
                    ("value".into(), guarded),
                ],
            };
            let input = context.dtype(&argument)?.to_arrow()?;
            let udf = vail_execution::scan_scopes::transformed(
                descriptor(&child)?,
                input.clone(),
                vail_execution::scan_scopes::Transform::Filter,
            );
            let mut result = Built::identity(udf, argument, input);
            result.finish = child.finish(Expr::LambdaVariable {
                name: result.parameter.clone(),
            })?;
            Ok(result)
        }
        Expr::AggExplode {
            value,
            parameter,
            body,
        }
        | Expr::AggArray {
            value,
            parameter,
            body,
        } => {
            let element = match context.dtype(value)? {
                Type::Array { array } | Type::Set { set: array } => *array,
                _ => {
                    return Err(TableError::Invalid(
                        "scan array scope requires array or set".into(),
                    ));
                }
            };
            let mut variables = context.variables.clone();
            variables.push((parameter.clone(), element));
            let nested = Context {
                variables,
                ..*context
            };
            let child = compose(body, &nested)?;
            let argument = Expr::ArrayMap {
                value: value.clone(),
                parameter: parameter.clone(),
                body: Box::new(child.argument.clone()),
            };
            let input = context.dtype(&argument)?.to_arrow()?;
            let udf = if matches!(expr, Expr::AggArray { .. }) {
                vail_execution::scan_scopes::indexed(
                    descriptor(&child)?,
                    input.clone(),
                    DataType::Int64,
                    Type::Array {
                        array: Box::new(Type::from_arrow(
                            &child.udf.return_type(&[child.input.clone()])?,
                        )?),
                    }
                    .to_arrow()?,
                    true,
                )?
            } else {
                vail_execution::scan_scopes::transformed(
                    descriptor(&child)?,
                    input.clone(),
                    vail_execution::scan_scopes::Transform::Explode,
                )
            };
            let mut result = Built::identity(udf, argument, input);
            let slot = Expr::LambdaVariable {
                name: result.parameter.clone(),
            };
            result.finish = if matches!(expr, Expr::AggArray { .. }) {
                let index_name = symbol();
                let index = Expr::LambdaVariable {
                    name: index_name.clone(),
                };
                let entry = Expr::ArrayIndex {
                    value: Box::new(slot.clone()),
                    index: Box::new(index.clone()),
                };
                let current = Expr::ArrayIndex {
                    value: value.clone(),
                    index: Box::new(index.clone()),
                };
                let body = substitute(&child.finish(entry)?, parameter, &current)?;
                Expr::ArrayMap {
                    value: Box::new(Expr::StreamRange {
                        start: Box::new(Expr::Literal {
                            dtype: Type::Primitive(PrimitiveType::Int32),
                            value: serde_json::json!(0),
                        }),
                        stop: Box::new(Expr::ArrayLength {
                            value: Box::new(slot),
                        }),
                        step: Box::new(Expr::Literal {
                            dtype: Type::Primitive(PrimitiveType::Int32),
                            value: serde_json::json!(1),
                        }),
                    }),
                    parameter: index_name,
                    body: Box::new(body),
                }
            } else {
                child.finish(slot)?
            };
            Ok(result)
        }
        Expr::AggGroupBy { key, body } => {
            let child = compose(body, context)?;
            let key_type = context.dtype(key)?;
            key_type.ensure_keyable()?;
            let empty_struct =
                matches!(&key_type, Type::Struct { r#struct } if r#struct.is_empty());
            if !key_type.native_key() && !empty_struct {
                return Err(TableError::Invalid(format!(
                    "scan.group_by does not support native key type {key_type:?}"
                )));
            }
            let key_arrow = key_type.to_arrow()?;
            let argument = Expr::Struct {
                fields: vec![
                    ("key".into(), (**key).clone()),
                    ("value".into(), child.argument.clone()),
                ],
            };
            let input = context.dtype(&argument)?.to_arrow()?;
            let raw_type = Type::Dict {
                dict: Box::new((
                    key_type.clone(),
                    Type::from_arrow(&child.udf.return_type(&[child.input.clone()])?)?,
                )),
            };
            let udf = vail_execution::scan_scopes::indexed(
                descriptor(&child)?,
                input.clone(),
                key_arrow,
                raw_type.to_arrow()?,
                false,
            )?;
            let mut result = Built::identity(udf, argument, input);
            let entry_name = symbol();
            let entry = Expr::LambdaVariable {
                name: entry_name.clone(),
            };
            let body = Expr::Struct {
                fields: vec![
                    (
                        "key".into(),
                        Expr::GetField {
                            value: Box::new(entry.clone()),
                            name: "key".into(),
                        },
                    ),
                    (
                        "value".into(),
                        child.finish(Expr::GetField {
                            value: Box::new(entry),
                            name: "value".into(),
                        })?,
                    ),
                ],
            };
            result.finish = Expr::CollectionCast {
                value: Box::new(Expr::ArrayMap {
                    value: Box::new(Expr::LambdaVariable {
                        name: result.parameter.clone(),
                    }),
                    parameter: entry_name,
                    body: Box::new(body),
                }),
                dtype: context.dtype(expr)?,
            };
            Ok(result)
        }
        _ => compose(expr, context),
    }
}
