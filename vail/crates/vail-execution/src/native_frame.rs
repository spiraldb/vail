//! Canonical dependency slots for native programs, without compiling or reading values.
use crate::expressions::{Expr, ExprError, RebuiltExpr, Type};
use arrow_schema::{DataType, Field, Schema};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

#[derive(Debug)]
pub struct NativeFrame {
    pub expression: Expr,
    pub schema: Arc<Schema>,
    pub arguments: Vec<Expr>,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Reference {
    Column(String),
    External(String),
    Global(String),
}
impl Reference {
    fn of(expr: &Expr) -> Option<Self> {
        match expr {
            Expr::Column { name } => Some(Self::Column(name.clone())),
            Expr::Global { name } => Some(Self::Global(name.clone())),
            Expr::ExternalGlobal { reference, .. } => Some(Self::External(reference.clone())),
            _ => None,
        }
    }
}
struct Dependency {
    expression: Expr,
    dtype: DataType,
    external_type: Option<Type>,
}
fn field_index(schema: &Schema) -> HashMap<&str, Option<&DataType>> {
    let mut fields = HashMap::with_capacity(schema.fields().len());
    for field in schema.fields() {
        fields
            .entry(field.name().as_str())
            .and_modify(|value| *value = None)
            .or_insert(Some(field.data_type()));
    }
    fields
}
fn field_type(
    fields: &HashMap<&str, Option<&DataType>>,
    name: &str,
    axis: &str,
) -> Result<DataType, ExprError> {
    match fields.get(name) {
        Some(Some(dtype)) => Ok((*dtype).clone()),
        Some(None) => Err(ExprError::Invalid(format!(
            "native {axis} reference {name:?} is ambiguous"
        ))),
        None => Err(ExprError::Invalid(format!(
            "native {axis} reference {name:?} is absent"
        ))),
    }
}

impl NativeFrame {
    pub fn try_new(
        expression: &Expr,
        row_schema: &Schema,
        globals: &Schema,
        external_types: &BTreeMap<String, DataType>,
    ) -> Result<Self, ExprError> {
        let row_fields = field_index(row_schema);
        let global_fields = field_index(globals);
        let mut dependencies = BTreeMap::<Reference, Dependency>::new();
        let mut error = None;
        expression.visit_unique(&mut |node| {
            if error.is_some() {
                return;
            }
            let Some(reference) = Reference::of(node) else {
                return;
            };
            let result = (|| -> Result<(), ExprError> {
                if !dependencies.contains_key(&reference) {
                    let (dtype, external_type) = match &reference {
                        Reference::Column(name) => (field_type(&row_fields, name, "column")?, None),
                        Reference::Global(name) => {
                            (field_type(&global_fields, name, "global")?, None)
                        }
                        Reference::External(name) => {
                            let dtype = external_types
                                .get(name)
                                .ok_or_else(|| {
                                    ExprError::Invalid(format!(
                                        "native external reference {name:?} is unbound"
                                    ))
                                })?
                                .clone();
                            let logical = Type::from_arrow(&dtype)?;
                            (dtype, Some(logical))
                        }
                    };
                    dependencies.insert(
                        reference.clone(),
                        Dependency {
                            expression: node.clone(),
                            dtype,
                            external_type,
                        },
                    );
                }
                if let Expr::ExternalGlobal {
                    reference: name,
                    dtype,
                } = node
                {
                    if dependencies[&reference].external_type.as_ref() != Some(dtype) {
                        return Err(ExprError::Invalid(format!(
                            "native external reference {name:?} type differs from its owner"
                        )));
                    }
                }
                Ok(())
            })();
            if let Err(failure) = result {
                error = Some(failure);
            }
        });
        if let Some(error) = error {
            return Err(error);
        }
        let mut fields = Vec::with_capacity(dependencies.len());
        let mut arguments = Vec::with_capacity(dependencies.len());
        let mut slots = BTreeMap::new();
        for (index, (reference, dependency)) in dependencies.into_iter().enumerate() {
            let slot = format!("__vail_arg_{index}");
            fields.push(Field::new(&slot, dependency.dtype, true));
            arguments.push(dependency.expression);
            slots.insert(reference, slot);
        }
        fn rewrite(
            expression: &Expr,
            shared: bool,
            slots: &BTreeMap<Reference, String>,
            memo: &mut HashMap<usize, Arc<Expr>>,
        ) -> Result<RebuiltExpr, ExprError> {
            let address = expression as *const Expr as usize;
            if shared && let Some(previous) = memo.get(&address) {
                return Ok(RebuiltExpr::Shared(previous.clone()));
            }
            let rewritten = if let Some(reference) = Reference::of(expression) {
                RebuiltExpr::Owned(Expr::Column {
                    name: slots[&reference].clone(),
                })
            } else {
                RebuiltExpr::Owned(expression.try_map_children_rebuilt(|child, shared| {
                    rewrite(child, shared, slots, memo)
                })?)
            };
            if shared {
                let value = Arc::new(rewritten.into_owned());
                memo.insert(address, value.clone());
                Ok(RebuiltExpr::Shared(value))
            } else {
                Ok(rewritten)
            }
        }
        let expression = rewrite(expression, false, &slots, &mut HashMap::new())?.into_owned();
        Ok(Self {
            expression,
            schema: Arc::new(Schema::new(fields)),
            arguments,
        })
    }
}
