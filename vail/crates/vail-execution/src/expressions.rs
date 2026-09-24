//! Typed, source-bound scalar expressions for in-memory Table projections.
//!
//! Evaluation uses DataFusion physical expressions for column references,
//! casts, comparisons, and Kleene Boolean operators. Hail's distinct integer
//! overflow, floor division, modulo, and missing-condition semantics are kept
//! explicit at this boundary.

use std::collections::HashSet;
use std::hash::Hash;
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, ListArray,
    RecordBatch, RecordBatchOptions, StringArray, StructArray, UInt32Array, new_null_array,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use arrow_select::{concat::concat, nullif::nullif, take::take};
use datafusion_common::{DataFusionError, ScalarValue, config::ConfigOptions};
use datafusion_expr::{ColumnarValue, Operator};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::ScalarFunctionExpr;
use datafusion_physical_expr::expressions::{
    CaseExpr, Column, IsNullExpr, Literal, NegativeExpr, NotExpr, binary, cast,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ExprError {
    #[error("invalid expression: {0}")]
    Invalid(String),
    #[error("Arrow expression failed: {0}")]
    Arrow(#[from] ArrowError),
    #[error("DataFusion expression failed: {0}")]
    DataFusion(#[from] DataFusionError),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrimitiveType {
    Int32,
    Int64,
    Float32,
    Float64,
    Bool,
    Str,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Type {
    Primitive(PrimitiveType),
    Struct { r#struct: Vec<(String, Type)> },
    Array { array: Box<Type> },
}

impl Type {
    pub fn to_arrow(&self) -> Result<DataType, ExprError> {
        Ok(match self {
            Self::Primitive(PrimitiveType::Int32) => DataType::Int32,
            Self::Primitive(PrimitiveType::Int64) => DataType::Int64,
            Self::Primitive(PrimitiveType::Float32) => DataType::Float32,
            Self::Primitive(PrimitiveType::Float64) => DataType::Float64,
            Self::Primitive(PrimitiveType::Bool) => DataType::Boolean,
            Self::Primitive(PrimitiveType::Str) => DataType::Utf8,
            Self::Struct { r#struct: fields } => {
                let mut names = HashSet::new();
                for (name, _) in fields {
                    if !names.insert(name) {
                        return Err(ExprError::Invalid(format!(
                            "duplicate struct field {name:?}"
                        )));
                    }
                }
                DataType::Struct(
                    fields
                        .iter()
                        .map(|(n, t)| Ok(Arc::new(Field::new(n, t.to_arrow()?, true))))
                        .collect::<Result<Vec<_>, ExprError>>()?
                        .into(),
                )
            }
            Self::Array { array } => {
                DataType::List(Arc::new(Field::new("item", array.to_arrow()?, true)))
            }
        })
    }

    pub fn from_arrow(dtype: &DataType) -> Result<Self, ExprError> {
        Ok(match dtype {
            DataType::Int32 => Self::Primitive(PrimitiveType::Int32),
            DataType::Int64 => Self::Primitive(PrimitiveType::Int64),
            DataType::Float32 => Self::Primitive(PrimitiveType::Float32),
            DataType::Float64 => Self::Primitive(PrimitiveType::Float64),
            DataType::Boolean => Self::Primitive(PrimitiveType::Bool),
            DataType::Utf8 => Self::Primitive(PrimitiveType::Str),
            DataType::Struct(fields) => Self::Struct {
                r#struct: fields
                    .iter()
                    .map(|f| Ok((f.name().clone(), Self::from_arrow(f.data_type())?)))
                    .collect::<Result<Vec<_>, ExprError>>()?,
            },
            DataType::List(field) => Self::Array {
                array: Box::new(Self::from_arrow(field.data_type())?),
            },
            other => {
                return Err(ExprError::Invalid(format!(
                    "unsupported expression type {other:?}"
                )));
            }
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BinaryOperator {
    Add,
    Sub,
    Mul,
    Div,
    Floordiv,
    Mod,
    Pow,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    And,
    Or,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnaryOperator {
    Neg,
    Pos,
    Not,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArrayReduction {
    Sum,
    Product,
    Min,
    Max,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Expr {
    Column {
        name: String,
    },
    Literal {
        dtype: Type,
        value: Value,
    },
    Binary {
        operator: BinaryOperator,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Unary {
        operator: UnaryOperator,
        value: Box<Expr>,
    },
    Cast {
        dtype: Type,
        value: Box<Expr>,
    },
    GetField {
        value: Box<Expr>,
        name: String,
    },
    Struct {
        fields: Vec<(String, Expr)>,
    },
    IfElse {
        condition: Box<Expr>,
        consequent: Box<Expr>,
        alternate: Box<Expr>,
    },
    IsMissing {
        value: Box<Expr>,
    },
    Array {
        element_type: Type,
        elements: Vec<Expr>,
    },
    ArrayLength {
        value: Box<Expr>,
    },
    ArrayIndex {
        value: Box<Expr>,
        index: Box<Expr>,
    },
    ArraySlice {
        value: Box<Expr>,
        start: Option<Box<Expr>>,
        stop: Option<Box<Expr>>,
        step: Option<Box<Expr>>,
    },
    ArrayReduce {
        operation: ArrayReduction,
        filter_missing: bool,
        value: Box<Expr>,
    },
}

/// Ordered output fields, serialized as `[[name, expression], ...]`.
pub type Projection = Vec<(String, Expr)>;

fn numeric(t: &Type) -> bool {
    matches!(
        t,
        Type::Primitive(
            PrimitiveType::Int32
                | PrimitiveType::Int64
                | PrimitiveType::Float32
                | PrimitiveType::Float64
        )
    )
}
fn promote(a: &Type, b: &Type) -> Result<Type, ExprError> {
    if a == b {
        return Ok(a.clone());
    }
    if !numeric(a) || !numeric(b) {
        return Err(ExprError::Invalid(format!(
            "incompatible types {a:?} and {b:?}"
        )));
    }
    let rank = |t: &Type| match t {
        Type::Primitive(PrimitiveType::Int32) => 0,
        Type::Primitive(PrimitiveType::Int64) => 1,
        Type::Primitive(PrimitiveType::Float32) => 2,
        _ => 3,
    };
    Ok(if rank(a) >= rank(b) {
        a.clone()
    } else {
        b.clone()
    })
}
fn ensure_bool(t: &Type, context: &str) -> Result<(), ExprError> {
    if *t == Type::Primitive(PrimitiveType::Bool) {
        Ok(())
    } else {
        Err(ExprError::Invalid(format!(
            "{context} requires bool, got {t:?}"
        )))
    }
}
fn field_type(fields: &[(String, Type)], name: &str) -> Result<Type, ExprError> {
    fields
        .iter()
        .find(|(n, _)| n == name)
        .map(|(_, t)| t.clone())
        .ok_or_else(|| ExprError::Invalid(format!("struct field {name:?} is absent")))
}
impl Expr {
    pub fn dtype(&self, schema: &Schema) -> Result<Type, ExprError> {
        match self {
            Self::Column { name } => {
                let i = schema
                    .index_of(name)
                    .map_err(|_| ExprError::Invalid(format!("source column {name:?} is absent")))?;
                Type::from_arrow(schema.field(i).data_type())
            }
            Self::Literal { dtype, value } => {
                validate_literal(value, dtype)?;
                Ok(dtype.clone())
            }
            Self::Array {
                element_type,
                elements,
            } => {
                element_type.to_arrow()?;
                for element in elements {
                    let actual = element.dtype(schema)?;
                    if actual != *element_type {
                        return Err(ExprError::Invalid(format!(
                            "array element has type {actual:?}, expected {element_type:?}"
                        )));
                    }
                }
                Ok(Type::Array {
                    array: Box::new(element_type.clone()),
                })
            }
            Self::ArrayLength { value } => {
                if !matches!(value.dtype(schema)?, Type::Array { .. }) {
                    return Err(ExprError::Invalid("array length requires array".into()));
                }
                Ok(Type::Primitive(PrimitiveType::Int32))
            }
            Self::ArrayIndex { value, index } => {
                let element = match value.dtype(schema)? {
                    Type::Array { array } => *array,
                    other => {
                        return Err(ExprError::Invalid(format!(
                            "array index requires array, got {other:?}"
                        )));
                    }
                };
                if index.dtype(schema)? != Type::Primitive(PrimitiveType::Int32) {
                    return Err(ExprError::Invalid("array index requires int32".into()));
                }
                Ok(element)
            }
            Self::ArraySlice {
                value,
                start,
                stop,
                step,
            } => {
                let array = match value.dtype(schema)? {
                    t @ Type::Array { .. } => t,
                    other => {
                        return Err(ExprError::Invalid(format!(
                            "array slice requires array, got {other:?}"
                        )));
                    }
                };
                for endpoint in [start, stop, step].into_iter().flatten() {
                    if endpoint.dtype(schema)? != Type::Primitive(PrimitiveType::Int32) {
                        return Err(ExprError::Invalid(
                            "array slice endpoints and step require int32".into(),
                        ));
                    }
                }
                Ok(array)
            }
            Self::ArrayReduce { value, .. } => {
                let element = match value.dtype(schema)? {
                    Type::Array { array } => *array,
                    other => {
                        return Err(ExprError::Invalid(format!(
                            "array reduction requires array, got {other:?}"
                        )));
                    }
                };
                if !numeric(&element) {
                    return Err(ExprError::Invalid(format!(
                        "array reduction requires numeric elements, got {element:?}"
                    )));
                }
                Ok(element)
            }
            Self::Cast { dtype, value } => {
                value.dtype(schema)?;
                dtype.to_arrow()?;
                Ok(dtype.clone())
            }
            Self::GetField { value, name } => match value.dtype(schema)? {
                Type::Struct { r#struct: fields } => field_type(&fields, name),
                other => Err(ExprError::Invalid(format!(
                    "get_field requires struct, got {other:?}"
                ))),
            },
            Self::Struct { fields } => {
                let mut names = HashSet::new();
                let mut typed = Vec::with_capacity(fields.len());
                for (name, expr) in fields {
                    if !names.insert(name) {
                        return Err(ExprError::Invalid(format!(
                            "duplicate struct field {name:?}"
                        )));
                    }
                    typed.push((name.clone(), expr.dtype(schema)?));
                }
                Ok(Type::Struct { r#struct: typed })
            }
            Self::IsMissing { value } => {
                value.dtype(schema)?;
                Ok(Type::Primitive(PrimitiveType::Bool))
            }
            Self::Unary { operator, value } => {
                let t = value.dtype(schema)?;
                match operator {
                    UnaryOperator::Not => ensure_bool(&t, "not")?,
                    _ if !numeric(&t) => {
                        return Err(ExprError::Invalid(format!(
                            "sign requires numeric type, got {t:?}"
                        )));
                    }
                    _ => {}
                }
                Ok(t)
            }
            Self::Binary {
                operator,
                left,
                right,
            } => {
                let a = left.dtype(schema)?;
                let b = right.dtype(schema)?;
                match operator {
                    BinaryOperator::And | BinaryOperator::Or => {
                        ensure_bool(&a, "boolean operator")?;
                        ensure_bool(&b, "boolean operator")?;
                        Ok(Type::Primitive(PrimitiveType::Bool))
                    }
                    BinaryOperator::Eq
                    | BinaryOperator::Ne
                    | BinaryOperator::Lt
                    | BinaryOperator::Le
                    | BinaryOperator::Gt
                    | BinaryOperator::Ge => {
                        if a != b {
                            promote(&a, &b)?;
                        }
                        Ok(Type::Primitive(PrimitiveType::Bool))
                    }
                    BinaryOperator::Div => {
                        if !numeric(&a) || !numeric(&b) {
                            return Err(ExprError::Invalid(
                                "division requires numeric operands".into(),
                            ));
                        }
                        Ok(Type::Primitive(
                            if matches!(a, Type::Primitive(PrimitiveType::Float32))
                                || matches!(b, Type::Primitive(PrimitiveType::Float32))
                            {
                                if matches!(a, Type::Primitive(PrimitiveType::Float64))
                                    || matches!(b, Type::Primitive(PrimitiveType::Float64))
                                {
                                    PrimitiveType::Float64
                                } else {
                                    PrimitiveType::Float32
                                }
                            } else {
                                PrimitiveType::Float64
                            },
                        ))
                    }
                    BinaryOperator::Pow => {
                        if !numeric(&a) || !numeric(&b) {
                            return Err(ExprError::Invalid(
                                "power requires numeric operands".into(),
                            ));
                        }
                        Ok(Type::Primitive(PrimitiveType::Float64))
                    }
                    _ => {
                        if !numeric(&a) || !numeric(&b) {
                            return Err(ExprError::Invalid(format!(
                                "arithmetic requires numeric operands, got {a:?} and {b:?}"
                            )));
                        }
                        promote(&a, &b)
                    }
                }
            }
            Self::IfElse {
                condition,
                consequent,
                alternate,
            } => {
                ensure_bool(&condition.dtype(schema)?, "if_else condition")?;
                let a = consequent.dtype(schema)?;
                let b = alternate.dtype(schema)?;
                if a == b { Ok(a) } else { promote(&a, &b) }
            }
        }
    }
}

fn validate_literal(value: &Value, dtype: &Type) -> Result<(), ExprError> {
    if value.is_null() {
        return Ok(());
    }
    match dtype {
        Type::Primitive(PrimitiveType::Int32) => {
            let n = value
                .as_i64()
                .ok_or_else(|| ExprError::Invalid("int32 literal requires integer".into()))?;
            i32::try_from(n)
                .map_err(|_| ExprError::Invalid("int32 literal out of range".into()))?;
        }
        Type::Primitive(PrimitiveType::Int64) => {
            value
                .as_i64()
                .ok_or_else(|| ExprError::Invalid("int64 literal requires integer".into()))?;
        }
        Type::Primitive(PrimitiveType::Float32 | PrimitiveType::Float64) => {
            float_json(value)?;
        }
        Type::Primitive(PrimitiveType::Bool) => {
            value
                .as_bool()
                .ok_or_else(|| ExprError::Invalid("bool literal requires boolean".into()))?;
        }
        Type::Primitive(PrimitiveType::Str) => {
            value
                .as_str()
                .ok_or_else(|| ExprError::Invalid("str literal requires string".into()))?;
        }
        Type::Struct { r#struct: fields } => {
            let obj = value
                .as_object()
                .ok_or_else(|| ExprError::Invalid("struct literal requires object".into()))?;
            if obj.len() != fields.len() || fields.iter().any(|(n, _)| !obj.contains_key(n)) {
                return Err(ExprError::Invalid(
                    "struct literal fields do not match dtype".into(),
                ));
            }
            for (n, t) in fields {
                validate_literal(&obj[n], t)?;
            }
        }
        Type::Array { array } => {
            let items = value
                .as_array()
                .ok_or_else(|| ExprError::Invalid("array literal requires JSON array".into()))?;
            for item in items {
                validate_literal(item, array)?;
            }
        }
    }
    Ok(())
}
fn float_json(v: &Value) -> Result<f64, ExprError> {
    if let Some(n) = v.as_f64() {
        return Ok(n);
    }
    match v.get("$float").and_then(Value::as_str) {
        Some("nan") => Ok(f64::NAN),
        Some("inf") => Ok(f64::INFINITY),
        Some("-inf") => Ok(f64::NEG_INFINITY),
        _ => Err(ExprError::Invalid(
            "float literal requires number or $float tag".into(),
        )),
    }
}
fn literal_array(v: &Value, t: &Type, len: usize) -> Result<ArrayRef, ExprError> {
    let dt = t.to_arrow()?;
    if v.is_null() {
        return Ok(new_null_array(&dt, len));
    }
    let a: ArrayRef = match t {
        Type::Primitive(PrimitiveType::Int32) => {
            Arc::new(Int32Array::from(vec![
                Some(v.as_i64().unwrap() as i32);
                len
            ]))
        }
        Type::Primitive(PrimitiveType::Int64) => {
            Arc::new(Int64Array::from(vec![Some(v.as_i64().unwrap()); len]))
        }
        Type::Primitive(PrimitiveType::Float32) => {
            Arc::new(Float32Array::from(vec![Some(float_json(v)? as f32); len]))
        }
        Type::Primitive(PrimitiveType::Float64) => {
            Arc::new(Float64Array::from(vec![Some(float_json(v)?); len]))
        }
        Type::Primitive(PrimitiveType::Bool) => {
            Arc::new(BooleanArray::from(vec![Some(v.as_bool().unwrap()); len]))
        }
        Type::Primitive(PrimitiveType::Str) => {
            Arc::new(StringArray::from(vec![Some(v.as_str().unwrap()); len]))
        }
        Type::Struct { r#struct: fields } => {
            let obj = v.as_object().unwrap();
            let values = fields
                .iter()
                .map(|(n, t)| {
                    Ok((
                        Arc::new(Field::new(n, t.to_arrow()?, true)),
                        literal_array(&obj[n], t, len)?,
                    ))
                })
                .collect::<Result<Vec<_>, ExprError>>()?;
            if values.is_empty() {
                Arc::new(StructArray::new(
                    Vec::<Arc<Field>>::new().into(),
                    Vec::new(),
                    Some(NullBuffer::new_valid(len)),
                ))
            } else {
                Arc::new(StructArray::from(values))
            }
        }
        Type::Array { array } => {
            let items = v.as_array().unwrap();
            let n = items
                .len()
                .checked_mul(len)
                .ok_or_else(|| ExprError::Invalid("array literal too large".into()))?;
            if n > i32::MAX as usize {
                return Err(ExprError::Invalid(
                    "array literal exceeds Arrow offsets".into(),
                ));
            }
            let mut children = Vec::with_capacity(n);
            for _ in 0..len {
                for item in items {
                    children.push(literal_array(item, array, 1)?);
                }
            }
            let child: ArrayRef = if children.is_empty() {
                new_null_array(&array.to_arrow()?, 0)
            } else {
                concat(&children.iter().map(|a| a.as_ref()).collect::<Vec<_>>())?
            };
            let offsets = OffsetBuffer::from_lengths(std::iter::repeat_n(items.len(), len));
            Arc::new(ListArray::new(
                Arc::new(Field::new("item", array.to_arrow()?, true)),
                offsets,
                child,
                None,
            ))
        }
    };
    Ok(a)
}

pub fn validate_projection(
    schema: &Schema,
    fields: &[(String, Expr)],
) -> Result<SchemaRef, ExprError> {
    let mut names = HashSet::new();
    let mut output = Vec::with_capacity(fields.len());
    for (name, expr) in fields {
        if !names.insert(name.as_str()) {
            return Err(ExprError::Invalid(format!(
                "duplicate projection field {name:?}"
            )));
        }
        output.push(Field::new(name, expr.dtype(schema)?.to_arrow()?, true));
    }
    Ok(Arc::new(Schema::new(output)))
}

fn custom_numeric(
    op: BinaryOperator,
    left: &ArrayRef,
    right: &ArrayRef,
    t: &Type,
) -> Result<ArrayRef, ExprError> {
    let mut values = Vec::with_capacity(left.len());
    for i in 0..left.len() {
        let l = scalar_at(left, i)?;
        let r = scalar_at(right, i)?;
        if l.is_null() || r.is_null() {
            values.push(ScalarValue::try_from(&t.to_arrow()?)?);
            continue;
        }
        let result = match (l, r, t) {
            (
                ScalarValue::Int32(Some(a)),
                ScalarValue::Int32(Some(b)),
                Type::Primitive(PrimitiveType::Int32),
            ) => {
                let n = match op {
                    BinaryOperator::Add => a.wrapping_add(b),
                    BinaryOperator::Sub => a.wrapping_sub(b),
                    BinaryOperator::Mul => a.wrapping_mul(b),
                    BinaryOperator::Floordiv => {
                        if b == 0 {
                            return Err(ExprError::Invalid("integer division by zero".into()));
                        }
                        let q = a.wrapping_div(b);
                        q.wrapping_sub(i32::from(a.wrapping_rem(b) != 0 && (a < 0) != (b < 0)))
                    }
                    BinaryOperator::Mod => {
                        if b == 0 {
                            return Err(ExprError::Invalid("modulo by zero".into()));
                        }
                        let rem = a.wrapping_rem(b);
                        rem.wrapping_add(if rem != 0 && (a < 0) != (b < 0) { b } else { 0 })
                    }
                    _ => unreachable!(),
                };
                ScalarValue::Int32(Some(n))
            }
            (
                ScalarValue::Int64(Some(a)),
                ScalarValue::Int64(Some(b)),
                Type::Primitive(PrimitiveType::Int64),
            ) => {
                let n = match op {
                    BinaryOperator::Add => a.wrapping_add(b),
                    BinaryOperator::Sub => a.wrapping_sub(b),
                    BinaryOperator::Mul => a.wrapping_mul(b),
                    BinaryOperator::Floordiv => {
                        if b == 0 {
                            return Err(ExprError::Invalid("integer division by zero".into()));
                        }
                        let q = a.wrapping_div(b);
                        q.wrapping_sub(i64::from(a.wrapping_rem(b) != 0 && (a < 0) != (b < 0)))
                    }
                    BinaryOperator::Mod => {
                        if b == 0 {
                            return Err(ExprError::Invalid("modulo by zero".into()));
                        }
                        let rem = a.wrapping_rem(b);
                        rem.wrapping_add(if rem != 0 && (a < 0) != (b < 0) { b } else { 0 })
                    }
                    _ => unreachable!(),
                };
                ScalarValue::Int64(Some(n))
            }
            (
                ScalarValue::Float32(Some(a)),
                ScalarValue::Float32(Some(b)),
                Type::Primitive(PrimitiveType::Float32),
            ) => {
                let n = match op {
                    BinaryOperator::Add => a + b,
                    BinaryOperator::Sub => a - b,
                    BinaryOperator::Mul => a * b,
                    BinaryOperator::Floordiv => (f64::from(a) / f64::from(b)).floor() as f32,
                    BinaryOperator::Mod => {
                        if b == 0.0 {
                            return Err(ExprError::Invalid("modulo by zero".into()));
                        }
                        let rem = a % b;
                        if rem < 0.0 { rem + b } else { rem }
                    }
                    _ => unreachable!(),
                };
                ScalarValue::Float32(Some(n))
            }
            (
                ScalarValue::Float64(Some(a)),
                ScalarValue::Float64(Some(b)),
                Type::Primitive(PrimitiveType::Float64),
            ) => {
                let n = match op {
                    BinaryOperator::Add => a + b,
                    BinaryOperator::Sub => a - b,
                    BinaryOperator::Mul => a * b,
                    BinaryOperator::Div => a / b,
                    BinaryOperator::Floordiv => (a / b).floor(),
                    BinaryOperator::Mod => {
                        if b == 0.0 {
                            return Err(ExprError::Invalid("modulo by zero".into()));
                        }
                        let rem = a % b;
                        if rem < 0.0 { rem + b } else { rem }
                    }
                    BinaryOperator::Pow => a.powf(b),
                    _ => unreachable!(),
                };
                ScalarValue::Float64(Some(n))
            }
            _ => return Err(ExprError::Invalid("numeric operand type mismatch".into())),
        };
        values.push(result);
    }
    if values.is_empty() {
        return Ok(new_null_array(&t.to_arrow()?, 0));
    }
    Ok(ScalarValue::iter_to_array(values.into_iter())?)
}
fn scalar_at(a: &ArrayRef, i: usize) -> Result<ScalarValue, ExprError> {
    Ok(ScalarValue::try_from_array(a.as_ref(), i)?)
}

fn list_array(a: &ArrayRef) -> Result<&ListArray, ExprError> {
    a.as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| ExprError::Invalid("collection operation requires ListArray".into()))
}

fn array_index(
    array: &ArrayRef,
    indices: &ArrayRef,
    dtype: &DataType,
) -> Result<ArrayRef, ExprError> {
    let lists = list_array(array)?;
    let ix = indices
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| ExprError::Invalid("array index requires int32".into()))?;
    let mut take_indices = Vec::with_capacity(lists.len());
    for row in 0..lists.len() {
        if lists.is_null(row) || ix.is_null(row) {
            take_indices.push(None);
            continue;
        }
        let len = lists.value_length(row) as i64;
        let requested = i64::from(ix.value(row));
        let normalized = if requested < 0 {
            len + requested
        } else {
            requested
        };
        if normalized < 0 || normalized >= len {
            return Err(ExprError::Invalid(format!(
                "array index out of bounds: index={}, length={len}",
                ix.value(row)
            )));
        }
        take_indices.push(Some(
            (lists.value_offsets()[row] as i64 + normalized) as u32,
        ));
    }
    let index_array = UInt32Array::from(take_indices);
    let child = take(lists.values().as_ref(), &index_array, None)?;
    if child.data_type() != dtype {
        return Err(ExprError::Invalid(
            "array index result type mismatch".into(),
        ));
    }
    Ok(child)
}

fn array_slice(
    args: &[ArrayRef],
    has_start: bool,
    has_stop: bool,
    has_step: bool,
    dtype: &DataType,
) -> Result<ArrayRef, ExprError> {
    let lists = list_array(&args[0])?;
    let mut next = 1;
    let start = if has_start {
        let x = args[next]
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| ExprError::Invalid("slice start requires int32".into()))?;
        next += 1;
        Some(x)
    } else {
        None
    };
    let stop = if has_stop {
        let x = args[next]
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| ExprError::Invalid("slice stop requires int32".into()))?;
        next += 1;
        Some(x)
    } else {
        None
    };
    let step = if has_step {
        let x = args[next]
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| ExprError::Invalid("slice step requires int32".into()))?;
        Some(x)
    } else {
        None
    };
    let mut lengths = Vec::with_capacity(lists.len());
    let mut validity = Vec::with_capacity(lists.len());
    let mut child_indices = Vec::new();
    for row in 0..lists.len() {
        let n = lists.value_length(row) as i64;
        if lists.is_null(row)
            || start.is_some_and(|x| x.is_null(row))
            || stop.is_some_and(|x| x.is_null(row))
            || step.is_some_and(|x| x.is_null(row))
        {
            lengths.push(0);
            validity.push(false);
            continue;
        }
        let stride = step.map_or(1_i64, |x| i64::from(x.value(row)));
        if stride == 0 {
            return Err(ExprError::Invalid(
                "step cannot be 0 for array slice".into(),
            ));
        }
        let min_bound = if stride > 0 { 0 } else { -1 };
        let max_bound = if stride > 0 { n } else { n - 1 };
        let requested_start =
            start.map_or(if stride > 0 { 0 } else { -1 }, |x| i64::from(x.value(row)));
        let requested_stop = stop.map_or(if stride > 0 { n } else { -n - 1 }, |x| {
            i64::from(x.value(row))
        });
        let clamp = |x: i64, is_start: bool| {
            // Match Hail's emitted ArraySlice kernel: start clamps at >= len,
            // stop at > len; explicit negative stops retain Hail's own rule.
            let beyond = if is_start { x >= n } else { x > n };
            let within = if is_start { x + n >= 0 } else { x + n > 0 };
            if beyond {
                max_bound
            } else if x >= 0 {
                x
            } else if within {
                x + n
            } else {
                min_bound
            }
        };
        let first = clamp(requested_start, true);
        let end = clamp(requested_stop, false);
        let mut count = 0_usize;
        let mut at = first;
        while if stride > 0 { at < end } else { at > end } {
            child_indices.push(Some((lists.value_offsets()[row] as i64 + at) as u32));
            count += 1;
            at += stride;
        }
        lengths.push(count);
        validity.push(true);
    }
    let ids = UInt32Array::from(child_indices);
    let child = take(lists.values().as_ref(), &ids, None)?;
    let field = match dtype {
        DataType::List(f) => f.clone(),
        _ => return Err(ExprError::Invalid("slice result dtype is not list".into())),
    };
    Ok(Arc::new(ListArray::new(
        field,
        OffsetBuffer::from_lengths(lengths),
        child,
        Some(NullBuffer::from(validity)),
    )))
}

fn hail_f32_min(a: f32, b: f32) -> f32 {
    if a.is_nan() {
        a
    } else if b.is_nan() {
        b
    } else if a == b && a == 0.0 {
        f32::from_bits(a.to_bits() | b.to_bits())
    } else if a < b {
        a
    } else {
        b
    }
}
fn hail_f32_max(a: f32, b: f32) -> f32 {
    if a.is_nan() {
        a
    } else if b.is_nan() {
        b
    } else if a == b && a == 0.0 {
        f32::from_bits(a.to_bits() & b.to_bits())
    } else if a > b {
        a
    } else {
        b
    }
}
fn hail_f64_min(a: f64, b: f64) -> f64 {
    if a.is_nan() {
        a
    } else if b.is_nan() {
        b
    } else if a == b && a == 0.0 {
        f64::from_bits(a.to_bits() | b.to_bits())
    } else if a < b {
        a
    } else {
        b
    }
}
fn hail_f64_max(a: f64, b: f64) -> f64 {
    if a.is_nan() {
        a
    } else if b.is_nan() {
        b
    } else if a == b && a == 0.0 {
        f64::from_bits(a.to_bits() & b.to_bits())
    } else if a > b {
        a
    } else {
        b
    }
}

fn array_reduce(
    array: &ArrayRef,
    operation: ArrayReduction,
    filter_missing: bool,
    dtype: &DataType,
) -> Result<ArrayRef, ExprError> {
    let lists = list_array(array)?;
    // Hail folds in the element's native type from typed 0/1 identities.
    // DataFusion array_sum/product widen and return NULL on empty/all-null,
    // so these four small typed folds preserve Hail precision and identities.
    macro_rules! fold {
        ($array_ty:ty, $value_ty:ty, $zero:expr, $one:expr, $add:expr, $mul:expr, $min:expr, $max:expr) => {{
            let mut out: Vec<Option<$value_ty>> = Vec::with_capacity(lists.len());
            for row in 0..lists.len() {
                if lists.is_null(row) {
                    out.push(None);
                    continue;
                }
                let child = lists.value(row);
                let values = child.as_any().downcast_ref::<$array_ty>().ok_or_else(|| {
                    ExprError::Invalid("array reduction child type mismatch".into())
                })?;
                let mut acc = match operation {
                    ArrayReduction::Sum => Some($zero),
                    ArrayReduction::Product => Some($one),
                    _ => None,
                };
                let mut saw_missing = false;
                for i in 0..values.len() {
                    if values.is_null(i) {
                        if !filter_missing {
                            saw_missing = true;
                            break;
                        } else {
                            continue;
                        }
                    }
                    let x = values.value(i);
                    acc = Some(match (operation, acc) {
                        (ArrayReduction::Sum, Some(a)) => ($add)(a, x),
                        (ArrayReduction::Product, Some(a)) => ($mul)(a, x),
                        (ArrayReduction::Min, Some(a)) => ($min)(a, x),
                        (ArrayReduction::Max, Some(a)) => ($max)(a, x),
                        (_, None) => x,
                    });
                }
                out.push(if saw_missing { None } else { acc });
            }
            Ok(Arc::new(<$array_ty>::from(out)) as ArrayRef)
        }};
    }
    match dtype {
        DataType::Int32 => fold!(
            Int32Array,
            i32,
            0,
            1,
            i32::wrapping_add,
            i32::wrapping_mul,
            i32::min,
            i32::max
        ),
        DataType::Int64 => fold!(
            Int64Array,
            i64,
            0,
            1,
            i64::wrapping_add,
            i64::wrapping_mul,
            i64::min,
            i64::max
        ),
        DataType::Float32 => fold!(
            Float32Array,
            f32,
            0.0,
            1.0,
            |a, b| a + b,
            |a, b| a * b,
            hail_f32_min,
            hail_f32_max
        ),
        DataType::Float64 => fold!(
            Float64Array,
            f64,
            0.0,
            1.0,
            |a, b| a + b,
            |a, b| a * b,
            hail_f64_min,
            hail_f64_max
        ),
        _ => Err(ExprError::Invalid(
            "array reduction requires numeric array".into(),
        )),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum HailKind {
    Numeric(BinaryOperator),
    GetField(String),
    Struct(Vec<String>),
    ArrayIndex,
    ArraySlice { start: bool, stop: bool, step: bool },
    ArrayReduce(ArrayReduction, bool),
}

#[derive(Debug)]
struct HailPhysicalExpr {
    kind: HailKind,
    args: Vec<Arc<dyn PhysicalExpr>>,
    dtype: DataType,
}
impl PartialEq for HailPhysicalExpr {
    fn eq(&self, other: &Self) -> bool {
        self.kind == other.kind && self.dtype == other.dtype && self.args == other.args
    }
}
impl Eq for HailPhysicalExpr {}
impl std::hash::Hash for HailPhysicalExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.kind.hash(state);
        self.dtype.hash(state);
        for arg in &self.args {
            arg.hash(state);
        }
    }
}
impl std::fmt::Display for HailPhysicalExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Hail({:?})", self.kind)
    }
}
impl PhysicalExpr for HailPhysicalExpr {
    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
    fn data_type(&self, _: &Schema) -> datafusion_common::Result<DataType> {
        Ok(self.dtype.clone())
    }
    fn nullable(&self, _: &Schema) -> datafusion_common::Result<bool> {
        Ok(true)
    }
    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.args.iter().collect()
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion_common::Result<Arc<dyn PhysicalExpr>> {
        if children.len() != self.args.len() {
            return Err(DataFusionError::Internal(
                "Hail expression child count mismatch".into(),
            ));
        }
        Ok(Arc::new(Self {
            kind: self.kind.clone(),
            args: children,
            dtype: self.dtype.clone(),
        }))
    }
    fn evaluate(&self, batch: &RecordBatch) -> datafusion_common::Result<ColumnarValue> {
        let len = batch.num_rows();
        let args = self
            .args
            .iter()
            .map(|x| x.evaluate(batch)?.into_array(len))
            .collect::<datafusion_common::Result<Vec<_>>>()?;
        let result = (|| -> Result<ArrayRef, ExprError> {
            match &self.kind {
                HailKind::Numeric(op) => {
                    let t = Type::from_arrow(&self.dtype)?;
                    custom_numeric(*op, &args[0], &args[1], &t)
                }
                HailKind::GetField(name) => {
                    let parent = &args[0];
                    let st = parent
                        .as_any()
                        .downcast_ref::<StructArray>()
                        .ok_or_else(|| {
                            ExprError::Invalid("get_field requires struct array".into())
                        })?;
                    let child = st.column_by_name(name).ok_or_else(|| {
                        ExprError::Invalid(format!("struct field {name:?} absent"))
                    })?;
                    let mask = Arc::new(BooleanArray::from(
                        (0..len).map(|i| parent.is_null(i)).collect::<Vec<_>>(),
                    )) as ArrayRef;
                    let mask = mask.as_any().downcast_ref::<BooleanArray>().unwrap();
                    Ok(nullif(child.as_ref(), mask)?)
                }
                HailKind::Struct(names) => {
                    let fields = names
                        .iter()
                        .zip(args)
                        .map(|(name, array)| {
                            (
                                Arc::new(Field::new(name, array.data_type().clone(), true)),
                                array,
                            )
                        })
                        .collect::<Vec<_>>();
                    if fields.is_empty() {
                        Ok(Arc::new(StructArray::new(
                            Vec::<Arc<Field>>::new().into(),
                            Vec::new(),
                            Some(NullBuffer::new_valid(len)),
                        )))
                    } else {
                        Ok(Arc::new(StructArray::from(fields)))
                    }
                }
                HailKind::ArrayIndex => array_index(&args[0], &args[1], &self.dtype),
                HailKind::ArraySlice { start, stop, step } => {
                    array_slice(&args, *start, *stop, *step, &self.dtype)
                }
                HailKind::ArrayReduce(operation, filter_missing) => {
                    array_reduce(&args[0], *operation, *filter_missing, &self.dtype)
                }
            }
        })();
        result
            .map(ColumnarValue::Array)
            .map_err(|e| DataFusionError::Execution(e.to_string()))
    }
}

fn custom(
    kind: HailKind,
    args: Vec<Arc<dyn PhysicalExpr>>,
    dtype: DataType,
) -> Arc<dyn PhysicalExpr> {
    Arc::new(HailPhysicalExpr { kind, args, dtype })
}
fn lower_cast(
    child: Arc<dyn PhysicalExpr>,
    source: &Schema,
    target: &DataType,
) -> Result<Arc<dyn PhysicalExpr>, ExprError> {
    Ok(cast(child, source, target.clone())?)
}

/// Lower the complete typed IR to a DataFusion physical-expression tree.
/// The source schema is fixed at this compilation boundary.
pub fn lower(expr: &Expr, schema: &Schema) -> Result<Arc<dyn PhysicalExpr>, ExprError> {
    let dtype = expr.dtype(schema)?.to_arrow()?;
    Ok(match expr {
        Expr::Column { name } => Arc::new(Column::new(
            name,
            schema
                .index_of(name)
                .map_err(|_| ExprError::Invalid(format!("source column {name:?} is absent")))?,
        )),
        Expr::Literal { dtype, value } => {
            let one = literal_array(value, dtype, 1)?;
            Arc::new(Literal::new(ScalarValue::try_from_array(one.as_ref(), 0)?))
        }
        Expr::Cast { dtype, value } => {
            lower_cast(lower(value, schema)?, schema, &dtype.to_arrow()?)?
        }
        Expr::IsMissing { value } => Arc::new(IsNullExpr::new(lower(value, schema)?)),
        Expr::Array {
            element_type,
            elements,
        } => {
            if elements.is_empty() {
                let ty = Type::Array {
                    array: Box::new(element_type.clone()),
                };
                let a = literal_array(&json!([]), &ty, 1)?;
                Arc::new(Literal::new(ScalarValue::try_from_array(a.as_ref(), 0)?))
            } else {
                let children = elements
                    .iter()
                    .map(|e| lower(e, schema))
                    .collect::<Result<Vec<_>, _>>()?;
                Arc::new(ScalarFunctionExpr::try_new(
                    datafusion_functions_nested::make_array::make_array_udf(),
                    children,
                    schema,
                    Arc::new(ConfigOptions::default()),
                )?)
            }
        }
        Expr::ArrayLength { value } => {
            let child = lower(value, schema)?;
            let udf = Arc::new(ScalarFunctionExpr::try_new(
                datafusion_functions_nested::length::array_length_udf(),
                vec![child],
                schema,
                Arc::new(ConfigOptions::default()),
            )?);
            cast(udf, schema, dtype.clone())?
        }
        Expr::ArrayIndex { value, index } => custom(
            HailKind::ArrayIndex,
            vec![lower(value, schema)?, lower(index, schema)?],
            dtype,
        ),
        Expr::ArraySlice {
            value,
            start,
            stop,
            step,
        } => {
            let mut args = vec![lower(value, schema)?];
            for e in [start, stop, step].into_iter().flatten() {
                args.push(lower(e, schema)?);
            }
            custom(
                HailKind::ArraySlice {
                    start: start.is_some(),
                    stop: stop.is_some(),
                    step: step.is_some(),
                },
                args,
                dtype,
            )
        }
        Expr::ArrayReduce {
            operation,
            filter_missing,
            value,
        } => custom(
            HailKind::ArrayReduce(*operation, *filter_missing),
            vec![lower(value, schema)?],
            dtype,
        ),
        Expr::GetField { value, name } => custom(
            HailKind::GetField(name.clone()),
            vec![lower(value, schema)?],
            dtype,
        ),
        Expr::Struct { fields } => custom(
            HailKind::Struct(fields.iter().map(|(n, _)| n.clone()).collect()),
            fields
                .iter()
                .map(|(_, e)| lower(e, schema))
                .collect::<Result<Vec<_>, _>>()?,
            dtype,
        ),
        Expr::Unary { operator, value } => {
            let child = lower(value, schema)?;
            match operator {
                UnaryOperator::Pos => child,
                UnaryOperator::Neg => Arc::new(NegativeExpr::new(child)),
                UnaryOperator::Not => Arc::new(NotExpr::new(child)),
            }
        }
        Expr::Binary {
            operator,
            left,
            right,
        } => {
            let lt = left.dtype(schema)?;
            let rt = right.dtype(schema)?;
            let mut l = lower(left, schema)?;
            let mut r = lower(right, schema)?;
            match operator {
                BinaryOperator::And | BinaryOperator::Or => binary(
                    l,
                    if *operator == BinaryOperator::And {
                        Operator::And
                    } else {
                        Operator::Or
                    },
                    r,
                    schema,
                )?,
                BinaryOperator::Eq
                | BinaryOperator::Ne
                | BinaryOperator::Lt
                | BinaryOperator::Le
                | BinaryOperator::Gt
                | BinaryOperator::Ge => {
                    let common = if lt == rt { lt } else { promote(&lt, &rt)? };
                    let common = common.to_arrow()?;
                    l = lower_cast(l, schema, &common)?;
                    r = lower_cast(r, schema, &common)?;
                    let op = match operator {
                        BinaryOperator::Eq => Operator::Eq,
                        BinaryOperator::Ne => Operator::NotEq,
                        BinaryOperator::Lt => Operator::Lt,
                        BinaryOperator::Le => Operator::LtEq,
                        BinaryOperator::Gt => Operator::Gt,
                        BinaryOperator::Ge => Operator::GtEq,
                        _ => unreachable!(),
                    };
                    binary(l, op, r, schema)?
                }
                BinaryOperator::Div => {
                    l = lower_cast(l, schema, &dtype)?;
                    r = lower_cast(r, schema, &dtype)?;
                    binary(l, Operator::Divide, r, schema)?
                }
                _ => {
                    l = lower_cast(l, schema, &dtype)?;
                    r = lower_cast(r, schema, &dtype)?;
                    custom(HailKind::Numeric(*operator), vec![l, r], dtype)
                }
            }
        }
        Expr::IfElse {
            condition,
            consequent,
            alternate,
        } => {
            let cond = lower(condition, schema)?;
            let yes = lower_cast(lower(consequent, schema)?, schema, &dtype)?;
            let no = lower_cast(lower(alternate, schema)?, schema, &dtype)?;
            let missing: Arc<dyn PhysicalExpr> =
                Arc::new(Literal::new(ScalarValue::try_from(&dtype)?));
            let is_null: Arc<dyn PhysicalExpr> = Arc::new(IsNullExpr::new(cond.clone()));
            Arc::new(CaseExpr::try_new(
                None,
                vec![(is_null, missing), (cond, yes)],
                Some(no),
            )?)
        }
    })
}

/// Compile each projection expression once for reuse across ordered batches.
pub struct CompiledProjection {
    source_schema: SchemaRef,
    schema: SchemaRef,
    expressions: Vec<Arc<dyn PhysicalExpr>>,
}
impl CompiledProjection {
    pub fn new(source: &Schema, fields: &[(String, Expr)]) -> Result<Self, ExprError> {
        let schema = validate_projection(source, fields)?;
        let expressions = fields
            .iter()
            .map(|(_, e)| lower(e, source))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            source_schema: Arc::new(source.clone()),
            schema,
            expressions,
        })
    }
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }
    pub fn project_batch(&self, batch: &RecordBatch) -> Result<RecordBatch, ExprError> {
        if batch.schema() != self.source_schema {
            return Err(ExprError::Invalid(
                "projection batch schema differs from compiled source schema".into(),
            ));
        }
        let arrays = self
            .expressions
            .iter()
            .map(|e| Ok(e.evaluate(batch)?.into_array(batch.num_rows())?))
            .collect::<Result<Vec<_>, ExprError>>()?;
        Ok(RecordBatch::try_new_with_options(
            self.schema.clone(),
            arrays,
            &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
        )?)
    }
}

pub fn project_batch(
    batch: &RecordBatch,
    fields: &[(String, Expr)],
) -> Result<RecordBatch, ExprError> {
    CompiledProjection::new(&batch.schema(), fields)?.project_batch(batch)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn source(values: Vec<Option<i32>>) -> RecordBatch {
        let len = values.len();
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("x", DataType::Int32, true),
                Field::new("flag", DataType::Boolean, true),
            ])),
            vec![
                Arc::new(Int32Array::from(values)),
                Arc::new(BooleanArray::from(
                    (0..len)
                        .map(|i| match i % 3 {
                            0 => Some(true),
                            1 => Some(false),
                            _ => None,
                        })
                        .collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }
    fn expr(v: Value) -> Expr {
        serde_json::from_value(v).unwrap()
    }
    fn values(a: &ArrayRef) -> Vec<ScalarValue> {
        (0..a.len())
            .map(|i| ScalarValue::try_from_array(a.as_ref(), i).unwrap())
            .collect()
    }

    #[test]
    fn json_lowering_chained_numeric_and_kleene() {
        let b = source(vec![Some(i32::MAX), Some(-3), None]);
        let fields:Projection=serde_json::from_value(json!([
            ["wrap",{"op":"binary","operator":"add","left":{"op":"column","name":"x"},"right":{"op":"literal","dtype":"int32","value":1}}],
            ["floor",{"op":"binary","operator":"floordiv","left":{"op":"column","name":"x"},"right":{"op":"literal","dtype":"int32","value":-2}}],
            ["kleene",{"op":"binary","operator":"and","left":{"op":"column","name":"flag"},"right":{"op":"literal","dtype":"bool","value":false}}]
        ])).unwrap();
        let plan = CompiledProjection::new(&b.schema(), &fields).unwrap();
        assert_eq!(plan.expressions.len(), 3);
        let out = plan.project_batch(&b).unwrap();
        assert_eq!(
            values(out.column(0)),
            vec![
                ScalarValue::Int32(Some(i32::MIN)),
                ScalarValue::Int32(Some(-2)),
                ScalarValue::Int32(None)
            ]
        );
        assert_eq!(
            values(out.column(1)),
            vec![
                ScalarValue::Int32(Some(-1073741824)),
                ScalarValue::Int32(Some(1)),
                ScalarValue::Int32(None)
            ]
        );
        assert_eq!(
            values(out.column(2)),
            vec![ScalarValue::Boolean(Some(false)); 3]
        );
    }

    #[test]
    fn lazy_if_else_and_missing_condition() {
        let b = source(vec![Some(10), Some(20), Some(30)]);
        let divide_zero = json!({"op":"binary","operator":"floordiv","left":{"op":"column","name":"x"},"right":{"op":"literal","dtype":"int32","value":0}});
        let e = expr(
            json!({"op":"if_else","condition":{"op":"literal","dtype":"bool","value":true},"consequent":{"op":"column","name":"x"},"alternate":divide_zero}),
        );
        let result = project_batch(&b, &[("safe".into(), e)]).unwrap();
        assert_eq!(
            values(result.column(0)),
            vec![
                ScalarValue::Int32(Some(10)),
                ScalarValue::Int32(Some(20)),
                ScalarValue::Int32(Some(30))
            ]
        );
        let e = expr(
            json!({"op":"if_else","condition":{"op":"literal","dtype":"bool","value":null},"consequent":{"op":"column","name":"x"},"alternate":divide_zero}),
        );
        let result = project_batch(&b, &[("missing".into(), e)]).unwrap();
        assert_eq!(values(result.column(0)), vec![ScalarValue::Int32(None); 3]);
    }

    #[test]
    fn struct_parent_nulls_array_literals_and_zero_fields() {
        let parent = StructArray::new(
            vec![Arc::new(Field::new("a", DataType::Int32, true))].into(),
            vec![Arc::new(Int32Array::from(vec![Some(7), Some(8)]))],
            Some(NullBuffer::from(vec![true, false])),
        );
        let b = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "s",
                parent.data_type().clone(),
                true,
            )])),
            vec![Arc::new(parent)],
        )
        .unwrap();
        let e = expr(json!({"op":"get_field","value":{"op":"column","name":"s"},"name":"a"}));
        let array = expr(json!({"op":"literal","dtype":{"array":"int32"},"value":[1,null,3]}));
        let out = project_batch(&b, &[("a".into(), e), ("arr".into(), array)]).unwrap();
        assert_eq!(
            values(out.column(0)),
            vec![ScalarValue::Int32(Some(7)), ScalarValue::Int32(None)]
        );
        assert_eq!(
            out.column(1).data_type(),
            &DataType::List(Arc::new(Field::new("item", DataType::Int32, true)))
        );
        let empty = project_batch(&b, &[]).unwrap();
        assert_eq!(empty.num_columns(), 0);
        assert_eq!(empty.num_rows(), 2);
        assert_eq!(project_batch(&empty, &[]).unwrap().num_rows(), 2);
    }

    #[test]
    fn source_validation_and_float_special_values() {
        let b = source(vec![Some(1)]);
        assert!(
            validate_projection(
                &b.schema(),
                &[("bad".into(), expr(json!({"op":"column","name":"missing"})))]
            )
            .is_err()
        );
        assert!(
            validate_projection(
                &b.schema(),
                &[
                    (
                        "a".into(),
                        expr(json!({"op":"literal","dtype":"int32","value":1}))
                    ),
                    (
                        "a".into(),
                        expr(json!({"op":"literal","dtype":"int32","value":2}))
                    )
                ]
            )
            .is_err()
        );
        let fields = vec![(
            "nan".into(),
            expr(json!({"op":"literal","dtype":"float64","value":{"$float":"nan"}})),
        )];
        let out = project_batch(&b, &fields).unwrap();
        assert!(matches!(values(out.column(0))[0],ScalarValue::Float64(Some(v)) if v.is_nan()));
        let bad = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)])),
            vec![Arc::new(Int64Array::from(vec![Some(1)]))],
        )
        .unwrap();
        assert!(
            CompiledProjection::new(&b.schema(), &fields)
                .unwrap()
                .project_batch(&bad)
                .is_err()
        );
    }

    #[test]
    fn array_ir_rejects_bad_types_and_handles_zero_rows() {
        let empty = source(vec![]);
        let int32 = Type::Primitive(PrimitiveType::Int32);
        let bad_index = Expr::ArrayIndex {
            value: Box::new(Expr::Literal {
                dtype: Type::Array {
                    array: Box::new(int32.clone()),
                },
                value: json!([1]),
            }),
            index: Box::new(Expr::Literal {
                dtype: Type::Primitive(PrimitiveType::Float64),
                value: json!(0.0),
            }),
        };
        assert!(validate_projection(&empty.schema(), &[("bad".into(), bad_index)]).is_err());

        let bad_reduction = Expr::ArrayReduce {
            operation: ArrayReduction::Sum,
            filter_missing: true,
            value: Box::new(Expr::Literal {
                dtype: Type::Array {
                    array: Box::new(Type::Primitive(PrimitiveType::Bool)),
                },
                value: json!([true]),
            }),
        };
        assert!(validate_projection(&empty.schema(), &[("bad".into(), bad_reduction)]).is_err());

        let array = Expr::Array {
            element_type: int32.clone(),
            elements: vec![
                Expr::Column { name: "x".into() },
                Expr::Literal {
                    dtype: int32,
                    value: json!(9),
                },
            ],
        };
        let projected = project_batch(&empty, &[("values".into(), array)]).unwrap();
        assert_eq!(projected.num_rows(), 0);
        assert!(projected.column(0).is_empty());
    }
}
