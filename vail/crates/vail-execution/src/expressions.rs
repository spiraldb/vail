//! Typed, source-bound scalar expressions for in-memory Table projections.
//!
//! Evaluation uses DataFusion physical expressions for column references,
//! casts, comparisons, and Kleene Boolean operators. Hail's distinct integer
//! overflow, floor division, modulo, and missing-condition semantics are kept
//! explicit at this boundary.

use std::collections::HashSet;
use std::hash::Hash;
use std::sync::Arc;

use crate::reference::ReferenceDefinition;
use arrow_array::types::{Float32Type, Float64Type, Int32Type, Int64Type};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, ListArray,
    RecordBatch, RecordBatchOptions, StringArray, StructArray, UInt32Array, new_null_array,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_ord::sort::{SortColumn, SortOptions, lexsort_to_indices, sort_to_indices};
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
    #[error("unsupported operation: {0}")]
    Unsupported(String),
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
    Call,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Type {
    Primitive(PrimitiveType),
    Interval { interval: Box<Type> },
    Struct { r#struct: Vec<(String, Type)> },
    Array { array: Box<Type> },
    Set { set: Box<Type> },
    Dict { dict: Box<(Type, Type)> },
    Tuple { tuple: Vec<Type> },
    Ndarray { ndarray: Box<(Type, usize)> },
    Locus { locus: ReferenceDefinition },
}

impl Type {
    pub fn contains_unordered(&self) -> bool {
        match self {
            Self::Set { .. } | Self::Dict { .. } => true,
            Self::Struct { r#struct: fields } => fields.iter().any(|(_, t)| t.contains_unordered()),
            Self::Tuple { tuple } => tuple.iter().any(Self::contains_unordered),
            Self::Array { array } | Self::Interval { interval: array } => {
                array.contains_unordered()
            }
            Self::Ndarray { ndarray } => ndarray.0.contains_unordered(),
            _ => false,
        }
    }
    pub fn ensure_keyable(&self) -> Result<(), ExprError> {
        if self.contains_unordered() || self.contains_ndarray() {
            Err(ExprError::Unsupported(
                "Set/Dictionary/ndarray ordering and keying".into(),
            ))
        } else {
            Ok(())
        }
    }
    fn contains_ndarray(&self) -> bool {
        match self {
            Self::Ndarray { .. } => true,
            Self::Struct { r#struct: fields } => fields.iter().any(|(_, t)| t.contains_ndarray()),
            Self::Tuple { tuple } => tuple.iter().any(Self::contains_ndarray),
            Self::Array { array } | Self::Interval { interval: array } => array.contains_ndarray(),
            _ => false,
        }
    }
    pub fn native_key(&self) -> bool {
        match self {
            Self::Set { .. } | Self::Dict { .. } | Self::Ndarray { .. } => false,
            Self::Struct { r#struct: fields } => fields.iter().all(|(_, t)| t.native_key()),
            Self::Tuple { tuple } => tuple.iter().all(Self::native_key),
            Self::Array { array } | Self::Interval { interval: array } => array.native_key(),
            _ => true,
        }
    }
    fn storage_type(&self) -> Option<Type> {
        match self {
            Self::Tuple { tuple } if tuple.is_empty() => Some(Self::Struct {
                // Preserve the logical distinction from a zero-field struct through
                // Arrow/DataFusion schemas and Vortex storage. This is a type tag.
                r#struct: vec![(
                    "__vail_tuple_empty".into(),
                    Type::Primitive(PrimitiveType::Bool),
                )],
            }),
            Self::Tuple { tuple } => Some(Self::Struct {
                r#struct: tuple
                    .iter()
                    .enumerate()
                    .map(|(i, t)| (format!("__vail_tuple_{i}"), t.clone()))
                    .collect(),
            }),
            Self::Primitive(PrimitiveType::Call) => Some(Self::Struct {
                r#struct: vec![(
                    "__vail_call_word".into(),
                    Self::Primitive(PrimitiveType::Int32),
                )],
            }),
            Self::Dict { dict } => Some(Self::Array {
                array: Box::new(Self::Struct {
                    r#struct: vec![
                        ("key".into(), dict.0.clone()),
                        ("value".into(), dict.1.clone()),
                    ],
                }),
            }),
            Self::Ndarray { ndarray } => Some(Self::Struct {
                r#struct: vec![
                    (
                        "__vail_ndarray_data".into(),
                        Self::Array {
                            array: Box::new(ndarray.0.clone()),
                        },
                    ),
                    (
                        "__vail_ndarray_shape".into(),
                        Self::Array {
                            array: Box::new(Self::Primitive(PrimitiveType::Int64)),
                        },
                    ),
                ],
            }),
            Self::Interval { interval } => Some(Self::Struct {
                r#struct: vec![
                    ("__vail_interval_start".into(), (**interval).clone()),
                    (
                        "__vail_interval_start_exclusive".into(),
                        Self::Primitive(PrimitiveType::Bool),
                    ),
                    ("__vail_interval_end".into(), (**interval).clone()),
                    (
                        "__vail_interval_end_inclusive".into(),
                        Self::Primitive(PrimitiveType::Bool),
                    ),
                ],
            }),
            _ => None,
        }
    }

    pub fn to_arrow(&self) -> Result<DataType, ExprError> {
        let dtype = match self {
            Self::Primitive(PrimitiveType::Int32) => DataType::Int32,
            Self::Primitive(PrimitiveType::Int64) => DataType::Int64,
            Self::Primitive(PrimitiveType::Float32) => DataType::Float32,
            Self::Primitive(PrimitiveType::Float64) => DataType::Float64,
            Self::Primitive(PrimitiveType::Bool) => DataType::Boolean,
            Self::Primitive(PrimitiveType::Str) => DataType::Utf8,
            Self::Tuple { .. } | Self::Primitive(PrimitiveType::Call) | Self::Interval { .. } => {
                self.storage_type().unwrap().to_arrow()?
            }
            Self::Locus { locus } => {
                locus.validate().map_err(ExprError::Invalid)?;
                let metadata = [(
                    "vail.reference".to_string(),
                    serde_json::to_string(locus).map_err(|e| ExprError::Invalid(e.to_string()))?,
                )]
                .into_iter()
                .collect();
                DataType::Struct(
                    vec![
                        Arc::new(Field::new("__vail_locus_ordinal", DataType::Int32, true)),
                        Arc::new(
                            Field::new("__vail_locus_contig", DataType::Utf8, true)
                                .with_metadata(metadata),
                        ),
                        Arc::new(Field::new("__vail_locus_position", DataType::Int32, true)),
                    ]
                    .into(),
                )
            }
            Self::Dict { .. } => {
                let DataType::List(field) = self.storage_type().unwrap().to_arrow()? else {
                    unreachable!()
                };
                DataType::List(Arc::new(Field::new(
                    "__vail_dict_entry",
                    field.data_type().clone(),
                    true,
                )))
            }
            Self::Ndarray { ndarray } => DataType::Struct(
                vec![
                    Arc::new(Field::new(
                        "__vail_ndarray_data",
                        DataType::List(Arc::new(Field::new(
                            format!("__vail_ndarray_item_{}", ndarray.1),
                            ndarray.0.to_arrow()?,
                            true,
                        ))),
                        true,
                    )),
                    Arc::new(Field::new(
                        "__vail_ndarray_shape",
                        DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                        true,
                    )),
                ]
                .into(),
            ),
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
            // Use the list element field name, not metadata: DataFusion's
            // scalar/cross-join paths retain names but can drop field metadata.
            Self::Set { set } => DataType::List(Arc::new(Field::new(
                "__vail_set_item",
                set.to_arrow()?,
                true,
            ))),
            Self::Array { array } => {
                DataType::List(Arc::new(Field::new("item", array.to_arrow()?, true)))
            }
        };
        if matches!(
            self,
            Self::Tuple { .. }
                | Self::Set { .. }
                | Self::Dict { .. }
                | Self::Interval { .. }
                | Self::Ndarray { .. }
                | Self::Locus { .. }
                | Self::Primitive(PrimitiveType::Call)
        ) {
            let logical =
                serde_json::to_value(self).map_err(|e| ExprError::Invalid(e.to_string()))?;
            Ok(vail_ordering::tag_type(dtype, &logical))
        } else {
            Ok(dtype)
        }
    }

    pub fn from_arrow(dtype: &DataType) -> Result<Self, ExprError> {
        if let Some(logical) =
            vail_ordering::type_metadata(dtype).map_err(|e| ExprError::Invalid(e.to_string()))?
        {
            match dtype {
                DataType::Struct(fields) => {
                    for field in fields {
                        Self::from_arrow(field.data_type())?;
                    }
                }
                DataType::List(field) => {
                    Self::from_arrow(field.data_type())?;
                }
                _ => (),
            }
            let ty: Self = serde_json::from_value(logical)
                .map_err(|e| ExprError::Invalid(format!("invalid Arrow logical type: {e}")))?;
            if let Self::Locus { locus } = &ty {
                locus.validate().map_err(ExprError::Invalid)?;
                let DataType::Struct(fields) = dtype else {
                    return Err(ExprError::Invalid("invalid Locus storage".into()));
                };
                let stored = fields
                    .get(1)
                    .and_then(|f| f.metadata().get("vail.reference"))
                    .ok_or_else(|| ExprError::Invalid("Locus reference metadata missing".into()))?;
                let reference: ReferenceDefinition =
                    serde_json::from_str(stored).map_err(|e| ExprError::Invalid(e.to_string()))?;
                if &reference != locus {
                    return Err(ExprError::Invalid(
                        "Locus reference metadata disagrees with its logical type".into(),
                    ));
                }
            }
            if !vail_ordering::same_layout(dtype, &ty.to_arrow()?) {
                return Err(ExprError::Invalid(
                    "Arrow logical type metadata does not match physical layout".into(),
                ));
            }
            return Ok(ty);
        }
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
                    .collect::<Result<_, ExprError>>()?,
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
    /// Bound at execution time from an Arrow singleton, never accepted on the wire.
    #[serde(skip)]
    RuntimeLiteral {
        value: ScalarValue,
    },
    Aggregate {
        function: String,
        value: Option<Box<Expr>>,
        #[serde(default)]
        options: Value,
        #[serde(default)]
        init: Vec<(String, Expr)>,
    },
    AggregateFold {
        initial: Box<Expr>,
        seq: Box<Expr>,
        combine: Box<Expr>,
        state_parameter: String,
        left_parameter: String,
        right_parameter: String,
    },
    AggGroupBy {
        key: Box<Expr>,
        body: Box<Expr>,
    },
    AggArray {
        value: Box<Expr>,
        parameter: String,
        body: Box<Expr>,
    },
    AggFilter {
        predicate: Box<Expr>,
        body: Box<Expr>,
    },
    AggExplode {
        value: Box<Expr>,
        parameter: String,
        body: Box<Expr>,
    },
    LambdaVariable {
        name: String,
    },
    /// Built-in DataFusion collection function; no Vail evaluation kernel.
    NestedFunction {
        function: String,
        args: Vec<Expr>,
        dtype: Type,
    },
    /// Change logical collection tags without changing the element representation.
    CollectionCast {
        value: Box<Expr>,
        dtype: Type,
    },
    IntegerBinary {
        operator: BinaryOperator,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    StringFunction {
        function: String,
        args: Vec<Expr>,
    },
    Tuple {
        elements: Vec<Expr>,
    },
    ArrayMap {
        value: Box<Expr>,
        parameter: String,
        body: Box<Expr>,
    },
    ArrayFold {
        value: Box<Expr>,
        zero: Box<Expr>,
        accumulator: String,
        parameter: String,
        body: Box<Expr>,
        scan: bool,
    },
    ArrayPredicate {
        value: Box<Expr>,
        parameter: String,
        predicate: Box<Expr>,
        first: bool,
    },
    ArrayFilter {
        value: Box<Expr>,
        parameter: String,
        predicate: Box<Expr>,
    },
    StreamRange {
        start: Box<Expr>,
        stop: Box<Expr>,
        step: Box<Expr>,
    },
    ArrayGrouped {
        value: Box<Expr>,
        size: Box<Expr>,
    },
    ExternalGlobal {
        reference: String,
        dtype: Type,
    },
    Global {
        name: String,
    },
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
    ToString {
        value: Box<Expr>,
    },
    GetField {
        value: Box<Expr>,
        name: String,
    },
    Interval {
        start: Box<Expr>,
        end: Box<Expr>,
        includes_start: Box<Expr>,
        includes_end: Box<Expr>,
    },
    IntervalContains {
        interval: Box<Expr>,
        point: Box<Expr>,
    },
    IntervalOverlaps {
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Struct {
        fields: Vec<(String, Expr)>,
    },
    IfElse {
        condition: Box<Expr>,
        consequent: Box<Expr>,
        alternate: Box<Expr>,
    },
    Die {
        message: Box<Expr>,
        dtype: Type,
    },
    IsMissing {
        value: Box<Expr>,
    },
    Array {
        element_type: Type,
        elements: Vec<Expr>,
    },
    ArrayFlatten {
        value: Box<Expr>,
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
    Locus {
        reference: ReferenceDefinition,
        contig: Box<Expr>,
        position: Box<Expr>,
    },
    LocusOp {
        operation: String,
        value: Box<Expr>,
    },
    LocusValid {
        reference: ReferenceDefinition,
        contig: Box<Expr>,
        position: Box<Expr>,
    },
    LocusFromGlobalPosition {
        reference: ReferenceDefinition,
        position: Box<Expr>,
    },
    ParseLocus {
        reference: ReferenceDefinition,
        value: Box<Expr>,
    },
    ParseCsvLine {
        value: Box<Expr>,
        delimiter: String,
        quote: Option<String>,
        #[serde(default)]
        missing: Vec<String>,
        expected_fields: usize,
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
    /// Visit typed expression nodes, never literal payloads or type metadata.
    pub fn visit(&self, f: &mut impl FnMut(&Expr)) {
        f(self);
        match self {
            Self::Aggregate { value, init, .. } => {
                if let Some(value) = value {
                    value.visit(f);
                }
                for (_, expr) in init {
                    expr.visit(f);
                }
            }
            Self::AggregateFold {
                initial,
                seq,
                combine,
                ..
            } => {
                initial.visit(f);
                seq.visit(f);
                combine.visit(f);
            }
            Self::AggGroupBy { key, body } => {
                key.visit(f);
                body.visit(f);
            }
            Self::AggArray { value, body, .. } => {
                value.visit(f);
                body.visit(f);
            }
            Self::AggFilter { predicate, body } => {
                predicate.visit(f);
                body.visit(f);
            }
            Self::AggExplode { value, body, .. } => {
                value.visit(f);
                body.visit(f);
            }
            Self::LambdaVariable { .. }
            | Self::ExternalGlobal { .. }
            | Self::Global { .. }
            | Self::Column { .. }
            | Self::Literal { .. }
            | Self::RuntimeLiteral { .. } => (),
            Self::Tuple { elements: args }
            | Self::StringFunction { args, .. }
            | Self::NestedFunction { args, .. } => {
                for arg in args {
                    arg.visit(f);
                }
            }
            Self::ArrayMap { value, body, .. } => {
                value.visit(f);
                body.visit(f);
            }
            Self::ArrayFold {
                value, zero, body, ..
            } => {
                value.visit(f);
                zero.visit(f);
                body.visit(f);
            }
            Self::ArrayPredicate {
                value, predicate, ..
            }
            | Self::ArrayFilter {
                value, predicate, ..
            } => {
                value.visit(f);
                predicate.visit(f);
            }
            Self::StreamRange { start, stop, step } => {
                start.visit(f);
                stop.visit(f);
                step.visit(f);
            }
            Self::ArrayGrouped { value, size } => {
                value.visit(f);
                size.visit(f);
            }
            Self::IntegerBinary { left, right, .. } | Self::Binary { left, right, .. } => {
                left.visit(f);
                right.visit(f);
            }
            Self::Die { message: value, .. }
            | Self::Unary { value, .. }
            | Self::Cast { value, .. }
            | Self::CollectionCast { value, .. }
            | Self::ToString { value }
            | Self::GetField { value, .. }
            | Self::IsMissing { value }
            | Self::ArrayFlatten { value }
            | Self::ArrayLength { value }
            | Self::ArrayReduce { value, .. } => value.visit(f),
            Self::Interval {
                start,
                end,
                includes_start,
                includes_end,
            } => {
                start.visit(f);
                end.visit(f);
                includes_start.visit(f);
                includes_end.visit(f);
            }
            Self::IntervalContains { interval, point } => {
                interval.visit(f);
                point.visit(f);
            }
            Self::IntervalOverlaps { left, right } => {
                left.visit(f);
                right.visit(f);
            }
            Self::Struct { fields } => {
                for (_, expr) in fields {
                    expr.visit(f);
                }
            }
            Self::IfElse {
                condition,
                consequent,
                alternate,
            } => {
                condition.visit(f);
                consequent.visit(f);
                alternate.visit(f);
            }
            Self::Array { elements, .. } => {
                for expr in elements {
                    expr.visit(f);
                }
            }
            Self::ArrayIndex { value, index } => {
                value.visit(f);
                index.visit(f);
            }
            Self::Locus {
                contig, position, ..
            } => {
                contig.visit(f);
                position.visit(f);
            }
            Self::LocusOp { value, .. }
            | Self::ParseLocus { value, .. }
            | Self::ParseCsvLine { value, .. } => value.visit(f),
            Self::LocusValid {
                contig, position, ..
            } => {
                contig.visit(f);
                position.visit(f);
            }
            Self::LocusFromGlobalPosition { position, .. } => position.visit(f),
            Self::ArraySlice {
                value,
                start,
                stop,
                step,
            } => {
                value.visit(f);
                for expr in [start, stop, step].into_iter().flatten() {
                    expr.visit(f);
                }
            }
        }
    }

    /// Rewrite immediate typed children, excluding literal payloads and types.
    pub fn try_map_children<E>(
        &self,
        mut f: impl FnMut(&Expr) -> Result<Expr, E>,
    ) -> Result<Expr, E> {
        let mut result = self.clone();
        match &mut result {
            Self::Aggregate { value, init, .. } => {
                if let Some(value) = value {
                    **value = f(value)?;
                }
                for (_, expr) in init {
                    *expr = f(expr)?;
                }
            }
            Self::AggregateFold {
                initial,
                seq,
                combine,
                ..
            } => {
                **initial = f(initial)?;
                **seq = f(seq)?;
                **combine = f(combine)?;
            }
            Self::AggGroupBy { key, body } => {
                **key = f(key)?;
                **body = f(body)?;
            }
            Self::AggArray { value, body, .. } => {
                **value = f(value)?;
                **body = f(body)?;
            }
            Self::AggFilter { predicate, body } => {
                **predicate = f(predicate)?;
                **body = f(body)?;
            }
            Self::Tuple { elements: args }
            | Self::StringFunction { args, .. }
            | Self::NestedFunction { args, .. } => {
                for arg in args {
                    *arg = f(arg)?;
                }
            }
            Self::AggExplode { value, body, .. } | Self::ArrayMap { value, body, .. } => {
                **value = f(value)?;
                **body = f(body)?;
            }
            Self::ArrayFold {
                value, zero, body, ..
            } => {
                **value = f(value)?;
                **zero = f(zero)?;
                **body = f(body)?;
            }
            Self::ArrayPredicate {
                value, predicate, ..
            }
            | Self::ArrayFilter {
                value, predicate, ..
            } => {
                **value = f(value)?;
                **predicate = f(predicate)?;
            }
            Self::StreamRange { start, stop, step } => {
                **start = f(start)?;
                **stop = f(stop)?;
                **step = f(step)?;
            }
            Self::ArrayGrouped { value, size } => {
                **value = f(value)?;
                **size = f(size)?;
            }
            Self::IntegerBinary { left, right, .. } | Self::Binary { left, right, .. } => {
                **left = f(left)?;
                **right = f(right)?;
            }
            Self::Die { message: value, .. }
            | Self::Unary { value, .. }
            | Self::Cast { value, .. }
            | Self::CollectionCast { value, .. }
            | Self::ToString { value }
            | Self::GetField { value, .. }
            | Self::IsMissing { value }
            | Self::ArrayFlatten { value }
            | Self::ArrayLength { value }
            | Self::ArrayReduce { value, .. } => **value = f(value)?,
            Self::Interval {
                start,
                end,
                includes_start,
                includes_end,
            } => {
                **start = f(start)?;
                **end = f(end)?;
                **includes_start = f(includes_start)?;
                **includes_end = f(includes_end)?;
            }
            Self::IntervalContains { interval, point } => {
                **interval = f(interval)?;
                **point = f(point)?;
            }
            Self::IntervalOverlaps { left, right } => {
                **left = f(left)?;
                **right = f(right)?;
            }
            Self::Struct { fields } => {
                for (_, expr) in fields {
                    *expr = f(expr)?;
                }
            }
            Self::IfElse {
                condition,
                consequent,
                alternate,
            } => {
                **condition = f(condition)?;
                **consequent = f(consequent)?;
                **alternate = f(alternate)?;
            }
            Self::Array { elements, .. } => {
                for expr in elements {
                    *expr = f(expr)?;
                }
            }
            Self::ArrayIndex { value, index } => {
                **value = f(value)?;
                **index = f(index)?;
            }
            Self::Locus {
                contig, position, ..
            } => {
                **contig = f(contig)?;
                **position = f(position)?;
            }
            Self::LocusOp { value, .. }
            | Self::ParseLocus { value, .. }
            | Self::ParseCsvLine { value, .. } => **value = f(value)?,
            Self::LocusValid {
                contig, position, ..
            } => {
                **contig = f(contig)?;
                **position = f(position)?;
            }
            Self::LocusFromGlobalPosition { position, .. } => **position = f(position)?,
            Self::ArraySlice {
                value,
                start,
                stop,
                step,
            } => {
                **value = f(value)?;
                for expr in [start, stop, step].into_iter().flatten() {
                    **expr = f(expr)?;
                }
            }
            Self::Column { .. }
            | Self::Global { .. }
            | Self::ExternalGlobal { .. }
            | Self::Literal { .. }
            | Self::RuntimeLiteral { .. }
            | Self::LambdaVariable { .. } => (),
        }
        Ok(result)
    }

    pub fn contains_scan(&self) -> bool {
        let mut found = false;
        self.visit(&mut |node| {
            found |= matches!(node, Self::Aggregate { options, .. }
            if options.get("__vail_scan").and_then(Value::as_bool) == Some(true));
        });
        found
    }

    pub fn dtype_with_lexical(
        &self,
        schema: &Schema,
        globals: &Schema,
        variables: &[(String, Type)],
    ) -> Result<Type, ExprError> {
        let scope = variables
            .iter()
            .map(|(name, ty)| (name.clone(), ty.clone(), 0))
            .collect::<Vec<_>>();
        self.dtype_scoped(schema, globals, &scope)
    }

    pub fn dtype(&self, schema: &Schema) -> Result<Type, ExprError> {
        self.dtype_with_globals(schema, &Schema::empty())
    }

    pub fn dtype_with_globals(&self, schema: &Schema, globals: &Schema) -> Result<Type, ExprError> {
        self.dtype_scoped(schema, globals, &[])
    }

    fn dtype_scoped(
        &self,
        schema: &Schema,
        globals: &Schema,
        scope: &[(String, Type, usize)],
    ) -> Result<Type, ExprError> {
        match self {
            Self::Aggregate {
                function,
                value,
                options,
                ..
            } if options.get("__vail_scan").and_then(Value::as_bool) == Some(true) => {
                if function == "__scan_fold" {
                    if let Some(value) = value {
                        if let Self::AggregateFold { initial, .. } = value.as_ref() {
                            return initial.dtype_scoped(schema, globals, scope);
                        }
                    }
                    return Err(ExprError::Invalid("scan fold requires fold IR".into()));
                }
                if let Some(dtype) = options.get("__vail_result_type") {
                    return serde_json::from_value(dtype.clone())
                        .map_err(|e| ExprError::Invalid(format!("invalid scan result type: {e}")));
                }
                let value_type = value
                    .as_ref()
                    .map(|v| v.dtype_scoped(schema, globals, scope))
                    .transpose()?;
                let i64_type = Type::Primitive(PrimitiveType::Int64);
                let f64_type = Type::Primitive(PrimitiveType::Float64);
                Ok(match function.as_str() {
                    "count" | "count_where" => i64_type,
                    "any" | "all" => Type::Primitive(PrimitiveType::Bool),
                    "fraction" | "mean" | "stddev" => f64_type,
                    "sum" | "product" => match value_type {
                        Some(Type::Primitive(PrimitiveType::Float32 | PrimitiveType::Float64)) => {
                            f64_type
                        }
                        _ => i64_type,
                    },
                    "collect" => {
                        Type::Array {
                            array: Box::new(value_type.ok_or_else(|| {
                                ExprError::Invalid("collect requires input".into())
                            })?),
                        }
                    }
                    "take" => Type::Array {
                        array: Box::new(
                            value_type
                                .ok_or_else(|| ExprError::Invalid("take requires input".into()))?,
                        ),
                    },
                    "collect_as_set" => Type::Set {
                        set: Box::new(value_type.ok_or_else(|| {
                            ExprError::Invalid("collect_as_set requires input".into())
                        })?),
                    },
                    "corr" => f64_type,
                    "ndarray_sum" => value_type
                        .ok_or_else(|| ExprError::Invalid("ndarray_sum requires input".into()))?,
                    "min" | "max" => value_type
                        .ok_or_else(|| ExprError::Invalid("aggregate requires input".into()))?,
                    _ => {
                        return Err(ExprError::Invalid(format!(
                            "unsupported scan aggregate {function:?}"
                        )));
                    }
                })
            }
            Self::AggExplode {
                value,
                parameter,
                body,
            }
            | Self::AggArray {
                value,
                parameter,
                body,
            } if body.contains_scan() => {
                let element = match value.dtype_scoped(schema, globals, scope)? {
                    Type::Array { array } | Type::Set { set: array } => *array,
                    _ => {
                        return Err(ExprError::Invalid(
                            "scan collection scope requires array or set".into(),
                        ));
                    }
                };
                let mut nested = scope.to_vec();
                nested.push((parameter.clone(), element, 0));
                let result = body.dtype_scoped(schema, globals, &nested)?;
                if matches!(self, Self::AggArray { .. }) {
                    Ok(Type::Array {
                        array: Box::new(result),
                    })
                } else {
                    Ok(result)
                }
            }
            Self::AggGroupBy { key, body } if body.contains_scan() => Ok(Type::Dict {
                dict: Box::new((
                    key.dtype_scoped(schema, globals, scope)?,
                    body.dtype_scoped(schema, globals, scope)?,
                )),
            }),
            Self::Aggregate { .. }
            | Self::AggExplode { .. }
            | Self::AggGroupBy { .. }
            | Self::AggArray { .. }
            | Self::AggregateFold { .. } => Err(ExprError::Invalid(
                "aggregation expressions require Table.aggregate and cannot be nested".into(),
            )),
            Self::AggFilter { predicate, body } => {
                let mut scan_filter = false;
                body.visit(&mut |node| {
                    scan_filter |= matches!(node, Self::Aggregate { options, .. }
                        if options.get("__vail_scan").and_then(Value::as_bool) == Some(true));
                });
                if scan_filter {
                    if predicate.dtype_scoped(schema, globals, scope)?
                        != Type::Primitive(PrimitiveType::Bool)
                    {
                        return Err(ExprError::Invalid(
                            "scan.filter predicate must be Boolean".into(),
                        ));
                    }
                    body.dtype_scoped(schema, globals, scope)
                } else {
                    Err(ExprError::Invalid(
                        "aggregation expressions require Table.aggregate and cannot be nested"
                            .into(),
                    ))
                }
            }
            Self::LambdaVariable { name } => scope
                .iter()
                .rev()
                .find(|(n, _, _)| n == name)
                .map(|(_, ty, _)| ty.clone())
                .ok_or_else(|| ExprError::Invalid(format!("unbound lambda variable {name:?}"))),
            Self::IntegerBinary {
                operator,
                left,
                right,
            } => {
                let int64 = Type::Primitive(PrimitiveType::Int64);
                if left.dtype_scoped(schema, globals, scope)? != int64
                    || right.dtype_scoped(schema, globals, scope)? != int64
                {
                    return Err(ExprError::Invalid(
                        "integer binary requires int64 arguments".into(),
                    ));
                }
                match operator {
                    BinaryOperator::Add | BinaryOperator::Sub => Ok(int64),
                    BinaryOperator::Eq | BinaryOperator::Lt | BinaryOperator::Le => {
                        Ok(Type::Primitive(PrimitiveType::Bool))
                    }
                    _ => Err(ExprError::Invalid(
                        "unsupported integer binary operator".into(),
                    )),
                }
            }
            Self::StringFunction { function, args } => {
                let (_, expected, output) = string_function(function)?;
                if args.len() != expected.len() {
                    return Err(ExprError::Invalid(format!(
                        "{function} argument count mismatch"
                    )));
                }
                for (arg, expected) in args.iter().zip(expected) {
                    if arg.dtype_scoped(schema, globals, scope)? != expected {
                        return Err(ExprError::Invalid(format!(
                            "{function} argument type mismatch"
                        )));
                    }
                }
                Ok(output)
            }
            Self::Tuple { elements } => Ok(Type::Tuple {
                tuple: elements
                    .iter()
                    .map(|element| element.dtype_scoped(schema, globals, scope))
                    .collect::<Result<Vec<_>, ExprError>>()?,
            }),
            Self::NestedFunction {
                function,
                args,
                dtype,
            } => {
                // Validate the declared result against DataFusion's own type inference.
                let udf = nested_function(function)?;
                let args = args
                    .iter()
                    .map(|arg| arg.dtype_scoped(schema, globals, scope)?.to_arrow())
                    .collect::<Result<Vec<_>, ExprError>>()?;
                let expected = if function == "array_sort" {
                    3
                } else if function == "array_element"
                    || function == "arrays_zip"
                    || matches!(
                        function.as_str(),
                        "array_union" | "array_intersect" | "array_except" | "array_concat"
                    )
                {
                    2
                } else {
                    1
                };
                if args.len() != expected || !matches!(args.first(), Some(DataType::List(_))) {
                    return Err(ExprError::Invalid(format!("invalid {function} arguments")));
                }
                if function == "array_sort"
                    && (args[1] != DataType::Utf8 || args[2] != DataType::Utf8)
                {
                    return Err(ExprError::Invalid(
                        "array_sort ordering arguments must be strings".into(),
                    ));
                }
                if function == "array_element" && args[1] != DataType::Int64 {
                    return Err(ExprError::Invalid(
                        "array_element index must be int64".into(),
                    ));
                }
                if expected == 2
                    && !matches!(function.as_str(), "array_element" | "arrays_zip")
                    && !same_collection_storage(&args[0], &args[1])
                {
                    return Err(ExprError::Invalid(
                        "collection operands must have matching storage".into(),
                    ));
                }
                if matches!(function.as_str(), "array_sum" | "array_product")
                    && !matches!(&args[0], DataType::List(field) if field.data_type() == &DataType::Float64)
                {
                    return Err(ExprError::Invalid(
                        "array_sum/product require float64 elements".into(),
                    ));
                }
                if matches!(
                    function.as_str(),
                    "array_distinct" | "array_union" | "array_intersect" | "array_except"
                ) {
                    let input = args[0].clone();
                    let DataType::List(field) = input else {
                        unreachable!()
                    };
                    if !datafusion_distinct_compatible(&Type::from_arrow(field.data_type())?) {
                        return Err(ExprError::Invalid("DataFusion distinct does not preserve Vail key equality for this element type".into()));
                    }
                }
                let result = udf.return_type(&args)?;
                if !same_collection_storage(&result, &dtype.to_arrow()?) {
                    return Err(ExprError::Invalid(format!(
                        "{function} result type mismatch"
                    )));
                }
                Ok(dtype.clone())
            }
            Self::CollectionCast { value, dtype } => {
                let input = value.dtype_scoped(schema, globals, scope)?.to_arrow()?;
                let output = dtype.to_arrow()?;
                if !matches!(input, DataType::List(_))
                    || !matches!(output, DataType::List(_))
                    || !same_collection_storage(&input, &output)
                {
                    return Err(ExprError::Invalid(
                        "collection cast must preserve element storage".into(),
                    ));
                }
                Ok(dtype.clone())
            }
            Self::ArrayFold {
                value,
                zero,
                accumulator,
                parameter,
                body,
                scan,
            } => {
                let Type::Array { array } = value.dtype_scoped(schema, globals, scope)? else {
                    return Err(ExprError::Invalid("fold requires an array".into()));
                };
                let output = zero.dtype_scoped(schema, globals, scope)?;
                if accumulator == parameter
                    || scope
                        .iter()
                        .any(|(name, _, _)| name == accumulator || name == parameter)
                {
                    return Err(ExprError::Invalid(
                        "duplicate lambda parameter identity".into(),
                    ));
                }
                let mut nested = scope.to_vec();
                nested.push((accumulator.clone(), output.clone(), schema.fields().len()));
                nested.push((parameter.clone(), *array, schema.fields().len() + 1));
                if body.dtype_scoped(schema, globals, &nested)? != output {
                    return Err(ExprError::Invalid(
                        "fold callback type must equal initial value type".into(),
                    ));
                }
                Ok(if *scan {
                    Type::Array {
                        array: Box::new(output),
                    }
                } else {
                    output
                })
            }
            Self::ArrayMap {
                value,
                parameter,
                body,
            } => {
                let input_type = value.dtype_scoped(schema, globals, scope)?;
                let array = match input_type {
                    Type::Array { array } | Type::Set { set: array } => *array,
                    Type::Dict { dict } => Type::Struct {
                        r#struct: vec![("key".into(), dict.0), ("value".into(), dict.1)],
                    },
                    other => {
                        return Err(ExprError::Invalid(format!(
                            "array map requires array, set, or dict, got {other:?}"
                        )));
                    }
                };
                if scope.iter().any(|(name, _, _)| name == parameter) {
                    return Err(ExprError::Invalid(
                        "duplicate lambda parameter identity".into(),
                    ));
                }
                let mut nested = scope.to_vec();
                nested.push((parameter.clone(), array, schema.fields().len()));
                Ok(Type::Array {
                    array: Box::new(body.dtype_scoped(schema, globals, &nested)?),
                })
            }
            Self::ArrayPredicate {
                value,
                parameter,
                predicate,
                first,
            } => {
                let element = match value.dtype_scoped(schema, globals, scope)? {
                    Type::Array { array } | Type::Set { set: array } => *array,
                    _ => {
                        return Err(ExprError::Invalid(
                            "predicate requires an array or set".into(),
                        ));
                    }
                };
                if scope.iter().any(|(name, _, _)| name == parameter) {
                    return Err(ExprError::Invalid(
                        "duplicate lambda parameter identity".into(),
                    ));
                }
                let mut nested = scope.to_vec();
                nested.push((parameter.clone(), element.clone(), schema.fields().len()));
                if predicate.dtype_scoped(schema, globals, &nested)?
                    != Type::Primitive(PrimitiveType::Bool)
                {
                    return Err(ExprError::Invalid(
                        "collection predicate requires bool".into(),
                    ));
                }
                Ok(if *first {
                    element
                } else {
                    Type::Primitive(PrimitiveType::Bool)
                })
            }
            Self::ArrayFilter {
                value,
                parameter,
                predicate,
            } => {
                let array = match value.dtype_scoped(schema, globals, scope)? {
                    Type::Array { array } | Type::Set { set: array } => array,
                    Type::Dict { dict } => Box::new(Type::Struct {
                        r#struct: vec![("key".into(), dict.0), ("value".into(), dict.1)],
                    }),
                    _ => return Err(ExprError::Invalid("filter requires a collection".into())),
                };
                if scope.iter().any(|(name, _, _)| name == parameter) {
                    return Err(ExprError::Invalid(
                        "duplicate lambda parameter identity".into(),
                    ));
                }
                let mut nested = scope.to_vec();
                nested.push((parameter.clone(), *array, schema.fields().len()));
                let predicate_type = predicate.dtype_scoped(schema, globals, &nested)?;
                if predicate_type != Type::Primitive(PrimitiveType::Bool) {
                    return Err(ExprError::Invalid(format!(
                        "array filter predicate requires bool, got {predicate_type:?}"
                    )));
                }
                Ok(value.dtype_scoped(schema, globals, scope)?)
            }
            Self::StreamRange { start, stop, step } => {
                let int32 = Type::Primitive(PrimitiveType::Int32);
                for (label, argument) in [("start", start), ("stop", stop), ("step", step)] {
                    let actual = argument.dtype_scoped(schema, globals, scope)?;
                    if actual != int32 {
                        return Err(ExprError::Invalid(format!(
                            "stream_range {label} requires int32, got {actual:?}"
                        )));
                    }
                }
                Ok(Type::Array {
                    array: Box::new(int32),
                })
            }
            Self::ArrayGrouped { value, size } => {
                let Type::Array { array } = value.dtype_scoped(schema, globals, scope)? else {
                    return Err(ExprError::Invalid("array_grouped requires array".into()));
                };
                let size_type = size.dtype_scoped(schema, globals, scope)?;
                if size_type != Type::Primitive(PrimitiveType::Int32) {
                    return Err(ExprError::Invalid(format!(
                        "array_grouped size requires int32, got {size_type:?}"
                    )));
                }
                Ok(Type::Array {
                    array: Box::new(Type::Array { array }),
                })
            }

            Self::ExternalGlobal { dtype, .. } => Ok(dtype.clone()),
            Self::Global { name } => {
                let i = globals
                    .index_of(name)
                    .map_err(|_| ExprError::Invalid(format!("global field {name:?} is absent")))?;
                Type::from_arrow(globals.field(i).data_type())
            }
            Self::Column { name } => {
                let i = schema
                    .index_of(name)
                    .map_err(|_| ExprError::Invalid(format!("source column {name:?} is absent")))?;
                Type::from_arrow(schema.field(i).data_type())
            }
            Self::RuntimeLiteral { value } => Type::from_arrow(&value.data_type()),
            Self::Literal { dtype, value } => {
                validate_literal(value, dtype)?;
                Ok(dtype.clone())
            }
            Self::ToString { value } => {
                value.dtype_scoped(schema, globals, scope)?;
                Ok(Type::Primitive(PrimitiveType::Str))
            }
            Self::Locus {
                reference,
                contig,
                position,
            } => {
                reference.validate().map_err(ExprError::Invalid)?;
                if contig.dtype_scoped(schema, globals, scope)?
                    != Type::Primitive(PrimitiveType::Str)
                    || position.dtype_scoped(schema, globals, scope)?
                        != Type::Primitive(PrimitiveType::Int32)
                {
                    return Err(ExprError::Invalid(
                        "locus requires string contig and int32 position".into(),
                    ));
                }
                Ok(Type::Locus {
                    locus: reference.clone(),
                })
            }
            Self::LocusValid {
                reference,
                contig,
                position,
            } => {
                reference.validate().map_err(ExprError::Invalid)?;
                if contig.dtype_scoped(schema, globals, scope)?
                    != Type::Primitive(PrimitiveType::Str)
                    || position.dtype_scoped(schema, globals, scope)?
                        != Type::Primitive(PrimitiveType::Int32)
                {
                    return Err(ExprError::Invalid(
                        "locus_valid requires string contig and int32 position".into(),
                    ));
                }
                Ok(Type::Primitive(PrimitiveType::Bool))
            }
            Self::LocusFromGlobalPosition {
                reference,
                position,
            } => {
                reference.validate().map_err(ExprError::Invalid)?;
                if position.dtype_scoped(schema, globals, scope)?
                    != Type::Primitive(PrimitiveType::Int64)
                {
                    return Err(ExprError::Invalid("global position requires int64".into()));
                }
                Ok(Type::Locus {
                    locus: reference.clone(),
                })
            }
            Self::ParseLocus { reference, value } => {
                reference.validate().map_err(ExprError::Invalid)?;
                if value.dtype_scoped(schema, globals, scope)?
                    != Type::Primitive(PrimitiveType::Str)
                {
                    return Err(ExprError::Invalid("parse_locus requires string".into()));
                }
                Ok(Type::Locus {
                    locus: reference.clone(),
                })
            }
            Self::ParseCsvLine {
                value,
                delimiter,
                quote,
                ..
            } => {
                if value.dtype_scoped(schema, globals, scope)?
                    != Type::Primitive(PrimitiveType::Str)
                {
                    return Err(ExprError::Invalid(
                        "parse_csv_line requires string input".into(),
                    ));
                }
                if delimiter.len() != 1
                    || !delimiter.as_bytes()[0].is_ascii()
                    || matches!(delimiter.as_bytes()[0], b'\r' | b'\n' | 0)
                {
                    return Err(ExprError::Invalid(
                        "CSV delimiter must be one ASCII byte".into(),
                    ));
                }
                if quote.as_ref().is_some_and(|quote| {
                    quote.len() != 1
                        || !quote.as_bytes()[0].is_ascii()
                        || matches!(quote.as_bytes()[0], b'\r' | b'\n' | 0)
                }) {
                    return Err(ExprError::Invalid(
                        "CSV quote must be one ASCII byte".into(),
                    ));
                }
                if quote.as_deref() == Some(delimiter.as_str()) {
                    return Err(ExprError::Invalid(
                        "CSV quote and delimiter must differ".into(),
                    ));
                }
                Ok(Type::Array {
                    array: Box::new(Type::Primitive(PrimitiveType::Str)),
                })
            }
            Self::LocusOp { operation, value } => {
                let Type::Locus { .. } = value.dtype_scoped(schema, globals, scope)? else {
                    return Err(ExprError::Invalid(
                        "locus operation requires a locus".into(),
                    ));
                };
                if ![
                    "contig",
                    "position",
                    "global_position",
                    "is_x",
                    "is_y",
                    "is_mt",
                    "is_autosomal",
                    "in_x_par",
                    "in_y_par",
                ]
                .contains(&operation.as_str())
                {
                    return Err(ExprError::Invalid(format!(
                        "unsupported locus operation {operation:?}"
                    )));
                }
                Ok(match operation.as_str() {
                    "contig" => Type::Primitive(PrimitiveType::Str),
                    "position" => Type::Primitive(PrimitiveType::Int32),
                    "global_position" => Type::Primitive(PrimitiveType::Int64),
                    _ => Type::Primitive(PrimitiveType::Bool),
                })
            }
            Self::Interval {
                start,
                end,
                includes_start,
                includes_end,
            } => {
                let point = start.dtype_scoped(schema, globals, scope)?;
                if point != end.dtype_scoped(schema, globals, scope)? {
                    return Err(ExprError::Invalid(
                        "interval endpoints must have the same type".into(),
                    ));
                }
                if includes_start.dtype_scoped(schema, globals, scope)?
                    != Type::Primitive(PrimitiveType::Bool)
                    || includes_end.dtype_scoped(schema, globals, scope)?
                        != Type::Primitive(PrimitiveType::Bool)
                {
                    return Err(ExprError::Invalid(
                        "interval inclusion flags must be bool".into(),
                    ));
                }
                Ok(Type::Interval {
                    interval: Box::new(point),
                })
            }
            Self::IntervalContains { interval, point } => {
                let Type::Interval {
                    interval: point_type,
                } = interval.dtype_scoped(schema, globals, scope)?
                else {
                    return Err(ExprError::Invalid(
                        "interval_contains requires an interval".into(),
                    ));
                };
                if *point_type != point.dtype_scoped(schema, globals, scope)? {
                    return Err(ExprError::Invalid(
                        "interval_contains point type mismatch".into(),
                    ));
                }
                Ok(Type::Primitive(PrimitiveType::Bool))
            }
            Self::IntervalOverlaps { left, right } => {
                let left_type = left.dtype_scoped(schema, globals, scope)?;
                if !matches!(left_type, Type::Interval { .. })
                    || left_type != right.dtype_scoped(schema, globals, scope)?
                {
                    return Err(ExprError::Invalid(
                        "interval_overlaps requires matching interval types".into(),
                    ));
                }
                Ok(Type::Primitive(PrimitiveType::Bool))
            }
            Self::Array {
                element_type,
                elements,
            } => {
                element_type.to_arrow()?;
                for element in elements {
                    let actual = element.dtype_scoped(schema, globals, scope)?;
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
            Self::ArrayFlatten { value } => match value.dtype_scoped(schema, globals, scope)? {
                Type::Array { array } if matches!(*array, Type::Array { .. }) => Ok(*array),
                _ => Err(ExprError::Invalid(
                    "array flatten requires an array of arrays".into(),
                )),
            },
            Self::ArrayLength { value } => {
                if !matches!(
                    value.dtype_scoped(schema, globals, scope)?,
                    Type::Array { .. } | Type::Set { .. } | Type::Dict { .. }
                ) {
                    return Err(ExprError::Invalid("length requires a collection".into()));
                }
                Ok(Type::Primitive(PrimitiveType::Int32))
            }
            Self::ArrayIndex { value, index } => {
                let element = match value.dtype_scoped(schema, globals, scope)? {
                    Type::Array { array } => *array,
                    other => {
                        return Err(ExprError::Invalid(format!(
                            "array index requires array, got {other:?}"
                        )));
                    }
                };
                if index.dtype_scoped(schema, globals, scope)?
                    != Type::Primitive(PrimitiveType::Int32)
                {
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
                let array = match value.dtype_scoped(schema, globals, scope)? {
                    t @ Type::Array { .. } => t,
                    other => {
                        return Err(ExprError::Invalid(format!(
                            "array slice requires array, got {other:?}"
                        )));
                    }
                };
                for endpoint in [start, stop, step].into_iter().flatten() {
                    if endpoint.dtype_scoped(schema, globals, scope)?
                        != Type::Primitive(PrimitiveType::Int32)
                    {
                        return Err(ExprError::Invalid(
                            "array slice endpoints and step require int32".into(),
                        ));
                    }
                }
                Ok(array)
            }
            Self::ArrayReduce { value, .. } => {
                let element = match value.dtype_scoped(schema, globals, scope)? {
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
                value.dtype_scoped(schema, globals, scope)?;
                dtype.to_arrow()?;
                Ok(dtype.clone())
            }
            Self::GetField { value, name } => match value.dtype_scoped(schema, globals, scope)? {
                Type::Struct { r#struct: fields } => field_type(&fields, name),
                Type::Tuple { tuple } => name
                    .strip_prefix("__vail_tuple_")
                    .and_then(|i| i.parse::<usize>().ok())
                    .and_then(|i| tuple.get(i).cloned())
                    .ok_or_else(|| ExprError::Invalid(format!("invalid tuple field {name:?}"))),
                Type::Interval { interval } => match name.as_str() {
                    "start" | "end" => Ok(*interval),
                    "includes_start" | "includes_end" => Ok(Type::Primitive(PrimitiveType::Bool)),
                    _ => Err(ExprError::Invalid(format!(
                        "invalid interval field {name:?}"
                    ))),
                },
                Type::Primitive(PrimitiveType::Call) => match name.as_str() {
                    "__vail_call_alleles" => Ok(Type::Array {
                        array: Box::new(Type::Primitive(PrimitiveType::Int32)),
                    }),
                    "__vail_call_phased" => Ok(Type::Primitive(PrimitiveType::Bool)),
                    "__vail_call_word" => Ok(Type::Primitive(PrimitiveType::Int32)),
                    _ => Err(ExprError::Invalid(format!(
                        "invalid call storage field {name:?}"
                    ))),
                },
                Type::Ndarray { ndarray } => match name.as_str() {
                    "__vail_ndarray_data" => Ok(Type::Array {
                        array: Box::new(ndarray.0),
                    }),
                    "__vail_ndarray_shape" => Ok(Type::Array {
                        array: Box::new(Type::Primitive(PrimitiveType::Int64)),
                    }),
                    _ => Err(ExprError::Invalid(format!(
                        "invalid ndarray storage field {name:?}"
                    ))),
                },
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
                    typed.push((name.clone(), expr.dtype_scoped(schema, globals, scope)?));
                }
                Ok(Type::Struct { r#struct: typed })
            }
            Self::Die { message, dtype } => {
                if message.dtype_scoped(schema, globals, scope)?
                    != Type::Primitive(PrimitiveType::Str)
                {
                    return Err(ExprError::Invalid(
                        "error message must have str type".into(),
                    ));
                }
                dtype.to_arrow()?;
                Ok(dtype.clone())
            }
            Self::IsMissing { value } => {
                value.dtype_scoped(schema, globals, scope)?;
                Ok(Type::Primitive(PrimitiveType::Bool))
            }
            Self::Unary { operator, value } => {
                let t = value.dtype_scoped(schema, globals, scope)?;
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
                let a = left.dtype_scoped(schema, globals, scope)?;
                let b = right.dtype_scoped(schema, globals, scope)?;
                if matches!(
                    operator,
                    BinaryOperator::Lt
                        | BinaryOperator::Le
                        | BinaryOperator::Gt
                        | BinaryOperator::Ge
                ) {
                    a.ensure_keyable()?;
                    b.ensure_keyable()?;
                }
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
                ensure_bool(
                    &condition.dtype_scoped(schema, globals, scope)?,
                    "if_else condition",
                )?;
                let a = consequent.dtype_scoped(schema, globals, scope)?;
                let b = alternate.dtype_scoped(schema, globals, scope)?;
                if a == b { Ok(a) } else { promote(&a, &b) }
            }
        }
    }
}

fn validate_literal(value: &Value, dtype: &Type) -> Result<(), ExprError> {
    if value.is_null() {
        return Ok(());
    }
    if !matches!(dtype, Type::Interval { .. }) {
        if let Some(storage) = dtype.storage_type() {
            validate_literal(value, &storage)?;
            if *dtype == Type::Primitive(PrimitiveType::Call) {
                let word = value["__vail_call_word"]
                    .as_i64()
                    .ok_or_else(|| ExprError::Invalid("Call word must be present".into()))?
                    as i32;
                vail_ordering::call::unpack(word).map_err(ExprError::Invalid)?;
            }
            return Ok(());
        }
    }
    match dtype {
        Type::Locus { locus } => {
            locus.validate().map_err(ExprError::Invalid)?;
            let obj = value
                .as_object()
                .ok_or_else(|| ExprError::Invalid("locus literal requires object".into()))?;
            let has_logical =
                obj.len() == 2 && obj.contains_key("contig") && obj.contains_key("position");
            let has_physical = obj.len() == 3
                && obj.contains_key("__vail_locus_ordinal")
                && obj.contains_key("__vail_locus_contig")
                && obj.contains_key("__vail_locus_position");
            if !has_logical && !has_physical {
                return Err(ExprError::Invalid(
                    "locus literal requires contig and position or physical locus fields".into(),
                ));
            }
            let contig_value = if has_physical {
                &obj["__vail_locus_contig"]
            } else {
                &obj["contig"]
            };
            let position_value = if has_physical {
                &obj["__vail_locus_position"]
            } else {
                &obj["position"]
            };
            let contig = contig_value
                .as_str()
                .ok_or_else(|| ExprError::Invalid("locus contig requires string".into()))?;
            let position = position_value
                .as_i64()
                .and_then(|v| i32::try_from(v).ok())
                .ok_or_else(|| ExprError::Invalid("locus position requires int32".into()))?;
            let expected_ordinal = locus
                .ordinal(contig)
                .ok_or_else(|| ExprError::Invalid(format!("unknown locus contig {contig:?}")))?;
            let ordinal = if has_physical {
                obj["__vail_locus_ordinal"]
                    .as_i64()
                    .and_then(|n| i32::try_from(n).ok())
                    .ok_or_else(|| ExprError::Invalid("locus ordinal requires int32".into()))?
            } else {
                expected_ordinal
            };
            locus
                .validate_coordinate(ordinal, contig, position)
                .map_err(ExprError::Invalid)?;
        }
        Type::Primitive(PrimitiveType::Call)
        | Type::Dict { .. }
        | Type::Ndarray { .. }
        | Type::Tuple { .. } => unreachable!(),
        Type::Interval { interval } => {
            let object = value
                .as_object()
                .ok_or_else(|| ExprError::Invalid("interval literal requires object".into()))?;
            let expected = ["start", "end", "includes_start", "includes_end"];
            if object.len() != 4 || expected.iter().any(|key| !object.contains_key(*key)) {
                return Err(ExprError::Invalid(
                    "interval literal requires start, end, includes_start, and includes_end".into(),
                ));
            }
            validate_literal(&object["start"], interval)?;
            validate_literal(&object["end"], interval)?;
            if object["includes_start"].is_null() || object["includes_end"].is_null() {
                return Err(ExprError::Invalid(
                    "interval inclusion flags must be present".into(),
                ));
            }
            validate_literal(
                &object["includes_start"],
                &Type::Primitive(PrimitiveType::Bool),
            )?;
            validate_literal(
                &object["includes_end"],
                &Type::Primitive(PrimitiveType::Bool),
            )?;
        }
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
        Type::Primitive(PrimitiveType::Float32) => {
            float32_json(value)?;
        }
        Type::Primitive(PrimitiveType::Float64) => {
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
        Type::Array { array } | Type::Set { set: array } => {
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
/// Deduplicate set literals with Arrow's typed scalar equality, then use Arrow's
/// native ordering to give set values their canonical iteration order.
pub fn unique_set_elements(value: &Value, element_type: &Type) -> Result<Vec<Value>, ExprError> {
    let items = value
        .as_array()
        .ok_or_else(|| ExprError::Invalid("set requires a JSON array".into()))?;
    let mut unique = Vec::with_capacity(items.len());
    let mut scalars = Vec::with_capacity(items.len());
    for item in items {
        let array = literal_array(item, element_type, 1)?;
        let scalar = scalar_at(&array, 0)?;
        if !scalars.contains(&scalar) {
            unique.push(item.clone());
            scalars.push(scalar);
        }
    }
    if unique.len() < 2 {
        return Ok(unique);
    }
    let values = unique
        .iter()
        .map(|item| {
            let array = literal_array(item, element_type, 1)?;
            scalar_at(&array, 0)
        })
        .collect::<Result<Vec<_>, ExprError>>()?;
    let array = ScalarValue::iter_to_array(values)?;
    let order = set_literal_sort_indices(array.as_ref())?;
    let sorted = (0..order.len())
        .map(|i| unique[order.value(i) as usize].clone())
        .collect();
    Ok(sorted)
}

fn set_literal_sort_indices(array: &dyn Array) -> Result<UInt32Array, ExprError> {
    fn columns(array: &dyn Array, output: &mut Vec<SortColumn>) -> Result<(), ExprError> {
        if let Some(structure) = array.as_any().downcast_ref::<StructArray>() {
            // A missing struct sorts after present structs, including a present
            // struct whose fields are all missing.
            output.push(SortColumn {
                values: Arc::new(BooleanArray::from(
                    (0..array.len())
                        .map(|i| array.is_valid(i))
                        .collect::<Vec<_>>(),
                )),
                options: Some(SortOptions {
                    descending: true,
                    nulls_first: false,
                }),
            });
            for child in structure.columns() {
                columns(child.as_ref(), output)?;
            }
        } else {
            output.push(SortColumn {
                values: arrow_array::make_array(array.to_data()),
                options: Some(SortOptions {
                    descending: false,
                    nulls_first: false,
                }),
            });
        }
        Ok(())
    }

    if array.as_any().is::<StructArray>() {
        let mut keys = Vec::new();
        columns(array, &mut keys)?;
        Ok(lexsort_to_indices(&keys, None)?)
    } else {
        Ok(sort_to_indices(
            array,
            Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
            None,
        )?)
    }
}

fn float_json(v: &Value) -> Result<f64, ExprError> {
    if let Some(n) = v.as_f64() {
        return Ok(n);
    }
    if let Some(object) = v.as_object() {
        match (object.len(), object.get("$float").and_then(Value::as_str)) {
            (1, Some("nan")) => return Ok(f64::NAN),
            (1, Some("inf")) => return Ok(f64::INFINITY),
            (1, Some("-inf")) => return Ok(f64::NEG_INFINITY),
            (2, Some("nan")) => {
                let bits = nan_bits(object.get("bits"), 16)?;
                if bits & 0x7ff0000000000000 != 0x7ff0000000000000 || bits & 0x000fffffffffffff == 0
                {
                    return Err(ExprError::Invalid(
                        "NaN bit pattern does not encode a NaN".into(),
                    ));
                }
                return Ok(f64::from_bits(bits));
            }
            _ => (),
        }
    }
    Err(ExprError::Invalid(
        "float literal requires number or $float tag".into(),
    ))
}

fn float32_json(v: &Value) -> Result<f32, ExprError> {
    if let Some(n) = v.as_f64() {
        return Ok(n as f32);
    }
    if let Some(object) = v.as_object() {
        match (object.len(), object.get("$float").and_then(Value::as_str)) {
            (1, Some("nan")) => return Ok(f32::NAN),
            (1, Some("inf")) => return Ok(f32::INFINITY),
            (1, Some("-inf")) => return Ok(f32::NEG_INFINITY),
            (2, Some("nan")) => {
                let bits = nan_bits(object.get("bits"), 8)? as u32;
                if bits & 0x7f800000 != 0x7f800000 || bits & 0x007fffff == 0 {
                    return Err(ExprError::Invalid(
                        "NaN bit pattern does not encode a NaN".into(),
                    ));
                }
                return Ok(f32::from_bits(bits));
            }
            _ => (),
        }
    }
    Err(ExprError::Invalid(
        "float literal requires number or $float tag".into(),
    ))
}

fn nan_bits(value: Option<&Value>, digits: usize) -> Result<u64, ExprError> {
    let text = value.and_then(Value::as_str).ok_or_else(|| {
        ExprError::Invalid("NaN bit pattern must be a fixed-width hexadecimal string".into())
    })?;
    if text.len() != digits || !text.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(ExprError::Invalid(
            "NaN bit pattern must be a fixed-width hexadecimal string".into(),
        ));
    }
    u64::from_str_radix(text, 16).map_err(|_| ExprError::Invalid("invalid NaN bit pattern".into()))
}
pub(crate) fn literal_array(v: &Value, t: &Type, len: usize) -> Result<ArrayRef, ExprError> {
    let dt = t.to_arrow()?;
    if v.is_null() {
        return Ok(new_null_array(&dt, len));
    }
    if let Some(storage) = t.storage_type() {
        let storage_value = match t {
            Type::Interval { .. } => json!({
                "__vail_interval_start": v["start"],
                "__vail_interval_end": v["end"],
                "__vail_interval_start_exclusive": !v["includes_start"].as_bool().ok_or_else(|| ExprError::Invalid("interval start inclusivity must be a boolean".into()))?,
                "__vail_interval_end_inclusive": v["includes_end"]
            }),
            _ => v.clone(),
        };
        let array = literal_array(&storage_value, &storage, len)?;
        return Ok(arrow_cast::cast(&array, &dt)?);
    }
    let a: ArrayRef = match t {
        Type::Locus { locus } => {
            let contig = v["__vail_locus_contig"].as_str().unwrap();
            let position = v["__vail_locus_position"].as_i64().unwrap() as i32;
            let ordinal = v["__vail_locus_ordinal"].as_i64().unwrap() as i32;
            locus
                .validate_coordinate(ordinal, contig, position)
                .map_err(ExprError::Invalid)?;
            let metadata = [(
                "vail.reference".to_string(),
                serde_json::to_string(locus).map_err(|e| ExprError::Invalid(e.to_string()))?,
            )]
            .into_iter()
            .collect();
            let fields = vec![
                Arc::new(Field::new("__vail_locus_ordinal", DataType::Int32, true)),
                Arc::new(
                    Field::new("__vail_locus_contig", DataType::Utf8, true).with_metadata(metadata),
                ),
                Arc::new(Field::new("__vail_locus_position", DataType::Int32, true)),
            ];
            Arc::new(StructArray::try_new(
                fields.into(),
                vec![
                    Arc::new(Int32Array::from(vec![Some(ordinal); len])),
                    Arc::new(StringArray::from(vec![Some(contig); len])),
                    Arc::new(Int32Array::from(vec![Some(position); len])),
                ],
                None,
            )?)
        }
        Type::Primitive(PrimitiveType::Call)
        | Type::Dict { .. }
        | Type::Ndarray { .. }
        | Type::Tuple { .. }
        | Type::Interval { .. } => unreachable!(),
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
            Arc::new(Float32Array::from(vec![Some(float32_json(v)?); len]))
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
                Arc::new(StructArray::new_empty_fields(len, None))
            } else {
                Arc::new(StructArray::from(values))
            }
        }
        Type::Array { array } | Type::Set { set: array } => {
            let unique;
            let items = if matches!(t, Type::Set { .. }) {
                unique = unique_set_elements(v, array)?;
                &unique
            } else {
                v.as_array().unwrap()
            };
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
                match &dt {
                    DataType::List(field) => field.clone(),
                    _ => unreachable!(),
                },
                offsets,
                child,
                None,
            ))
        }
    };
    Ok(arrow_cast::cast(&a, &dt)?)
}

pub fn validate_projection(
    schema: &Schema,
    fields: &[(String, Expr)],
) -> Result<SchemaRef, ExprError> {
    validate_projection_with_globals(schema, &Schema::empty(), fields)
}

pub fn validate_projection_with_globals(
    schema: &Schema,
    globals: &Schema,
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
        output.push(Field::new(
            name,
            expr.dtype_with_globals(schema, globals)?.to_arrow()?,
            true,
        ));
    }
    Ok(Arc::new(Schema::new(output)))
}

// Floor division and modulo require Hail-specific sign/overflow behavior.
// Arrow owns the typed array iteration, output buffers, and validity handling;
// no operand is extracted as a ScalarValue.
fn columnar_numeric(
    op: BinaryOperator,
    left: &ArrayRef,
    right: &ArrayRef,
    t: &Type,
) -> Result<ArrayRef, ExprError> {
    use arrow_array::types::{Float32Type, Float64Type, Int32Type, Int64Type};
    macro_rules! integer {
        ($ty:ty, $native:ty) => {{
            let l = down::<arrow_array::PrimitiveArray<$ty>>(left.as_ref())?;
            let r = down::<arrow_array::PrimitiveArray<$ty>>(right.as_ref())?;
            let result =
                arrow_arith::arity::try_binary::<_, _, _, $ty>(l, r, |a: $native, b: $native| {
                    if b == 0 {
                        return Err(ArrowError::ComputeError(
                            "integer division or modulo by zero".into(),
                        ));
                    }
                    let rem = a.wrapping_rem(b);
                    Ok(match op {
                        BinaryOperator::Floordiv => a
                            .wrapping_div(b)
                            .wrapping_sub(<$native>::from(rem != 0 && (a < 0) != (b < 0))),
                        BinaryOperator::Mod => {
                            rem.wrapping_add(if rem != 0 && (a < 0) != (b < 0) { b } else { 0 })
                        }
                        _ => unreachable!("basic arithmetic lowers to DataFusion"),
                    })
                })?;
            Ok(Arc::new(result) as ArrayRef)
        }};
    }
    macro_rules! float {
        ($ty:ty, $native:ty) => {{
            let l = down::<arrow_array::PrimitiveArray<$ty>>(left.as_ref())?;
            let r = down::<arrow_array::PrimitiveArray<$ty>>(right.as_ref())?;
            let result =
                arrow_arith::arity::try_binary::<_, _, _, $ty>(l, r, |a: $native, b: $native| {
                    Ok(match op {
                        BinaryOperator::Floordiv => ((a as f64) / (b as f64)).floor() as $native,
                        BinaryOperator::Mod => {
                            if b == 0.0 {
                                return Err(ArrowError::ComputeError("modulo by zero".into()));
                            }
                            let rem = a % b;
                            if rem < 0.0 { rem + b } else { rem }
                        }
                        BinaryOperator::Pow => a.powf(b),
                        _ => unreachable!("basic arithmetic lowers to DataFusion"),
                    })
                })?;
            Ok(Arc::new(result) as ArrayRef)
        }};
    }
    match t {
        Type::Primitive(PrimitiveType::Int32) => integer!(Int32Type, i32),
        Type::Primitive(PrimitiveType::Int64) => integer!(Int64Type, i64),
        Type::Primitive(PrimitiveType::Float32) => float!(Float32Type, f32),
        Type::Primitive(PrimitiveType::Float64) => float!(Float64Type, f64),
        _ => Err(ExprError::Invalid("numeric operand type mismatch".into())),
    }
}
pub fn scalar_at(a: &ArrayRef, i: usize) -> Result<ScalarValue, ExprError> {
    // DF's list scalar builder drops item-field metadata. Slice the original
    // nested array so set types survive scalar literals and singleton globals.
    if let Some(list) = a.as_any().downcast_ref::<ListArray>() {
        return Ok(ScalarValue::List(Arc::new(list.slice(i, 1))));
    }
    if let Some(value) = a.as_any().downcast_ref::<StructArray>() {
        return Ok(ScalarValue::Struct(Arc::new(value.slice(i, 1))));
    }
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
    if operation == ArrayReduction::Sum {
        // Arrow's typed aggregate uses vectorized, wrapping arithmetic for
        // primitive sums. Preserve Hail's empty/all-null identity and the
        // filter_missing=false behavior at the list boundary.
        macro_rules! sum_rows {
            ($array_ty:ty, $marker:ty) => {{
                let mut out: Vec<
                    Option<<$marker as arrow_array::types::ArrowPrimitiveType>::Native>,
                > = Vec::with_capacity(lists.len());
                for row in 0..lists.len() {
                    if lists.is_null(row) {
                        out.push(None);
                        continue;
                    }
                    let child = lists.value(row);
                    let values = child.as_any().downcast_ref::<$array_ty>().ok_or_else(|| {
                        ExprError::Invalid("array reduction child type mismatch".into())
                    })?;
                    if !filter_missing && values.null_count() > 0 {
                        out.push(None);
                    } else {
                        let sum =
                            arrow_arith::aggregate::sum::<$marker>(values).unwrap_or_default();
                        out.push(Some(sum));
                    }
                }
                Ok(Arc::new(<$array_ty>::from(out)) as ArrayRef)
            }};
        }
        return match dtype {
            DataType::Int32 => sum_rows!(Int32Array, Int32Type),
            DataType::Int64 => sum_rows!(Int64Array, Int64Type),
            DataType::Float32 => sum_rows!(Float32Array, Float32Type),
            DataType::Float64 => sum_rows!(Float64Array, Float64Type),
            _ => Err(ExprError::Invalid(
                "array reduction requires numeric array".into(),
            )),
        };
    }
    // Hail folds in the element's native type from typed 0/1 identities.
    // Keep product/min/max in that scalar fold; sum uses Arrow's aggregate
    // kernel above to retain the same precision and identity semantics.
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
    RestoreArrowType,
    Die,
    Numeric(BinaryOperator),
    ToString(String),
    GetField(String),
    Struct(Vec<String>),
    Interval,
    // DF flatten ignores inner validity. Compact null lists before flattening.
    CompactInnerNullLists,
    ArrayIndex,
    ArraySlice {
        start: bool,
        stop: bool,
        step: bool,
    },
    ArrayReduce(ArrayReduction, bool),
    Locus {
        operation: String,
        reference_json: String,
    },
}

#[derive(Debug)]
struct HailPhysicalExpr {
    kind: HailKind,
    args: Vec<Arc<dyn PhysicalExpr>>,
    dtype: DataType,
}

fn locus_eval(
    op: &str,
    reference: &ReferenceDefinition,
    args: &[ArrayRef],
    _dtype: &DataType,
    len: usize,
) -> Result<ArrayRef, ExprError> {
    let is_missing = |i: usize| args.iter().any(|a| a.is_null(i));
    if op == "create" || op == "from_global" || op == "parse" {
        let mut ordinals = Vec::with_capacity(len);
        let mut contigs = Vec::with_capacity(len);
        let mut positions = Vec::with_capacity(len);
        for i in 0..len {
            if is_missing(i) {
                ordinals.push(None);
                contigs.push(None);
                positions.push(None);
                continue;
            }
            let (contig, position) = match op {
                "create" => (
                    down::<StringArray>(args[0].as_ref())?.value(i).to_string(),
                    down::<Int32Array>(args[1].as_ref())?.value(i),
                ),
                "parse" => {
                    let s = down::<StringArray>(args[0].as_ref())?.value(i);
                    let (c, p) = s
                        .rsplit_once(':')
                        .ok_or_else(|| ExprError::Invalid(format!("invalid locus string {s:?}")))?;
                    (
                        c.to_string(),
                        p.parse::<i32>().map_err(|_| {
                            ExprError::Invalid(format!("invalid locus position in {s:?}"))
                        })?,
                    )
                }
                _ => {
                    let mut p = down::<Int64Array>(args[0].as_ref())?.value(i);
                    if p < 0 {
                        return Err(ExprError::Invalid(
                            "global position cannot be negative".into(),
                        ));
                    }
                    let mut found = None;
                    for c in &reference.contigs {
                        let n = i64::from(reference.lengths[c]);
                        if p < n {
                            found = Some((c.clone(), (p + 1) as i32));
                            break;
                        }
                        p -= n;
                    }
                    found.ok_or_else(|| {
                        ExprError::Invalid("global position exceeds reference length".into())
                    })?
                }
            };
            let ordinal = reference
                .ordinal(&contig)
                .ok_or_else(|| ExprError::Invalid(format!("unknown locus contig {contig:?}")))?;
            reference
                .validate_coordinate(ordinal, &contig, position)
                .map_err(ExprError::Invalid)?;
            ordinals.push(Some(ordinal));
            contigs.push(Some(contig));
            positions.push(Some(position));
        }
        let metadata = [(
            "vail.reference".to_string(),
            serde_json::to_string(reference).map_err(|e| ExprError::Invalid(e.to_string()))?,
        )]
        .into_iter()
        .collect();
        let valid = ordinals.iter().map(Option::is_some).collect::<Vec<_>>();
        return Ok(Arc::new(StructArray::try_new(
            vec![
                Arc::new(Field::new("__vail_locus_ordinal", DataType::Int32, true)),
                Arc::new(
                    Field::new("__vail_locus_contig", DataType::Utf8, true).with_metadata(metadata),
                ),
                Arc::new(Field::new("__vail_locus_position", DataType::Int32, true)),
            ]
            .into(),
            vec![
                Arc::new(Int32Array::from(ordinals)),
                Arc::new(StringArray::from(contigs)),
                Arc::new(Int32Array::from(positions)),
            ],
            Some(NullBuffer::from(valid)),
        )?));
    }
    if op == "valid" {
        let mut out = Vec::with_capacity(len);
        for i in 0..len {
            if is_missing(i) {
                out.push(None);
            } else {
                let c = down::<StringArray>(args[0].as_ref())?.value(i);
                let p = down::<Int32Array>(args[1].as_ref())?.value(i);
                out.push(Some(
                    reference
                        .ordinal(c)
                        .is_some_and(|o| reference.validate_coordinate(o, c, p).is_ok()),
                ));
            }
        }
        return Ok(Arc::new(BooleanArray::from(out)));
    }
    let st = down::<StructArray>(args[0].as_ref())?;
    let contigs = down::<StringArray>(
        st.column_by_name("__vail_locus_contig")
            .ok_or_else(|| ExprError::Invalid("invalid locus struct".into()))?
            .as_ref(),
    )?;
    let ordinals = down::<Int32Array>(st.column_by_name("__vail_locus_ordinal").unwrap().as_ref())?;
    let positions =
        down::<Int32Array>(st.column_by_name("__vail_locus_position").unwrap().as_ref())?;
    for i in 0..len {
        if !st.is_null(i) {
            if contigs.is_null(i) || ordinals.is_null(i) || positions.is_null(i) {
                return Err(ExprError::Invalid(
                    "non-missing locus has missing coordinate fields".into(),
                ));
            }
            reference
                .validate_coordinate(ordinals.value(i), contigs.value(i), positions.value(i))
                .map_err(ExprError::Invalid)?;
        }
    }
    match op {
        "contig" => Ok(Arc::new(contigs.clone())),
        "position" => Ok(Arc::new(positions.clone())),
        "global_position" => {
            let out = (0..len)
                .map(|i| {
                    if st.is_null(i) {
                        None
                    } else {
                        let base: i64 = reference
                            .contigs
                            .iter()
                            .take(ordinals.value(i) as usize)
                            .map(|c| i64::from(reference.lengths[c]))
                            .sum();
                        Some(base + i64::from(positions.value(i) - 1))
                    }
                })
                .collect::<Vec<_>>();
            Ok(Arc::new(Int64Array::from(out)))
        }
        "is_x" | "is_y" | "is_mt" | "is_autosomal" => {
            let out = (0..len)
                .map(|i| {
                    if st.is_null(i) {
                        None
                    } else {
                        let c = contigs.value(i);
                        Some(match op {
                            "is_x" => reference.x_contigs.iter().any(|v| v == c),
                            "is_y" => reference.y_contigs.iter().any(|v| v == c),
                            "is_mt" => reference.mt_contigs.iter().any(|v| v == c),
                            _ => {
                                !reference.x_contigs.iter().any(|v| v == c)
                                    && !reference.y_contigs.iter().any(|v| v == c)
                                    && !reference.mt_contigs.iter().any(|v| v == c)
                            }
                        })
                    }
                })
                .collect::<Vec<_>>();
            Ok(Arc::new(BooleanArray::from(out)))
        }
        "in_x_par" | "in_y_par" => {
            let out = (0..len)
                .map(|i| {
                    if st.is_null(i) {
                        None
                    } else {
                        let c = contigs.value(i);
                        let p = positions.value(i);
                        Some(
                            reference
                                .par
                                .iter()
                                .any(|r| r.contig == c && p >= r.start && p < r.end)
                                && if op == "in_x_par" {
                                    reference.x_contigs.iter().any(|v| v == c)
                                } else {
                                    reference.y_contigs.iter().any(|v| v == c)
                                },
                        )
                    }
                })
                .collect::<Vec<_>>();
            Ok(Arc::new(BooleanArray::from(out)))
        }
        _ => Err(ExprError::Invalid(format!(
            "unsupported locus operation {op:?}"
        ))),
    }
}
fn down<T: Array + 'static>(array: &dyn Array) -> Result<&T, ExprError> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        ExprError::Invalid("Arrow array does not match locus expression type".into())
    })
}

/// Hail `Type.str` differs from Arrow casts: containers and structs use Hail's
/// compact JSON annotation export, while floats and a few logical types have
/// their own textual representations. Keep this in the batch evaluator so a
/// table projection never materializes rows in Python.
fn hail_to_string(array: &ArrayRef, dtype: &Type) -> Result<ArrayRef, ExprError> {
    let mut values = Vec::with_capacity(array.len());
    for row in 0..array.len() {
        if array.is_null(row) {
            values.push(None);
        } else if matches!(dtype, Type::Primitive(PrimitiveType::Str)) {
            values.push(Some(
                down::<StringArray>(array.as_ref())?.value(row).to_owned(),
            ));
        } else {
            values.push(Some(hail_value_string(array, row, dtype, true)?));
        }
    }
    Ok(Arc::new(StringArray::from(values)))
}

fn json_quote(value: &str) -> Result<String, ExprError> {
    serde_json::to_string(value).map_err(|e| ExprError::Invalid(e.to_string()))
}

fn float_text(value: f64) -> String {
    if value.is_nan() {
        "NaN".into()
    } else if value == f64::INFINITY {
        "Infinity".into()
    } else if value == f64::NEG_INFINITY {
        "-Infinity".into()
    } else {
        value.to_string()
    }
}

// Java uses ordinary decimal notation in [1e-3, 1e7), scientific
// notation elsewhere, and at least two significant digits. Rust's formatter
// supplies the binary-to-decimal conversion; this adapter only changes notation.
fn java_float_notation(value: f64, shortest: String, fixed: String, two_digits: String) -> String {
    if !value.is_finite() {
        return float_text(value);
    }
    if value == 0.0 {
        return if value.is_sign_negative() {
            "-0.0"
        } else {
            "0.0"
        }
        .into();
    }
    if (1e-3..1e7).contains(&value.abs()) {
        return if fixed.contains('.') {
            fixed
        } else {
            format!("{fixed}.0")
        };
    }
    let (mantissa, _) = shortest
        .split_once('e')
        .expect("Rust scientific notation has an exponent");
    let scientific = if mantissa.contains('.') {
        shortest
    } else {
        two_digits
    };
    let (mantissa, exponent) = scientific
        .split_once('e')
        .expect("Rust scientific notation has an exponent");
    let exponent: i32 = exponent
        .parse()
        .expect("Rust scientific exponent is an integer");
    format!("{mantissa}E{exponent}")
}

fn java_float_text(value: f64) -> String {
    java_float_notation(
        value,
        format!("{value:e}"),
        value.to_string(),
        format!("{value:.1e}"),
    )
}

fn java_float32_text(value: f32) -> String {
    java_float_notation(
        f64::from(value),
        format!("{value:e}"),
        value.to_string(),
        format!("{value:.1e}"),
    )
}

fn hail_float_string(value: f64) -> String {
    if !value.is_finite() {
        return float_text(value);
    }
    if value == 0.0 {
        return if value.is_sign_negative() {
            "-0.00000e+00"
        } else {
            "0.00000e+00"
        }
        .into();
    }
    let decimal = java_float_text(value)
        .parse::<bigdecimal::BigDecimal>()
        .expect("finite float has a decimal representation");
    let scientific = format!("{decimal:e}");
    let (_, exponent) = scientific
        .split_once('e')
        .expect("BigDecimal emits scientific notation");
    let exponent: i32 = exponent
        .parse()
        .expect("BigDecimal emits an integer exponent");
    let rounded =
        decimal.with_scale_round(i64::from(5 - exponent), bigdecimal::RoundingMode::HalfUp);
    let rendered = format!("{rounded:.5e}");
    let (mantissa, exponent) = rendered
        .split_once('e')
        .expect("BigDecimal emits scientific notation");
    let exponent: i32 = exponent
        .parse()
        .expect("BigDecimal emits an integer exponent");
    format!(
        "{mantissa}e{}{abs:02}",
        if exponent < 0 { '-' } else { '+' },
        abs = exponent.abs()
    )
}

fn annotation_text(array: &ArrayRef, row: usize, dtype: &Type) -> Result<String, ExprError> {
    if array.is_null(row) {
        return Ok("null".into());
    }
    Ok(match dtype {
        Type::Primitive(PrimitiveType::Int32) => {
            down::<Int32Array>(array.as_ref())?.value(row).to_string()
        }
        Type::Primitive(PrimitiveType::Int64) => {
            down::<Int64Array>(array.as_ref())?.value(row).to_string()
        }
        Type::Primitive(PrimitiveType::Float32) => {
            java_float32_text(down::<Float32Array>(array.as_ref())?.value(row))
        }
        Type::Primitive(PrimitiveType::Float64) => {
            java_float_text(down::<Float64Array>(array.as_ref())?.value(row))
        }
        Type::Primitive(PrimitiveType::Bool) => {
            down::<BooleanArray>(array.as_ref())?.value(row).to_string()
        }
        Type::Primitive(PrimitiveType::Str) => {
            down::<StringArray>(array.as_ref())?.value(row).to_owned()
        }
        Type::Array { array: element } => {
            let list = down::<ListArray>(array.as_ref())?;
            let values = list.value(row);
            let mut parts = Vec::new();
            for i in 0..values.len() {
                parts.push(annotation_text(&values, i, element)?);
            }
            format!("[{}]", parts.join(","))
        }
        Type::Set { set: element } => {
            let list = down::<ListArray>(array.as_ref())?;
            let values = list.value(row);
            let mut parts = Vec::new();
            for i in 0..values.len() {
                parts.push(annotation_text(&values, i, element)?);
            }
            format!("Set({})", parts.join(", "))
        }
        Type::Dict { dict } => {
            let list = down::<ListArray>(array.as_ref())?;
            let entry_values = list.value(row);
            let entries = down::<StructArray>(entry_values.as_ref())?;
            let keys = entries
                .column_by_name("key")
                .ok_or_else(|| ExprError::Invalid("dict key field missing".into()))?;
            let values = entries
                .column_by_name("value")
                .ok_or_else(|| ExprError::Invalid("dict value field missing".into()))?;
            let mut parts = Vec::new();
            for i in 0..entries.len() {
                parts.push(format!(
                    "{} -> {}",
                    annotation_text(keys, i, &dict.0)?,
                    annotation_text(values, i, &dict.1)?
                ));
            }
            format!("Map({})", parts.join(", "))
        }
        Type::Struct { r#struct: fields } => {
            let st = down::<StructArray>(array.as_ref())?;
            let mut parts = Vec::new();
            for (name, ty) in fields {
                parts.push(format!(
                    "{name}: {}",
                    annotation_text(
                        st.column_by_name(name)
                            .ok_or_else(|| ExprError::Invalid(format!(
                                "struct field {name:?} missing"
                            )))?,
                        row,
                        ty
                    )?
                ));
            }
            format!("{{{}}}", parts.join(", "))
        }
        Type::Tuple { tuple } => {
            let st = down::<StructArray>(array.as_ref())?;
            let mut parts = Vec::new();
            for (i, ty) in tuple.iter().enumerate() {
                parts.push(annotation_text(
                    st.column_by_name(&format!("__vail_tuple_{i}"))
                        .ok_or_else(|| ExprError::Invalid("tuple field missing".into()))?,
                    row,
                    ty,
                )?);
            }
            format!("({})", parts.join(", "))
        }
        Type::Interval { interval } => {
            let st = down::<StructArray>(array.as_ref())?;
            let start = annotation_text(
                st.column_by_name("__vail_interval_start")
                    .ok_or_else(|| ExprError::Invalid("interval start missing".into()))?,
                row,
                interval,
            )?;
            let end = annotation_text(
                st.column_by_name("__vail_interval_end")
                    .ok_or_else(|| ExprError::Invalid("interval end missing".into()))?,
                row,
                interval,
            )?;
            let si = !down::<BooleanArray>(
                st.column_by_name("__vail_interval_start_exclusive")
                    .ok_or_else(|| ExprError::Invalid("interval flag missing".into()))?
                    .as_ref(),
            )?
            .value(row);
            let ei = down::<BooleanArray>(
                st.column_by_name("__vail_interval_end_inclusive")
                    .ok_or_else(|| ExprError::Invalid("interval flag missing".into()))?
                    .as_ref(),
            )?
            .value(row);
            format!(
                "{}{start}-{end}{}",
                if si { "[" } else { "(" },
                if ei { "]" } else { ")" }
            )
        }
        Type::Ndarray { ndarray } => {
            let st = down::<StructArray>(array.as_ref())?;
            let shape = st
                .column_by_name("__vail_ndarray_shape")
                .ok_or_else(|| ExprError::Invalid("ndarray shape missing".into()))?;
            let data = st
                .column_by_name("__vail_ndarray_data")
                .ok_or_else(|| ExprError::Invalid("ndarray data missing".into()))?;
            let (dims, data) = ndarray_parts(shape, data, row, ndarray.1)?;
            let mut parts = Vec::new();
            for i in 0..data.len() {
                parts.push(annotation_text(&data, i, &ndarray.0)?);
            }
            format!(
                "UnsafeNDArray of shape ({}) with elements ArraySeq({})",
                dims.iter()
                    .map(i64::to_string)
                    .collect::<Vec<_>>()
                    .join(", "),
                parts.join(", ")
            )
        }
        Type::Locus { locus } => {
            let st = down::<StructArray>(array.as_ref())?;
            let contig_array = down::<StringArray>(
                st.column_by_name("__vail_locus_contig")
                    .ok_or_else(|| ExprError::Invalid("locus contig missing".into()))?
                    .as_ref(),
            )?;
            let position_array = down::<Int32Array>(
                st.column_by_name("__vail_locus_position")
                    .ok_or_else(|| ExprError::Invalid("locus position missing".into()))?
                    .as_ref(),
            )?;
            let ordinal_array = down::<Int32Array>(
                st.column_by_name("__vail_locus_ordinal")
                    .ok_or_else(|| ExprError::Invalid("locus ordinal missing".into()))?
                    .as_ref(),
            )?;
            if contig_array.is_null(row)
                || position_array.is_null(row)
                || ordinal_array.is_null(row)
            {
                return Err(ExprError::Invalid(
                    "nonnull locus has missing coordinate fields".into(),
                ));
            }
            let contig = contig_array.value(row);
            let position = position_array.value(row);
            locus
                .validate_coordinate(ordinal_array.value(row), contig, position)
                .map_err(ExprError::Invalid)?;
            format!("{contig}:{position}")
        }
        Type::Primitive(PrimitiveType::Call) => call_raw(array, row)?,
    })
}

fn call_word(array: &ArrayRef, row: usize) -> Result<i32, ExprError> {
    let st = down::<StructArray>(array.as_ref())?;
    let word = down::<Int32Array>(st.column(0).as_ref())?;
    if word.is_null(row) {
        return Err(ExprError::Invalid("present Call has missing word".into()));
    }
    let word = word.value(row);
    vail_ordering::call::unpack(word).map_err(ExprError::Invalid)?;
    Ok(word)
}
fn call_raw(array: &ArrayRef, row: usize) -> Result<String, ExprError> {
    Ok(call_word(array, row)?.to_string())
}

fn hail_value_string(
    array: &ArrayRef,
    row: usize,
    dtype: &Type,
    top: bool,
) -> Result<String, ExprError> {
    if array.is_null(row) {
        return Ok(if top { "NA".into() } else { "null".into() });
    }
    if let Some(storage) = dtype.storage_type() {
        return match dtype {
            Type::Interval { interval } => {
                let st = down::<StructArray>(array.as_ref())?;
                let start = st
                    .column_by_name("__vail_interval_start")
                    .ok_or_else(|| ExprError::Invalid("interval start field missing".into()))?;
                let end = st
                    .column_by_name("__vail_interval_end")
                    .ok_or_else(|| ExprError::Invalid("interval end field missing".into()))?;
                let a = if top {
                    annotation_text(start, row, interval)?
                } else {
                    hail_value_string(start, row, interval, false)?
                };
                let b = if top {
                    annotation_text(end, row, interval)?
                } else {
                    hail_value_string(end, row, interval, false)?
                };
                let si = !down::<BooleanArray>(
                    st.column_by_name("__vail_interval_start_exclusive")
                        .ok_or_else(|| {
                            ExprError::Invalid("interval includes_start field missing".into())
                        })?
                        .as_ref(),
                )?
                .value(row);
                let ei = down::<BooleanArray>(
                    st.column_by_name("__vail_interval_end_inclusive")
                        .ok_or_else(|| {
                            ExprError::Invalid("interval includes_end field missing".into())
                        })?
                        .as_ref(),
                )?
                .value(row);
                if top {
                    Ok(format!(
                        "{}{a}-{b}{}",
                        if si { "[" } else { "(" },
                        if ei { "]" } else { ")" }
                    ))
                } else {
                    Ok(format!(
                        "{{\"start\":{a},\"end\":{b},\"includeStart\":{si},\"includeEnd\":{ei}}}"
                    ))
                }
            }
            Type::Primitive(PrimitiveType::Call) => {
                let (alleles, phased) = vail_ordering::call::unpack(call_word(array, row)?)
                    .map_err(ExprError::Invalid)?;
                let call = if alleles.is_empty() {
                    if phased { "|-".into() } else { "-".into() }
                } else if phased && alleles.len() == 1 {
                    format!("|{}", alleles[0])
                } else {
                    alleles
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(if phased { "|" } else { "/" })
                };
                Ok(if top { call } else { json_quote(&call)? })
            }
            Type::Dict { dict } => {
                let list = down::<ListArray>(array.as_ref())?;
                let vals = list.value(row);
                let entries = down::<StructArray>(vals.as_ref())?;
                let keys = entries
                    .column_by_name("key")
                    .ok_or_else(|| ExprError::Invalid("dict key field missing".into()))?;
                let values = entries
                    .column_by_name("value")
                    .ok_or_else(|| ExprError::Invalid("dict value field missing".into()))?;
                let mut out = Vec::new();
                for i in 0..entries.len() {
                    out.push(format!(
                        "{{\"key\":{},\"value\":{}}}",
                        hail_value_string(keys, i, &dict.0, false)?,
                        hail_value_string(values, i, &dict.1, false)?
                    ));
                }
                Ok(format!("[{}]", out.join(",")))
            }
            Type::Tuple { tuple } => {
                let st = down::<StructArray>(array.as_ref())?;
                let mut out = Vec::new();
                for (i, ty) in tuple.iter().enumerate() {
                    let field = st
                        .column_by_name(&format!("__vail_tuple_{i}"))
                        .ok_or_else(|| ExprError::Invalid("tuple storage field missing".into()))?;
                    out.push(hail_value_string(field, row, ty, false)?);
                }
                Ok(format!("[{}]", out.join(",")))
            }
            Type::Ndarray { ndarray } => ndarray_string(array, row, &ndarray.0, ndarray.1, top),
            _ => hail_value_string(array, row, &storage, top),
        };
    }
    Ok(match dtype {
        Type::Primitive(PrimitiveType::Int32) => {
            down::<Int32Array>(array.as_ref())?.value(row).to_string()
        }
        Type::Primitive(PrimitiveType::Int64) => {
            down::<Int64Array>(array.as_ref())?.value(row).to_string()
        }
        Type::Primitive(PrimitiveType::Float32) => {
            let v = down::<Float32Array>(array.as_ref())?.value(row);
            if top {
                hail_float_string(v as f64)
            } else if v.is_finite() {
                java_float_text(v as f64)
            } else {
                json_quote(&float_text(v as f64))?
            }
        }
        Type::Primitive(PrimitiveType::Float64) => {
            let v = down::<Float64Array>(array.as_ref())?.value(row);
            if top {
                hail_float_string(v)
            } else if v.is_finite() {
                java_float_text(v)
            } else {
                json_quote(&float_text(v))?
            }
        }
        Type::Primitive(PrimitiveType::Bool) => {
            down::<BooleanArray>(array.as_ref())?.value(row).to_string()
        }
        Type::Primitive(PrimitiveType::Str) => {
            json_quote(down::<StringArray>(array.as_ref())?.value(row))?
        }
        Type::Array { array: element } | Type::Set { set: element } => {
            let list = down::<ListArray>(array.as_ref())?;
            let vals = list.value(row);
            let mut out = Vec::new();
            for i in 0..vals.len() {
                out.push(hail_value_string(&vals, i, element, false)?);
            }
            format!("[{}]", out.join(","))
        }
        Type::Struct { r#struct: fields } => {
            let st = down::<StructArray>(array.as_ref())?;
            let mut out = Vec::new();
            for (name, ty) in fields {
                let child = st
                    .column_by_name(name)
                    .ok_or_else(|| ExprError::Invalid(format!("struct field {name:?} missing")))?;
                out.push(format!(
                    "{}:{}",
                    json_quote(name)?,
                    hail_value_string(child, row, ty, false)?
                ));
            }
            format!("{{{}}}", out.join(","))
        }
        Type::Locus { .. } => {
            let st = down::<StructArray>(array.as_ref())?;
            let contig = down::<StringArray>(
                st.column_by_name("__vail_locus_contig")
                    .ok_or_else(|| ExprError::Invalid("locus contig missing".into()))?
                    .as_ref(),
            )?
            .value(row);
            let position = down::<Int32Array>(
                st.column_by_name("__vail_locus_position")
                    .ok_or_else(|| ExprError::Invalid("locus position missing".into()))?
                    .as_ref(),
            )?
            .value(row);
            if top {
                format!("{contig}:{position}")
            } else {
                format!(
                    "{{\"contig\":{},\"position\":{position}}}",
                    json_quote(contig)?
                )
            }
        }
        // These are represented through storage_type above.
        Type::Tuple { .. }
        | Type::Primitive(PrimitiveType::Call)
        | Type::Dict { .. }
        | Type::Ndarray { .. }
        | Type::Interval { .. } => unreachable!(),
    })
}

fn ndarray_string(
    array: &ArrayRef,
    row: usize,
    element: &Type,
    ndims: usize,
    top: bool,
) -> Result<String, ExprError> {
    let st = down::<StructArray>(array.as_ref())?;
    let data = st
        .column_by_name("__vail_ndarray_data")
        .ok_or_else(|| ExprError::Invalid("ndarray data missing".into()))?;
    let shape = st
        .column_by_name("__vail_ndarray_shape")
        .ok_or_else(|| ExprError::Invalid("ndarray shape missing".into()))?;
    let (dims, data_list) = ndarray_parts(shape, data, row, ndims)?;
    let mut flat = Vec::new();
    for i in 0..data_list.len() {
        flat.push(hail_value_string(&data_list, i, element, false)?);
    }
    let mut index = 0;
    fn nest(dims: &[i64], flat: &[String], index: &mut usize) -> String {
        if dims.is_empty() {
            let v = flat[*index].clone();
            *index += 1;
            return v;
        }
        let mut parts = Vec::new();
        for _ in 0..dims[0] {
            parts.push(nest(&dims[1..], flat, index));
        }
        format!("[{}]", parts.join(", "))
    }
    let nested = nest(&dims, &flat, &mut index);
    if top {
        Ok(format!(
            "ndarray{{shape=({}), data={nested}}}",
            dims.iter()
                .map(i64::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        ))
    } else {
        Ok(format!(
            "{{\"shape\":[{}],\"data\":[{}]}}",
            dims.iter()
                .map(i64::to_string)
                .collect::<Vec<_>>()
                .join(","),
            flat.join(",")
        ))
    }
}

fn ndarray_parts(
    shape: &ArrayRef,
    data: &ArrayRef,
    row: usize,
    ndims: usize,
) -> Result<(Vec<i64>, ArrayRef), ExprError> {
    if shape.is_null(row) || data.is_null(row) {
        return Err(ExprError::Invalid(
            "nonnull ndarray has missing shape or data".into(),
        ));
    }
    let shape = down::<ListArray>(shape.as_ref())?.value(row);
    let shape = down::<Int64Array>(shape.as_ref())?;
    if shape.null_count() != 0 {
        return Err(ExprError::Invalid(
            "ndarray shape contains a missing dimension".into(),
        ));
    }
    if shape.len() != ndims {
        return Err(ExprError::Invalid(
            "ndarray shape rank does not match its type".into(),
        ));
    }
    let dims = shape.values().to_vec();
    if dims.iter().any(|d| *d < 0) {
        return Err(ExprError::Invalid(
            "ndarray dimensions cannot be negative".into(),
        ));
    }
    let data = down::<ListArray>(data.as_ref())?.value(row);
    let expected = dims
        .iter()
        .try_fold(1_usize, |n, d| n.checked_mul(*d as usize))
        .ok_or_else(|| ExprError::Invalid("ndarray shape is too large".into()))?;
    if expected != data.len() {
        return Err(ExprError::Invalid(
            "ndarray shape does not match data length".into(),
        ));
    }
    Ok((dims, data))
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
        if matches!(self.kind, HailKind::RestoreArrowType) {
            write!(f, "RestoreArrowType({})", self.args[0])
        } else {
            write!(f, "Hail({:?})", self.kind)
        }
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
                HailKind::RestoreArrowType => Ok(args[0].clone()),
                HailKind::Die => {
                    if len == 0 {
                        return Ok(arrow_array::new_empty_array(&self.dtype));
                    }
                    let message = if args[0].is_null(0) {
                        "missing error message".to_string()
                    } else {
                        let value = ScalarValue::try_from_array(&args[0], 0)?;
                        match value {
                            ScalarValue::Utf8(Some(message))
                            | ScalarValue::Utf8View(Some(message)) => message,
                            _ => {
                                return Err(ExprError::Invalid(
                                    "error message must have str type".into(),
                                ));
                            }
                        }
                    };
                    Err(DataFusionError::Execution(message).into())
                }
                HailKind::Numeric(op) => {
                    let t = Type::from_arrow(&self.dtype)?;
                    columnar_numeric(*op, &args[0], &args[1], &t)
                }
                HailKind::ToString(input_type) => {
                    let input_type: Type = serde_json::from_str(input_type)
                        .map_err(|e| ExprError::Invalid(e.to_string()))?;
                    hail_to_string(&args[0], &input_type)
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
                        Ok(Arc::new(StructArray::new_empty_fields(len, None)))
                    } else {
                        Ok(Arc::new(StructArray::from(fields)))
                    }
                }
                HailKind::Interval => {
                    let fields = vec![
                        Arc::new(Field::new(
                            "__vail_interval_start",
                            args[0].data_type().clone(),
                            true,
                        )),
                        Arc::new(Field::new(
                            "__vail_interval_start_exclusive",
                            DataType::Boolean,
                            true,
                        )),
                        Arc::new(Field::new(
                            "__vail_interval_end",
                            args[1].data_type().clone(),
                            true,
                        )),
                        Arc::new(Field::new(
                            "__vail_interval_end_inclusive",
                            DataType::Boolean,
                            true,
                        )),
                    ];
                    let nulls = NullBuffer::from(
                        (0..len)
                            .map(|i| !args[2].is_null(i) && !args[3].is_null(i))
                            .collect::<Vec<_>>(),
                    );
                    Ok(Arc::new(StructArray::try_new(
                        fields.into(),
                        vec![
                            args[0].clone(),
                            Arc::new(arrow_arith::boolean::not(down::<BooleanArray>(
                                args[2].as_ref(),
                            )?)?),
                            args[1].clone(),
                            args[3].clone(),
                        ],
                        Some(nulls),
                    )?))
                }
                HailKind::Locus {
                    operation,
                    reference_json,
                } => {
                    let reference: ReferenceDefinition = serde_json::from_str(reference_json)
                        .map_err(|e| ExprError::Invalid(e.to_string()))?;
                    locus_eval(operation, &reference, &args, &self.dtype, len)
                }
                HailKind::CompactInnerNullLists => {
                    let outer = args[0]
                        .as_any()
                        .downcast_ref::<ListArray>()
                        .ok_or_else(|| ExprError::Invalid("expected nested list".into()))?;
                    let (field, offsets, values, nulls) = outer.clone().into_parts();
                    let values = datafusion_common::utils::remove_list_null_values(&values)?;
                    Ok(Arc::new(ListArray::try_new(field, offsets, values, nulls)?) as ArrayRef)
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
            .and_then(|array| Ok(arrow_cast::cast(&array, &self.dtype)?))
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
pub fn restore_physical_type(
    value: Arc<dyn PhysicalExpr>,
    dtype: DataType,
) -> Arc<dyn PhysicalExpr> {
    if vail_ordering::has_type_metadata(&dtype) {
        custom(HailKind::RestoreArrowType, vec![value], dtype)
    } else {
        value
    }
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
    let globals = RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(1)),
    )?;
    lower_with_globals(expr, schema, &globals)
}

/// Bind global references to columns of a singleton relation joined to the rows.
pub fn lower_with_global_columns(
    expr: &Expr,
    schema: &Schema,
    globals: &Schema,
) -> Result<Arc<dyn PhysicalExpr>, ExprError> {
    lower_with_bindings(expr, schema, globals, &std::collections::BTreeMap::new())
}

/// External globals are bound by native IR identity, not by user field name.
pub fn lower_with_bindings(
    expr: &Expr,
    schema: &Schema,
    globals: &Schema,
    external: &std::collections::BTreeMap<String, usize>,
) -> Result<Arc<dyn PhysicalExpr>, ExprError> {
    lower_bound(expr, schema, GlobalBinding::Columns(globals, external))
}

pub fn lower_with_globals(
    expr: &Expr,
    schema: &Schema,
    globals: &RecordBatch,
) -> Result<Arc<dyn PhysicalExpr>, ExprError> {
    if globals.num_rows() != 1 {
        return Err(ExprError::Invalid(
            "globals must have exactly one row".into(),
        ));
    }
    lower_bound(expr, schema, GlobalBinding::Values(globals))
}

#[derive(Clone, Copy)]
enum GlobalBinding<'a> {
    Values(&'a RecordBatch),
    Columns(&'a Schema, &'a std::collections::BTreeMap<String, usize>),
}
impl GlobalBinding<'_> {
    fn schema(&self) -> &Schema {
        match self {
            Self::Values(batch) => batch.schema_ref(),
            Self::Columns(schema, _) => schema,
        }
    }
}
fn lower_bound(
    expr: &Expr,
    schema: &Schema,
    globals: GlobalBinding<'_>,
) -> Result<Arc<dyn PhysicalExpr>, ExprError> {
    lower_scoped(expr, schema, globals, &[])
}

pub(crate) fn string_function(
    name: &str,
) -> Result<(Arc<datafusion_expr::ScalarUDF>, Vec<Type>, Type), ExprError> {
    use datafusion_functions::{regex, string, unicode};
    let text = Type::Primitive(PrimitiveType::Str);
    let int32 = Type::Primitive(PrimitiveType::Int32);
    let int64 = Type::Primitive(PrimitiveType::Int64);
    let boolean = Type::Primitive(PrimitiveType::Bool);
    let strings = Type::Array {
        array: Box::new(text.clone()),
    };
    Ok(match name {
        "lower" => (string::lower(), vec![text.clone()], text),
        "upper" => (string::upper(), vec![text.clone()], text),
        "reverse" => (unicode::reverse(), vec![text.clone()], text),
        "btrim" => (string::btrim(), vec![text.clone(), text.clone()], text),
        "concat" => (string::concat(), vec![text.clone(), text.clone()], text),
        "repeat" => (string::repeat(), vec![text.clone(), int64], text),
        "contains" => (string::contains(), vec![text.clone(), text], boolean),
        "starts_with" => (string::starts_with(), vec![text.clone(), text], boolean),
        "ends_with" => (string::ends_with(), vec![text.clone(), text], boolean),
        "character_length" => (unicode::character_length(), vec![text], int32),
        "regexp_count" => (regex::regexp_count(), vec![text.clone(), text], int64),
        "regexp_like" => (regex::regexp_like(), vec![text.clone(), text], boolean),
        "strpos" => (unicode::strpos(), vec![text.clone(), text], int32),
        "substr" => (
            unicode::substr(),
            vec![text.clone(), int64.clone(), int64],
            text,
        ),
        "translate" => (
            unicode::translate(),
            vec![text.clone(), text.clone(), text.clone()],
            text,
        ),
        "replace" => (
            string::replace(),
            vec![text.clone(), text.clone(), text.clone()],
            text,
        ),
        "string_to_array" => (
            datafusion_functions_nested::string::string_to_array_udf(),
            vec![text.clone(), text],
            strings,
        ),
        "array_to_string" => (
            datafusion_functions_nested::string::array_to_string_udf(),
            vec![strings, text.clone(), text.clone()],
            text,
        ),
        _ => {
            return Err(ExprError::Invalid(format!(
                "unsupported string function {name:?}"
            )));
        }
    })
}

fn datafusion_distinct_compatible(dtype: &Type) -> bool {
    match dtype {
        Type::Set { .. } | Type::Dict { .. } | Type::Ndarray { .. } => false,
        Type::Array { array } => datafusion_distinct_compatible(array),
        Type::Struct { r#struct } => r#struct
            .iter()
            .all(|(_, ty)| datafusion_distinct_compatible(ty)),
        Type::Tuple { tuple } => tuple.iter().all(datafusion_distinct_compatible),
        Type::Interval { interval } => datafusion_distinct_compatible(interval),
        _ => true,
    }
}

fn same_collection_storage(left: &DataType, right: &DataType) -> bool {
    match (left, right) {
        (DataType::List(l), DataType::List(r)) => {
            same_collection_storage(l.data_type(), r.data_type())
        }
        // Struct field names remain significant: tuple/dictionary conversion is a projection.
        _ => left == right,
    }
}

pub(crate) fn nested_function(name: &str) -> Result<Arc<datafusion_expr::ScalarUDF>, ExprError> {
    use datafusion_functions_nested as nested;
    Ok(match name {
        "array_sort" => nested::sort::array_sort_udf(),
        "array_distinct" => nested::set_ops::array_distinct_udf(),
        "array_union" => nested::set_ops::array_union_udf(),
        "array_intersect" => nested::set_ops::array_intersect_udf(),
        "array_except" => nested::except::array_except_udf(),
        "array_element" => nested::extract::array_element_udf(),
        "array_concat" => nested::concat::array_concat_udf(),
        "arrays_zip" => nested::arrays_zip::arrays_zip_udf(),
        "array_compact" => nested::array_compact::array_compact_udf(),
        "flatten" => nested::flatten::flatten_udf(),
        "array_sum" => nested::array_sum::array_sum_udf(),
        "array_product" => nested::array_product::array_product_udf(),
        "array_min" => nested::min_max::array_min_udf(),
        "array_max" => nested::min_max::array_max_udf(),
        _ => {
            return Err(ExprError::Invalid(format!(
                "unsupported collection function {name:?}"
            )));
        }
    })
}

fn lower_array_lambda(
    value: &Expr,
    parameter: &str,
    callback: &Expr,
    operation: &str,
    schema: &Schema,
    globals: GlobalBinding<'_>,
    scope: &[(String, Type, usize)],
) -> Result<Arc<dyn PhysicalExpr>, ExprError> {
    let input = lower_scoped(value, schema, globals, scope)?;
    let array = match value.dtype_scoped(schema, globals.schema(), scope)? {
        Type::Array { array } | Type::Set { set: array } => *array,
        Type::Dict { dict } => Type::Struct {
            r#struct: vec![("key".into(), dict.0), ("value".into(), dict.1)],
        },
        _ => unreachable!("array lambda typing validates its input"),
    };
    // The list's container tag is not a lambda-variable field tag. Give DF
    // an ordinary array item field, retaining the element's nested typing.
    let input = cast(
        input,
        schema,
        Type::Array {
            array: Box::new(array.clone()),
        }
        .to_arrow()?,
    )?;
    let mut fields = schema.fields().to_vec();
    fields.push(Arc::new(Field::new(parameter, array.to_arrow()?, true)));
    let nested_schema = Schema::new(fields);
    let mut nested = scope.to_vec();
    nested.push((parameter.to_owned(), array, schema.fields().len()));
    let callback = lower_scoped(callback, &nested_schema, globals, &nested)?;
    let lambda = Arc::new(datafusion_physical_expr::expressions::LambdaExpr::try_new(
        vec![parameter.to_owned()],
        callback,
    )?);
    let function = match operation {
        "filter" => datafusion_functions_nested::array_filter::array_filter_higher_order_function(),
        "any" => {
            datafusion_functions_nested::array_any_match::array_any_match_higher_order_function()
        }
        "first" => datafusion_functions_nested::array_first::array_first_higher_order_function(),
        "map" => {
            datafusion_functions_nested::array_transform::array_transform_higher_order_function()
        }
        _ => unreachable!("lowering chooses a known higher-order function"),
    };
    Ok(Arc::new(
        datafusion_physical_expr::higher_order_function::HigherOrderFunctionExpr::try_new_with_schema(
            function,
            vec![input, lambda],
            schema,
            Arc::new(ConfigOptions::default()),
        )?,
    ))
}

// Hail's interval predicates return missing when the first boundary is
// missing, and inspect the second boundary only when the first passes.
// SQL AND alone would turn (missing, false) into false.
fn interval_boundary_sequence(
    first: Arc<dyn PhysicalExpr>,
    second: Arc<dyn PhysicalExpr>,
) -> Result<Arc<dyn PhysicalExpr>, ExprError> {
    let missing: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Boolean(None)));
    let no: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Boolean(Some(false))));
    Ok(Arc::new(CaseExpr::try_new(
        None,
        vec![
            (Arc::new(IsNullExpr::new(first.clone())), missing),
            (first, second),
        ],
        Some(no),
    )?))
}

fn lower_scoped(
    expr: &Expr,
    schema: &Schema,
    globals: GlobalBinding<'_>,
    scope: &[(String, Type, usize)],
) -> Result<Arc<dyn PhysicalExpr>, ExprError> {
    let dtype = expr
        .dtype_scoped(schema, globals.schema(), scope)?
        .to_arrow()?;
    let output_type = dtype.clone();
    let result: Arc<dyn PhysicalExpr> = match expr {
        Expr::Aggregate { .. }
        | Expr::AggFilter { .. }
        | Expr::AggExplode { .. }
        | Expr::AggGroupBy { .. }
        | Expr::AggArray { .. }
        | Expr::AggregateFold { .. } => {
            return Err(ExprError::Invalid(
                "aggregation expressions require Table.aggregate".into(),
            ));
        }
        Expr::LambdaVariable { name } => {
            let (_, ty, index) = scope
                .iter()
                .rev()
                .find(|(n, _, _)| n == name)
                .ok_or_else(|| ExprError::Invalid(format!("unbound lambda variable {name:?}")))?;
            Arc::new(datafusion_physical_expr::expressions::LambdaVariable::new(
                *index,
                Arc::new(Field::new(name, ty.to_arrow()?, true)),
            ))
        }
        Expr::NestedFunction { function, args, .. } => {
            let args = args
                .iter()
                .map(|arg| {
                    let value = lower_scoped(arg, schema, globals, scope)?;
                    let ty = arg
                        .dtype_scoped(schema, globals.schema(), scope)?
                        .to_arrow()?;
                    Ok(cast(
                        value,
                        schema,
                        vail_ordering::without_type_metadata(&ty),
                    )?)
                })
                .collect::<Result<Vec<_>, ExprError>>()?;
            let function = Arc::new(ScalarFunctionExpr::try_new(
                nested_function(function)?,
                args,
                schema,
                Arc::new(ConfigOptions::default()),
            )?);
            cast(function, schema, dtype.clone())?
        }
        Expr::CollectionCast { value, .. } => cast(
            lower_scoped(value, schema, globals, scope)?,
            schema,
            dtype.clone(),
        )?,
        Expr::IntegerBinary {
            operator,
            left,
            right,
        } => {
            let operator = match operator {
                BinaryOperator::Add => Operator::Plus,
                BinaryOperator::Sub => Operator::Minus,
                BinaryOperator::Eq => Operator::Eq,
                BinaryOperator::Lt => Operator::Lt,
                BinaryOperator::Le => Operator::LtEq,
                _ => unreachable!("typing validates integer operators"),
            };
            binary(
                lower_scoped(left, schema, globals, scope)?,
                operator,
                lower_scoped(right, schema, globals, scope)?,
                schema,
            )?
        }
        Expr::StringFunction { function, args } => {
            let (udf, _, _) = string_function(function)?;
            let args = args
                .iter()
                .map(|arg| lower_scoped(arg, schema, globals, scope))
                .collect::<Result<Vec<_>, ExprError>>()?;
            let function = Arc::new(ScalarFunctionExpr::try_new(
                udf,
                args,
                schema,
                Arc::new(ConfigOptions::default()),
            )?);
            cast(function, schema, dtype.clone())?
        }
        Expr::Tuple { elements } => {
            let mut args: Vec<Arc<dyn PhysicalExpr>> =
                Vec::with_capacity(elements.len().max(1) * 2);
            if elements.is_empty() {
                args.push(Arc::new(Literal::new(ScalarValue::Utf8(Some(
                    "__vail_tuple_empty".into(),
                )))));
                args.push(Arc::new(Literal::new(ScalarValue::Boolean(Some(false)))));
            } else {
                for (index, element) in elements.iter().enumerate() {
                    args.push(Arc::new(Literal::new(ScalarValue::Utf8(Some(format!(
                        "__vail_tuple_{index}"
                    ))))));
                    args.push(lower_scoped(element, schema, globals, scope)?);
                }
            }
            let tuple = Arc::new(ScalarFunctionExpr::try_new(
                datafusion_functions::core::named_struct(),
                args,
                schema,
                Arc::new(ConfigOptions::default()),
            )?);
            cast(tuple, schema, dtype.clone())?
        }
        Expr::GetField { value, name }
            if matches!(
                value.dtype_scoped(schema, globals.schema(), scope)?,
                Type::Tuple { .. }
            ) =>
        {
            let args: Vec<Arc<dyn PhysicalExpr>> = vec![
                lower_scoped(value, schema, globals, scope)?,
                Arc::new(Literal::new(ScalarValue::Utf8(Some(name.clone())))),
            ];
            Arc::new(ScalarFunctionExpr::try_new(
                datafusion_functions::core::get_field(),
                args,
                schema,
                Arc::new(ConfigOptions::default()),
            )?)
        }
        Expr::ArrayFold {
            value,
            zero,
            accumulator,
            parameter,
            body,
            scan,
        } => {
            let Type::Array { array } = value.dtype_scoped(schema, globals.schema(), scope)? else {
                unreachable!()
            };
            let accumulator_type = zero.dtype_scoped(schema, globals.schema(), scope)?;
            let mut fields = schema.fields().to_vec();
            fields.push(Arc::new(Field::new(
                accumulator,
                accumulator_type.to_arrow()?,
                true,
            )));
            fields.push(Arc::new(Field::new(parameter, array.to_arrow()?, true)));
            let nested_schema = Arc::new(Schema::new(fields));
            let mut nested = scope.to_vec();
            nested.push((accumulator.clone(), accumulator_type, schema.fields().len()));
            nested.push((parameter.clone(), *array, schema.fields().len() + 1));
            Arc::new(crate::array_fold::ArrayFoldExpr::new(
                lower_scoped(value, schema, globals, scope)?,
                lower_scoped(zero, schema, globals, scope)?,
                lower_scoped(body, &nested_schema, globals, &nested)?,
                nested_schema,
                dtype.clone(),
                *scan,
            ))
        }
        Expr::ArrayMap {
            value,
            parameter,
            body,
        } => lower_array_lambda(value, parameter, body, "map", schema, globals, scope)?,
        Expr::ArrayPredicate {
            value,
            parameter,
            predicate,
            first,
        } => lower_array_lambda(
            value,
            parameter,
            predicate,
            if *first { "first" } else { "any" },
            schema,
            globals,
            scope,
        )?,
        Expr::ArrayFilter {
            value,
            parameter,
            predicate,
        } => lower_array_lambda(
            value, parameter, predicate, "filter", schema, globals, scope,
        )?,
        Expr::StreamRange { start, stop, step } => {
            let int64 = DataType::Int64;
            let args = [start, stop, step]
                .iter()
                .map(|arg| {
                    let value = lower_scoped(arg, schema, globals, scope)?;
                    Ok(cast(value, schema, int64.clone())?)
                })
                .collect::<Result<Vec<_>, ExprError>>()?;
            let range = Arc::new(ScalarFunctionExpr::try_new(
                datafusion_functions_nested::range::range_udf(),
                args,
                schema,
                Arc::new(ConfigOptions::default()),
            )?);
            cast(range, schema, dtype)?
        }
        Expr::ArrayGrouped { value, size } => {
            let input_type = value.dtype_scoped(schema, globals.schema(), scope)?;
            let Type::Array { .. } = input_type else {
                unreachable!("typing validates array_grouped")
            };
            let mut parameter = "__vail_group_start".to_owned();
            while scope.iter().any(|(name, _, _)| name == &parameter) {
                parameter.push('_');
            }
            let variable = Expr::LambdaVariable {
                name: parameter.clone(),
            };
            let length = Expr::ArrayLength {
                value: value.clone(),
            };
            let remaining = Expr::Binary {
                operator: BinaryOperator::Sub,
                left: Box::new(length.clone()),
                right: Box::new(variable.clone()),
            };
            let capped_stop = Expr::IfElse {
                condition: Box::new(Expr::Binary {
                    operator: BinaryOperator::Ge,
                    left: size.clone(),
                    right: Box::new(remaining),
                }),
                consequent: Box::new(length),
                alternate: Box::new(Expr::Binary {
                    operator: BinaryOperator::Add,
                    left: Box::new(variable.clone()),
                    right: size.clone(),
                }),
            };
            let grouped = Expr::ArrayMap {
                value: Box::new(Expr::StreamRange {
                    start: Box::new(Expr::Literal {
                        dtype: Type::Primitive(PrimitiveType::Int32),
                        value: json!(0),
                    }),
                    stop: Box::new(Expr::ArrayLength {
                        value: value.clone(),
                    }),
                    step: size.clone(),
                }),
                parameter,
                body: Box::new(Expr::ArraySlice {
                    value: value.clone(),
                    start: Some(Box::new(variable)),
                    stop: Some(Box::new(capped_stop)),
                    step: None,
                }),
            };
            lower_scoped(&grouped, schema, globals, scope)?
        }

        Expr::ExternalGlobal { reference, dtype } => {
            let GlobalBinding::Columns(_, bindings) = globals else {
                return Err(ExprError::Invalid(
                    "external globals require a relational binding".into(),
                ));
            };
            let index = *bindings
                .get(reference)
                .ok_or_else(|| ExprError::Invalid("unbound external globals".into()))?;
            let field = schema
                .fields()
                .get(index)
                .ok_or_else(|| ExprError::Invalid("external global column is absent".into()))?;
            let actual = Type::from_arrow(field.data_type())?;
            if actual != *dtype {
                return Err(ExprError::Invalid(format!(
                    "external globals type mismatch: expected {dtype:?}, found {actual:?}"
                )));
            }
            Arc::new(Column::new(field.name(), index))
        }
        Expr::Global { name } => match globals {
            GlobalBinding::Values(batch) => {
                let index = batch.schema().index_of(name)?;
                Arc::new(Literal::new(scalar_at(batch.column(index), 0)?))
            }
            GlobalBinding::Columns(_, _) => Arc::new(Column::new(name, schema.index_of(name)?)),
        },
        Expr::Column { name } => Arc::new(Column::new(
            name,
            schema
                .index_of(name)
                .map_err(|_| ExprError::Invalid(format!("source column {name:?} is absent")))?,
        )),
        Expr::RuntimeLiteral { value } => Arc::new(Literal::new(value.clone())),
        Expr::Literal { dtype, value } => {
            let one = literal_array(value, dtype, 1)?;
            Arc::new(Literal::new(scalar_at(&one, 0)?))
        }
        Expr::Cast {
            dtype: Type::Primitive(PrimitiveType::Call),
            value,
        } if matches!(value.dtype_scoped(schema,globals.schema(),scope)?, Type::Struct { r#struct: fields } if fields.iter().any(|(name,_)| name=="__vail_call_alleles")) => {
            lower_scoped(&crate::calls::construct(value), schema, globals, scope)?
        }
        Expr::GetField { value, name }
            if value.dtype_scoped(schema, globals.schema(), scope)?
                == Type::Primitive(PrimitiveType::Call)
                && crate::calls::getter(value, name).is_some() =>
        {
            lower_scoped(
                &crate::calls::getter(value, name).unwrap(),
                schema,
                globals,
                scope,
            )?
        }
        Expr::Cast { dtype, value } => lower_cast(
            lower_scoped(value, schema, globals, scope)?,
            schema,
            &dtype.to_arrow()?,
        )?,
        Expr::ToString { value } => {
            let input_type = value.dtype_scoped(schema, globals.schema(), scope)?;
            let child = lower_scoped(value, schema, globals, scope)?;
            if matches!(input_type, Type::Primitive(PrimitiveType::Str)) {
                child
            } else {
                if matches!(
                    input_type,
                    Type::Primitive(
                        PrimitiveType::Int32 | PrimitiveType::Int64 | PrimitiveType::Bool
                    )
                ) {
                    lower_cast(child, schema, &DataType::Utf8)?
                } else {
                    custom(
                        HailKind::ToString(
                            serde_json::to_string(&input_type)
                                .map_err(|e| ExprError::Invalid(e.to_string()))?,
                        ),
                        vec![child],
                        DataType::Utf8,
                    )
                }
            }
        }
        Expr::IsMissing { value } => Arc::new(IsNullExpr::new(lower_scoped(
            value, schema, globals, scope,
        )?)),
        Expr::Array {
            element_type,
            elements,
        } => {
            if elements.is_empty() {
                let ty = Type::Array {
                    array: Box::new(element_type.clone()),
                };
                let a = literal_array(&json!([]), &ty, 1)?;
                Arc::new(Literal::new(scalar_at(&a, 0)?))
            } else {
                let children = elements
                    .iter()
                    .map(|e| {
                        let value = lower_scoped(e, schema, globals, scope)?;
                        let ty = e
                            .dtype_scoped(schema, globals.schema(), scope)?
                            .to_arrow()?;
                        Ok(cast(
                            value,
                            schema,
                            vail_ordering::without_type_metadata(&ty),
                        )?)
                    })
                    .collect::<Result<Vec<_>, ExprError>>()?;
                Arc::new(ScalarFunctionExpr::try_new(
                    datafusion_functions_nested::make_array::make_array_udf(),
                    children,
                    schema,
                    Arc::new(ConfigOptions::default()),
                )?)
            }
        }
        Expr::ArrayFlatten { value } => {
            let child = lower_scoped(value, schema, globals, scope)?;
            let input_type = child.data_type(schema)?;
            let normalized = custom(HailKind::CompactInnerNullLists, vec![child], input_type);
            Arc::new(ScalarFunctionExpr::try_new(
                datafusion_functions_nested::flatten::flatten_udf(),
                vec![normalized],
                schema,
                Arc::new(ConfigOptions::default()),
            )?)
        }
        Expr::ArrayLength { value } => {
            let child = lower_scoped(value, schema, globals, scope)?;
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
            vec![
                lower_scoped(value, schema, globals, scope)?,
                lower_scoped(index, schema, globals, scope)?,
            ],
            dtype,
        ),
        Expr::ArraySlice {
            value,
            start,
            stop,
            step,
        } => {
            let mut args = vec![lower_scoped(value, schema, globals, scope)?];
            for e in [start, stop, step].into_iter().flatten() {
                args.push(lower_scoped(e, schema, globals, scope)?);
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
            vec![lower_scoped(value, schema, globals, scope)?],
            dtype,
        ),
        Expr::GetField { value, name } => {
            let invert = name == "includes_start"
                && matches!(
                    value.dtype_scoped(schema, globals.schema(), scope)?,
                    Type::Interval { .. }
                );
            let field = custom(
                HailKind::GetField(match value.dtype_scoped(schema, globals.schema(), scope)? {
                    Type::Interval { .. } => match name.as_str() {
                        "start" => "__vail_interval_start".into(),
                        "end" => "__vail_interval_end".into(),
                        "includes_start" => "__vail_interval_start_exclusive".into(),
                        "includes_end" => "__vail_interval_end_inclusive".into(),
                        _ => name.clone(),
                    },
                    _ => name.clone(),
                }),
                vec![lower_scoped(value, schema, globals, scope)?],
                dtype,
            );
            if invert {
                Arc::new(NotExpr::new(field)) as Arc<dyn PhysicalExpr>
            } else {
                field
            }
        }
        Expr::Interval {
            start,
            end,
            includes_start,
            includes_end,
        } => custom(
            HailKind::Interval,
            vec![
                lower_scoped(start, schema, globals, scope)?,
                lower_scoped(end, schema, globals, scope)?,
                lower_scoped(includes_start, schema, globals, scope)?,
                lower_scoped(includes_end, schema, globals, scope)?,
            ],
            dtype,
        ),
        Expr::Locus {
            reference,
            contig,
            position,
        } => custom(
            HailKind::Locus {
                operation: "create".into(),
                reference_json: serde_json::to_string(reference)
                    .map_err(|e| ExprError::Invalid(e.to_string()))?,
            },
            vec![
                lower_scoped(contig, schema, globals, scope)?,
                lower_scoped(position, schema, globals, scope)?,
            ],
            dtype,
        ),
        Expr::LocusValid {
            reference,
            contig,
            position,
        } => custom(
            HailKind::Locus {
                operation: "valid".into(),
                reference_json: serde_json::to_string(reference)
                    .map_err(|e| ExprError::Invalid(e.to_string()))?,
            },
            vec![
                lower_scoped(contig, schema, globals, scope)?,
                lower_scoped(position, schema, globals, scope)?,
            ],
            dtype,
        ),
        Expr::LocusFromGlobalPosition {
            reference,
            position,
        } => custom(
            HailKind::Locus {
                operation: "from_global".into(),
                reference_json: serde_json::to_string(reference)
                    .map_err(|e| ExprError::Invalid(e.to_string()))?,
            },
            vec![lower_scoped(position, schema, globals, scope)?],
            dtype,
        ),
        Expr::ParseLocus { reference, value } => custom(
            HailKind::Locus {
                operation: "parse".into(),
                reference_json: serde_json::to_string(reference)
                    .map_err(|e| ExprError::Invalid(e.to_string()))?,
            },
            vec![lower_scoped(value, schema, globals, scope)?],
            dtype,
        ),
        Expr::ParseCsvLine {
            value,
            delimiter,
            quote,
            missing,
            expected_fields,
        } => {
            let udf = Arc::new(crate::csv_line::parse_csv_line_udf(
                delimiter.clone(),
                quote.clone(),
                missing.clone(),
                *expected_fields,
            )?);
            let child = lower_scoped(value, schema, globals, scope)?;
            let function = Arc::new(ScalarFunctionExpr::try_new(
                udf,
                vec![child],
                schema,
                Arc::new(ConfigOptions::default()),
            )?);
            cast(function, schema, dtype.clone())?
        }
        Expr::LocusOp { operation, value } => {
            let Type::Locus { locus } = value.dtype_scoped(schema, globals.schema(), scope)? else {
                unreachable!()
            };
            custom(
                HailKind::Locus {
                    operation: operation.clone(),
                    reference_json: serde_json::to_string(&locus)
                        .map_err(|e| ExprError::Invalid(e.to_string()))?,
                },
                vec![lower_scoped(value, schema, globals, scope)?],
                dtype,
            )
        }
        Expr::IntervalContains { interval, point } => {
            let Type::Interval {
                interval: point_type,
            } = interval.dtype_scoped(schema, globals.schema(), scope)?
            else {
                unreachable!("validated interval")
            };
            point_type.ensure_keyable()?;
            if !point_type.native_key() {
                return Err(ExprError::Unsupported(format!(
                    "interval comparison for {point_type:?}"
                )));
            }
            let get = |name: &str| Expr::GetField {
                value: interval.clone(),
                name: name.into(),
            };
            let start = lower_scoped(&get("start"), schema, globals, scope)?;
            let end = lower_scoped(&get("end"), schema, globals, scope)?;
            let point = lower_scoped(point, schema, globals, scope)?;
            let includes_start = lower_scoped(&get("includes_start"), schema, globals, scope)?;
            let includes_end = lower_scoped(&get("includes_end"), schema, globals, scope)?;
            let start_gt = binary(point.clone(), Operator::Gt, start.clone(), schema)?;
            let start_eq = binary(point.clone(), Operator::Eq, start, schema)?;
            let above_start = binary(
                start_gt,
                Operator::Or,
                binary(start_eq, Operator::And, includes_start, schema)?,
                schema,
            )?;
            let end_lt = binary(point.clone(), Operator::Lt, end.clone(), schema)?;
            let end_eq = binary(point, Operator::Eq, end, schema)?;
            let below_end = binary(
                end_lt,
                Operator::Or,
                binary(end_eq, Operator::And, includes_end, schema)?,
                schema,
            )?;
            interval_boundary_sequence(above_start, below_end)?
        }
        Expr::IntervalOverlaps { left, right } => {
            let Type::Interval {
                interval: point_type,
            } = left.dtype_scoped(schema, globals.schema(), scope)?
            else {
                unreachable!("validated interval")
            };
            point_type.ensure_keyable()?;
            if !point_type.native_key() {
                return Err(ExprError::Unsupported(format!(
                    "interval comparison for {point_type:?}"
                )));
            }
            let get = |value: &Expr, name: &str| Expr::GetField {
                value: Box::new(value.clone()),
                name: name.into(),
            };
            let l_start = lower_scoped(&get(left, "start"), schema, globals, scope)?;
            let l_end = lower_scoped(&get(left, "end"), schema, globals, scope)?;
            let r_start = lower_scoped(&get(right, "start"), schema, globals, scope)?;
            let r_end = lower_scoped(&get(right, "end"), schema, globals, scope)?;
            let l_inc_start = lower_scoped(&get(left, "includes_start"), schema, globals, scope)?;
            let l_inc_end = lower_scoped(&get(left, "includes_end"), schema, globals, scope)?;
            let r_inc_start = lower_scoped(&get(right, "includes_start"), schema, globals, scope)?;
            let r_inc_end = lower_scoped(&get(right, "includes_end"), schema, globals, scope)?;
            let first_lt = binary(l_start.clone(), Operator::Lt, r_end.clone(), schema)?;
            let first_eq = binary(l_start, Operator::Eq, r_end, schema)?;
            let first_touch = binary(
                binary(first_eq, Operator::And, l_inc_start, schema)?,
                Operator::And,
                r_inc_end,
                schema,
            )?;
            let first = binary(first_lt, Operator::Or, first_touch, schema)?;
            let second_lt = binary(r_start.clone(), Operator::Lt, l_end.clone(), schema)?;
            let second_eq = binary(r_start, Operator::Eq, l_end, schema)?;
            let second_touch = binary(
                binary(second_eq, Operator::And, r_inc_start, schema)?,
                Operator::And,
                l_inc_end,
                schema,
            )?;
            let second = binary(second_lt, Operator::Or, second_touch, schema)?;
            interval_boundary_sequence(second, first)?
        }
        Expr::Struct { fields } => custom(
            HailKind::Struct(fields.iter().map(|(n, _)| n.clone()).collect()),
            fields
                .iter()
                .map(|(_, e)| lower_scoped(e, schema, globals, scope))
                .collect::<Result<Vec<_>, _>>()?,
            dtype,
        ),
        Expr::Unary { operator, value } => {
            let child = lower_scoped(value, schema, globals, scope)?;
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
            let lt = left.dtype_scoped(schema, globals.schema(), scope)?;
            let rt = right.dtype_scoped(schema, globals.schema(), scope)?;
            let mut l = lower_scoped(left, schema, globals, scope)?;
            let mut r = lower_scoped(right, schema, globals, scope)?;
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
                    l = lower_cast(l, schema, &common_arrow)?;
                    r = lower_cast(r, schema, &common_arrow)?;
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
                BinaryOperator::Add | BinaryOperator::Sub | BinaryOperator::Mul => {
                    l = lower_cast(l, schema, &dtype)?;
                    r = lower_cast(r, schema, &dtype)?;
                    let op = match operator {
                        BinaryOperator::Add => Operator::Plus,
                        BinaryOperator::Sub => Operator::Minus,
                        BinaryOperator::Mul => Operator::Multiply,
                        _ => unreachable!(),
                    };
                    // DataFusion's default BinaryExpr wraps integer overflow.
                    // It also preserves scalar operands and uses Arrow kernels.
                    binary(l, op, r, schema)?
                }
                BinaryOperator::Pow => {
                    l = lower_cast(l, schema, &dtype)?;
                    r = lower_cast(r, schema, &dtype)?;
                    Arc::new(ScalarFunctionExpr::try_new(
                        datafusion_functions::math::power(),
                        vec![l, r],
                        schema,
                        Arc::new(ConfigOptions::default()),
                    )?)
                }
                BinaryOperator::Floordiv
                    if matches!(dtype, DataType::Float32 | DataType::Float64) =>
                {
                    l = lower_cast(l, schema, &DataType::Float64)?;
                    r = lower_cast(r, schema, &DataType::Float64)?;
                    let divided = binary(l, Operator::Divide, r, schema)?;
                    let floored = Arc::new(ScalarFunctionExpr::try_new(
                        datafusion_functions::math::floor(),
                        vec![divided],
                        schema,
                        Arc::new(ConfigOptions::default()),
                    )?);
                    lower_cast(floored, schema, &dtype)?
                }
                _ => {
                    l = lower_cast(l, schema, &dtype)?;
                    r = lower_cast(r, schema, &dtype)?;
                    custom(HailKind::Numeric(*operator), vec![l, r], dtype)
                }
            }
        }
        Expr::Die { message, .. } => custom(
            HailKind::Die,
            vec![lower_scoped(message, schema, globals, scope)?],
            dtype.clone(),
        ),
        Expr::IfElse {
            condition,
            consequent,
            alternate,
        } => {
            let cond = lower_scoped(condition, schema, globals, scope)?;
            let yes = lower_cast(
                lower_scoped(consequent, schema, globals, scope)?,
                schema,
                &dtype,
            )?;
            let no = lower_cast(
                lower_scoped(alternate, schema, globals, scope)?,
                schema,
                &dtype,
            )?;
            let missing: Arc<dyn PhysicalExpr> =
                Arc::new(Literal::new(ScalarValue::try_from(&dtype)?));
            let is_null: Arc<dyn PhysicalExpr> = Arc::new(IsNullExpr::new(cond.clone()));
            Arc::new(CaseExpr::try_new(
                None,
                vec![(is_null, missing), (cond, yes)],
                Some(no),
            )?)
        }
    };
    if vail_ordering::has_type_metadata(&output_type) {
        // Cast arrays, not DF scalars: scalar list reconstruction can discard
        // item-field metadata even when its advertised type is unchanged.
        Ok(custom(
            HailKind::RestoreArrowType,
            vec![result],
            output_type,
        ))
    } else {
        Ok(result)
    }
}

/// Compile each projection expression once for reuse across ordered batches.
pub struct CompiledProjection {
    source_schema: SchemaRef,
    schema: SchemaRef,
    expressions: Vec<Arc<dyn PhysicalExpr>>,
}
impl CompiledProjection {
    pub fn new(source: &Schema, fields: &[(String, Expr)]) -> Result<Self, ExprError> {
        let globals = RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(1)),
        )?;
        Self::new_with_globals(source, fields, &globals)
    }
    pub fn new_with_globals(
        source: &Schema,
        fields: &[(String, Expr)],
        globals: &RecordBatch,
    ) -> Result<Self, ExprError> {
        let schema = validate_projection_with_globals(source, globals.schema_ref(), fields)?;
        let expressions = fields
            .iter()
            .map(|(_, e)| lower_with_globals(e, source, globals))
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

    #[test]
    fn arrow_array_sum_keeps_wrapping_identity_and_missing_semantics() {
        let lists = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
            Some(vec![Some(i64::MAX), Some(1)]),
            Some(vec![None, None]),
            Some(vec![]),
            None,
        ]);
        let lists: ArrayRef = Arc::new(lists);
        let filtered = array_reduce(&lists, ArrayReduction::Sum, true, &DataType::Int64).unwrap();
        let filtered = filtered.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(
            filtered.iter().collect::<Vec<_>>(),
            [Some(i64::MIN), Some(0), Some(0), None,]
        );

        let strict = array_reduce(&lists, ArrayReduction::Sum, false, &DataType::Int64).unwrap();
        let strict = strict.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(
            strict.iter().collect::<Vec<_>>(),
            [Some(i64::MIN), None, Some(0), None,]
        );
    }

    #[test]
    fn parse_csv_line_runs_as_a_physical_dataframe_expression() {
        let schema = Arc::new(Schema::new(vec![Field::new("line", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![
                Some("a,\"b,c\","),
                Some("x,y,z"),
            ]))],
        )
        .unwrap();
        let expression = Expr::ParseCsvLine {
            value: Box::new(Expr::Column {
                name: "line".into(),
            }),
            delimiter: ",".into(),
            quote: Some("\"".into()),
            missing: vec!["".into()],
            expected_fields: 3,
        };
        let result = project_batch(&batch, &[("fields".into(), expression)]).unwrap();
        let lists = result
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let fields = lists
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(fields.value(0), "a");
        assert_eq!(fields.value(1), "b,c");
        assert!(fields.is_null(2));
        assert_eq!(fields.value(3), "x");
        assert_eq!(fields.value(4), "y");
        assert_eq!(fields.value(5), "z");

        let comma = Expr::ParseCsvLine {
            value: Box::new(Expr::Column {
                name: "line".into(),
            }),
            delimiter: ",".into(),
            quote: Some("\"".into()),
            missing: vec![],
            expected_fields: 3,
        };
        let pipe = Expr::ParseCsvLine {
            value: Box::new(Expr::Column {
                name: "line".into(),
            }),
            delimiter: "|".into(),
            quote: Some("\"".into()),
            missing: vec![],
            expected_fields: 3,
        };
        let comma_physical = lower(&comma, &batch.schema()).unwrap();
        let pipe_physical = lower(&pipe, &batch.schema()).unwrap();
        assert!(comma_physical.as_ref() != pipe_physical.as_ref());
    }

    fn eval_string(value_type: Type, value: Value) -> Option<String> {
        let batch = source(vec![Some(0)]);
        let expression = Expr::ToString {
            value: Box::new(Expr::Literal {
                dtype: value_type,
                value,
            }),
        };
        let result = lower(&expression, &batch.schema())
            .unwrap()
            .evaluate(&batch)
            .unwrap()
            .into_array(1)
            .unwrap();
        let strings = result.as_any().downcast_ref::<StringArray>().unwrap();
        (!strings.is_null(0)).then(|| strings.value(0).to_owned())
    }

    #[test]
    fn hail_string_formats_nested_json_missing_and_float_scalars() {
        let int32 = Type::Primitive(PrimitiveType::Int32);
        assert_eq!(
            eval_string(
                Type::Array {
                    array: Box::new(int32.clone())
                },
                json!([1, null, -2])
            )
            .as_deref(),
            Some("[1,null,-2]")
        );
        assert_eq!(
            eval_string(Type::Primitive(PrimitiveType::Float64), json!(1.25)).as_deref(),
            Some("1.25000e+00")
        );
        assert_eq!(
            eval_string(Type::Primitive(PrimitiveType::Float64), Value::Null),
            None
        );
        let structure = Type::Struct {
            r#struct: vec![
                ("z".into(), int32.clone()),
                ("a".into(), Type::Primitive(PrimitiveType::Str)),
            ],
        };
        assert_eq!(
            eval_string(structure, json!({"z": 3, "a": "x"})).as_deref(),
            Some(r#"{"z":3,"a":"x"}"#)
        );
    }

    #[test]
    fn ndarray_string_rejects_invalid_shape_without_indexing_past_data() {
        let shape = ListArray::new(
            Arc::new(Field::new("item", DataType::Int64, true)),
            OffsetBuffer::new(vec![0_i32, 1].into()),
            Arc::new(Int64Array::from(vec![Some(2)])),
            None,
        );
        let data = ListArray::new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            OffsetBuffer::new(vec![0_i32, 0].into()),
            Arc::new(Int32Array::from(Vec::<Option<i32>>::new())),
            None,
        );
        let shape: ArrayRef = Arc::new(shape);
        let data: ArrayRef = Arc::new(data);
        assert!(ndarray_parts(&shape, &data, 0, 2).is_err());
    }
    fn values(a: &ArrayRef) -> Vec<ScalarValue> {
        (0..a.len())
            .map(|i| ScalarValue::try_from_array(a.as_ref(), i).unwrap())
            .collect()
    }

    fn test_reference() -> ReferenceDefinition {
        ReferenceDefinition {
            name: "test".into(),
            contigs: vec!["chrA".into(), "chr10".into(), "chrX".into(), "chrY".into()],
            lengths: [
                ("chrA".into(), 199),
                ("chr10".into(), 10),
                ("chrX".into(), 60),
                ("chrY".into(), 60),
            ]
            .into(),
            x_contigs: vec!["chrX".into()],
            y_contigs: vec!["chrY".into()],
            mt_contigs: vec![],
            par: vec![
                crate::reference::ParInterval {
                    contig: "chrX".into(),
                    start: 10,
                    end: 21,
                },
                crate::reference::ParInterval {
                    contig: "chrY".into(),
                    start: 30,
                    end: 41,
                },
            ],
        }
    }

    #[test]
    fn locus_coordinates_classification_boundaries_and_global_positions() {
        let reference = test_reference();
        let loci = locus_eval(
            "create",
            &reference,
            &[
                Arc::new(StringArray::from(vec![
                    Some("chrY"),
                    Some("chrY"),
                    Some("chrY"),
                    Some("chrY"),
                ])) as ArrayRef,
                Arc::new(Int32Array::from(vec![
                    Some(29),
                    Some(30),
                    Some(40),
                    Some(41),
                ])),
            ],
            &DataType::Null,
            4,
        )
        .unwrap();
        let in_par = locus_eval(
            "in_y_par",
            &reference,
            &[loci.clone()],
            &DataType::Boolean,
            4,
        )
        .unwrap();
        let in_par = in_par.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(
            (0..4).map(|i| in_par.value(i)).collect::<Vec<_>>(),
            vec![false, true, true, false]
        );

        let global = locus_eval(
            "from_global",
            &reference,
            &[Arc::new(Int64Array::from(vec![Some(199), Some(200), None]))],
            &DataType::Null,
            3,
        )
        .unwrap();
        let global = global.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(
            global
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "chr10"
        );
        assert_eq!(
            global
                .column(2)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            1
        );
        assert_eq!(
            global
                .column(2)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(1),
            2
        );
        assert!(global.is_null(2));

        assert!(
            locus_eval(
                "create",
                &reference,
                &[
                    Arc::new(StringArray::from(vec![Some("chrA")])) as ArrayRef,
                    Arc::new(Int32Array::from(vec![Some(200)]))
                ],
                &DataType::Null,
                1
            )
            .is_err()
        );
        assert!(
            locus_eval(
                "from_global",
                &reference,
                &[Arc::new(Int64Array::from(vec![Some(1000)]))],
                &DataType::Null,
                1
            )
            .is_err()
        );
    }

    #[test]
    fn locus_literal_uses_frontend_physical_value_and_checks_ordinal() {
        let literal = Expr::Literal {
            dtype: Type::Locus {
                locus: test_reference(),
            },
            value: json!({"__vail_locus_ordinal": 1, "__vail_locus_contig": "chr10", "__vail_locus_position": 4}),
        };
        let empty = RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(1)),
        )
        .unwrap();
        let projected = project_batch(&empty, &[("locus".into(), literal)]).unwrap();
        let loci = projected
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(
            loci.column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            1
        );
        assert_eq!(
            loci.column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "chr10"
        );

        let forged = Expr::Literal {
            dtype: Type::Locus {
                locus: test_reference(),
            },
            value: json!({"__vail_locus_ordinal": 0, "__vail_locus_contig": "chr10", "__vail_locus_position": 4}),
        };
        assert!(forged.dtype(&Schema::empty()).is_err());
    }

    #[test]
    fn stream_range_uses_exclusive_stop_and_reports_zero_step() {
        let b = source(vec![Some(0)]);
        let range = expr(json!({
            "op":"stream_range",
            "start":{"op":"literal","dtype":"int32","value":1},
            "stop":{"op":"literal","dtype":"int32","value":8},
            "step":{"op":"literal","dtype":"int32","value":3}
        }));
        let out = project_batch(&b, &[("range".into(), range)]).unwrap();
        let lists = out.column(0).as_any().downcast_ref::<ListArray>().unwrap();
        let values = lists
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(values.values(), &[1, 4, 7]);

        let missing_stop = expr(json!({
            "op":"stream_range",
            "start":{"op":"literal","dtype":"int32","value":0},
            "stop":{"op":"literal","dtype":"int32","value":null},
            "step":{"op":"literal","dtype":"int32","value":1}
        }));
        let missing = project_batch(&b, &[("range".into(), missing_stop)]).unwrap();
        assert!(missing.column(0).is_null(0));

        let zero_step = expr(json!({
            "op":"stream_range",
            "start":{"op":"literal","dtype":"int32","value":0},
            "stop":{"op":"literal","dtype":"int32","value":5},
            "step":{"op":"literal","dtype":"int32","value":0}
        }));
        assert!(project_batch(&b, &[("range".into(), zero_step)]).is_err());
    }

    #[test]
    fn array_grouped_emits_short_final_group() {
        let b = source(vec![Some(0)]);
        let int = Type::Primitive(PrimitiveType::Int32);
        let array = Expr::Array {
            element_type: int.clone(),
            elements: (0..5)
                .map(|i| Expr::Literal {
                    dtype: int.clone(),
                    value: json!(i),
                })
                .collect(),
        };
        let grouped = Expr::ArrayGrouped {
            value: Box::new(array),
            size: Box::new(Expr::Literal {
                dtype: int,
                value: json!(2),
            }),
        };
        let out = project_batch(&b, &[("groups".into(), grouped)]).unwrap();
        let groups = out.column(0).as_any().downcast_ref::<ListArray>().unwrap();
        let rows = groups
            .values()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let flat = rows.values().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(flat.values(), &[0, 1, 2, 3, 4]);
        assert_eq!(rows.value_length(0), 2);
        assert_eq!(rows.value_length(1), 2);
        assert_eq!(rows.value_length(2), 1);
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
    fn set_literals_use_typed_arrow_equality_without_order_encoding() {
        let set_type = Type::Set {
            set: Box::new(Type::Primitive(PrimitiveType::Int64)),
        };
        let empty = literal_array(&json!([]), &set_type, 1).unwrap();
        assert_eq!(empty.len(), 1);
        assert_eq!(empty.null_count(), 0);
        let values = literal_array(&json!([1, null, 1, 2]), &set_type, 1).unwrap();
        let values = values.as_any().downcast_ref::<ListArray>().unwrap();
        let values = values.value(0);
        assert_eq!(values.len(), 3);
        assert_eq!(values.null_count(), 1);

        let nested_type = Type::Set {
            set: Box::new(Type::Struct {
                r#struct: vec![("x".into(), Type::Primitive(PrimitiveType::Int32))],
            }),
        };
        let nested = literal_array(&json!([{"x": 1}, {"x": 1}]), &nested_type, 1).unwrap();
        let nested = nested.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(nested.value(0).len(), 1);

        assert!(set_type.ensure_keyable().is_err());
        let dict_type = Type::Dict {
            dict: Box::new((
                Type::Primitive(PrimitiveType::Int32),
                Type::Primitive(PrimitiveType::Str),
            )),
        };
        assert!(dict_type.ensure_keyable().is_err());
        let ndarray_type = Type::Ndarray {
            ndarray: Box::new((Type::Primitive(PrimitiveType::Int32), 1)),
        };
        assert!(!ndarray_type.native_key());
        assert!(ndarray_type.ensure_keyable().is_err());
        let ndarray_struct = Type::Struct {
            r#struct: vec![("x".into(), ndarray_type)],
        };
        assert!(ndarray_struct.ensure_keyable().is_err());
        let empty_struct = Type::Struct { r#struct: vec![] };
        assert!(empty_struct.ensure_keyable().is_ok());
        assert!(empty_struct.native_key());
    }

    #[test]
    fn interval_types_literals_construction_and_field_projection() {
        let interval_type = Type::Interval {
            interval: Box::new(Type::Primitive(PrimitiveType::Int32)),
        };
        assert_eq!(
            Type::from_arrow(&interval_type.to_arrow().unwrap()).unwrap(),
            interval_type
        );
        let literal = json!({"start": 3, "end": 9, "includes_start": true, "includes_end": false});
        let array = literal_array(&literal, &interval_type, 1).unwrap();
        assert_eq!(array.data_type(), &interval_type.to_arrow().unwrap());
        let batch = source(vec![Some(4), Some(11), None]);
        let interval_expr = json!({
            "op":"interval",
            "start":{"op":"literal","dtype":"int32","value":2},
            "end":{"op":"column","name":"x"},
            "includes_start":{"op":"literal","dtype":"bool","value":true},
            "includes_end":{"op":"literal","dtype":"bool","value":false}
        });
        let projected = project_batch(&batch, &[
            ("range".into(), expr(interval_expr)),
            ("end".into(), expr(json!({"op":"get_field","value":{"op":"interval","start":{"op":"literal","dtype":"int32","value":2},"end":{"op":"column","name":"x"},"includes_start":{"op":"literal","dtype":"bool","value":true},"includes_end":{"op":"literal","dtype":"bool","value":false}},"name":"end"}))),
        ]).unwrap();
        assert_eq!(
            Type::from_arrow(projected.schema().field(0).data_type()).unwrap(),
            interval_type
        );
        assert_eq!(
            values(projected.column(1)),
            vec![
                ScalarValue::Int32(Some(4)),
                ScalarValue::Int32(Some(11)),
                ScalarValue::Int32(None)
            ]
        );
        let array_type = Type::Array {
            array: Box::new(interval_type.clone()),
        };
        assert_eq!(
            Type::from_arrow(&array_type.to_arrow().unwrap()).unwrap(),
            array_type
        );
    }

    #[test]
    fn interval_contains_and_overlaps_respect_bounds_and_missingness() {
        let batch = source(vec![
            Some(1),
            Some(2),
            Some(4),
            Some(5),
            Some(8),
            Some(11),
            None,
        ]);
        let make_interval = |start: Value, end: Value, includes_start: bool, includes_end: bool| {
            json!({
                "op":"interval",
                "start":{"op":"literal","dtype":"int32","value":start},
                "end":{"op":"literal","dtype":"int32","value":end},
                "includes_start":{"op":"literal","dtype":"bool","value":includes_start},
                "includes_end":{"op":"literal","dtype":"bool","value":includes_end}
            })
        };
        let open = make_interval(json!(2), json!(5), true, false);
        let closed = make_interval(json!(2), json!(5), true, true);
        let right = make_interval(json!(5), json!(9), true, false);
        let contains =
            json!({"op":"interval_contains","interval":open,"point":{"op":"column","name":"x"}});
        let abutting_open = json!({"op":"interval_overlaps","left":make_interval(json!(2),json!(5),true,false),"right":right});
        let abutting_closed = json!({"op":"interval_overlaps","left":closed,"right":make_interval(json!(5),json!(9),true,false)});
        let output = project_batch(
            &batch,
            &[
                ("contains".into(), expr(contains)),
                ("abutting_open".into(), expr(abutting_open)),
                ("abutting_closed".into(), expr(abutting_closed)),
            ],
        )
        .unwrap();
        assert_eq!(
            values(output.column(0)),
            vec![
                ScalarValue::Boolean(Some(false)),
                ScalarValue::Boolean(Some(true)),
                ScalarValue::Boolean(Some(true)),
                ScalarValue::Boolean(Some(false)),
                ScalarValue::Boolean(Some(false)),
                ScalarValue::Boolean(Some(false)),
                ScalarValue::Boolean(None),
            ]
        );
        assert_eq!(
            values(output.column(1)),
            vec![ScalarValue::Boolean(Some(false)); 7]
        );
        assert_eq!(
            values(output.column(2)),
            vec![ScalarValue::Boolean(Some(true)); 7]
        );
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
    fn float_nan_wire_preserves_bits_and_rejects_malformed_tags() {
        for (dtype, bits, expected) in [
            (
                "float32",
                0x7f80_0001u64,
                ScalarValue::Float32(Some(f32::from_bits(0x7f80_0001))),
            ),
            (
                "float32",
                0xffc0_1234u64,
                ScalarValue::Float32(Some(f32::from_bits(0xffc0_1234))),
            ),
            (
                "float64",
                0x7ff0_0000_0000_0001u64,
                ScalarValue::Float64(Some(f64::from_bits(0x7ff0_0000_0000_0001))),
            ),
            (
                "float64",
                0xfff8_0000_0000_1234u64,
                ScalarValue::Float64(Some(f64::from_bits(0xfff8_0000_0000_1234))),
            ),
        ] {
            let (tag, actual_bits) = if dtype == "float32" {
                (
                    json!({"$float":"nan", "bits": format!("{:08x}", bits as u32)}),
                    bits as u32 as u64,
                )
            } else {
                (
                    json!({"$float":"nan", "bits": format!("{:016x}", bits)}),
                    bits,
                )
            };
            let field = expr(json!({"op":"literal", "dtype":dtype, "value":tag}));
            let output = project_batch(&source(vec![Some(1)]), &[("value".into(), field)]).unwrap();
            match (expected, values(output.column(0))[0].clone()) {
                (ScalarValue::Float32(Some(_)), ScalarValue::Float32(Some(value))) => {
                    assert_eq!(value.to_bits() as u64, actual_bits)
                }
                (ScalarValue::Float64(Some(_)), ScalarValue::Float64(Some(value))) => {
                    assert_eq!(value.to_bits(), actual_bits)
                }
                other => panic!("unexpected float output: {other:?}"),
            }
        }

        for tag in [
            json!({"$float":"nan", "bits":"7ff"}),
            json!({"$float":"nan", "bits":"7ff0000000000000"}),
            json!({"$float":"nan", "bits":"7ff8000000000001", "extra":true}),
        ] {
            let bad = expr(json!({"op":"literal", "dtype":"float64", "value":tag}));
            assert!(
                validate_projection(&source(vec![Some(1)]).schema(), &[("value".into(), bad)])
                    .is_err()
            );
        }
    }

    #[test]
    fn flatten_skips_hidden_values_under_null_inner_lists() {
        // Arrow permits nonempty value spans under null lists. Those values
        // must not appear when Hail skips a missing callback result.
        let inner = ListArray::new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            OffsetBuffer::new(vec![0_i32, 1, 3, 4].into()),
            Arc::new(Int32Array::from(vec![Some(1), Some(99), Some(98), None])),
            Some(NullBuffer::from(vec![true, false, true])),
        );
        let outer = ListArray::new(
            Arc::new(Field::new("item", inner.data_type().clone(), true)),
            OffsetBuffer::new(vec![0_i32, 1, 3].into()),
            Arc::new(inner),
            None,
        );
        // Also exercise a nonzero outer offset, as can occur in DF batches.
        let outer = outer.slice(1, 1);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "a",
            outer.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(outer)]).unwrap();
        let expression = Expr::ArrayFlatten {
            value: Box::new(Expr::Column { name: "a".into() }),
        };
        let result = lower(&expression, &schema)
            .unwrap()
            .evaluate(&batch)
            .unwrap()
            .into_array(1)
            .unwrap();
        let list = result.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(list.value_length(0), 1);
        assert!(list.value(0).is_null(0));
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
