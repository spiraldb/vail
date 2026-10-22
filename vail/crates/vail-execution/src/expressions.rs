//! Typed, source-bound scalar expressions for in-memory Table projections.
//!
//! Evaluation uses DataFusion physical expressions for column references,
//! casts, comparisons, and Kleene Boolean operators. Hail's distinct integer
//! overflow, floor division, modulo, and missing-condition semantics are kept
//! explicit at this boundary.

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::{Arc, Mutex, OnceLock};

use crate::reference::{PreparedReference, ReferenceDefinition};
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
use datafusion_expr::{ColumnarValue, Operator, Volatility};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::ScalarFunctionExpr;
use datafusion_physical_expr::expressions::{
    CaseExpr, Column, IsNullExpr, Literal, NegativeExpr, NotExpr, binary,
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
    /// Casts preserve logical shape and parameters. Primitive conversions and
    /// recursively numeric array conversions use Arrow's checked cast kernels;
    /// Call construction is the sole explicit structural conversion.
    pub fn ensure_castable_to(&self, target: &Type) -> Result<(), ExprError> {
        if self == target {
            return Ok(());
        }
        let primitive = |ty: &Type| matches!(ty, Type::Primitive(p) if *p != PrimitiveType::Call);
        let compatible = if primitive(self) && primitive(target) {
            arrow_cast::can_cast_types(&self.to_arrow()?, &target.to_arrow()?)
        } else if let (Type::Array { array: left }, Type::Array { array: right }) = (self, target) {
            left.ensure_castable_to(right).is_ok()
                && arrow_cast::can_cast_types(&self.to_arrow()?, &target.to_arrow()?)
        } else if let (Type::Struct { r#struct: fields }, Type::Primitive(PrimitiveType::Call)) =
            (self, target)
        {
            fields.len() == 2
                && fields.iter().any(|(name, ty)| name == "__vail_call_alleles"
                    && matches!(ty, Type::Array { array } if **array == Type::Primitive(PrimitiveType::Int32)))
                && fields.iter().any(|(name, ty)| name == "__vail_call_phased"
                    && *ty == Type::Primitive(PrimitiveType::Bool))
        } else {
            false
        };
        if compatible {
            Ok(())
        } else {
            Err(ExprError::Invalid(format!(
                "cannot cast {self:?} to {target:?}"
            )))
        }
    }

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
            Self::Primitive(PrimitiveType::Call) => crate::calls::storage_type(),
            Self::Tuple { .. } | Self::Interval { .. } => {
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
            vail_ordering::tag_type(dtype, &logical).map_err(|e| ExprError::Invalid(e.to_string()))
        } else {
            Ok(dtype)
        }
    }

    pub fn from_arrow(dtype: &DataType) -> Result<Self, ExprError> {
        if let Some(logical) =
            vail_ordering::type_metadata(dtype).map_err(|e| ExprError::Invalid(e.to_string()))?
        {
            let ty: Self = serde_json::from_value(logical)
                .map_err(|e| ExprError::Invalid(format!("invalid Arrow logical type: {e}")))?;
            if ty != Self::Primitive(PrimitiveType::Call) {
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
            }
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
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                Self::Primitive(PrimitiveType::Str)
            }
            DataType::Struct(fields) => {
                let mut names = HashSet::with_capacity(fields.len());
                if fields.iter().any(|field| !names.insert(field.name())) {
                    return Err(ExprError::Invalid(
                        "duplicate struct field in Arrow input".into(),
                    ));
                }
                Self::Struct {
                    r#struct: fields
                        .iter()
                        .map(|f| Ok((f.name().clone(), Self::from_arrow(f.data_type())?)))
                        .collect::<Result<_, ExprError>>()?,
                }
            }
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Expr {
    /// Frontend scopes are native nodes and must be resolved before lowering.
    MatrixField {
        axis: String,
        name: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        dtype: Option<Type>,
    },
    MatrixScope {
        axis: String,
        value: Arc<Expr>,
    },
    MatrixNumCols {},
    MatrixColumnPosition {},
    TableLookup {
        reference: String,
        keys: Vec<Expr>,
        all_matches: bool,
        query_interval: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        dtype: Option<Type>,
    },
    /// Native Arrow scalar retained directly; never deserialized as an expression.
    #[serde(skip)]
    RuntimeLiteral {
        value: ScalarValue,
    },
    Aggregate {
        function: String,
        value: Option<Arc<Expr>>,
        #[serde(default)]
        options: Value,
        #[serde(default)]
        result_type: Option<Type>,
        #[serde(default)]
        init: Vec<(String, Expr)>,
    },
    AggregateFold {
        initial: Arc<Expr>,
        seq: Arc<Expr>,
        combine: Arc<Expr>,
        state_parameter: String,
        left_parameter: String,
        right_parameter: String,
    },
    AggGroupBy {
        key: Arc<Expr>,
        body: Arc<Expr>,
    },
    AggArray {
        value: Arc<Expr>,
        parameter: String,
        body: Arc<Expr>,
    },
    AggFilter {
        predicate: Arc<Expr>,
        body: Arc<Expr>,
    },
    AggExplode {
        value: Arc<Expr>,
        parameter: String,
        body: Arc<Expr>,
    },
    LambdaVariable {
        name: String,
        /// Construction-time typing may use this annotation; execution still
        /// requires a real lexical binder with the same type.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        dtype: Option<Type>,
    },
    /// Built-in DataFusion collection function; no Vail evaluation kernel.
    NestedFunction {
        function: String,
        args: Vec<Expr>,
        dtype: Type,
    },
    /// Change logical collection tags without changing the element representation.
    CollectionCast {
        value: Arc<Expr>,
        dtype: Type,
    },
    IntegerBinary {
        operator: BinaryOperator,
        left: Arc<Expr>,
        right: Arc<Expr>,
    },
    // Keep the original wire spelling while sharing the scalar UDF registry.
    #[serde(alias = "scalar_function")]
    StringFunction {
        function: String,
        args: Vec<Expr>,
    },
    Tuple {
        elements: Vec<Expr>,
    },
    /// One row's entry-list map evaluated against aligned row, entry, and
    /// column inputs. Unlike ArrayMap this node owns the outer matrix row
    /// loop and keeps the shared column arrays out of each entry's data.
    AlignedMatrixMap {
        entries: Arc<Expr>,
        columns: Arc<Expr>,
        rows: Arc<Expr>,
        globals: Arc<Expr>,
        body: Arc<Expr>,
        dtype: Type,
        #[serde(default)]
        preserve_cell: bool,
    },
    /// A placeholder used only by an AlignedMatrixMap body. Its immutable
    /// physical counterpart is rebound once per outer row to a scalar or a
    /// borrowed Arrow child array.
    AlignedMapInput {
        axis: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        dtype: Type,
    },
    ArrayMap {
        value: Arc<Expr>,
        parameter: String,
        body: Arc<Expr>,
    },
    /// Exclusive scans backed by the ordinary aggregate accumulator.
    ArrayAggregateScan {
        function: String,
        value: Arc<Expr>,
        predicate: Arc<Expr>,
        options: Value,
        dtype: Type,
    },
    ArrayFold {
        value: Arc<Expr>,
        zero: Arc<Expr>,
        accumulator: String,
        parameter: String,
        body: Arc<Expr>,
        scan: bool,
    },
    ArrayPredicate {
        value: Arc<Expr>,
        parameter: String,
        predicate: Arc<Expr>,
        first: bool,
    },
    ArrayFilter {
        value: Arc<Expr>,
        parameter: String,
        predicate: Arc<Expr>,
    },
    StreamRange {
        start: Arc<Expr>,
        stop: Arc<Expr>,
        step: Arc<Expr>,
    },
    ArrayGrouped {
        value: Arc<Expr>,
        size: Arc<Expr>,
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
        left: Arc<Expr>,
        right: Arc<Expr>,
    },
    Unary {
        operator: UnaryOperator,
        value: Arc<Expr>,
    },
    Cast {
        dtype: Type,
        value: Arc<Expr>,
    },
    ToString {
        value: Arc<Expr>,
    },
    GetField {
        value: Arc<Expr>,
        name: String,
    },
    Interval {
        start: Arc<Expr>,
        end: Arc<Expr>,
        includes_start: Arc<Expr>,
        includes_end: Arc<Expr>,
    },
    IntervalContains {
        interval: Arc<Expr>,
        point: Arc<Expr>,
    },
    IntervalOverlaps {
        left: Arc<Expr>,
        right: Arc<Expr>,
    },
    Struct {
        fields: Vec<(String, Expr)>,
    },
    /// Bind the selector once; Boolean conditions and result expressions use
    /// the lexical parameter inside ordered, lazy branches.
    Switch {
        value: Arc<Expr>,
        parameter: String,
        cases: Vec<(Expr, Expr)>,
        default: Arc<Expr>,
        missing: Option<Arc<Expr>>,
    },
    IfElse {
        condition: Arc<Expr>,
        consequent: Arc<Expr>,
        alternate: Arc<Expr>,
    },
    Die {
        message: Arc<Expr>,
        dtype: Type,
    },
    IsMissing {
        value: Arc<Expr>,
    },
    Array {
        element_type: Type,
        elements: Vec<Expr>,
    },
    ArrayFlatten {
        value: Arc<Expr>,
    },
    ArrayLength {
        value: Arc<Expr>,
    },
    ArrayIndex {
        value: Arc<Expr>,
        index: Arc<Expr>,
    },
    ArraySlice {
        value: Arc<Expr>,
        start: Option<Arc<Expr>>,
        stop: Option<Arc<Expr>>,
        step: Option<Arc<Expr>>,
    },
    ArrayReduce {
        operation: ArrayReduction,
        filter_missing: bool,
        value: Arc<Expr>,
    },
    Locus {
        reference: ReferenceDefinition,
        contig: Arc<Expr>,
        position: Arc<Expr>,
    },
    LocusOp {
        operation: String,
        value: Arc<Expr>,
    },
    LocusValid {
        reference: ReferenceDefinition,
        contig: Arc<Expr>,
        position: Arc<Expr>,
    },
    LocusFromGlobalPosition {
        reference: ReferenceDefinition,
        position: Arc<Expr>,
    },
    ParseLocus {
        reference: ReferenceDefinition,
        value: Arc<Expr>,
    },
    ParseCsvLine {
        value: Arc<Expr>,
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
/// A compilation request owns all temporary lowering IR and memo tables. Native
/// addresses are safe keys while the caller's root and this arena remain alive.
/// Binding contexts include actual Arrow metadata and lexical field positions;
/// neither node identity alone nor a process-global cache can describe typing.
#[derive(Default)]
struct CompilationMemo {
    contexts: HashMap<CompilationBindings, usize>,
    types: HashMap<(usize, usize), Type>,
    physical: HashMap<(usize, usize), Arc<dyn PhysicalExpr>>,
    physical_schemas: HashMap<usize, Arc<Schema>>,
    generated: Vec<Arc<Expr>>,
    scans: HashMap<*const Expr, bool>,
    root_expression: usize,
}
#[derive(Clone, PartialEq, Eq, Hash)]
struct CompilationBindings {
    schema: Schema,
    globals: Schema,
    scope: Vec<(String, Type, usize)>,
}
impl CompilationMemo {
    fn context(
        &mut self,
        schema: &Schema,
        globals: &Schema,
        scope: &[(String, Type, usize)],
    ) -> usize {
        let descriptor = CompilationBindings {
            schema: schema.clone(),
            globals: globals.clone(),
            scope: scope.to_vec(),
        };
        let next = self.contexts.len();
        *self.contexts.entry(descriptor).or_insert(next)
    }
}

impl Expr {
    /// Visit typed expression occurrences, never literal payloads or types.
    pub fn visit(&self, f: &mut impl FnMut(&Expr)) {
        f(self);
        self.for_each_child(&mut |child| child.visit(f));
    }
    /// Visit each shared native node once. This traversal follows a DAG rather
    /// than expanding shared edges into an exponentially larger expression tree.
    pub fn visit_unique(&self, f: &mut impl FnMut(&Expr)) {
        fn walk(
            expr: &Expr,
            seen: &mut std::collections::HashSet<usize>,
            f: &mut impl FnMut(&Expr),
        ) {
            if !seen.insert(expr as *const Expr as usize) {
                return;
            }
            f(expr);
            expr.for_each_child(&mut |child| walk(child, seen, f));
        }
        walk(self, &mut std::collections::HashSet::new(), f);
    }
    /// Free lexical parameters, summarized once per native DAG node.
    pub fn free_parameters(&self) -> std::collections::HashSet<String> {
        fn summarize(
            node: &Expr,
            memo: &mut std::collections::HashMap<*const Expr, std::collections::HashSet<String>>,
        ) -> std::collections::HashSet<String> {
            let key = node as *const Expr;
            if let Some(names) = memo.get(&key) {
                return names.clone();
            }
            let mut names = std::collections::HashSet::new();
            if let Expr::LambdaVariable { name, .. } = node {
                names.insert(name.clone());
            }
            let mut position = 0;
            node.visit_children(&mut |child| {
                let mut child_names = summarize(child, memo);
                for binder in node.child_binders(position) {
                    child_names.remove(binder);
                }
                position += 1;
                names.extend(child_names);
            });
            memo.insert(key, names.clone());
            names
        }
        summarize(self, &mut std::collections::HashMap::new())
    }

    /// Whether this expression can be evaluated in an independent singleton
    /// global scope. Type hints do not establish lexical/source ownership.
    pub fn is_closed_scalar(&self) -> bool {
        if !self.free_parameters().is_empty() {
            return false;
        }
        let mut closed = true;
        self.visit_unique(&mut |node| {
            closed &= !matches!(
                node,
                Expr::Column { .. }
                    | Expr::TableLookup { .. }
                    | Expr::MatrixField { .. }
                    | Expr::MatrixScope { .. }
                    | Expr::MatrixNumCols { .. }
                    | Expr::MatrixColumnPosition { .. }
                    | Expr::AlignedMapInput { .. }
                    | Expr::AlignedMatrixMap { .. }
                    | Expr::Aggregate { .. }
                    | Expr::AggregateFold { .. }
                    | Expr::AggFilter { .. }
                    | Expr::AggExplode { .. }
                    | Expr::AggGroupBy { .. }
                    | Expr::AggArray { .. }
            );
        });
        closed
    }

    /// Names introduced for a child edge, in the same order as for_each_child.
    /// This is the shared lexical contract for native analyses and rewrites.
    pub fn child_binders(&self, position: usize) -> Vec<&str> {
        match self {
            Self::ArrayMap { parameter, .. }
            | Self::ArrayFilter { parameter, .. }
            | Self::ArrayPredicate { parameter, .. }
            | Self::AggArray { parameter, .. }
            | Self::AggExplode { parameter, .. }
                if position == 1 =>
            {
                vec![parameter]
            }
            Self::Switch { parameter, .. } if position > 0 => vec![parameter],
            Self::ArrayFold {
                accumulator,
                parameter,
                ..
            } if position == 2 => vec![accumulator, parameter],
            Self::AggregateFold {
                state_parameter, ..
            } if position == 1 => vec![state_parameter],
            Self::AggregateFold {
                left_parameter,
                right_parameter,
                ..
            } if position == 2 => vec![left_parameter, right_parameter],
            _ => vec![],
        }
    }

    /// Visit immediate native children, excluding literal buffers.
    pub fn visit_children<'a>(&'a self, f: &mut impl FnMut(&'a Expr)) {
        self.for_each_child(f);
    }
    /// Borrow immediate typed children in their semantic order.
    pub fn for_each_child<'a>(&'a self, f: &mut impl FnMut(&'a Expr)) {
        match self {
            Self::MatrixScope { value, .. } => f(value),
            Self::TableLookup { keys, .. } => {
                for key in keys {
                    f(key);
                }
            }
            Self::MatrixField { .. }
            | Self::MatrixNumCols { .. }
            | Self::MatrixColumnPosition { .. } => {}
            Self::Aggregate { value, init, .. } => {
                if let Some(value) = value {
                    f(value);
                }
                for (_, expr) in init {
                    f(expr);
                }
            }
            Self::AggregateFold {
                initial,
                seq,
                combine,
                ..
            } => {
                f(initial);
                f(seq);
                f(combine);
            }
            Self::AggGroupBy { key, body } => {
                f(key);
                f(body);
            }
            Self::AggArray { value, body, .. } => {
                f(value);
                f(body);
            }
            Self::AggFilter { predicate, body } => {
                f(predicate);
                f(body);
            }
            Self::AggExplode { value, body, .. } => {
                f(value);
                f(body);
            }
            Self::LambdaVariable { .. }
            | Self::AlignedMapInput { .. }
            | Self::ExternalGlobal { .. }
            | Self::Global { .. }
            | Self::Column { .. }
            | Self::Literal { .. }
            | Self::RuntimeLiteral { .. } => (),
            Self::Tuple { elements: args }
            | Self::StringFunction { args, .. }
            | Self::NestedFunction { args, .. } => {
                for arg in args {
                    f(arg);
                }
            }
            Self::ArrayMap { value, body, .. } => {
                f(value);
                f(body);
            }
            Self::AlignedMatrixMap {
                entries,
                columns,
                rows,
                globals: globals_expr,
                body,
                ..
            } => {
                f(entries);
                f(columns);
                f(rows);
                f(globals_expr);
                f(body);
            }
            Self::ArrayFold {
                value, zero, body, ..
            } => {
                f(value);
                f(zero);
                f(body);
            }
            Self::ArrayAggregateScan {
                value, predicate, ..
            } => {
                f(value);
                f(predicate);
            }
            Self::ArrayPredicate {
                value, predicate, ..
            }
            | Self::ArrayFilter {
                value, predicate, ..
            } => {
                f(value);
                f(predicate);
            }
            Self::StreamRange { start, stop, step } => {
                f(start);
                f(stop);
                f(step);
            }
            Self::ArrayGrouped { value, size } => {
                f(value);
                f(size);
            }
            Self::IntegerBinary { left, right, .. } | Self::Binary { left, right, .. } => {
                f(left);
                f(right);
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
            | Self::ArrayReduce { value, .. } => f(value),
            Self::Interval {
                start,
                end,
                includes_start,
                includes_end,
            } => {
                f(start);
                f(end);
                f(includes_start);
                f(includes_end);
            }
            Self::IntervalContains { interval, point } => {
                f(interval);
                f(point);
            }
            Self::IntervalOverlaps { left, right } => {
                f(left);
                f(right);
            }
            Self::Struct { fields } => {
                for (_, expr) in fields {
                    f(expr);
                }
            }
            Self::Switch {
                value,
                cases,
                default,
                missing,
                ..
            } => {
                f(value);
                for (condition, result) in cases {
                    f(condition);
                    f(result);
                }
                f(default);
                if let Some(missing) = missing {
                    f(missing);
                }
            }
            Self::IfElse {
                condition,
                consequent,
                alternate,
            } => {
                f(condition);
                f(consequent);
                f(alternate);
            }
            Self::Array { elements, .. } => {
                for expr in elements {
                    f(expr);
                }
            }
            Self::ArrayIndex { value, index } => {
                f(value);
                f(index);
            }
            Self::Locus {
                contig, position, ..
            } => {
                f(contig);
                f(position);
            }
            Self::LocusOp { value, .. }
            | Self::ParseLocus { value, .. }
            | Self::ParseCsvLine { value, .. } => f(value),
            Self::LocusValid {
                contig, position, ..
            } => {
                f(contig);
                f(position);
            }
            Self::LocusFromGlobalPosition { position, .. } => f(position),
            Self::ArraySlice {
                value,
                start,
                stop,
                step,
            } => {
                f(value);
                for expr in [start, stop, step].into_iter().flatten() {
                    f(expr);
                }
            }
        }
    }

    /// Map original borrowed children, retaining stable identity for DAG rewrites.
    pub fn try_map_original_children<E>(
        &self,
        mut f: impl FnMut(&Expr) -> Result<Expr, E>,
    ) -> Result<Expr, E> {
        let mut children = Vec::new();
        self.visit_children(&mut |child| children.push(child));
        let mut children = children.into_iter();
        self.try_map_children(|_| f(children.next().expect("typed child traversal order")))
    }

    /// Rewrite immediate typed children, excluding literal payloads and types.
    pub fn try_map_children<E>(
        &self,
        mut f: impl FnMut(&Expr) -> Result<Expr, E>,
    ) -> Result<Expr, E> {
        let mut result = self.clone();
        match &mut result {
            Self::MatrixScope { value, .. } => *value = Arc::new(f(value)?),
            Self::TableLookup { keys, .. } => {
                for key in keys {
                    *key = f(key)?;
                }
            }
            Self::MatrixField { .. }
            | Self::MatrixNumCols { .. }
            | Self::MatrixColumnPosition { .. } => {}
            Self::AlignedMapInput { .. } => {}
            Self::AlignedMatrixMap {
                entries,
                columns,
                rows,
                globals: globals_expr,
                body,
                ..
            } => {
                *entries = Arc::new(f(entries)?);
                *columns = Arc::new(f(columns)?);
                *rows = Arc::new(f(rows)?);
                *globals_expr = Arc::new(f(globals_expr)?);
                *body = Arc::new(f(body)?);
            }
            Self::Aggregate { value, init, .. } => {
                if let Some(value) = value {
                    *value = Arc::new(f(value)?);
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
                *initial = Arc::new(f(initial)?);
                *seq = Arc::new(f(seq)?);
                *combine = Arc::new(f(combine)?);
            }
            Self::AggGroupBy { key, body } => {
                *key = Arc::new(f(key)?);
                *body = Arc::new(f(body)?);
            }
            Self::AggArray { value, body, .. } => {
                *value = Arc::new(f(value)?);
                *body = Arc::new(f(body)?);
            }
            Self::AggFilter { predicate, body } => {
                *predicate = Arc::new(f(predicate)?);
                *body = Arc::new(f(body)?);
            }
            Self::Tuple { elements: args }
            | Self::StringFunction { args, .. }
            | Self::NestedFunction { args, .. } => {
                for arg in args {
                    *arg = f(arg)?;
                }
            }
            Self::AggExplode { value, body, .. } | Self::ArrayMap { value, body, .. } => {
                *value = Arc::new(f(value)?);
                *body = Arc::new(f(body)?);
            }
            Self::ArrayFold {
                value, zero, body, ..
            } => {
                *value = Arc::new(f(value)?);
                *zero = Arc::new(f(zero)?);
                *body = Arc::new(f(body)?);
            }
            Self::ArrayAggregateScan {
                value, predicate, ..
            } => {
                *value = Arc::new(f(value)?);
                *predicate = Arc::new(f(predicate)?);
            }
            Self::ArrayPredicate {
                value, predicate, ..
            }
            | Self::ArrayFilter {
                value, predicate, ..
            } => {
                *value = Arc::new(f(value)?);
                *predicate = Arc::new(f(predicate)?);
            }
            Self::StreamRange { start, stop, step } => {
                *start = Arc::new(f(start)?);
                *stop = Arc::new(f(stop)?);
                *step = Arc::new(f(step)?);
            }
            Self::ArrayGrouped { value, size } => {
                *value = Arc::new(f(value)?);
                *size = Arc::new(f(size)?);
            }
            Self::IntegerBinary { left, right, .. } | Self::Binary { left, right, .. } => {
                *left = Arc::new(f(left)?);
                *right = Arc::new(f(right)?);
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
            | Self::ArrayReduce { value, .. } => *value = Arc::new(f(value)?),
            Self::Interval {
                start,
                end,
                includes_start,
                includes_end,
            } => {
                *start = Arc::new(f(start)?);
                *end = Arc::new(f(end)?);
                *includes_start = Arc::new(f(includes_start)?);
                *includes_end = Arc::new(f(includes_end)?);
            }
            Self::IntervalContains { interval, point } => {
                *interval = Arc::new(f(interval)?);
                *point = Arc::new(f(point)?);
            }
            Self::IntervalOverlaps { left, right } => {
                *left = Arc::new(f(left)?);
                *right = Arc::new(f(right)?);
            }
            Self::Struct { fields } => {
                for (_, expr) in fields {
                    *expr = f(expr)?;
                }
            }
            Self::Switch {
                value,
                cases,
                default,
                missing,
                ..
            } => {
                *value = Arc::new(f(value)?);
                for (condition, result) in cases {
                    *condition = f(condition)?;
                    *result = f(result)?;
                }
                *default = Arc::new(f(default)?);
                if let Some(missing) = missing {
                    *missing = Arc::new(f(missing)?);
                }
            }
            Self::IfElse {
                condition,
                consequent,
                alternate,
            } => {
                *condition = Arc::new(f(condition)?);
                *consequent = Arc::new(f(consequent)?);
                *alternate = Arc::new(f(alternate)?);
            }
            Self::Array { elements, .. } => {
                for expr in elements {
                    *expr = f(expr)?;
                }
            }
            Self::ArrayIndex { value, index } => {
                *value = Arc::new(f(value)?);
                *index = Arc::new(f(index)?);
            }
            Self::Locus {
                contig, position, ..
            } => {
                *contig = Arc::new(f(contig)?);
                *position = Arc::new(f(position)?);
            }
            Self::LocusOp { value, .. }
            | Self::ParseLocus { value, .. }
            | Self::ParseCsvLine { value, .. } => *value = Arc::new(f(value)?),
            Self::LocusValid {
                contig, position, ..
            } => {
                *contig = Arc::new(f(contig)?);
                *position = Arc::new(f(position)?);
            }
            Self::LocusFromGlobalPosition { position, .. } => *position = Arc::new(f(position)?),
            Self::ArraySlice {
                value,
                start,
                stop,
                step,
            } => {
                *value = Arc::new(f(value)?);
                for expr in [start, stop, step].into_iter().flatten() {
                    *expr = Arc::new(f(expr)?);
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
        self.scan_presence(&mut std::collections::HashMap::new())
    }
    fn scan_presence(&self, memo: &mut std::collections::HashMap<*const Expr, bool>) -> bool {
        let mut pending = vec![(self, false)];
        while let Some((node, expanded)) = pending.pop() {
            let key = node as *const Expr;
            if memo.contains_key(&key) {
                continue;
            }
            if matches!(node,Self::Aggregate { options,.. } if options.get("__vail_scan").and_then(Value::as_bool)==Some(true))
            {
                memo.insert(key, true);
            } else if expanded {
                let mut found = false;
                node.for_each_child(&mut |child| found |= memo[&(child as *const Expr)]);
                memo.insert(key, found);
            } else {
                pending.push((node, true));
                node.for_each_child(&mut |child| pending.push((child, false)));
            }
        }
        memo[&(self as *const Expr)]
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
        self.dtype_in_scope(schema, globals, &scope, &mut CompilationMemo::default())
    }

    pub fn dtype(&self, schema: &Schema) -> Result<Type, ExprError> {
        self.dtype_with_globals(schema, &Schema::empty())
    }

    pub fn dtype_with_globals(&self, schema: &Schema, globals: &Schema) -> Result<Type, ExprError> {
        self.dtype_in_scope(schema, globals, &[], &mut CompilationMemo::default())
    }

    fn dtype_in_scope(
        &self,
        schema: &Schema,
        globals: &Schema,
        scope: &[(String, Type, usize)],
        memo: &mut CompilationMemo,
    ) -> Result<Type, ExprError> {
        let context = memo.context(schema, globals, scope);
        self.dtype_scoped(schema, globals, scope, memo, context)
    }
    fn dtype_scoped(
        &self,
        schema: &Schema,
        globals: &Schema,
        scope: &[(String, Type, usize)],
        memo: &mut CompilationMemo,
        context: usize,
    ) -> Result<Type, ExprError> {
        let key = (context, self as *const Expr as usize);
        if let Some(dtype) = memo.types.get(&key) {
            return Ok(dtype.clone());
        }
        let dtype = stacker::maybe_grow(512 * 1024, 2 * 1024 * 1024, || {
            self.dtype_uncached(schema, globals, scope, memo, context)
        })?;
        memo.types.insert(key, dtype.clone());
        Ok(dtype)
    }

    fn dtype_uncached(
        &self,
        schema: &Schema,
        globals: &Schema,
        scope: &[(String, Type, usize)],
        memo: &mut CompilationMemo,
        context: usize,
    ) -> Result<Type, ExprError> {
        match self {
            Self::AlignedMapInput { dtype, .. } => Ok(dtype.clone()),
            Self::AlignedMatrixMap {
                entries,
                columns,
                rows,
                globals: globals_expr,
                body,
                dtype,
                preserve_cell,
            } => {
                let Type::Array { .. } =
                    entries.dtype_scoped(schema, globals, scope, memo, context)?
                else {
                    return Err(ExprError::Invalid(
                        "aligned matrix map entries must be an array".into(),
                    ));
                };
                let Type::Array { .. } =
                    columns.dtype_scoped(schema, globals, scope, memo, context)?
                else {
                    return Err(ExprError::Invalid(
                        "aligned matrix map columns must be an array".into(),
                    ));
                };
                globals_expr.dtype_scoped(schema, globals, scope, memo, context)?;
                let Type::Struct { .. } =
                    rows.dtype_scoped(schema, globals, scope, memo, context)?
                else {
                    return Err(ExprError::Invalid(
                        "aligned matrix map rows must be a struct".into(),
                    ));
                };
                let body_type = body.dtype_scoped(schema, globals, scope, memo, context)?;
                let Type::Array {
                    array: result_element,
                } = dtype
                else {
                    return Err(ExprError::Invalid(
                        "aligned matrix map result must be an array".into(),
                    ));
                };
                if *preserve_cell {
                    let Type::Struct { r#struct: fields } = result_element.as_ref() else {
                        return Err(ExprError::Invalid(
                            "aligned matrix map cell result must be a struct".into(),
                        ));
                    };
                    if fields
                        .iter()
                        .find(|(name, _)| name == "value")
                        .map(|(_, ty)| ty)
                        != Some(&body_type)
                    {
                        return Err(ExprError::Invalid(
                            "aligned matrix map body type disagrees with value field".into(),
                        ));
                    }
                } else if result_element.as_ref() != &body_type {
                    return Err(ExprError::Invalid(
                        "aligned matrix map body type disagrees with result element".into(),
                    ));
                }
                Ok(dtype.clone())
            }
            Self::MatrixScope { value, .. } => {
                value.dtype_scoped(schema, globals, scope, memo, context)
            }
            Self::MatrixNumCols {} | Self::MatrixColumnPosition {} => {
                Ok(Type::Primitive(PrimitiveType::Int32))
            }
            Self::MatrixField { dtype, .. } | Self::TableLookup { dtype, .. } => dtype
                .clone()
                .ok_or_else(|| ExprError::Invalid("unresolved frontend expression type".into())),
            Self::Aggregate {
                function,
                value,
                options,
                result_type,
                init,
            } if options.get("__vail_scan").and_then(Value::as_bool) == Some(true) => {
                let inferred = if function == "__scan_fold" {
                    if !init.is_empty()
                        || options
                            .as_object()
                            .is_none_or(|o| o.keys().any(|k| k != "__vail_scan"))
                    {
                        return Err(ExprError::Invalid(
                            "invalid scan fold options/initializers".into(),
                        ));
                    }
                    let Some(Self::AggregateFold {
                        initial,
                        seq,
                        combine,
                        state_parameter,
                        left_parameter,
                        right_parameter,
                    }) = value.as_deref()
                    else {
                        return Err(ExprError::Invalid("scan fold requires fold IR".into()));
                    };
                    let state = initial.dtype_in_scope(&Schema::empty(), globals, &[], memo)?;
                    let mut sequence_scope = scope.to_vec();
                    sequence_scope.push((state_parameter.clone(), state.clone(), 0));
                    let combine_scope = vec![
                        (left_parameter.clone(), state.clone(), 0),
                        (right_parameter.clone(), state.clone(), 0),
                    ];
                    if seq.dtype_in_scope(schema, globals, &sequence_scope, memo)? != state
                        || combine.dtype_in_scope(
                            &Schema::empty(),
                            globals,
                            &combine_scope,
                            memo,
                        )? != state
                    {
                        return Err(ExprError::Invalid(
                            "scan fold initial/update/combine types must agree".into(),
                        ));
                    }
                    state
                } else {
                    let input = value
                        .as_ref()
                        .map(|v| v.dtype_in_scope(schema, globals, scope, memo))
                        .transpose()?;
                    let input =
                        if options.get("__vail_exploded").and_then(Value::as_bool) == Some(true) {
                            match input {
                                Some(Type::Array { array }) => Some(*array),
                                _ => {
                                    return Err(ExprError::Invalid(
                                        "exploded scan requires array input".into(),
                                    ));
                                }
                            }
                        } else {
                            input
                        };
                    // Initializers are closed over globals, never row/scan state.
                    let initializers = init
                        .iter()
                        .map(|(name, expr)| {
                            Ok((
                                name.clone(),
                                expr.dtype_in_scope(&Schema::empty(), globals, &[], memo)?,
                            ))
                        })
                        .collect::<Result<Vec<_>, ExprError>>()?;
                    crate::aggregate_contract::result_type(
                        function,
                        input.as_ref(),
                        options,
                        &initializers,
                    )?
                };
                if result_type
                    .as_ref()
                    .is_some_and(|annotation| *annotation != inferred)
                {
                    return Err(ExprError::Invalid(
                        "scan result type annotation disagrees with aggregate result".into(),
                    ));
                }
                Ok(inferred)
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
                let element = match value.dtype_scoped(schema, globals, scope, memo, context)? {
                    Type::Array { array } | Type::Set { set: array } => *array,
                    _ => {
                        return Err(ExprError::Invalid(
                            "scan collection scope requires array or set".into(),
                        ));
                    }
                };
                let mut nested = scope.to_vec();
                nested.push((parameter.clone(), element, 0));
                let result = body.dtype_in_scope(schema, globals, &nested, memo)?;
                if matches!(self, Self::AggArray { .. }) {
                    Ok(Type::Array {
                        array: Box::new(result),
                    })
                } else {
                    Ok(result)
                }
            }
            Self::AggGroupBy { key, body } if body.scan_presence(&mut memo.scans) => {
                Ok(Type::Dict {
                    dict: Box::new((
                        key.dtype_scoped(schema, globals, scope, memo, context)?,
                        body.dtype_scoped(schema, globals, scope, memo, context)?,
                    )),
                })
            }
            Self::Aggregate { .. }
            | Self::AggExplode { .. }
            | Self::AggGroupBy { .. }
            | Self::AggArray { .. }
            | Self::AggregateFold { .. } => Err(ExprError::Invalid(
                "aggregation expressions require Table.aggregate and cannot be nested".into(),
            )),
            Self::AggFilter { predicate, body } => {
                let mut scan_filter = false;
                body.visit_unique(&mut |node| {
                    scan_filter |= matches!(node, Self::Aggregate { options, .. }
                        if options.get("__vail_scan").and_then(Value::as_bool) == Some(true));
                });
                if scan_filter {
                    if predicate.dtype_scoped(schema, globals, scope, memo, context)?
                        != Type::Primitive(PrimitiveType::Bool)
                    {
                        return Err(ExprError::Invalid(
                            "scan.filter predicate must be Boolean".into(),
                        ));
                    }
                    body.dtype_scoped(schema, globals, scope, memo, context)
                } else {
                    Err(ExprError::Invalid(
                        "aggregation expressions require Table.aggregate and cannot be nested"
                            .into(),
                    ))
                }
            }
            Self::LambdaVariable { name, dtype } => {
                let bound = scope
                    .iter()
                    .rev()
                    .find(|(n, _, _)| n == name)
                    .map(|(_, ty, _)| ty);
                match (bound, dtype) {
                    (Some(bound), Some(declared)) if bound != declared => Err(ExprError::Invalid(
                        format!("lambda variable {name:?} type disagrees with binder"),
                    )),
                    (Some(bound), _) => Ok(bound.clone()),
                    (None, Some(declared)) => Ok(declared.clone()),
                    (None, None) => Err(ExprError::Invalid(format!(
                        "unbound lambda variable {name:?} in native type checking"
                    ))),
                }
            }
            Self::IntegerBinary {
                operator,
                left,
                right,
            } => {
                let int64 = Type::Primitive(PrimitiveType::Int64);
                if left.dtype_scoped(schema, globals, scope, memo, context)? != int64
                    || right.dtype_scoped(schema, globals, scope, memo, context)? != int64
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
                    if arg.dtype_scoped(schema, globals, scope, memo, context)? != expected {
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
                    .map(|element| element.dtype_scoped(schema, globals, scope, memo, context))
                    .collect::<Result<Vec<_>, ExprError>>()?,
            }),
            Self::ArrayAggregateScan {
                function,
                value,
                predicate,
                options,
                dtype,
            } => {
                let input = value
                    .dtype_scoped(schema, globals, scope, memo, context)?
                    .to_arrow()?;
                if predicate.dtype_scoped(schema, globals, scope, memo, context)?
                    != (Type::Array {
                        array: Box::new(Type::Primitive(PrimitiveType::Bool)),
                    })
                {
                    return Err(ExprError::Invalid(
                        "aggregate scan predicate requires array<bool>".into(),
                    ));
                }
                let udf = crate::array_aggregate_scan::make_udf(function, &input, options)?;
                let actual = udf.return_type(&[
                    input,
                    predicate
                        .dtype_scoped(schema, globals, scope, memo, context)?
                        .to_arrow()?,
                ])?;
                if actual != dtype.to_arrow()? {
                    return Err(ExprError::Invalid(
                        "aggregate scan result type mismatch".into(),
                    ));
                }
                Ok(dtype.clone())
            }
            Self::NestedFunction {
                function,
                args,
                dtype,
            } => {
                if function == "__vail_parse_json"
                    || matches!(
                        function.as_str(),
                        "__vail_export_annotation" | "__vail_json_annotation"
                    )
                {
                    if args.len() != 1 {
                        return Err(ExprError::Invalid(
                            "typed JSON/text kernel requires one argument".into(),
                        ));
                    }
                    let input = args[0].dtype_scoped(schema, globals, scope, memo, context)?;
                    if function == "__vail_parse_json"
                        && input != Type::Primitive(PrimitiveType::Str)
                    {
                        return Err(ExprError::Invalid(
                            "parse_json requires string input".into(),
                        ));
                    }
                    if matches!(
                        function.as_str(),
                        "__vail_export_annotation" | "__vail_json_annotation"
                    ) && dtype != &Type::Primitive(PrimitiveType::Str)
                    {
                        return Err(ExprError::Invalid(
                            "annotation export requires string output".into(),
                        ));
                    }
                    dtype.to_arrow()?;
                    return Ok(dtype.clone());
                }
                if let Some(function) = function.strip_prefix("random_") {
                    let types = args
                        .iter()
                        .map(|arg| arg.dtype_scoped(schema, globals, scope, memo, context))
                        .collect::<Result<Vec<_>, _>>()?;
                    crate::randomness::validate(function, &types, dtype)?;
                    return Ok(dtype.clone());
                }
                if function == "coalesce" {
                    let boolean = Type::Primitive(PrimitiveType::Bool);
                    if dtype != &boolean || args.len() != 2 {
                        return Err(ExprError::Invalid(
                            "Boolean coalesce requires two Boolean arguments and Boolean output"
                                .into(),
                        ));
                    }
                    for arg in args {
                        if arg.dtype_in_scope(schema, globals, scope, memo)? != boolean {
                            return Err(ExprError::Invalid(
                                "Boolean coalesce requires two Boolean arguments".into(),
                            ));
                        }
                    }
                    return Ok(boolean);
                }
                // Validate the declared result against DataFusion's own type inference.
                let udf = nested_function(function)?;
                let args = args
                    .iter()
                    .map(|arg| {
                        arg.dtype_scoped(schema, globals, scope, memo, context)?
                            .to_arrow()
                    })
                    .collect::<Result<Vec<_>, ExprError>>()?;
                // Reducer adapters validate their own argument/init contracts;
                // they are not unary collection-to-collection functions.
                if function.starts_with("__vail_list_reduce_")
                    || matches!(function.as_str(), "__vail_list_call_stats" | "abs" | "isnan")
                    || function.starts_with("__vail_ndarray_reshape_")
                {
                    let result = udf.return_type(&args)?;
                    if !same_collection_storage(&result, &dtype.to_arrow()?) {
                        return Err(ExprError::Invalid(format!(
                            "{function} result type mismatch"
                        )));
                    }
                    return Ok(dtype.clone());
                }
                if function.starts_with("__vail_collection_group_aggregate_") {
                    let result = udf.return_type(&args)?;
                    if !same_collection_storage(&result, &dtype.to_arrow()?) {
                        return Err(ExprError::Invalid(format!(
                            "{function} result type mismatch"
                        )));
                    }
                    return Ok(dtype.clone());
                }
                let matrix_function = matches!(
                    function.as_str(),
                    "__vail_dict_lookup"
                        | "__vail_index_values"
                        | "__vail_list_take"
                        | "__vail_list_take_groups"
                        | "__vail_dense_cells"
                        | "__vail_matrix_columns"
                        | "__vail_matrix_aligned"
                );
                let expected = if function == "arrays_zip" {
                    args.len()
                } else if function == "array_sort" {
                    3
                } else if function == "array_element"
                    || function == "array_repeat"
                    || function == "arrays_zip"
                    || matches!(
                        function.as_str(),
                        "__vail_dict_lookup"
                            | "__vail_list_take"
                            | "__vail_list_take_groups"
                            | "__vail_dense_cells"
                            | "__vail_matrix_aligned"
                    )
                    || matches!(
                        function.as_str(),
                        "array_union" | "array_intersect" | "array_except" | "array_concat"
                    )
                {
                    2
                } else {
                    1
                };
                if args.len() != expected
                    || (function != "array_repeat"
                        && !matches!(args.first(), Some(DataType::List(_))))
                {
                    return Err(ExprError::Invalid(format!("invalid {function} arguments")));
                }
                if function == "arrays_zip"
                    && args.iter().any(|arg| !matches!(arg, DataType::List(_)))
                {
                    return Err(ExprError::Invalid(
                        "arrays_zip requires array arguments".into(),
                    ));
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
                if function == "array_repeat" && args[1] != DataType::Int64 {
                    return Err(ExprError::Invalid(
                        "array_repeat count must be int64".into(),
                    ));
                }
                if expected == 2
                    && !matrix_function
                    && !matches!(
                        function.as_str(),
                        "array_element" | "arrays_zip" | "array_repeat"
                    )
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
                let input = value
                    .dtype_scoped(schema, globals, scope, memo, context)?
                    .to_arrow()?;
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
                let Type::Array { array } =
                    value.dtype_scoped(schema, globals, scope, memo, context)?
                else {
                    return Err(ExprError::Invalid("fold requires an array".into()));
                };
                let output = zero.dtype_scoped(schema, globals, scope, memo, context)?;
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
                if body.dtype_in_scope(schema, globals, &nested, memo)? != output {
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
                let input_type = value.dtype_scoped(schema, globals, scope, memo, context)?;
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
                    array: Box::new(body.dtype_in_scope(schema, globals, &nested, memo)?),
                })
            }
            Self::ArrayPredicate {
                value,
                parameter,
                predicate,
                first,
            } => {
                let element = match value.dtype_scoped(schema, globals, scope, memo, context)? {
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
                if predicate.dtype_in_scope(schema, globals, &nested, memo)?
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
                let array = match value.dtype_scoped(schema, globals, scope, memo, context)? {
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
                let predicate_type = predicate.dtype_in_scope(schema, globals, &nested, memo)?;
                if predicate_type != Type::Primitive(PrimitiveType::Bool) {
                    return Err(ExprError::Invalid(format!(
                        "array filter predicate requires bool, got {predicate_type:?}"
                    )));
                }
                Ok(value.dtype_scoped(schema, globals, scope, memo, context)?)
            }
            Self::StreamRange { start, stop, step } => {
                let int32 = Type::Primitive(PrimitiveType::Int32);
                for (label, argument) in [("start", start), ("stop", stop), ("step", step)] {
                    let actual = argument.dtype_scoped(schema, globals, scope, memo, context)?;
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
                let Type::Array { array } = value.dtype_in_scope(schema, globals, scope, memo)?
                else {
                    return Err(ExprError::Invalid("array_grouped requires array".into()));
                };
                let constant = match size.as_ref() {
                    Self::Literal { value, .. } => value.as_i64(),
                    Self::RuntimeLiteral {
                        value: ScalarValue::Int32(value),
                    } => value.map(i64::from),
                    _ => None,
                };
                if constant.is_some_and(|n| n <= 0) {
                    return Err(ExprError::Invalid("group size must be positive".into()));
                }
                let size_type = size.dtype_in_scope(schema, globals, scope, memo)?;
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
                value.dtype_scoped(schema, globals, scope, memo, context)?;
                Ok(Type::Primitive(PrimitiveType::Str))
            }
            Self::Locus {
                reference,
                contig,
                position,
            } => {
                reference.validate().map_err(ExprError::Invalid)?;
                if contig.dtype_scoped(schema, globals, scope, memo, context)?
                    != Type::Primitive(PrimitiveType::Str)
                    || position.dtype_scoped(schema, globals, scope, memo, context)?
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
                if contig.dtype_scoped(schema, globals, scope, memo, context)?
                    != Type::Primitive(PrimitiveType::Str)
                    || position.dtype_scoped(schema, globals, scope, memo, context)?
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
                if position.dtype_scoped(schema, globals, scope, memo, context)?
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
                if value.dtype_scoped(schema, globals, scope, memo, context)?
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
                if value.dtype_scoped(schema, globals, scope, memo, context)?
                    != Type::Primitive(PrimitiveType::Str)
                {
                    return Err(ExprError::Invalid(
                        "parse_csv_line requires string input".into(),
                    ));
                }
                if delimiter.is_empty()
                    || (delimiter.len() == 1
                        && (!delimiter.as_bytes()[0].is_ascii()
                            || matches!(delimiter.as_bytes()[0], b'\r' | b'\n' | 0)))
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
                let Type::Locus { .. } =
                    value.dtype_scoped(schema, globals, scope, memo, context)?
                else {
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
                let point = start.dtype_scoped(schema, globals, scope, memo, context)?;
                if point != end.dtype_scoped(schema, globals, scope, memo, context)? {
                    return Err(ExprError::Invalid(
                        "interval endpoints must have the same type".into(),
                    ));
                }
                if includes_start.dtype_scoped(schema, globals, scope, memo, context)?
                    != Type::Primitive(PrimitiveType::Bool)
                    || includes_end.dtype_scoped(schema, globals, scope, memo, context)?
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
                } = interval.dtype_scoped(schema, globals, scope, memo, context)?
                else {
                    return Err(ExprError::Invalid(
                        "interval_contains requires an interval".into(),
                    ));
                };
                if *point_type != point.dtype_scoped(schema, globals, scope, memo, context)? {
                    return Err(ExprError::Invalid(
                        "interval_contains point type mismatch".into(),
                    ));
                }
                Ok(Type::Primitive(PrimitiveType::Bool))
            }
            Self::IntervalOverlaps { left, right } => {
                let left_type = left.dtype_scoped(schema, globals, scope, memo, context)?;
                if !matches!(left_type, Type::Interval { .. })
                    || left_type != right.dtype_scoped(schema, globals, scope, memo, context)?
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
                    let actual = element.dtype_scoped(schema, globals, scope, memo, context)?;
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
            Self::ArrayFlatten { value } => {
                match value.dtype_scoped(schema, globals, scope, memo, context)? {
                    Type::Array { array } if matches!(*array, Type::Array { .. }) => Ok(*array),
                    _ => Err(ExprError::Invalid(
                        "array flatten requires an array of arrays".into(),
                    )),
                }
            }
            Self::ArrayLength { value } => {
                if !matches!(
                    value.dtype_scoped(schema, globals, scope, memo, context)?,
                    Type::Array { .. } | Type::Set { .. } | Type::Dict { .. }
                ) {
                    return Err(ExprError::Invalid("length requires a collection".into()));
                }
                Ok(Type::Primitive(PrimitiveType::Int32))
            }
            Self::ArrayIndex { value, index } => {
                let element = match value.dtype_scoped(schema, globals, scope, memo, context)? {
                    Type::Array { array } => *array,
                    other => {
                        return Err(ExprError::Invalid(format!(
                            "array index requires array, got {other:?}"
                        )));
                    }
                };
                if index.dtype_scoped(schema, globals, scope, memo, context)?
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
                let array = match value.dtype_scoped(schema, globals, scope, memo, context)? {
                    t @ Type::Array { .. } => t,
                    other => {
                        return Err(ExprError::Invalid(format!(
                            "array slice requires array, got {other:?}"
                        )));
                    }
                };
                for endpoint in [start, stop, step].into_iter().flatten() {
                    if endpoint.dtype_scoped(schema, globals, scope, memo, context)?
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
                let element = match value.dtype_scoped(schema, globals, scope, memo, context)? {
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
                let source = value.dtype_in_scope(schema, globals, scope, memo)?;
                dtype.to_arrow()?;
                source.ensure_castable_to(dtype)?;
                Ok(dtype.clone())
            }
            Self::GetField { value, name } => match value
                .dtype_scoped(schema, globals, scope, memo, context)?
            {
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
                Type::Primitive(PrimitiveType::Call) => crate::calls::CallAccessor::for_field(name)
                    .map(|accessor| accessor.result_type())
                    .ok_or_else(|| {
                        ExprError::Invalid(format!("invalid call storage field {name:?}"))
                    }),
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
                    typed.push((
                        name.clone(),
                        expr.dtype_scoped(schema, globals, scope, memo, context)?,
                    ));
                }
                Ok(Type::Struct { r#struct: typed })
            }
            Self::Die { message, dtype } => {
                if message.dtype_scoped(schema, globals, scope, memo, context)?
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
                value.dtype_scoped(schema, globals, scope, memo, context)?;
                Ok(Type::Primitive(PrimitiveType::Bool))
            }
            Self::Unary { operator, value } => {
                let t = value.dtype_scoped(schema, globals, scope, memo, context)?;
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
                let a = left.dtype_scoped(schema, globals, scope, memo, context)?;
                let b = right.dtype_scoped(schema, globals, scope, memo, context)?;
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
            Self::Switch {
                value,
                parameter,
                cases,
                default,
                missing,
            } => {
                let selector = value.dtype_in_scope(schema, globals, scope, memo)?;
                if scope.iter().any(|(name, _, _)| name == parameter) {
                    return Err(ExprError::Invalid(
                        "duplicate switch parameter identity".into(),
                    ));
                }
                let mut nested = scope.to_vec();
                nested.push((parameter.clone(), selector, schema.fields().len()));
                let output = default.dtype_in_scope(schema, globals, &nested, memo)?;
                if let Some(missing) = missing {
                    if missing.dtype_in_scope(schema, globals, &nested, memo)? != output {
                        return Err(ExprError::Invalid(
                            "switch missing/default types differ".into(),
                        ));
                    }
                }
                for (condition, result) in cases {
                    ensure_bool(
                        &condition.dtype_in_scope(schema, globals, &nested, memo)?,
                        "switch condition",
                    )?;
                    if result.dtype_in_scope(schema, globals, &nested, memo)? != output {
                        return Err(ExprError::Invalid("switch branch types differ".into()));
                    }
                }
                Ok(output)
            }
            Self::IfElse {
                condition,
                consequent,
                alternate,
            } => {
                ensure_bool(
                    &condition.dtype_scoped(schema, globals, scope, memo, context)?,
                    "if_else condition",
                )?;
                let a = consequent.dtype_scoped(schema, globals, scope, memo, context)?;
                let b = alternate.dtype_scoped(schema, globals, scope, memo, context)?;
                if a == b { Ok(a) } else { promote(&a, &b) }
            }
        }
    }
}

fn validate_literal(value: &Value, dtype: &Type) -> Result<(), ExprError> {
    if value.is_null() {
        return Ok(());
    }
    if *dtype == Type::Primitive(PrimitiveType::Call) {
        return crate::calls::literal_call(value).map(|_| ());
    }
    if !matches!(dtype, Type::Interval { .. }) {
        if let Some(storage) = dtype.storage_type() {
            validate_literal(value, &storage)?;
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
/// Canonicalize sets in native literal buffers, using the same scalar equality
/// and sort order as the legacy literal reader. Ordinary buffers stay shared.
pub fn normalize_literal_array(array: &ArrayRef, dtype: &Type) -> Result<ArrayRef, ExprError> {
    fn has_sets(t: &Type) -> bool {
        match t {
            Type::Set { .. } => true,
            Type::Array { array } | Type::Interval { interval: array } => has_sets(array),
            Type::Struct { r#struct: fields } => fields.iter().any(|(_, t)| has_sets(t)),
            Type::Tuple { tuple } => tuple.iter().any(has_sets),
            Type::Dict { dict } => has_sets(&dict.0) || has_sets(&dict.1),
            _ => false,
        }
    }
    if !has_sets(dtype) {
        return Ok(array.clone());
    }
    if let Some(storage) = dtype.storage_type() {
        return normalize_literal_array(array, &storage);
    }
    match dtype {
        Type::Array { array: element } => {
            let lists = down::<ListArray>(array.as_ref())?;
            Ok(Arc::new(ListArray::new(
                match lists.data_type() {
                    DataType::List(f) => f.clone(),
                    _ => unreachable!(),
                },
                lists.offsets().clone(),
                normalize_literal_array(lists.values(), element)?,
                lists.nulls().cloned(),
            )))
        }
        Type::Set { set: element } => {
            let lists = down::<ListArray>(array.as_ref())?;
            let mut chunks = Vec::new();
            let mut lengths = Vec::new();
            for row in 0..lists.len() {
                let values = normalize_literal_array(&lists.value(row), element)?;
                let mut unique = HashSet::new();
                let mut indices = Vec::new();
                for i in 0..values.len() {
                    let scalar = scalar_at(&values, i)?;
                    if unique.insert(scalar) {
                        indices.push(i as u32);
                    }
                }
                let selected = take(values.as_ref(), &UInt32Array::from(indices), None)?;
                let order = set_literal_sort_indices(selected.as_ref())?;
                let selected = take(selected.as_ref(), &order, None)?;
                lengths.push(selected.len());
                chunks.push(selected);
            }
            let values = if chunks.is_empty() {
                arrow_array::new_empty_array(&element.to_arrow()?)
            } else {
                concat(&chunks.iter().map(|a| a.as_ref()).collect::<Vec<_>>())?
            };
            Ok(Arc::new(ListArray::new(
                match lists.data_type() {
                    DataType::List(f) => f.clone(),
                    _ => unreachable!(),
                },
                OffsetBuffer::from_lengths(lengths),
                values,
                lists.nulls().cloned(),
            )))
        }
        Type::Struct { r#struct: fields } => {
            let structure = down::<StructArray>(array.as_ref())?;
            let columns = structure
                .columns()
                .iter()
                .zip(fields)
                .map(|(a, (_, t))| normalize_literal_array(a, t))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Arc::new(StructArray::try_new(
                structure.fields().clone(),
                columns,
                structure.nulls().cloned(),
            )?))
        }
        _ => Ok(array.clone()),
    }
}

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
    if *t == Type::Primitive(PrimitiveType::Call) {
        return crate::calls::literal_array(v, &dt, len);
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
    array_index_rows(list_array(array)?, indices, dtype, false)
}

fn array_index_rows(
    lists: &ListArray,
    indices: &ArrayRef,
    dtype: &DataType,
    scalar_source: bool,
) -> Result<ArrayRef, ExprError> {
    let ix = indices
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| ExprError::Invalid("array index requires int32".into()))?;
    let mut take_indices = Vec::with_capacity(ix.len());
    for row in 0..ix.len() {
        let source_row = if scalar_source { 0 } else { row };
        if lists.is_null(source_row) || ix.is_null(row) {
            take_indices.push(None);
            continue;
        }
        let len = lists.value_length(source_row) as i64;
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
            (lists.value_offsets()[source_row] as i64 + normalized) as u32,
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
    Random(String),
    RestoreArrowType,
    Die,
    Numeric(BinaryOperator),
    ToString(Type),
    GetField(String),
    CallConstruct,
    CallAccess(crate::calls::CallAccessor),
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
        reference: Arc<ReferenceDefinition>,
    },
}

#[derive(Debug)]
struct HailPhysicalExpr {
    kind: HailKind,
    args: Vec<Arc<dyn PhysicalExpr>>,
    dtype: DataType,
    prepared_reference: Arc<OnceLock<Result<Arc<PreparedReference>, String>>>,
}

fn locus_eval(
    op: &str,
    _reference: &ReferenceDefinition,
    prepared: &PreparedReference,
    args: &[ArrayRef],
    dtype: &DataType,
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
                    let p = down::<Int64Array>(args[0].as_ref())?.value(i);
                    if p < 0 {
                        return Err(ExprError::Invalid(
                            "global position cannot be negative".into(),
                        ));
                    }
                    let (contig, position) = prepared.from_global(p).ok_or_else(|| {
                        ExprError::Invalid("global position exceeds reference length".into())
                    })?;
                    (contig.to_owned(), position)
                }
            };
            let ordinal = prepared
                .ordinal(&contig)
                .ok_or_else(|| ExprError::Invalid(format!("unknown locus contig {contig:?}")))?;
            prepared
                .validate_coordinate(ordinal, &contig, position)
                .map_err(ExprError::Invalid)?;
            ordinals.push(Some(ordinal));
            contigs.push(Some(contig));
            positions.push(Some(position));
        }
        let DataType::Struct(fields) = dtype else {
            return Err(ExprError::Invalid(
                "locus construction requires its declared Arrow type".into(),
            ));
        };
        let valid = ordinals.iter().map(Option::is_some).collect::<Vec<_>>();
        return Ok(Arc::new(StructArray::try_new(
            fields.clone(),
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
                    prepared
                        .ordinal(c)
                        .is_some_and(|o| prepared.validate_coordinate(o, c, p).is_ok()),
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
            prepared
                .validate_coordinate(ordinals.value(i), contigs.value(i), positions.value(i))
                .map_err(ExprError::Invalid)?;
        }
    }
    match op {
        "contig" => Ok(Arc::new(StringArray::new(
            contigs.offsets().clone(),
            contigs.values().clone(),
            NullBuffer::union(contigs.nulls(), st.nulls()),
        ))),
        "position" => Ok(Arc::new(Int32Array::new(
            positions.values().clone(),
            NullBuffer::union(positions.nulls(), st.nulls()),
        ))),
        "global_position" => {
            let out = (0..len)
                .map(|i| {
                    if st.is_null(i) {
                        None
                    } else {
                        let ordinal = ordinals.value(i) as usize;
                        prepared.global_position(ordinal, positions.value(i))
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
                        let ordinal = ordinals.value(i) as usize;
                        Some(match op {
                            "is_x" => prepared.is_x(ordinal),
                            "is_y" => prepared.is_y(ordinal),
                            "is_mt" => prepared.is_mt(ordinal),
                            _ => {
                                !prepared.is_x(ordinal)
                                    && !prepared.is_y(ordinal)
                                    && !prepared.is_mt(ordinal)
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
                        let p = positions.value(i);
                        let ordinal = ordinals.value(i) as usize;
                        Some(prepared.in_par(ordinal, p, op == "in_x_par"))
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
    scientific_float_string(value, 5)
}

fn scientific_float_string(value: f64, precision: usize) -> String {
    if !value.is_finite() {
        return float_text(value);
    }
    if value == 0.0 {
        return format!(
            "{}{:.*}e+00",
            if value.is_sign_negative() { "-" } else { "" },
            precision,
            0.0
        );
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
    let rounded = decimal.with_scale_round(
        precision as i64 - i64::from(exponent),
        bigdecimal::RoundingMode::HalfUp,
    );
    let rendered = format!("{rounded:.precision$e}");
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

pub(crate) fn json_annotation(
    array: &ArrayRef,
    row: usize,
    dtype: &Type,
) -> Result<String, ExprError> {
    hail_value_string(array, row, dtype, false)
}

pub(crate) fn export_annotation(
    array: &ArrayRef,
    row: usize,
    dtype: &Type,
) -> Result<String, ExprError> {
    Ok(match dtype {
        Type::Primitive(PrimitiveType::Float64) => {
            scientific_float_string(down::<Float64Array>(array.as_ref())?.value(row), 4)
        }
        Type::Primitive(PrimitiveType::Float32) => {
            java_float32_text(down::<Float32Array>(array.as_ref())?.value(row))
        }
        Type::Primitive(PrimitiveType::Str) => {
            down::<StringArray>(array.as_ref())?.value(row).to_owned()
        }
        Type::Primitive(_) | Type::Locus { .. } => annotation_text(array, row, dtype)?,
        _ => hail_value_string(array, row, dtype, false)?,
    })
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

fn call_raw(array: &ArrayRef, row: usize) -> Result<String, ExprError> {
    let call = crate::calls::CallRows::new(array)?
        .get(row)?
        .ok_or_else(|| ExprError::Invalid("missing Call".into()))?;
    Ok(match call.alleles() {
        [] => {
            if call.phased() {
                "|-".into()
            } else {
                "-".into()
            }
        }
        [a] => {
            if call.phased() {
                format!("|{a}")
            } else {
                a.to_string()
            }
        }
        [a, b] => format!("{a}{}{b}", if call.phased() { "|" } else { "/" }),
        _ => unreachable!(),
    })
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
    if let Some(storage) = dtype
        .storage_type()
        .or_else(|| (*dtype == Type::Primitive(PrimitiveType::Call)).then(|| dtype.clone()))
    {
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
                let call_value = crate::calls::CallRows::new(array)?
                    .get(row)?
                    .ok_or_else(|| ExprError::Invalid("missing Call".into()))?;
                let alleles = call_value.alleles();
                let phased = call_value.phased();
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
            prepared_reference: self.prepared_reference.clone(),
        }))
    }
    fn evaluate(&self, batch: &RecordBatch) -> datafusion_common::Result<ColumnarValue> {
        let len = batch.num_rows();
        if matches!(self.kind, HailKind::RestoreArrowType) {
            // Descriptor restoration must retain scalar lists/records. Expanding
            // a shared dictionary here defeats the lookup kernel's scalar index.
            let value = self.args[0].evaluate(batch)?;
            return crate::logical_expr::restore_arrow_value_type(value, &self.dtype);
        }
        if let HailKind::Random(function) = &self.kind {
            if len == 0 {
                return Ok(ColumnarValue::Array(arrow_array::new_empty_array(
                    &self.dtype,
                )));
            }
            let args = self
                .args
                .iter()
                .map(|x| x.evaluate(batch))
                .collect::<datafusion_common::Result<Vec<_>>>()?;
            return crate::randomness::evaluate(function, &args, &self.dtype, len)
                .map(ColumnarValue::Array)
                .map_err(|e| datafusion_common::DataFusionError::Execution(e.to_string()));
        }
        if len == 0 {
            return Ok(ColumnarValue::Array(arrow_array::new_empty_array(
                &self.dtype,
            )));
        }
        let evaluated = self
            .args
            .iter()
            .map(|argument| argument.evaluate(batch))
            .collect::<datafusion_common::Result<Vec<_>>>()?;
        // A captured scalar collection must not be expanded once per source
        // row merely to select individual elements. Arrow take copies only
        // selected children, preserving ownership of the original scalar.
        if matches!(self.kind, HailKind::ArrayIndex)
            && let Some(ColumnarValue::Scalar(ScalarValue::List(source))) = evaluated.first()
        {
            let scalar = matches!(evaluated[1], ColumnarValue::Scalar(_));
            let indices = evaluated[1]
                .clone()
                .into_array(if scalar { 1 } else { len })?;
            let output = array_index_rows(source, &indices, &self.dtype, true)
                .map_err(|error| DataFusionError::Execution(error.to_string()))?;
            return if scalar {
                Ok(ColumnarValue::Scalar(scalar_at(&output, 0).map_err(
                    |error| DataFusionError::Execution(error.to_string()),
                )?))
            } else {
                Ok(ColumnarValue::Array(output))
            };
        }
        let args = evaluated
            .into_iter()
            .map(|value| value.into_array(len))
            .collect::<datafusion_common::Result<Vec<_>>>()?;
        let result = (|| -> Result<ArrayRef, ExprError> {
            match &self.kind {
                HailKind::Random(_) => unreachable!("random inputs retain scalar form above"),
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
                HailKind::ToString(input_type) => hail_to_string(&args[0], input_type),
                HailKind::CallConstruct => crate::calls::construct(&args[0], &self.dtype),
                HailKind::CallAccess(accessor) => crate::calls::access(&args[0], *accessor),
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
                    if parent.null_count() == 0 {
                        Ok(child.clone())
                    } else {
                        let mask = arrow_arith::boolean::is_null(parent.as_ref())?;
                        Ok(nullif(child.as_ref(), &mask)?)
                    }
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
                    reference,
                } => {
                    let prepared = self
                        .prepared_reference
                        .get_or_init(|| reference.prepare().map(Arc::new));
                    let prepared = prepared
                        .as_ref()
                        .map_err(|error| ExprError::Invalid(error.clone()))?;
                    locus_eval(operation, reference, prepared, &args, &self.dtype, len)
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
            .and_then(|array| {
                Ok(crate::logical_expr::restore_arrow_array_type(
                    &array,
                    &self.dtype,
                )?)
            })
            .map(ColumnarValue::Array)
            .map_err(|e| DataFusionError::Execution(e.to_string()))
    }
}

fn custom(
    kind: HailKind,
    args: Vec<Arc<dyn PhysicalExpr>>,
    dtype: DataType,
) -> Arc<dyn PhysicalExpr> {
    Arc::new(HailPhysicalExpr {
        kind,
        args,
        dtype,
        prepared_reference: Arc::new(OnceLock::new()),
    })
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

/// Normalize a checked logical expression's physical Arrow storage.
/// Callers establish logical type agreement before requesting this conversion.
pub fn cast_physical_storage(
    value: Arc<dyn PhysicalExpr>,
    schema: &Schema,
    dtype: DataType,
) -> datafusion_common::Result<Arc<dyn PhysicalExpr>> {
    let value = cast(value, schema, dtype.clone())?;
    // Some compiled struct expressions promise canonical child storage while
    // their borrowed children retain alternate Arrow layouts at runtime.
    Ok(custom(HailKind::RestoreArrowType, vec![value], dtype))
}

fn cast(
    value: Arc<dyn PhysicalExpr>,
    schema: &Schema,
    dtype: DataType,
) -> datafusion_common::Result<Arc<dyn PhysicalExpr>> {
    let actual = value.data_type(schema)?;
    if actual == dtype {
        return Ok(value);
    }
    if vail_ordering::same_layout(&actual, &dtype) {
        return Ok(custom(HailKind::RestoreArrowType, vec![value], dtype));
    }
    datafusion_physical_expr::expressions::cast(value, schema, dtype)
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
    let mut memo = CompilationMemo {
        root_expression: expr as *const Expr as usize,
        ..CompilationMemo::default()
    };
    lower_scoped(expr, schema, globals, &[], &mut memo)
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
    if matches!(name, "variant_min_rep" | "variant_entry" | "unphased_diploid_gt_index_call") {
        return crate::variant_kernels::function(name);
    }
    Ok(match name {
        "hwe_both_counts" => (
            crate::hwe_counts::udf(true),
            vec![int64.clone(); 3],
            Type::from_arrow(&crate::hwe_counts::result_type(true))?,
        ),
        "hardy_weinberg_counts" => (
            crate::hwe_counts::udf(false),
            vec![int64.clone(), int64.clone(), int64, boolean],
            Type::Array {
                array: Box::new(Type::Primitive(PrimitiveType::Float64)),
            },
        ),
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
        "regexp_replace" => (
            regex::regexp_replace(),
            vec![text.clone(), text.clone(), text.clone(), text.clone()],
            text,
        ),
        "regex_split" => (
            crate::string_kernels::udf(name),
            vec![text.clone(), text, int32],
            strings,
        ),
        "first_match_in" => (
            crate::string_kernels::udf(name),
            vec![text.clone(), text],
            strings,
        ),
        "hamming" => (
            crate::string_kernels::udf(name),
            vec![text.clone(), text],
            int32,
        ),
        "entropy" => (
            crate::string_kernels::udf(name),
            vec![text],
            Type::Primitive(PrimitiveType::Float64),
        ),
        "unicode_slice" => (
            crate::string_kernels::udf(name),
            vec![
                text.clone(),
                int32.clone(),
                int32.clone(),
                int32.clone(),
                int32,
            ],
            text,
        ),
        "unicode_translate" => (
            crate::string_kernels::udf(name),
            vec![
                text.clone(),
                Type::Dict {
                    dict: Box::new((text.clone(), text.clone())),
                },
            ],
            text,
        ),
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
    if let Some(function) = crate::list_aggregate::udf(name) {
        return Ok(function);
    }
    if name == "__vail_ndarray_from_array" {
        return Ok(crate::ndarray::udf());
    }
    if let Some(rank) = name.strip_prefix("__vail_ndarray_reshape_") {
        let rank = rank.parse::<usize>().map_err(|_| ExprError::Invalid("invalid reshape rank".into()))?;
        return Ok(crate::ndarray::reshape_udf(rank));
    }
    if let Some(function) = crate::matrix_transpose::udf(name) {
        return Ok(function);
    }
    if let Some(function) = crate::collection_lookup::udf(name) {
        return Ok(function);
    }
    if let Some(function) = crate::collection_groups::udf(name) {
        return Ok(function);
    }
    if let Some(function) = crate::matrix_arrays::udf(name) {
        return Ok(function);
    }
    Ok(match name {
        "vail_array_mean" => crate::numeric_lists::mean_udf(false),
        "vail_array_mean_strict" => crate::numeric_lists::mean_udf(true),
        "abs" => datafusion_functions::math::abs(),
        "isnan" => datafusion_functions::math::isnan(),
        "array_sort" => nested::sort::array_sort_udf(),
        "array_distinct" => nested::set_ops::array_distinct_udf(),
        "array_union" => nested::set_ops::array_union_udf(),
        "array_intersect" => nested::set_ops::array_intersect_udf(),
        "array_except" => nested::except::array_except_udf(),
        "array_element" => nested::extract::array_element_udf(),
        "array_concat" => nested::concat::array_concat_udf(),
        "array_repeat" => nested::repeat::array_repeat_udf(),
        "arrays_zip" => nested::arrays_zip::arrays_zip_udf(),
        "array_compact" => nested::array_compact::array_compact_udf(),
        "flatten" => nested::flatten::flatten_udf(),
        "array_avg" => nested::array_avg::array_avg_udf(),
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

fn lower_scoped(
    expr: &Expr,
    schema: &Schema,
    globals: GlobalBinding<'_>,
    scope: &[(String, Type, usize)],
    memo: &mut CompilationMemo,
) -> Result<Arc<dyn PhysicalExpr>, ExprError> {
    let context = memo.context(schema, globals.schema(), scope);
    lower_cached(expr, schema, globals, scope, memo, context)
}
// Bind physical metadata once at each native compilation node. DataFusion
// recursively asks children for metadata; on a DAG that otherwise revisits
// shared subgraphs once per incoming path.
struct CompiledPhysical {
    inner: Arc<dyn PhysicalExpr>,
    // Closed expressions producing nested values can otherwise rebuild the
    // same large Arrow array for every input batch. Initialize only on the
    // first nonempty evaluation and publish successful results only.
    constant: Option<(OnceLock<ScalarValue>, Mutex<()>)>,
    primary: OnceLock<(Arc<Schema>, Arc<Field>)>,
    fields: std::sync::RwLock<HashMap<Arc<Schema>, Arc<Field>>>,
    label: String,
    fingerprint: u64,
}
impl CompiledPhysical {
    fn with_fields(
        inner: Arc<dyn PhysicalExpr>,
        initial: Option<(Arc<Schema>, Arc<Field>)>,
        cache_constant: bool,
    ) -> Arc<dyn PhysicalExpr> {
        if inner.children().is_empty() {
            return inner;
        }
        let label = if let Some(function) = inner.downcast_ref::<ScalarFunctionExpr>() {
            function.name().to_owned()
        } else if let Some(binary) =
            inner.downcast_ref::<datafusion_physical_expr::expressions::BinaryExpr>()
        {
            format!("Binary({})", binary.op())
        } else if let Some(hail) = inner.downcast_ref::<HailPhysicalExpr>() {
            match &hail.kind {
                HailKind::Random(name) => format!("random_{name}"),
                HailKind::RestoreArrowType => "RestoreArrowType".into(),
                HailKind::Locus { operation, .. } => format!("locus_{operation}"),
                _ => "Hail".into(),
            }
        } else {
            "physical".into()
        };
        let mut hash = std::collections::hash_map::DefaultHasher::new();
        inner.hash(&mut hash);
        let primary = OnceLock::new();
        if let Some(initial) = initial {
            let _ = primary.set(initial);
        }
        Arc::new(Self {
            inner,
            constant: cache_constant.then(|| (OnceLock::new(), Mutex::new(()))),
            primary,
            fields: Default::default(),
            label,
            fingerprint: std::hash::Hasher::finish(&hash),
        })
    }
    fn unbound(inner: Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
        Self::with_fields(inner, None, false)
    }
    fn bind(
        inner: Arc<dyn PhysicalExpr>,
        schema: Arc<Schema>,
        cache_constant: bool,
    ) -> datafusion_common::Result<Arc<dyn PhysicalExpr>> {
        let field = inner.return_field(schema.as_ref())?;
        // Compilation already interns this schema by lexical context. Keep
        // that shared Arc rather than cloning a wide field vector per node.
        Ok(Self::with_fields(
            inner,
            Some((schema, field)),
            cache_constant,
        ))
    }
    fn field(&self, schema: &Schema) -> datafusion_common::Result<Arc<Field>> {
        // Fields are Arc-backed inside Schema, so a clone of the original
        // descriptor compares without hashing/walking its wide field vector.
        if let Some((original, field)) = self.primary.get() {
            if std::ptr::eq(schema, original.as_ref()) || schema == original.as_ref() {
                return Ok(field.clone());
            }
        }
        let poisoned = || DataFusionError::Internal("physical metadata cache poisoned".into());
        if let Some(field) = self
            .fields
            .read()
            .map_err(|_| poisoned())?
            .get(schema)
            .cloned()
        {
            return Ok(field);
        }
        // No lock spans recursive child metadata queries. DF can remap lambda
        // indices before passing a projected schema; derive only against the
        // actual schema supplied by the caller, never a rewrite's old schema.
        let field = self.inner.return_field(schema)?;
        let descriptor = Arc::new(schema.clone());
        if self
            .primary
            .set((descriptor.clone(), field.clone()))
            .is_ok()
        {
            return Ok(field);
        }
        self.fields
            .write()
            .map_err(|_| poisoned())?
            .insert(descriptor, field.clone());
        Ok(field)
    }
}
impl std::fmt::Debug for CompiledPhysical {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompiledPhysical")
            .field("fingerprint", &self.fingerprint)
            .finish()
    }
}
impl std::fmt::Display for CompiledPhysical {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}_{:016x}", self.label, self.fingerprint)
    }
}
std::thread_local! {
    // Dynamic DF equality calls back through child wrappers. Scope memoization
    // to this synchronous comparison only; retain neither nodes nor results
    // between comparisons, including when a child comparison unwinds.
    static PHYSICAL_EQUALITY: std::cell::RefCell<Option<HashMap<(usize, usize), bool>>> = const { std::cell::RefCell::new(None) };
}
struct PhysicalEqualityScope(bool);
impl Drop for PhysicalEqualityScope {
    fn drop(&mut self) {
        if self.0 {
            PHYSICAL_EQUALITY.with(|memo| *memo.borrow_mut() = None);
        }
    }
}
impl PartialEq for CompiledPhysical {
    fn eq(&self, other: &Self) -> bool {
        if self.fingerprint != other.fingerprint {
            return false;
        }
        if Arc::ptr_eq(&self.inner, &other.inner) {
            return true;
        }
        let key = (
            Arc::as_ptr(&self.inner) as *const () as usize,
            Arc::as_ptr(&other.inner) as *const () as usize,
        );
        let root = PHYSICAL_EQUALITY.with(|memo| {
            let mut memo = memo.borrow_mut();
            let root = memo.is_none();
            if root {
                *memo = Some(HashMap::new());
            }
            root
        });
        let _scope = PhysicalEqualityScope(root);
        if let Some(result) =
            PHYSICAL_EQUALITY.with(|memo| memo.borrow().as_ref().unwrap().get(&key).copied())
        {
            return result;
        }
        let equal = self.inner.eq(&other.inner);
        PHYSICAL_EQUALITY.with(|memo| {
            memo.borrow_mut().as_mut().unwrap().insert(key, equal);
        });
        equal
    }
}
impl Eq for CompiledPhysical {}
impl std::hash::Hash for CompiledPhysical {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.fingerprint.hash(state);
    }
}
impl PhysicalExpr for CompiledPhysical {
    fn data_type(&self, schema: &Schema) -> datafusion_common::Result<DataType> {
        Ok(self.field(schema)?.data_type().clone())
    }
    fn nullable(&self, schema: &Schema) -> datafusion_common::Result<bool> {
        Ok(self.field(schema)?.is_nullable())
    }
    fn return_field(&self, schema: &Schema) -> datafusion_common::Result<Arc<Field>> {
        self.field(schema)
    }
    fn evaluate(&self, batch: &RecordBatch) -> datafusion_common::Result<ColumnarValue> {
        if let Some((cache, lock)) = &self.constant {
            if let Some(value) = cache.get() {
                return Ok(ColumnarValue::Scalar(value.clone()));
            }
            let _guard = lock.lock().map_err(|_| {
                DataFusionError::Internal("constant expression cache poisoned".into())
            })?;
            if let Some(value) = cache.get() {
                return Ok(ColumnarValue::Scalar(value.clone()));
            }
            let dtype = self.data_type(batch.schema_ref())?;
            if batch.num_rows() == 0 {
                return Ok(ColumnarValue::Array(new_null_array(&dtype, 0)));
            }
            // Evaluating one row avoids constructing a full repeated output.
            // Errors are deliberately not stored, so transient failures retry.
            let one_row = batch.slice(0, 1);
            let evaluated = self.inner.evaluate(&one_row)?;
            let scalar = match evaluated {
                ColumnarValue::Scalar(value) => value,
                ColumnarValue::Array(array) => {
                    if array.len() != 1 {
                        return Err(DataFusionError::Internal(format!(
                            "closed expression returned {} rows for a one-row input",
                            array.len()
                        )));
                    }
                    ScalarValue::try_from_array(array.as_ref(), 0)?
                }
            };
            if scalar.data_type() != dtype {
                return Err(DataFusionError::Internal(format!(
                    "closed expression returned {:?}, expected {dtype:?}",
                    scalar.data_type()
                )));
            }
            let _ = cache.set(scalar.clone());
            return Ok(ColumnarValue::Scalar(scalar));
        }
        self.inner.evaluate(batch)
    }
    fn evaluate_selection(
        &self,
        batch: &RecordBatch,
        selection: &BooleanArray,
    ) -> datafusion_common::Result<ColumnarValue> {
        if self.constant.is_some() {
            if batch.num_rows() != selection.len() {
                return Err(DataFusionError::Execution(format!(
                    "Selection array length does not match batch row count: {} != {}",
                    selection.len(),
                    batch.num_rows()
                )));
            }
            if !selection.has_true() {
                return Ok(ColumnarValue::Array(new_null_array(
                    &self.data_type(batch.schema_ref())?,
                    0,
                )));
            }
            return self.evaluate(batch);
        }
        self.inner.evaluate_selection(batch, selection)
    }
    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.inner.children()
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion_common::Result<Arc<dyn PhysicalExpr>> {
        Ok(Self::unbound(
            self.inner.clone().with_new_children(children)?,
        ))
    }
    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
    fn evaluate_bounds(
        &self,
        children: &[&datafusion_expr::interval_arithmetic::Interval],
    ) -> datafusion_common::Result<datafusion_expr::interval_arithmetic::Interval> {
        self.inner.evaluate_bounds(children)
    }
    fn propagate_constraints(
        &self,
        interval: &datafusion_expr::interval_arithmetic::Interval,
        children: &[&datafusion_expr::interval_arithmetic::Interval],
    ) -> datafusion_common::Result<Option<Vec<datafusion_expr::interval_arithmetic::Interval>>>
    {
        self.inner.propagate_constraints(interval, children)
    }
    fn get_properties(
        &self,
        children: &[datafusion_expr::sort_properties::ExprProperties],
    ) -> datafusion_common::Result<datafusion_expr::sort_properties::ExprProperties> {
        self.inner.get_properties(children)
    }
    fn snapshot(&self) -> datafusion_common::Result<Option<Arc<dyn PhysicalExpr>>> {
        Ok(self.inner.snapshot()?.map(Self::unbound))
    }
    fn snapshot_generation(&self) -> u64 {
        self.inner.snapshot_generation()
    }
    fn is_volatile_node(&self) -> bool {
        self.inner.is_volatile_node()
    }
    fn placement(&self) -> datafusion_expr::ExpressionPlacement {
        self.inner.placement()
    }
    fn expression_id(&self) -> Option<u64> {
        self.inner.expression_id()
    }
    #[allow(deprecated)]
    fn evaluate_statistics(
        &self,
        children: &[&datafusion_expr::statistics::Distribution],
    ) -> datafusion_common::Result<datafusion_expr::statistics::Distribution> {
        self.inner.evaluate_statistics(children)
    }
    #[allow(deprecated)]
    fn propagate_statistics(
        &self,
        parent: &datafusion_expr::statistics::Distribution,
        children: &[&datafusion_expr::statistics::Distribution],
    ) -> datafusion_common::Result<Option<Vec<datafusion_expr::statistics::Distribution>>> {
        self.inner.propagate_statistics(parent, children)
    }
}

fn lower_cached(
    expr: &Expr,
    schema: &Schema,
    globals: GlobalBinding<'_>,
    scope: &[(String, Type, usize)],
    memo: &mut CompilationMemo,
    context: usize,
) -> Result<Arc<dyn PhysicalExpr>, ExprError> {
    let key = (context, expr as *const Expr as usize);
    if let Some(value) = memo.physical.get(&key) {
        return Ok(value.clone());
    }
    let value = lower_uncached(expr, schema, globals, scope, memo, context)?;
    let bound_schema = memo
        .physical_schemas
        .entry(context)
        .or_insert_with(|| Arc::new(schema.clone()))
        .clone();
    let cache_constant = if expr as *const Expr as usize == memo.root_expression {
        let dtype = value.data_type(schema)?;
        is_nested_arrow_type(&dtype)
            && is_closed_physical_constant(expr)
            && !vail_ordering::has_type_metadata(&dtype)
    } else {
        false
    };
    let value = CompiledPhysical::bind(value, bound_schema, cache_constant)?;
    memo.physical.insert(key, value.clone());
    Ok(value)
}

/// A deliberately small whitelist for expressions whose result cannot depend
/// on rows, globals, execution context, or mutable function state. The default
/// is always non-cacheable.
fn closed_constant_children(expr: &Expr) -> Option<Vec<&Expr>> {
    Some(match expr {
        Expr::Literal { .. } | Expr::RuntimeLiteral { .. } => vec![],
        Expr::IfElse {
            condition,
            consequent,
            alternate,
        } => vec![condition, consequent, alternate],
        Expr::Die { message, .. } => vec![message],
        Expr::NestedFunction { function, args, .. } => {
            if function.starts_with("random_")
                || !nested_function(function)
                    .is_ok_and(|udf| udf.signature().volatility == Volatility::Immutable)
            {
                return None;
            }
            args.iter().collect()
        }
        Expr::StringFunction { function, args } => {
            if !string_function(function)
                .is_ok_and(|(udf, _, _)| udf.signature().volatility == Volatility::Immutable)
            {
                return None;
            }
            args.iter().collect()
        }
        Expr::IntegerBinary { left, right, .. } | Expr::Binary { left, right, .. } => {
            vec![left, right]
        }
        Expr::Unary { value, .. }
        | Expr::Cast { value, .. }
        | Expr::CollectionCast { value, .. }
        | Expr::ToString { value }
        | Expr::GetField { value, .. }
        | Expr::IsMissing { value }
        | Expr::ArrayFlatten { value }
        | Expr::ArrayLength { value }
        | Expr::ArrayReduce { value, .. } => vec![value],
        Expr::Tuple { elements } | Expr::Array { elements, .. } => elements.iter().collect(),
        Expr::Struct { fields } => fields.iter().map(|(_, value)| value).collect(),
        _ => return None,
    })
}

fn is_closed_physical_constant(expr: &Expr) -> bool {
    let mut pending = vec![expr];
    let mut visited = HashSet::new();
    while let Some(node) = pending.pop() {
        if !visited.insert(node as *const Expr as usize) {
            continue;
        }
        let Some(children) = closed_constant_children(node) else {
            return false;
        };
        pending.extend(children);
    }
    true
}

fn is_nested_arrow_type(dtype: &DataType) -> bool {
    matches!(
        dtype,
        DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Struct(_)
            | DataType::Map(_, _)
    )
}

fn lower_synthetic(
    expr: Expr,
    schema: &Schema,
    globals: GlobalBinding<'_>,
    scope: &[(String, Type, usize)],
    memo: &mut CompilationMemo,
    context: usize,
) -> Result<Arc<dyn PhysicalExpr>, ExprError> {
    let expr = Arc::new(expr);
    memo.generated.push(expr.clone());
    lower_cached(&expr, schema, globals, scope, memo, context)
}

fn lower_array_lambda(
    value: &Expr,
    parameter: &str,
    callback: &Expr,
    operation: &str,
    schema: &Schema,
    globals: GlobalBinding<'_>,
    scope: &[(String, Type, usize)],
    memo: &mut CompilationMemo,
    context: usize,
) -> Result<Arc<dyn PhysicalExpr>, ExprError> {
    let input = lower_cached(value, schema, globals, scope, memo, context)?;
    let array = match value.dtype_scoped(schema, globals.schema(), scope, memo, context)? {
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
    let callback = lower_scoped(callback, &nested_schema, globals, &nested, memo)?;
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

fn switch_capture_indices(
    expr: &Expr,
    schema: &Schema,
    globals: GlobalBinding<'_>,
    scope: &[(String, Type, usize)],
) -> Vec<usize> {
    let mut used = std::collections::BTreeSet::new();
    expr.visit_unique(&mut |node| {
        let index = match node {
            Expr::Column { name } => schema.index_of(name).ok(),
            Expr::Global { name } => match globals {
                GlobalBinding::Columns(_, _) => schema.index_of(name).ok(),
                _ => None,
            },
            Expr::ExternalGlobal { reference, .. } => match globals {
                GlobalBinding::Columns(_, bindings) => bindings.get(reference).copied(),
                _ => None,
            },
            Expr::LambdaVariable { name, .. } => scope
                .iter()
                .rev()
                .find(|(parameter, _, _)| parameter == name)
                .map(|(_, _, index)| *index),
            _ => None,
        };
        if let Some(index) = index {
            used.insert(index);
        }
    });
    used.into_iter().collect()
}

fn switch_callback(
    expr: &Expr,
    schema: &Schema,
    globals: GlobalBinding<'_>,
    scope: &[(String, Type, usize)],
    output: &DataType,
    memo: &mut CompilationMemo,
) -> Result<crate::conditionals::Callback, ExprError> {
    let indices = switch_capture_indices(expr, schema, globals, scope);
    let remap = indices
        .iter()
        .enumerate()
        .map(|(new, old)| (*old, new))
        .collect::<std::collections::BTreeMap<_, _>>();
    let compact = Arc::new(Schema::new(
        indices
            .iter()
            .map(|&index| schema.field(index).clone())
            .collect::<Vec<_>>(),
    ));
    let nested = scope
        .iter()
        .filter_map(|(name, ty, index)| remap.get(index).map(|&at| (name.clone(), ty.clone(), at)))
        .collect::<Vec<_>>();
    let bindings = match globals {
        GlobalBinding::Columns(_, bindings) => bindings
            .iter()
            .filter_map(|(name, index)| remap.get(index).map(|&at| (name.clone(), at)))
            .collect(),
        _ => std::collections::BTreeMap::new(),
    };
    let globals = match globals {
        GlobalBinding::Columns(schema, _) => GlobalBinding::Columns(schema, &bindings),
        other => other,
    };
    let expr = cast(
        lower_scoped(expr, &compact, globals, &nested, memo)?,
        &compact,
        output.clone(),
    )?;
    Ok(crate::conditionals::Callback {
        expr,
        indices,
        schema: compact,
    })
}

/// Both Boolean values are covered; a missing base selects only `missing`.
/// Simple CASE owns the decision value so evaluating a null guard never repeats it.
fn boolean_case(
    base: Arc<dyn PhysicalExpr>,
    yes: Arc<dyn PhysicalExpr>,
    no: Arc<dyn PhysicalExpr>,
    missing: Arc<dyn PhysicalExpr>,
) -> Result<Arc<dyn PhysicalExpr>, ExprError> {
    let yes_test: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Boolean(Some(true))));
    let no_test: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Boolean(Some(false))));
    Ok(Arc::new(CaseExpr::try_new(
        Some(base),
        vec![(yes_test, yes), (no_test, no)],
        Some(missing),
    )?))
}

fn lower_uncached(
    expr: &Expr,
    schema: &Schema,
    globals: GlobalBinding<'_>,
    scope: &[(String, Type, usize)],
    memo: &mut CompilationMemo,
    context: usize,
) -> Result<Arc<dyn PhysicalExpr>, ExprError> {
    let dtype = expr
        .dtype_scoped(schema, globals.schema(), scope, memo, context)?
        .to_arrow()?;
    let output_type = dtype.clone();
    let result: Arc<dyn PhysicalExpr> = match expr {
        Expr::AlignedMapInput { axis, name, dtype } => {
            let column_name = match axis.as_str() {
                "entry" => Some(crate::aligned_matrix_map::ENTRY_COLUMN),
                "col" => Some(crate::aligned_matrix_map::COLUMN_COLUMN),
                "entry_present" => Some(crate::aligned_matrix_map::ENTRY_PRESENT_COLUMN),
                _ => None,
            };
            if let Some(column_name) = column_name {
                let index = match axis.as_str() {
                    "entry" => 0,
                    "col" => 1,
                    _ => 2,
                };
                Arc::new(Column::new(column_name, index))
            } else {
                Arc::new(crate::aligned_matrix_map::BoundInputExpr::unbound(
                    axis.clone(),
                    name.clone(),
                    dtype.to_arrow()?,
                ))
            }
        }
        Expr::AlignedMatrixMap {
            entries,
            columns,
            rows,
            globals: globals_expr,
            body,
            dtype,
            preserve_cell,
        } => {
            let entry_expr_type =
                entries.dtype_scoped(schema, globals.schema(), scope, memo, context)?;
            let column_expr_type =
                columns.dtype_scoped(schema, globals.schema(), scope, memo, context)?;
            let entries = lower_cached(entries, schema, globals, scope, memo, context)?;
            let columns = lower_cached(columns, schema, globals, scope, memo, context)?;
            let rows = lower_cached(rows, schema, globals, scope, memo, context)?;
            let global_inputs = lower_cached(globals_expr, schema, globals, scope, memo, context)?;
            let Type::Array { array: entry_type } = entry_expr_type else {
                return Err(ExprError::Invalid(
                    "aligned entry expression must be an array".into(),
                ));
            };
            let Type::Array { array: column_type } = column_expr_type else {
                return Err(ExprError::Invalid(
                    "aligned column expression must be an array".into(),
                ));
            };
            #[derive(Default)]
            struct ColumnDemand {
                all: bool,
                fields: std::collections::BTreeMap<String, ColumnDemand>,
            }
            impl ColumnDemand {
                fn insert(&mut self, path: &[String]) {
                    if self.all { return; }
                    if path.is_empty() { self.all = true; self.fields.clear(); }
                    else { self.fields.entry(path[0].clone()).or_default().insert(&path[1..]); }
                }
                fn projected(&self, ty: &Type) -> Type {
                    if self.all { return ty.clone(); }
                    // Logical types such as Locus/Call must retain their entire
                    // physical descriptor; only ordinary structs are pruned.
                    let Type::Struct { r#struct: fields } = ty else { return ty.clone(); };
                    Type::Struct { r#struct: fields.iter().filter_map(|(name, child)| {
                        self.fields.get(name).map(|demand| (name.clone(), demand.projected(child)))
                    }).collect() }
                }
            }
            fn column_path(expr: &Expr) -> Option<Vec<String>> {
                match expr {
                    Expr::AlignedMapInput { axis, .. } if axis == "col" => Some(vec![]),
                    Expr::GetField { value, name } => {
                        let mut path = column_path(value)?;
                        path.push(name.clone()); Some(path)
                    }
                    _ => None,
                }
            }
            fn column_demand(expr: &Expr, demand: &mut ColumnDemand, seen: &mut HashSet<usize>) {
                if !seen.insert(expr as *const Expr as usize) { return; }
                if let Some(path) = column_path(expr) { demand.insert(&path); }
                else { expr.visit_children(&mut |child| column_demand(child, demand, seen)); }
            }
            let mut demand = ColumnDemand::default();
            column_demand(body, &mut demand, &mut HashSet::new());
            let column_dtype = if demand.all || !demand.fields.is_empty() {
                Some(demand.projected(&column_type).to_arrow()?)
            } else { None };
            let body_column_dtype = column_dtype.clone().unwrap_or_else(|| DataType::Struct(arrow_schema::Fields::empty()));
            let mut body_fields = vec![
                Field::new(
                    crate::aligned_matrix_map::ENTRY_COLUMN,
                    entry_type.to_arrow()?,
                    true,
                ),
                Field::new(
                    crate::aligned_matrix_map::COLUMN_COLUMN,
                    body_column_dtype,
                    true,
                ),
                Field::new(
                    crate::aligned_matrix_map::ENTRY_PRESENT_COLUMN,
                    DataType::Boolean,
                    false,
                ),
            ];
            let body_dtype = body
                .dtype_scoped(schema, globals.schema(), scope, memo, context)?
                .to_arrow()?;
            // Declare batch captures before lowering so nested fold/lambda/
            // conditional callbacks include these columns in their own schemas.
            let mut captures = std::collections::BTreeMap::new();
            body.visit_unique(&mut |node| {
                if let Expr::AlignedMapInput { axis, name, dtype } = node {
                    if !matches!(axis.as_str(), "entry" | "col" | "entry_present") {
                        captures.insert((axis.clone(), name.clone()), dtype.clone());
                    }
                }
            });
            let mut names = std::collections::BTreeMap::new();
            let mut capture_keys = Vec::new();
            for (key, ty) in captures {
                let name = format!("__vail_aligned_bound_{}", body_fields.len());
                body_fields.push(Field::new(&name, ty.to_arrow()?, true));
                capture_keys.push(key.clone());
                names.insert(key, name);
            }
            fn rewrite_batch_inputs(
                expr: &Expr,
                names: &std::collections::BTreeMap<(String, Option<String>), String>,
                memo: &mut std::collections::HashMap<usize, Expr>,
            ) -> Result<Expr, ExprError> {
                let id = expr as *const Expr as usize;
                if let Some(cached) = memo.get(&id) {
                    return Ok(cached.clone());
                }
                let rewritten = if let Expr::AlignedMapInput { axis, name, .. } = expr {
                    let column = match axis.as_str() {
                        "entry" => crate::aligned_matrix_map::ENTRY_COLUMN.to_owned(),
                        "col" => crate::aligned_matrix_map::COLUMN_COLUMN.to_owned(),
                        "entry_present" => {
                            crate::aligned_matrix_map::ENTRY_PRESENT_COLUMN.to_owned()
                        }
                        _ => names
                            .get(&(axis.clone(), name.clone()))
                            .cloned()
                            .ok_or_else(|| {
                                ExprError::Invalid("missing aligned batch capture".into())
                            })?,
                    };
                    Expr::Column { name: column }
                } else {
                    expr.try_map_original_children(|child| rewrite_batch_inputs(child, names, memo))?
                };
                memo.insert(id, rewritten.clone());
                Ok(rewritten)
            }
            let body = rewrite_batch_inputs(body, &names, &mut Default::default())?;
            let body_schema = Schema::new(body_fields);
            let body_context = memo.context(&body_schema, globals.schema(), scope);
            let body = lower_synthetic(body, &body_schema, globals, scope, memo, body_context)?;
            Arc::new(crate::aligned_matrix_map::AlignedMatrixMapExpr::new(
                entries,
                columns,
                rows,
                global_inputs,
                body,
                capture_keys,
                column_dtype,
                dtype.to_arrow()?,
                body_dtype,
                *preserve_cell,
            ))
        }
        Expr::MatrixScope { .. }
        | Expr::TableLookup { .. }
        | Expr::MatrixField { .. }
        | Expr::MatrixNumCols { .. }
        | Expr::MatrixColumnPosition { .. } => {
            return Err(ExprError::Invalid(
                "unresolved frontend expression scope".into(),
            ));
        }
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
        Expr::LambdaVariable { name, .. } => {
            let (_, ty, index) =
                scope
                    .iter()
                    .rev()
                    .find(|(n, _, _)| n == name)
                    .ok_or_else(|| {
                        ExprError::Invalid(format!(
                            "unbound lambda variable {name:?} in physical lowering"
                        ))
                    })?;
            Arc::new(datafusion_physical_expr::expressions::LambdaVariable::new(
                *index,
                Arc::new(Field::new(name, ty.to_arrow()?, true)),
            ))
        }
        Expr::NestedFunction { function, args, .. } if function == "coalesce" => {
            let base = lower_scoped(&args[0], schema, globals, scope, memo)?;
            let fallback = lower_scoped(&args[1], schema, globals, scope, memo)?;
            let yes = Arc::new(Literal::new(ScalarValue::Boolean(Some(true))));
            let no = Arc::new(Literal::new(ScalarValue::Boolean(Some(false))));
            boolean_case(base, yes, no, fallback)?
        }
        Expr::NestedFunction {
            function,
            args,
            dtype: target,
        } => {
            let special = if function == "__vail_parse_json" {
                Some(crate::json_io::parse_udf(target.clone())?)
            } else if matches!(
                function.as_str(),
                "__vail_export_annotation" | "__vail_json_annotation"
            ) {
                Some(crate::json_io::export_udf(
                    args[0].dtype_scoped(schema, globals.schema(), scope, memo, context)?,
                    function == "__vail_json_annotation",
                )?)
            } else {
                None
            };
            let args = args
                .iter()
                .map(|arg| {
                    let value = lower_cached(arg, schema, globals, scope, memo, context)?;
                    let ty = arg
                        .dtype_scoped(schema, globals.schema(), scope, memo, context)?
                        .to_arrow()?;
                    // Indexing constructs a logical tuple containing the original
                    // element. Its UDF must retain opaque logical descriptors
                    // (for example Call), rather than reinterpret their storage.
                    let input_type = if function == "__vail_index_values" {
                        ty
                    } else {
                        vail_ordering::without_type_metadata(&ty)
                    };
                    Ok(cast(value, schema, input_type)?)
                })
                .collect::<Result<Vec<_>, ExprError>>()?;
            if let Some(function) = function.strip_prefix("random_") {
                return Ok(Arc::new(HailPhysicalExpr {
                    kind: HailKind::Random(function.into()),
                    prepared_reference: Default::default(),
                    args,
                    dtype,
                }));
            }
            let function = Arc::new(ScalarFunctionExpr::try_new(
                match special {
                    Some(udf) => Arc::new(udf),
                    None => nested_function(function)?,
                },
                args,
                schema,
                Arc::new(ConfigOptions::default()),
            )?);
            cast(function, schema, dtype.clone())?
        }
        Expr::CollectionCast { value, .. } => cast(
            lower_cached(value, schema, globals, scope, memo, context)?,
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
                lower_cached(left, schema, globals, scope, memo, context)?,
                operator,
                lower_cached(right, schema, globals, scope, memo, context)?,
                schema,
            )?
        }
        Expr::StringFunction { function, args } => {
            let (udf, _, _) = string_function(function)?;
            let args = args
                .iter()
                .map(|arg| lower_cached(arg, schema, globals, scope, memo, context))
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
                    args.push(lower_cached(
                        element, schema, globals, scope, memo, context,
                    )?);
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
                value.dtype_scoped(schema, globals.schema(), scope, memo, context)?,
                Type::Tuple { .. }
            ) =>
        {
            let args: Vec<Arc<dyn PhysicalExpr>> = vec![
                lower_cached(value, schema, globals, scope, memo, context)?,
                Arc::new(Literal::new(ScalarValue::Utf8(Some(name.clone())))),
            ];
            Arc::new(ScalarFunctionExpr::try_new(
                datafusion_functions::core::get_field(),
                args,
                schema,
                Arc::new(ConfigOptions::default()),
            )?)
        }
        Expr::ArrayAggregateScan {
            function,
            value,
            predicate,
            options,
            ..
        } => {
            let input = value
                .dtype_scoped(schema, globals.schema(), scope, memo, context)?
                .to_arrow()?;
            Arc::new(ScalarFunctionExpr::try_new(
                crate::array_aggregate_scan::make_udf(function, &input, options)?,
                vec![
                    lower_cached(value, schema, globals, scope, memo, context)?,
                    lower_cached(predicate, schema, globals, scope, memo, context)?,
                ],
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
            let Type::Array { array } =
                value.dtype_scoped(schema, globals.schema(), scope, memo, context)?
            else {
                unreachable!()
            };
            let accumulator_type =
                zero.dtype_scoped(schema, globals.schema(), scope, memo, context)?;
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
                lower_cached(value, schema, globals, scope, memo, context)?,
                lower_cached(zero, schema, globals, scope, memo, context)?,
                lower_scoped(body, &nested_schema, globals, &nested, memo)?,
                nested_schema,
                dtype.clone(),
                *scan,
            ))
        }
        Expr::ArrayMap {
            value,
            parameter,
            body,
        } => lower_array_lambda(
            value, parameter, body, "map", schema, globals, scope, memo, context,
        )?,
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
            memo,
            context,
        )?,
        Expr::ArrayFilter {
            value,
            parameter,
            predicate,
        } => lower_array_lambda(
            value, parameter, predicate, "filter", schema, globals, scope, memo, context,
        )?,
        Expr::StreamRange { start, stop, step } => {
            let args = [start, stop, step]
                .iter()
                .map(|arg| lower_cached(arg, schema, globals, scope, memo, context))
                .collect::<Result<Vec<_>, ExprError>>()?;
            Arc::new(ScalarFunctionExpr::try_new(
                crate::numeric_lists::range_udf(),
                args,
                schema,
                Arc::new(ConfigOptions::default()),
            )?)
        }
        Expr::ArrayGrouped { value, size } => Arc::new(ScalarFunctionExpr::try_new(
            crate::collection_grouped::udf(),
            vec![
                lower_scoped(value, schema, globals, scope, memo)?,
                lower_scoped(size, schema, globals, scope, memo)?,
            ],
            schema,
            Arc::new(ConfigOptions::default()),
        )?),

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
        } if matches!(value.dtype_in_scope(schema,globals.schema(),scope, memo)?, Type::Struct { r#struct: fields } if fields.iter().any(|(name,_)| name=="__vail_call_alleles")) => {
            custom(
                HailKind::CallConstruct,
                vec![lower_scoped(value, schema, globals, scope, memo)?],
                dtype,
            )
        }
        Expr::GetField { value, name }
            if value.dtype_in_scope(schema, globals.schema(), scope, memo)?
                == Type::Primitive(PrimitiveType::Call) =>
        {
            let accessor = crate::calls::CallAccessor::for_field(name)
                .ok_or_else(|| ExprError::Invalid(format!("invalid Call field {name:?}")))?;
            custom(
                HailKind::CallAccess(accessor),
                vec![lower_scoped(value, schema, globals, scope, memo)?],
                dtype,
            )
        }
        Expr::Cast { dtype, value } => lower_cast(
            lower_cached(value, schema, globals, scope, memo, context)?,
            schema,
            &dtype.to_arrow()?,
        )?,
        Expr::ToString { value } => {
            let input_type = value.dtype_scoped(schema, globals.schema(), scope, memo, context)?;
            let child = lower_cached(value, schema, globals, scope, memo, context)?;
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
                    custom(HailKind::ToString(input_type), vec![child], DataType::Utf8)
                }
            }
        }
        Expr::IsMissing { value } => Arc::new(IsNullExpr::new(lower_cached(
            value, schema, globals, scope, memo, context,
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
                        let value = lower_cached(e, schema, globals, scope, memo, context)?;
                        let ty = e
                            .dtype_scoped(schema, globals.schema(), scope, memo, context)?
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
            let child = lower_cached(value, schema, globals, scope, memo, context)?;
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
            let child = lower_cached(value, schema, globals, scope, memo, context)?;
            let udf = Arc::new(ScalarFunctionExpr::try_new(
                datafusion_functions_nested::length::array_length_udf(),
                vec![child],
                schema,
                Arc::new(ConfigOptions::default()),
            )?);
            cast(udf, schema, dtype.clone())?
        }
        Expr::ArrayIndex { value, index } => {
            // Python bind uses map([argument], parameter => body)[0]. There
            // is exactly one callback value per input row, so bind it directly
            // instead of building, flattening and gathering singleton lists.
            let zero_index = match index.as_ref() {
                // The Python frontend retains data literals as Arrow scalars.
                Expr::RuntimeLiteral { value: ScalarValue::Int32(Some(0)) } => true,
                Expr::Literal { value, .. } => value.as_i64() == Some(0),
                _ => false,
            };
            let singleton = match value.as_ref() {
                Expr::ArrayMap { value, parameter, body } if zero_index => match value.as_ref() {
                    Expr::Array { elements, .. } if elements.len() == 1 => {
                        Some((&elements[0], parameter, body))
                    }
                    _ => None,
                },
                _ => None,
            };
            if let Some((argument, parameter, body)) = singleton {
                lower_synthetic(
                    Expr::Switch {
                        value: Arc::new(argument.clone()),
                        parameter: parameter.clone(),
                        cases: Vec::new(),
                        default: body.clone(),
                        missing: None,
                    },
                    schema,
                    globals,
                    scope,
                    memo,
                    context,
                )?
            } else {
                custom(
                    HailKind::ArrayIndex,
                    vec![
                        lower_cached(value, schema, globals, scope, memo, context)?,
                        lower_cached(index, schema, globals, scope, memo, context)?,
                    ],
                    dtype,
                )
            }
        }
        Expr::ArraySlice {
            value,
            start,
            stop,
            step,
        } => {
            let mut args = vec![lower_cached(value, schema, globals, scope, memo, context)?];
            for e in [start, stop, step].into_iter().flatten() {
                args.push(lower_cached(e, schema, globals, scope, memo, context)?);
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
            vec![lower_cached(value, schema, globals, scope, memo, context)?],
            dtype,
        ),
        Expr::GetField { value, name } => {
            let invert = name == "includes_start"
                && matches!(
                    value.dtype_scoped(schema, globals.schema(), scope, memo, context)?,
                    Type::Interval { .. }
                );
            let field = custom(
                HailKind::GetField(
                    match value.dtype_scoped(schema, globals.schema(), scope, memo, context)? {
                        Type::Interval { .. } => match name.as_str() {
                            "start" => "__vail_interval_start".into(),
                            "end" => "__vail_interval_end".into(),
                            "includes_start" => "__vail_interval_start_exclusive".into(),
                            "includes_end" => "__vail_interval_end_inclusive".into(),
                            _ => name.clone(),
                        },
                        _ => name.clone(),
                    },
                ),
                vec![lower_cached(value, schema, globals, scope, memo, context)?],
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
                lower_cached(start, schema, globals, scope, memo, context)?,
                lower_cached(end, schema, globals, scope, memo, context)?,
                lower_cached(includes_start, schema, globals, scope, memo, context)?,
                lower_cached(includes_end, schema, globals, scope, memo, context)?,
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
                reference: Arc::new(reference.clone()),
            },
            vec![
                lower_cached(contig, schema, globals, scope, memo, context)?,
                lower_cached(position, schema, globals, scope, memo, context)?,
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
                reference: Arc::new(reference.clone()),
            },
            vec![
                lower_cached(contig, schema, globals, scope, memo, context)?,
                lower_cached(position, schema, globals, scope, memo, context)?,
            ],
            dtype,
        ),
        Expr::LocusFromGlobalPosition {
            reference,
            position,
        } => custom(
            HailKind::Locus {
                operation: "from_global".into(),
                reference: Arc::new(reference.clone()),
            },
            vec![lower_cached(
                position, schema, globals, scope, memo, context,
            )?],
            dtype,
        ),
        Expr::ParseLocus { reference, value } => custom(
            HailKind::Locus {
                operation: "parse".into(),
                reference: Arc::new(reference.clone()),
            },
            vec![lower_cached(value, schema, globals, scope, memo, context)?],
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
            let child = lower_cached(value, schema, globals, scope, memo, context)?;
            let function = Arc::new(ScalarFunctionExpr::try_new(
                udf,
                vec![child],
                schema,
                Arc::new(ConfigOptions::default()),
            )?);
            cast(function, schema, dtype.clone())?
        }
        Expr::LocusOp { operation, value } => {
            let Type::Locus { locus } =
                value.dtype_scoped(schema, globals.schema(), scope, memo, context)?
            else {
                unreachable!()
            };
            custom(
                HailKind::Locus {
                    operation: operation.clone(),
                    reference: Arc::new(locus),
                },
                vec![lower_cached(value, schema, globals, scope, memo, context)?],
                dtype,
            )
        }
        Expr::IntervalContains { interval, point } => {
            let Type::Interval {
                interval: point_type,
            } = interval.dtype_scoped(schema, globals.schema(), scope, memo, context)?
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
            let start = lower_synthetic(get("start"), schema, globals, scope, memo, context)?;
            let end = lower_synthetic(get("end"), schema, globals, scope, memo, context)?;
            let point = lower_cached(point, schema, globals, scope, memo, context)?;
            let includes_start =
                lower_synthetic(get("includes_start"), schema, globals, scope, memo, context)?;
            let includes_end =
                lower_synthetic(get("includes_end"), schema, globals, scope, memo, context)?;
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
            } = left.dtype_scoped(schema, globals.schema(), scope, memo, context)?
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
                value: Arc::new(value.clone()),
                name: name.into(),
            };
            let l_start =
                lower_synthetic(get(left, "start"), schema, globals, scope, memo, context)?;
            let l_end = lower_synthetic(get(left, "end"), schema, globals, scope, memo, context)?;
            let r_start =
                lower_synthetic(get(right, "start"), schema, globals, scope, memo, context)?;
            let r_end = lower_synthetic(get(right, "end"), schema, globals, scope, memo, context)?;
            let l_inc_start = lower_synthetic(
                get(left, "includes_start"),
                schema,
                globals,
                scope,
                memo,
                context,
            )?;
            let l_inc_end = lower_synthetic(
                get(left, "includes_end"),
                schema,
                globals,
                scope,
                memo,
                context,
            )?;
            let r_inc_start = lower_synthetic(
                get(right, "includes_start"),
                schema,
                globals,
                scope,
                memo,
                context,
            )?;
            let r_inc_end = lower_synthetic(
                get(right, "includes_end"),
                schema,
                globals,
                scope,
                memo,
                context,
            )?;
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
                .map(|(_, e)| lower_cached(e, schema, globals, scope, memo, context))
                .collect::<Result<Vec<_>, _>>()?,
            dtype,
        ),
        Expr::Unary { operator, value } => {
            let child = lower_cached(value, schema, globals, scope, memo, context)?;
            match operator {
                UnaryOperator::Pos => child,
                UnaryOperator::Neg => match &dtype {
                    DataType::Int32 => binary(
                        Arc::new(Literal::new(ScalarValue::Int32(Some(0)))),
                        Operator::Minus,
                        child,
                        schema,
                    )?,
                    DataType::Int64 => binary(
                        Arc::new(Literal::new(ScalarValue::Int64(Some(0)))),
                        Operator::Minus,
                        child,
                        schema,
                    )?,
                    _ => Arc::new(NegativeExpr::new(child)),
                },
                UnaryOperator::Not => Arc::new(NotExpr::new(child)),
            }
        }
        Expr::Binary {
            operator,
            left,
            right,
        } => {
            let lt = left.dtype_scoped(schema, globals.schema(), scope, memo, context)?;
            let rt = right.dtype_scoped(schema, globals.schema(), scope, memo, context)?;
            let mut l = lower_cached(left, schema, globals, scope, memo, context)?;
            let mut r = lower_cached(right, schema, globals, scope, memo, context)?;
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
            vec![lower_cached(
                message, schema, globals, scope, memo, context,
            )?],
            dtype.clone(),
        ),
        Expr::Switch {
            value,
            parameter,
            cases,
            default,
            missing,
        } => {
            // Capture only fields used anywhere in this switch. In particular,
            // a surrounding array's payload must not become an implicit capture
            // merely because it occurs in the enclosing lambda's input schema.
            let indices = switch_capture_indices(expr, schema, globals, scope);
            let inputs = indices
                .iter()
                .map(|&index| {
                    let field = schema.fields()[index].clone();
                    if scope.iter().any(|(_, _, at)| *at == index) {
                        Arc::new(datafusion_physical_expr::expressions::LambdaVariable::new(
                            index, field,
                        )) as Arc<dyn PhysicalExpr>
                    } else {
                        Arc::new(Column::new(field.name(), index)) as Arc<dyn PhysicalExpr>
                    }
                })
                .collect();
            let remap = indices
                .iter()
                .enumerate()
                .map(|(new, old)| (*old, new))
                .collect::<std::collections::BTreeMap<_, _>>();
            let input_schema = Arc::new(Schema::new(
                indices
                    .iter()
                    .map(|&index| schema.field(index).clone())
                    .collect::<Vec<_>>(),
            ));
            let parent_scope = scope
                .iter()
                .filter_map(|(name, ty, index)| {
                    remap.get(index).map(|&at| (name.clone(), ty.clone(), at))
                })
                .collect::<Vec<_>>();
            let bindings = match globals {
                GlobalBinding::Columns(_, bindings) => bindings
                    .iter()
                    .filter_map(|(name, index)| remap.get(index).map(|&at| (name.clone(), at)))
                    .collect(),
                _ => std::collections::BTreeMap::new(),
            };
            let globals = match globals {
                GlobalBinding::Columns(schema, _) => GlobalBinding::Columns(schema, &bindings),
                other => other,
            };
            let schema = input_schema.as_ref();
            let scope = parent_scope.as_slice();
            let selector_type = value.dtype_in_scope(schema, globals.schema(), scope, memo)?;
            // Canonicalize storage once for the bound column. Utf8 views may
            // materialize here; comparisons never repeat that conversion.
            let selector = cast(
                lower_scoped(value, schema, globals, scope, memo)?,
                schema,
                selector_type.to_arrow()?,
            )?;
            let mut fields = schema.fields().to_vec();
            fields.push(Arc::new(Field::new(
                parameter,
                selector_type.to_arrow()?,
                true,
            )));
            let bound_schema = Arc::new(Schema::new(fields));
            let mut nested = scope.to_vec();
            nested.push((parameter.clone(), selector_type, schema.fields().len()));
            let cases = cases
                .iter()
                .map(|(condition, result)| {
                    Ok((
                        switch_callback(
                            condition,
                            &bound_schema,
                            globals,
                            &nested,
                            &DataType::Boolean,
                            memo,
                        )?,
                        switch_callback(result, &bound_schema, globals, &nested, &dtype, memo)?,
                    ))
                })
                .collect::<Result<Vec<_>, ExprError>>()?;
            let default = switch_callback(default, &bound_schema, globals, &nested, &dtype, memo)?;
            let missing = missing
                .as_ref()
                .map(|missing| {
                    switch_callback(missing, &bound_schema, globals, &nested, &dtype, memo)
                })
                .transpose()?;
            Arc::new(crate::conditionals::ScopedSwitch {
                inputs,
                value: selector,
                cases,
                default,
                missing,
                input_schema,
                bound_schema,
                dtype,
            })
        }

        Expr::IfElse {
            condition,
            consequent,
            alternate,
        } => {
            let cond = lower_cached(condition, schema, globals, scope, memo, context)?;
            let yes = lower_cast(
                lower_cached(consequent, schema, globals, scope, memo, context)?,
                schema,
                &dtype,
            )?;
            let no = lower_cast(
                lower_cached(alternate, schema, globals, scope, memo, context)?,
                schema,
                &dtype,
            )?;
            let missing: Arc<dyn PhysicalExpr> =
                Arc::new(Literal::new(ScalarValue::try_from(&dtype)?));
            let true_value: Arc<dyn PhysicalExpr> =
                Arc::new(Literal::new(ScalarValue::Boolean(Some(true))));
            let false_value: Arc<dyn PhysicalExpr> =
                Arc::new(Literal::new(ScalarValue::Boolean(Some(false))));
            Arc::new(CaseExpr::try_new(
                Some(cond),
                vec![(true_value, yes), (false_value, no)],
                Some(missing),
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
    #[test]
    fn singleton_bind_uses_a_lexical_frame_with_arrow_literal_index() {
        let batch = source(vec![Some(2), None, Some(4)]);
        let int32 = Type::Primitive(PrimitiveType::Int32);
        for index in [
            Expr::RuntimeLiteral { value: ScalarValue::Int32(Some(0)) },
            Expr::Literal { dtype: int32.clone(), value: json!(0) },
        ] {
            let variable = Arc::new(Expr::LambdaVariable { name: "bound".into(), dtype: None });
            let expression = Expr::ArrayIndex {
                value: Arc::new(Expr::ArrayMap {
                    value: Arc::new(Expr::Array {
                        element_type: int32.clone(),
                        elements: vec![Expr::Column { name: "x".into() }],
                    }),
                    parameter: "bound".into(),
                    body: Arc::new(Expr::Binary {
                        operator: BinaryOperator::Add,
                        left: variable.clone(),
                        right: variable,
                    }),
                }),
                index: Arc::new(index),
            };
            let compiled = lower(&expression, batch.schema().as_ref()).unwrap();
            // Semantics alone also pass through the slow singleton-list path.
            // Assert that the real Arrow-literal frontend form bypasses it.
            assert_eq!(compiled.to_string(), "ScopedSwitch");
            let output = compiled.evaluate(&batch).unwrap().into_array(3).unwrap();
            let output = output.as_any().downcast_ref::<Int32Array>().unwrap();
            assert_eq!(output.iter().collect::<Vec<_>>(), vec![Some(4), None, Some(8)]);
        }
    }

    fn expr(v: Value) -> Expr {
        serde_json::from_value(v).unwrap()
    }

    #[test]
    fn public_compilation_accepts_deep_shared_native_graphs() {
        let graph = |depth| {
            let mut expression = Arc::new(Expr::Column {
                name: "value".into(),
            });
            for _ in 0..depth {
                expression = Arc::new(Expr::Binary {
                    operator: BinaryOperator::Add,
                    left: expression.clone(),
                    right: expression,
                });
            }
            expression
        };
        let schema = Schema::new(vec![Field::new("value", DataType::Int64, false)]);
        // Keep the real 64-level graph. Run this public API test under an
        // external resource/time bound; never evaluate its exponential result.
        let expression = graph(64);
        assert_eq!(
            expression.dtype(&schema).unwrap(),
            Type::Primitive(PrimitiveType::Int64)
        );
        let compiled = lower(&expression, &schema).unwrap();
        assert_eq!(compiled.data_type(&schema).unwrap(), DataType::Int64);
        assert!(!compiled.nullable(&schema).unwrap());
        let logical_schema = datafusion_common::DFSchema::try_from(schema.clone()).unwrap();
        let logical = crate::logical_expr::lower_logical(&expression, &logical_schema).unwrap();
        use datafusion_expr::ExprSchemable;
        assert_eq!(logical.get_type(&logical_schema).unwrap(), DataType::Int64);
        let planned = datafusion_physical_expr::planner::create_physical_expr(
            &logical,
            &logical_schema,
            &datafusion_physical_expr::execution_props::ExecutionProps::new(),
            &datafusion_expr::physical_planning_context::PhysicalPlanningContext::default(),
        )
        .unwrap();
        assert_eq!(planned.data_type(&schema).unwrap(), DataType::Int64);
        // Actual execution remains covered at a modest size independently of
        // the large compilation/resource contract.
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(Int64Array::from(vec![2, 3]))],
        )
        .unwrap();
        let result = lower(&graph(8), &schema)
            .unwrap()
            .evaluate(&batch)
            .unwrap()
            .into_array(2)
            .unwrap();
        assert_eq!(result.as_ref(), &Int64Array::from(vec![512, 768]));
        let logical = crate::logical_expr::lower_logical(&graph(8), &logical_schema).unwrap();
        let planned = datafusion_physical_expr::planner::create_physical_expr(
            &logical,
            &logical_schema,
            &datafusion_physical_expr::execution_props::ExecutionProps::new(),
            &datafusion_expr::physical_planning_context::PhysicalPlanningContext::default(),
        )
        .unwrap();
        let logical_result = planned.evaluate(&batch).unwrap().into_array(2).unwrap();
        assert_eq!(logical_result.as_ref(), result.as_ref());
    }

    #[test]
    fn public_physical_metadata_queries_follow_actual_input_schema() {
        let value = Arc::new(Expr::Column {
            name: "value".into(),
        });
        let expression = Expr::Binary {
            operator: BinaryOperator::Add,
            left: value.clone(),
            right: value,
        };
        let integer = Schema::new(vec![Field::new("value", DataType::Int64, false)]);
        let float = Schema::new(vec![Field::new("value", DataType::Float64, true)]);
        let compiled = lower(&expression, &integer).unwrap();
        assert_eq!(compiled.data_type(&float).unwrap(), DataType::Float64);
        assert!(compiled.nullable(&float).unwrap());
        assert_eq!(compiled.data_type(&integer).unwrap(), DataType::Int64);
        assert!(!compiled.nullable(&integer).unwrap());
        let batch = RecordBatch::try_new(
            Arc::new(float),
            vec![Arc::new(Float64Array::from(vec![Some(2.5), None]))],
        )
        .unwrap();
        let result = compiled.evaluate(&batch).unwrap().into_array(2).unwrap();
        assert_eq!(result.as_ref(), &Float64Array::from(vec![Some(5.0), None]));
    }

    #[test]
    fn public_lambda_projection_rebinds_heterogeneous_source_columns() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new(
                    "unused",
                    DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                    true,
                ),
                Field::new("target", DataType::Int32, false),
            ])),
            vec![
                Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                    Some(vec![Some(9), Some(8)]),
                    Some(vec![]),
                ])),
                Arc::new(Int32Array::from(vec![1, 2])),
            ],
        )
        .unwrap();
        let expression = Expr::ArrayMap {
            value: Arc::new(Expr::Literal {
                dtype: Type::Array {
                    array: Box::new(Type::Primitive(PrimitiveType::Int32)),
                },
                value: json!([0, 1, 2]),
            }),
            parameter: "item".into(),
            body: Arc::new(Expr::Binary {
                operator: BinaryOperator::Eq,
                left: Arc::new(Expr::LambdaVariable {
                    name: "item".into(),
                    dtype: None,
                }),
                right: Arc::new(Expr::Column {
                    name: "target".into(),
                }),
            }),
        };
        let result = lower(&expression, batch.schema_ref())
            .unwrap()
            .evaluate(&batch)
            .unwrap()
            .into_array(2)
            .unwrap();
        let expected = literal_array(
            &json!([false, true, false]),
            &Type::Array {
                array: Box::new(Type::Primitive(PrimitiveType::Bool)),
            },
            1,
        )
        .unwrap();
        let second = literal_array(
            &json!([false, false, true]),
            &Type::Array {
                array: Box::new(Type::Primitive(PrimitiveType::Bool)),
            },
            1,
        )
        .unwrap();
        let expected = arrow_select::concat::concat(&[expected.as_ref(), second.as_ref()]).unwrap();
        assert_eq!(result.as_ref(), expected.as_ref());
    }

    #[test]
    fn memo_keeps_the_same_native_node_distinct_under_different_lexical_bindings() {
        let variable = Arc::new(Expr::LambdaVariable {
            name: "item".into(),
            dtype: None,
        });
        let map = |dtype: Type, values: Value| Expr::ArrayMap {
            value: Arc::new(Expr::Literal {
                dtype: Type::Array {
                    array: Box::new(dtype),
                },
                value: values,
            }),
            parameter: "item".into(),
            body: variable.clone(),
        };
        let expr = Expr::Struct {
            fields: vec![
                (
                    "numbers".into(),
                    map(Type::Primitive(PrimitiveType::Int32), json!([1, 2])),
                ),
                (
                    "strings".into(),
                    map(Type::Primitive(PrimitiveType::Str), json!(["a", "b"])),
                ),
            ],
        };
        let schema = Schema::empty();
        let expected = Type::Struct {
            r#struct: vec![
                (
                    "numbers".into(),
                    Type::Array {
                        array: Box::new(Type::Primitive(PrimitiveType::Int32)),
                    },
                ),
                (
                    "strings".into(),
                    Type::Array {
                        array: Box::new(Type::Primitive(PrimitiveType::Str)),
                    },
                ),
            ],
        };
        assert_eq!(expr.dtype(&schema).unwrap(), expected);
        let batch = RecordBatch::try_new_with_options(
            Arc::new(schema.clone()),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(1)),
        )
        .unwrap();
        let compiled = lower(&expr, &schema).unwrap();
        let values = compiled.evaluate(&batch).unwrap().into_array(1).unwrap();
        assert_eq!(values.data_type(), &expected.to_arrow().unwrap());
        let fields = values.as_any().downcast_ref::<StructArray>().unwrap();
        let numbers = fields
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .value(0);
        let strings = fields
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .value(0);
        assert_eq!(numbers.as_ref(), &Int32Array::from(vec![1, 2]));
        assert_eq!(strings.as_ref(), &StringArray::from(vec!["a", "b"]));
    }

    #[test]
    fn public_grouped_arrays_keep_independent_generated_expressions() {
        let grouped = |values: Value, n| Expr::ArrayGrouped {
            value: Arc::new(Expr::Literal {
                dtype: Type::Array {
                    array: Box::new(Type::Primitive(PrimitiveType::Int32)),
                },
                value: values,
            }),
            size: Arc::new(Expr::RuntimeLiteral {
                value: ScalarValue::Int32(Some(n)),
            }),
        };
        let expression = Expr::Struct {
            fields: vec![
                ("first".into(), grouped(json!([1, 2, 3]), 2)),
                ("second".into(), grouped(json!([8, 9]), 1)),
            ],
        };
        let schema = Schema::empty();
        let batch = RecordBatch::try_new_with_options(
            Arc::new(schema.clone()),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(1)),
        )
        .unwrap();
        let actual = lower(&expression, &schema)
            .unwrap()
            .evaluate(&batch)
            .unwrap()
            .into_array(1)
            .unwrap();
        let expected = literal_array(
            &json!({"first":[[1,2],[3]],"second":[[8],[9]]}),
            &expression.dtype(&schema).unwrap(),
            1,
        )
        .unwrap();
        assert_eq!(actual.as_ref(), expected.as_ref());
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
            value: Arc::new(Expr::Column {
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
            value: Arc::new(Expr::Column {
                name: "line".into(),
            }),
            delimiter: ",".into(),
            quote: Some("\"".into()),
            missing: vec![],
            expected_fields: 3,
        };
        let pipe = Expr::ParseCsvLine {
            value: Arc::new(Expr::Column {
                name: "line".into(),
            }),
            delimiter: "|".into(),
            quote: Some("\"".into()),
            missing: vec![],
            expected_fields: 3,
        };
        let comma_result = project_batch(&batch, &[("fields".into(), comma)]).unwrap();
        assert_eq!(comma_result.num_rows(), 2);
        assert!(project_batch(&batch, &[("fields".into(), pipe)]).is_err());
    }

    fn eval_string(value_type: Type, value: Value) -> Option<String> {
        let batch = source(vec![Some(0)]);
        let expression = Expr::ToString {
            value: Arc::new(Expr::Literal {
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
        let prepared = reference.prepare().unwrap();
        let dtype = Type::Locus {
            locus: reference.clone(),
        }
        .to_arrow()
        .unwrap();
        let loci = locus_eval(
            "create",
            &reference,
            &prepared,
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
            &dtype,
            4,
        )
        .unwrap();
        let in_par = locus_eval(
            "in_y_par",
            &reference,
            &prepared,
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
            &prepared,
            &[Arc::new(Int64Array::from(vec![Some(199), Some(200), None]))],
            &dtype,
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
                &prepared,
                &[
                    Arc::new(StringArray::from(vec![Some("chrA")])) as ArrayRef,
                    Arc::new(Int32Array::from(vec![Some(200)]))
                ],
                &dtype,
                1
            )
            .is_err()
        );
        assert!(
            locus_eval(
                "from_global",
                &reference,
                &prepared,
                &[Arc::new(Int64Array::from(vec![Some(1000)]))],
                &dtype,
                1
            )
            .is_err()
        );
    }

    #[test]
    fn prepared_reference_cache_does_not_change_physical_expression_identity() {
        let reference = test_reference();
        let kind = HailKind::Locus {
            operation: "from_global".into(),
            reference: Arc::new(reference.clone()),
        };
        let args: Vec<Arc<dyn PhysicalExpr>> = vec![];
        let dtype = DataType::Int64;
        let left_cache = Arc::new(OnceLock::new());
        let right_cache = Arc::new(OnceLock::new());
        left_cache
            .set(Ok(Arc::new(reference.prepare().unwrap())))
            .unwrap();
        let left = HailPhysicalExpr {
            kind: kind.clone(),
            args: args.clone(),
            dtype: dtype.clone(),
            prepared_reference: left_cache,
        };
        let right = HailPhysicalExpr {
            kind,
            args,
            dtype,
            prepared_reference: right_cache,
        };
        assert_eq!(left, right);
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
            value: Arc::new(array),
            size: Arc::new(Expr::Literal {
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
            value: Arc::new(Expr::Column { name: "a".into() }),
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
            value: Arc::new(Expr::Literal {
                dtype: Type::Array {
                    array: Box::new(int32.clone()),
                },
                value: json!([1]),
            }),
            index: Arc::new(Expr::Literal {
                dtype: Type::Primitive(PrimitiveType::Float64),
                value: json!(0.0),
            }),
        };
        assert!(validate_projection(&empty.schema(), &[("bad".into(), bad_index)]).is_err());

        let bad_reduction = Expr::ArrayReduce {
            operation: ArrayReduction::Sum,
            filter_missing: true,
            value: Arc::new(Expr::Literal {
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
