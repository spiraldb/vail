//! Bind a native metadata program to row columns and explicit scalar operands.
use crate::{TableNode, TableRef, early_aligned_product::BoundRegion};
use arrow_array::{Array, RecordBatch, RecordBatchOptions};
use arrow_schema::{DataType, Field, Schema};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr::{
    PhysicalExpr,
    expressions::{Column, Literal},
    scalar_subquery::ScalarSubqueryExpr,
};
use std::{
    collections::HashMap,
    fmt,
    hash::{Hash, Hasher},
    sync::Arc,
};
use vail_execution::{
    expressions::{Expr, PreparationOptions, lower_with_options},
    native_frame::NativeFrame,
};

// Bound repeated frame-index preparation, not runtime rows or value buffers.
const MAX_METADATA_FRAME_INDEX_WORK: usize = 65_536;

/// Computed fields each rebuild row/global indices and external type bindings.
/// Direct columns and literals use the existing index-free preparation paths.
pub(crate) fn metadata_preparation_allowed<'a>(
    expressions: impl IntoIterator<Item = &'a Expr>,
    schema: &Schema,
    owner: &TableRef,
) -> bool {
    let computed = expressions.into_iter().filter(|expression| !matches!(
        expression, Expr::Column { .. } | Expr::Literal { .. } | Expr::RuntimeLiteral { .. }
    )).count();
    if computed == 0 { return true; }
    fn index_work(schema: &Schema) -> usize {
        schema.fields().iter().fold(0usize, |work, field| {
            work.saturating_add(1).saturating_add(field.name().len())
        })
    }
    let bindings = match owner.node() {
        TableNode::MapRows { bindings, .. } | TableNode::Filter { bindings, .. } => Some(bindings),
        _ => None,
    };
    let external = bindings.into_iter().flat_map(|bindings| bindings.iter())
        .fold(0usize, |work, (name, value)| {
            work.saturating_add(1).saturating_add(name.len())
                .saturating_add(index_work(value.schema()))
        });
    let context = 1usize.saturating_add(index_work(schema))
        .saturating_add(index_work(owner.globals().schema())).saturating_add(external);
    computed.saturating_mul(context) <= MAX_METADATA_FRAME_INDEX_WORK
}

/// Shared by all forwarded fields in one preparation stage.
pub(crate) struct Columns<'a> {
    indices: HashMap<&'a str, Option<usize>>,
}
impl<'a> Columns<'a> {
    pub(crate) fn new(schema: &'a Schema) -> Self {
        let mut indices = HashMap::with_capacity(schema.fields().len());
        for (index, field) in schema.fields().iter().enumerate() {
            indices
                .entry(field.name().as_str())
                .and_modify(|value| *value = None)
                .or_insert(Some(index));
        }
        Self { indices }
    }
    fn index(&self, name: &str, schema: &Schema) -> Result<usize> {
        let index = match self.indices.get(name) {
            Some(Some(index)) => *index,
            Some(None) => return Err(DataFusionError::Plan("ambiguous native column".into())),
            None => return Err(DataFusionError::Plan("native column absent".into())),
        };
        if schema
            .fields()
            .get(index)
            .is_none_or(|field| field.name() != name)
        {
            return Err(DataFusionError::Plan("native column schema changed".into()));
        }
        Ok(index)
    }
    fn column(&self, name: &str, schema: &Schema) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Column::new(name, self.index(name, schema)?)))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Cardinality {
    Scalar,
    Array,
}
fn cardinality(expression: &Arc<dyn PhysicalExpr>) -> Option<Cardinality> {
    if expression.is::<Column>() {
        Some(Cardinality::Array)
    } else if expression.is::<Literal>()
        || expression.is::<ScalarSubqueryExpr>()
        || expression.is::<GlobalField>()
    {
        Some(Cardinality::Scalar)
    } else {
        None
    }
}
fn nested(dtype: &DataType) -> bool {
    dtype.primitive_width().is_none() && !matches!(dtype, DataType::Boolean | DataType::Null)
}

#[derive(Debug, Hash)]
struct GlobalField {
    owner: Arc<dyn PhysicalExpr>,
    name: String,
    index: usize,
    dtype: DataType,
}
impl GlobalField {
    fn new(owner: Arc<dyn PhysicalExpr>, name: String, dtype: DataType) -> Result<Self> {
        if cardinality(&owner) != Some(Cardinality::Scalar) {
            return Err(DataFusionError::Plan(
                "global owner has unknown scalar cardinality".into(),
            ));
        }
        let DataType::Struct(fields) = owner.data_type(&Schema::empty())? else {
            return Err(DataFusionError::Plan("global owner is not a struct".into()));
        };
        let mut found = fields
            .iter()
            .enumerate()
            .filter(|(_, field)| field.name() == &name);
        let Some((index, field)) = found.next() else {
            return Err(DataFusionError::Plan("global field absent".into()));
        };
        if found.next().is_some() || field.data_type() != &dtype {
            return Err(DataFusionError::Plan(
                "global field descriptor changed".into(),
            ));
        }
        Ok(Self {
            owner,
            name,
            index,
            dtype,
        })
    }
}
impl PartialEq for GlobalField {
    fn eq(&self, other: &Self) -> bool {
        self.owner.eq(&other.owner)
            && self.name == other.name
            && self.index == other.index
            && self.dtype == other.dtype
    }
}
impl Eq for GlobalField {}
impl fmt::Display for GlobalField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "global.{}", self.name)
    }
}
impl PhysicalExpr for GlobalField {
    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
    fn data_type(&self, _: &Schema) -> Result<DataType> {
        Ok(self.dtype.clone())
    }
    fn nullable(&self, _: &Schema) -> Result<bool> {
        Ok(true)
    }
    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.owner]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let [owner] = children.as_slice() else {
            return Err(DataFusionError::Plan(
                "global field requires one scalar owner".into(),
            ));
        };
        Ok(Arc::new(Self::new(
            owner.clone(),
            self.name.clone(),
            self.dtype.clone(),
        )?))
    }
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let ColumnarValue::Scalar(ScalarValue::Struct(owner)) = self.owner.evaluate(batch)? else {
            return Err(DataFusionError::Execution(
                "global operand lost scalar cardinality".into(),
            ));
        };
        if owner.len() != 1 {
            return Err(DataFusionError::Execution(
                "global operand is not one scalar row".into(),
            ));
        }
        if owner.is_null(0) {
            return Ok(ColumnarValue::Scalar(ScalarValue::try_new_null(
                &self.dtype,
            )?));
        }
        let value = owner
            .columns()
            .get(self.index)
            .ok_or_else(|| DataFusionError::Execution("global field absent".into()))?;
        if value.data_type() != &self.dtype {
            return Err(DataFusionError::Execution(
                "global field descriptor changed".into(),
            ));
        }
        Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
            value.as_ref(),
            0,
        )?))
    }
}

/// Arguments retain their scalar/array domains; column metadata is not broadcast.
pub(crate) fn arguments(
    frame: &NativeFrame,
    schema: &Schema,
    region: &BoundRegion,
    columns: &Columns<'_>,
) -> Result<Vec<Arc<dyn PhysicalExpr>>> {
    frame
        .arguments
        .iter()
        .zip(frame.schema.fields())
        .map(|(reference, field)| {
            let value: Arc<dyn PhysicalExpr> = match reference {
                Expr::Column { name } => columns.column(name, schema)?,
                Expr::ExternalGlobal { reference, .. } => {
                    region.bindings.get(reference).cloned().ok_or_else(|| {
                        DataFusionError::Plan("external scalar owner absent".into())
                    })?
                }
                Expr::Global { name } => Arc::new(GlobalField::new(
                    region
                        .globals
                        .clone()
                        .ok_or_else(|| DataFusionError::Plan("scalar globals absent".into()))?,
                    name.clone(),
                    field.data_type().clone(),
                )?),
                _ => {
                    return Err(DataFusionError::Plan(
                        "non-reference native argument".into(),
                    ));
                }
            };
            if cardinality(&value).is_none() {
                return Err(DataFusionError::Plan(
                    "native argument has unknown cardinality".into(),
                ));
            }
            Ok(value)
        })
        .collect()
}

#[derive(Debug)]
struct BoundNative {
    // A retained program instance is its identity. Rewrites preserve this token
    // only while retaining the program and its argument contracts.
    token: Arc<()>,
    source_schema: Arc<Schema>,
    frame_schema: Arc<Schema>,
    program: Arc<dyn PhysicalExpr>,
    arguments: Vec<Arc<dyn PhysicalExpr>>,
    cardinalities: Vec<Cardinality>,
}
impl PartialEq for BoundNative {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.token, &other.token)
            && self.cardinalities == other.cardinalities
            && self.arguments == other.arguments
    }
}
impl Eq for BoundNative {}
impl Hash for BoundNative {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.token).hash(state);
        self.cardinalities.hash(state);
        self.arguments.hash(state);
    }
}
impl fmt::Display for BoundNative {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BoundMetadata")
    }
}
impl PhysicalExpr for BoundNative {
    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
    fn data_type(&self, _: &Schema) -> Result<DataType> {
        self.program.data_type(&self.frame_schema)
    }
    fn nullable(&self, _: &Schema) -> Result<bool> {
        self.program.nullable(&self.frame_schema)
    }
    fn return_field(&self, _: &Schema) -> Result<Arc<Field>> {
        self.program.return_field(&self.frame_schema)
    }
    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.arguments.iter().collect()
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if children.len() != self.frame_schema.fields().len()
            || children
                .iter()
                .zip(self.frame_schema.fields())
                .any(|(child, field)| {
                    child.data_type(&self.source_schema).ok().as_ref() != Some(field.data_type())
                })
            || children
                .iter()
                .zip(&self.cardinalities)
                .any(|(child, expected)| cardinality(child) != Some(*expected))
        {
            return Err(DataFusionError::Plan(
                "native private argument frame changed".into(),
            ));
        }
        Ok(Arc::new(Self {
            token: self.token.clone(),
            source_schema: self.source_schema.clone(),
            frame_schema: self.frame_schema.clone(),
            program: self.program.clone(),
            cardinalities: self.cardinalities.clone(),
            arguments: children,
        }))
    }
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let arguments = self
            .arguments
            .iter()
            .map(|argument| argument.evaluate(batch))
            .collect::<Result<Vec<_>>>()?;
        for (value, expected) in arguments.iter().zip(&self.cardinalities) {
            let actual = if matches!(value, ColumnarValue::Scalar(_)) {
                Cardinality::Scalar
            } else {
                Cardinality::Array
            };
            if actual != *expected {
                return Err(DataFusionError::Execution(
                    "native argument cardinality changed".into(),
                ));
            }
        }
        let scalar = arguments
            .iter()
            .all(|value| matches!(value, ColumnarValue::Scalar(_)));
        if !scalar
            && arguments.iter().any(|value| {
                matches!(value,
            ColumnarValue::Scalar(value) if nested(&value.data_type()))
            })
        {
            return Err(DataFusionError::Execution(
                "mixed native arguments would broadcast a nested scalar owner".into(),
            ));
        }
        let rows = if scalar { 1 } else { batch.num_rows() };
        let arrays = arguments
            .into_iter()
            .map(|value| value.into_array(rows))
            .collect::<Result<Vec<_>>>()?;
        let frame = RecordBatch::try_new_with_options(
            self.frame_schema.clone(),
            arrays,
            &RecordBatchOptions::new().with_row_count(Some(rows)),
        )?;
        let value = self.program.evaluate(&frame)?;
        if scalar {
            let array = value.into_array(1)?;
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                array.as_ref(),
                0,
            )?))
        } else {
            Ok(value)
        }
    }
}

pub(crate) fn prepare(
    expression: &Expr,
    schema: Arc<Schema>,
    region: &BoundRegion,
    options: &PreparationOptions,
    columns: &Columns<'_>,
) -> Result<Arc<dyn PhysicalExpr>> {
    if let Expr::Column { name } = expression {
        return columns.column(name, &schema);
    }
    if let Expr::RuntimeLiteral { value } = expression {
        return Ok(Arc::new(Literal::new(value.clone())));
    }
    if matches!(expression, Expr::Literal { .. }) {
        // Literal preparation has no row dependencies, irrespective of how
        // wide the surrounding table schema is.
        return lower_with_options(expression, &Schema::empty(), options)
            .map_err(|error| DataFusionError::Plan(error.to_string()));
    }
    let bindings = match region.owner.node() {
        crate::TableNode::MapRows { bindings, .. } | crate::TableNode::Filter { bindings, .. } => {
            Some(bindings)
        }
        _ => None,
    };
    let external = bindings
        .into_iter()
        .flat_map(|bindings| bindings.iter())
        .map(|(name, owner)| {
            (
                name.clone(),
                DataType::Struct(owner.schema().fields().clone()),
            )
        })
        .collect();
    let frame = NativeFrame::try_new(
        expression,
        &schema,
        region.owner.globals().schema(),
        &external,
    )
    .map_err(|error| DataFusionError::Plan(error.to_string()))?;
    if mixed_nested_scalars(&frame) {
        return Err(DataFusionError::Plan(
            "mixed metadata would broadcast a nested scalar owner".into(),
        ));
    }
    let arguments = arguments(&frame, &schema, region, columns)?;
    let cardinalities = arguments
        .iter()
        .map(|argument| {
            cardinality(argument).ok_or_else(|| {
                DataFusionError::Plan("native argument has unknown cardinality".into())
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let program = lower_with_options(&frame.expression, &frame.schema, options)
        .map_err(|error| DataFusionError::Plan(error.to_string()))?;
    Ok(Arc::new(BoundNative {
        token: Arc::new(()),
        source_schema: schema,
        frame_schema: frame.schema,
        program,
        arguments,
        cardinalities,
    }))
}
fn mixed_nested_scalars(frame: &NativeFrame) -> bool {
    frame
        .arguments
        .iter()
        .any(|arg| matches!(arg, Expr::Column { .. }))
        && frame
            .arguments
            .iter()
            .zip(frame.schema.fields())
            .any(|(arg, field)| !matches!(arg, Expr::Column { .. }) && nested(field.data_type()))
}

/// Mixed metadata may broadcast primitive scalars, never nested global owners.
/// Unsupported cases retain the original table plan.
pub(crate) fn can_prepare(
    expression: &Expr,
    schema: &Schema,
    owner: &crate::TableRef,
    columns: &Columns<'_>,
) -> bool {
    if let Expr::Column { name } = expression {
        return columns.index(name, schema).is_ok();
    }
    if matches!(
        expression,
        Expr::Literal { .. } | Expr::RuntimeLiteral { .. }
    ) {
        return true;
    }
    let bindings = match owner.node() {
        crate::TableNode::MapRows { bindings, .. } | crate::TableNode::Filter { bindings, .. } => {
            Some(bindings)
        }
        _ => None,
    };
    let external = bindings
        .into_iter()
        .flat_map(|bindings| bindings.iter())
        .map(|(name, value)| {
            (
                name.clone(),
                DataType::Struct(value.schema().fields().clone()),
            )
        })
        .collect();
    NativeFrame::try_new(expression, schema, owner.globals().schema(), &external)
        .is_ok_and(|frame| !mixed_nested_scalars(&frame))
}
