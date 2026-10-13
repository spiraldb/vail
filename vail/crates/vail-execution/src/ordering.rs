//! DataFusion expression for the canonical Vail comparison key.
use arrow_array::{Array, BinaryArray, RecordBatch};
use arrow_schema::{DataType, Schema};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr::PhysicalExpr;
use std::{
    fmt,
    hash::{Hash, Hasher},
    sync::Arc,
};

#[derive(Debug)]
struct CanonicalKeyExpr {
    child: Arc<dyn PhysicalExpr>,
    logical: Option<String>,
    force_rows: bool,
}
impl CanonicalKeyExpr {
    fn supports_native(&self, dtype: &DataType) -> Result<bool> {
        let logical = match &self.logical {
            Some(raw) => serde_json::from_str::<crate::expressions::Type>(raw)
                .map_err(|e| DataFusionError::Plan(e.to_string()))?,
            None => crate::expressions::Type::from_arrow(dtype)
                .map_err(|e| DataFusionError::Plan(e.to_string()))?,
        };
        Ok(logical.native_key())
    }
}

impl PartialEq for CanonicalKeyExpr {
    fn eq(&self, other: &Self) -> bool {
        self.child.eq(&other.child)
            && self.logical == other.logical
            && self.force_rows == other.force_rows
    }
}
impl Eq for CanonicalKeyExpr {}
impl Hash for CanonicalKeyExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.child.hash(state);
        self.logical.hash(state);
        self.force_rows.hash(state);
    }
}
impl fmt::Display for CanonicalKeyExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "VailKey({})", self.child)
    }
}
impl PhysicalExpr for CanonicalKeyExpr {
    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
    fn data_type(&self, schema: &Schema) -> Result<DataType> {
        let ty = self.child.data_type(schema)?;
        if !self.force_rows && self.supports_native(&ty)? {
            Ok(ty)
        } else {
            Ok(DataType::Binary)
        }
    }
    fn nullable(&self, schema: &Schema) -> Result<bool> {
        if self.data_type(schema)? != DataType::Binary {
            self.child.nullable(schema)
        } else {
            Ok(false)
        }
    }
    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.child]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "VailKey requires one child".into(),
            ));
        }
        Ok(Arc::new(Self {
            child: children[0].clone(),
            logical: self.logical.clone(),
            force_rows: self.force_rows,
        }))
    }
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        if self.data_type(batch.schema().as_ref())? != DataType::Binary {
            return self.child.evaluate(batch);
        }
        let values = self.child.evaluate(batch)?.into_array(batch.num_rows())?;
        if self.force_rows && self.supports_native(values.data_type())? {
            let converter =
                arrow_row::RowConverter::new(vec![arrow_row::SortField::new_with_options(
                    values.data_type().clone(),
                    arrow_schema::SortOptions {
                        descending: false,
                        nulls_first: false,
                    },
                )])?;
            let rows = converter.convert_columns(&[values])?;
            return Ok(ColumnarValue::Array(Arc::new(
                BinaryArray::from_iter_values(rows.iter().map(|row| row.data())),
            )));
        }
        let encoded = if let Some(logical) = &self.logical {
            let logical: serde_json::Value = serde_json::from_str(logical).map_err(|e| {
                DataFusionError::Internal(format!("invalid canonical key type: {e}"))
            })?;
            (0..values.len())
                .map(|row| vail_ordering::encode_typed_value(values.as_ref(), row, Some(&logical)))
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| DataFusionError::Execution(e.to_string()))?
        } else {
            let encoded = vail_ordering::encode_array(values.as_ref())
                .map_err(|e| DataFusionError::Execution(e.to_string()))?;
            (0..encoded.len())
                .map(|row| encoded.value(row).to_vec())
                .collect()
        };
        Ok(ColumnarValue::Array(Arc::new(
            BinaryArray::from_iter_values(encoded),
        )))
    }
}

pub fn canonical_key(child: Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
    Arc::new(CanonicalKeyExpr {
        child,
        logical: None,
        force_rows: false,
    })
}

/// Batch row encoding for DataFusion range split points. No per-value key vectors.
pub fn range_key(child: Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
    Arc::new(CanonicalKeyExpr {
        child,
        logical: None,
        force_rows: true,
    })
}

pub fn canonical_key_typed(
    child: Arc<dyn PhysicalExpr>,
    logical: serde_json::Value,
) -> Arc<dyn PhysicalExpr> {
    Arc::new(CanonicalKeyExpr {
        child,
        logical: Some(logical.to_string()),
        force_rows: true,
    })
}

/// Comparison key for interval predicates. Unlike grouping keys, a missing
/// point stays null so DataFusion's comparisons retain three-valued logic.
#[derive(Debug)]
struct NullableCanonicalKeyExpr {
    child: Arc<dyn PhysicalExpr>,
    logical: String,
}

impl PartialEq for NullableCanonicalKeyExpr {
    fn eq(&self, other: &Self) -> bool {
        self.child.eq(&other.child) && self.logical == other.logical
    }
}
impl Eq for NullableCanonicalKeyExpr {}
impl Hash for NullableCanonicalKeyExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.child.hash(state);
        self.logical.hash(state);
    }
}
impl fmt::Display for NullableCanonicalKeyExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NullableVailKey({})", self.child)
    }
}
impl PhysicalExpr for NullableCanonicalKeyExpr {
    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
    fn data_type(&self, _: &Schema) -> Result<DataType> {
        Ok(DataType::Binary)
    }
    fn nullable(&self, _: &Schema) -> Result<bool> {
        Ok(true)
    }
    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.child]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "NullableVailKey requires one child".into(),
            ));
        }
        Ok(Arc::new(Self {
            child: children[0].clone(),
            logical: self.logical.clone(),
        }))
    }
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let values = self.child.evaluate(batch)?.into_array(batch.num_rows())?;
        let logical = serde_json::from_str(&self.logical)
            .map_err(|e| DataFusionError::Internal(format!("invalid Hail type: {e}")))?;
        let encoded = (0..values.len())
            .map(|row| {
                if values.is_null(row) {
                    Ok(None)
                } else {
                    vail_ordering::encode_typed_value(values.as_ref(), row, Some(&logical))
                        .map(Some)
                        .map_err(|e| DataFusionError::Execution(e.to_string()))
                }
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(ColumnarValue::Array(Arc::new(BinaryArray::from_iter(
            encoded.iter().map(|key| key.as_deref()),
        ))))
    }
}

pub fn canonical_key_nullable(
    child: Arc<dyn PhysicalExpr>,
    logical: serde_json::Value,
) -> Arc<dyn PhysicalExpr> {
    if serde_json::from_value::<crate::expressions::Type>(logical.clone())
        .is_ok_and(|dtype| dtype.native_key())
    {
        return child;
    }
    Arc::new(NullableCanonicalKeyExpr {
        child,
        logical: logical.to_string(),
    })
}

/// Versioned storage-bound comparison only. Execution comparisons and join keys
/// should use native expressions. The composite passed here is always present.
pub fn storage_bound_key(
    child: Arc<dyn PhysicalExpr>,
    logical: serde_json::Value,
) -> Arc<dyn PhysicalExpr> {
    Arc::new(NullableCanonicalKeyExpr {
        child,
        logical: logical.to_string(),
    })
}
