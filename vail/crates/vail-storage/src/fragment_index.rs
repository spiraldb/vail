//! In-memory interval planning for certified, whole-file keyed fragments.
//!
//! This module compares metadata only. A caller must separately prove each
//! fragment's row order and endpoints, persist an index with a snapshot, and
//! reconcile selected identities with the exact Iceberg tasks it will read.
//! Candidate selection may overselect a file; row filtering is a later step.

use std::cmp::Ordering;
use std::collections::HashSet;

use arrow_array::new_null_array;
use arrow_array::{ArrayRef, RecordBatch};
use arrow_ord::ord::make_comparator;
use arrow_schema::{DataType, Field, Schema, SortOptions};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

/// Legacy scalar comparator metadata used by test-only interval fixtures.
pub const PRIMITIVE_COMPARATOR_V1: &str = "vail-primitive-key-v1";
pub const HAIL_COMPARATOR_V2: &str = "vail-hail-key-v2";
pub const TOTAL_ORDER_COMPARATOR_V3: &str = "vail-total-order-key-v3";
pub const ARROW_TYPED_COMPARATOR_V4: &str = "vail-arrow-typed-key-v4";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum KeyType {
    Int32,
    Int64,
    Bool,
    AsciiString,
    Arrow(serde_json::Value),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum KeyValue {
    Missing,
    Int32(i32),
    Int64(i64),
    Bool(bool),
    AsciiString(String),
    /// Opaque Arrow IPC scalar payload. It is transport only; comparisons
    /// always decode this value to its physical Arrow type first.
    ArrowIpc(Vec<u8>),
}

pub type CompositeKey = Vec<KeyValue>;

/// A typed, decoded key. Each component is a one-row Arrow array, preserving
/// its physical Arrow type and null validity.
#[derive(Clone, Debug)]
pub struct TypedKey {
    pub components: Vec<ArrayRef>,
}

/// Compare two typed composite keys with Arrow's native value comparators.
pub fn compare_typed_key_components(
    left: &[ArrayRef],
    right: &[ArrayRef],
    options: &[SortOptions],
) -> Result<Ordering> {
    let comparators = make_typed_key_comparators(left, right, options)?;
    for comparator in comparators {
        let ordering = comparator(0, 0);
        if ordering != Ordering::Equal {
            return Ok(ordering);
        }
    }
    Ok(Ordering::Equal)
}

/// Build Arrow comparators once for two typed key column sets. Callers that
/// compare many row positions in the same arrays should retain these closures.
pub fn make_typed_key_comparators(
    left: &[ArrayRef],
    right: &[ArrayRef],
    options: &[SortOptions],
) -> Result<Vec<arrow_ord::ord::DynComparator>> {
    if left.len() != right.len() || left.len() != options.len() {
        return Err(IndexError::Invalid("typed key arity mismatch".into()));
    }
    left.iter()
        .zip(right)
        .zip(options)
        .map(|((left, right), options)| {
            make_comparator(left.as_ref(), right.as_ref(), *options)
                .map_err(|error| IndexError::Invalid(error.to_string()))
        })
        .collect()
}

pub fn compare_typed_keys(
    binding: &IndexBinding,
    left: &[ArrayRef],
    right: &[ArrayRef],
) -> Result<Ordering> {
    if left.len() != binding.key_fields.len() {
        return Err(IndexError::Invalid(
            "typed key arity differs from binding".into(),
        ));
    }
    let options = binding
        .key_fields
        .iter()
        .map(|field| SortOptions {
            descending: field.descending,
            nulls_first: !field.nulls_last,
        })
        .collect::<Vec<_>>();
    compare_typed_key_components(left, right, &options)
}

pub fn typed_key_from_transport(binding: &IndexBinding, key: &CompositeKey) -> Result<TypedKey> {
    if key.len() != binding.key_fields.len() {
        return Err(IndexError::Invalid(
            "composite key arity differs from binding".into(),
        ));
    }
    let components = key
        .iter()
        .zip(&binding.key_fields)
        .map(|(value, field)| key_value_array(value, field))
        .collect::<Result<Vec<_>>>()?;
    compare_typed_keys(binding, &components, &components)?;
    Ok(TypedKey { components })
}

/// Encode one typed scalar as Arrow IPC. The bytes are never ordered or
/// compared; they only make the index metadata persistable.
pub fn encode_typed_key_component(array: &ArrayRef) -> Result<Vec<u8>> {
    if array.len() != 1 {
        return Err(IndexError::Invalid(
            "typed key component must contain exactly one row".into(),
        ));
    }
    let schema = Arc::new(Schema::new(vec![Field::new(
        "key",
        array.data_type().clone(),
        true,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![array.clone()])
        .map_err(|error| IndexError::Invalid(error.to_string()))?;
    let mut bytes = Vec::new();
    {
        let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut bytes, &schema)
            .map_err(|error| IndexError::Invalid(error.to_string()))?;
        writer
            .write(&batch)
            .map_err(|error| IndexError::Invalid(error.to_string()))?;
        writer
            .finish()
            .map_err(|error| IndexError::Invalid(error.to_string()))?;
    }
    Ok(bytes)
}

/// Decode an opaque Arrow IPC key component once when loading an index.
pub fn decode_typed_key_component(bytes: &[u8]) -> Result<ArrayRef> {
    let cursor = std::io::Cursor::new(bytes);
    let mut reader = arrow_ipc::reader::StreamReader::try_new(cursor, None)
        .map_err(|error| IndexError::Invalid(format!("invalid typed key IPC: {error}")))?;
    let batch = reader
        .next()
        .transpose()
        .map_err(|error| IndexError::Invalid(format!("invalid typed key IPC: {error}")))?
        .ok_or_else(|| IndexError::Invalid("typed key IPC has no batch".into()))?;
    if batch.num_rows() != 1 || batch.num_columns() != 1 || reader.next().is_some() {
        return Err(IndexError::Invalid(
            "typed key IPC must contain one scalar value".into(),
        ));
    }
    Ok(batch.column(0).clone())
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KeyField {
    pub field_id: i32,
    pub data_type: KeyType,
    /// Exact physical Arrow type, including nested field metadata.
    pub physical_type: DataType,
    pub descending: bool,
    pub nulls_last: bool,
}

/// Required snapshot identity and comparator contract supplied by the caller.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IndexBinding {
    pub table_uuid: Uuid,
    pub snapshot_id: i64,
    pub schema_id: i32,
    pub comparator_version: String,
    pub live_files_sha256: String,
    pub key_fields: Vec<KeyField>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FragmentIdentity {
    pub file_path: String,
    /// Optional legacy checksum. New trusted-local snapshots omit it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_sha256: Option<String>,
}

/// Inclusive endpoints of an actual nonempty, whole-file run.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FragmentInterval {
    pub identity: FragmentIdentity,
    pub row_count: u64,
    pub first_key: CompositeKey,
    pub last_key: CompositeKey,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Endpoint {
    pub key: CompositeKey,
    pub inclusive: bool,
}

/// Query endpoints must contain the full composite key. Prefix-key queries
/// need a separate bound-expansion contract and are rejected by `select`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct QueryInterval {
    pub lower: Option<Endpoint>,
    pub upper: Option<Endpoint>,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum IndexError {
    #[error("invalid fragment index: {0}")]
    Invalid(String),
    #[error("fragment index binding differs from requested snapshot/schema/comparator")]
    BindingMismatch,
}

pub type Result<T> = std::result::Result<T, IndexError>;

/// Sorted interval dictionary for the monotone, non-interior-overlapping profile.
/// Construction sorts fragment metadata; no row data is touched or reordered.
#[derive(Clone, Debug, Serialize)]
pub struct FragmentIndex {
    binding: IndexBinding,
    fragments: Vec<FragmentInterval>,
    #[serde(skip)]
    typed_bounds: Vec<(TypedKey, TypedKey)>,
}

impl<'de> Deserialize<'de> for FragmentIndex {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct WireIndex {
            binding: IndexBinding,
            fragments: Vec<FragmentInterval>,
        }

        let wire = WireIndex::deserialize(deserializer)?;
        let serialized_fragments = wire.fragments.clone();
        let index = Self::new(wire.binding, wire.fragments).map_err(serde::de::Error::custom)?;
        if index.fragments != serialized_fragments {
            return Err(serde::de::Error::custom(
                "fragment index metadata is not stored in certified key order",
            ));
        }
        Ok(index)
    }
}

impl FragmentIndex {
    pub fn new(binding: IndexBinding, fragments: Vec<FragmentInterval>) -> Result<Self> {
        validate_binding(&binding)?;
        let mut paths = HashSet::new();
        let mut typed_fragments = Vec::with_capacity(fragments.len());
        for fragment in fragments {
            if fragment.identity.file_path.is_empty()
                || fragment
                    .identity
                    .content_sha256
                    .as_deref()
                    .is_some_and(|hash| !is_sha256(hash))
                || !paths.insert(fragment.identity.file_path.clone())
                || fragment.row_count == 0
            {
                return Err(IndexError::Invalid(
                    "fragment path, checksum, identity, or row count is invalid".into(),
                ));
            }
            validate_key(&binding, &fragment.first_key)?;
            validate_key(&binding, &fragment.last_key)?;
            let first = typed_key_from_transport(&binding, &fragment.first_key)?;
            let last = typed_key_from_transport(&binding, &fragment.last_key)?;
            if compare_typed_keys(&binding, &first.components, &last.components)?
                == Ordering::Greater
            {
                return Err(IndexError::Invalid(
                    "fragment first key exceeds last key".into(),
                ));
            }
            typed_fragments.push((fragment, first, last));
        }
        typed_fragments.sort_by(
            |(left, left_first, left_last), (right, right_first, right_last)| {
                compare_typed_keys(&binding, &left_first.components, &right_first.components)
                    .expect("decoded typed keys match binding")
                    .then_with(|| {
                        compare_typed_keys(&binding, &left_last.components, &right_last.components)
                            .expect("decoded typed keys match binding")
                    })
                    .then_with(|| left.identity.file_path.cmp(&right.identity.file_path))
            },
        );
        for pair in typed_fragments.windows(2) {
            if compare_typed_keys(&binding, &pair[0].2.components, &pair[1].1.components)?
                == Ordering::Greater
            {
                return Err(IndexError::Invalid(
                    "fragment intervals overlap in their interior".into(),
                ));
            }
        }
        let (fragments, typed_bounds): (Vec<_>, Vec<_>) = typed_fragments
            .into_iter()
            .map(|(fragment, first, last)| (fragment, (first, last)))
            .unzip();
        Ok(Self {
            binding,
            fragments,
            typed_bounds,
        })
    }

    pub fn binding(&self) -> &IndexBinding {
        &self.binding
    }

    pub fn fragments(&self) -> &[FragmentInterval] {
        &self.fragments
    }

    pub fn typed_fragment_bounds(&self) -> &[(TypedKey, TypedKey)] {
        &self.typed_bounds
    }

    /// Selects possible whole-file matches in key-interval order. Independently
    /// exclusive query endpoints prune files touching only the excluded key.
    pub fn select<'a>(
        &'a self,
        expected_binding: &IndexBinding,
        query: &QueryInterval,
    ) -> Result<Vec<&'a FragmentInterval>> {
        if &self.binding != expected_binding {
            return Err(IndexError::BindingMismatch);
        }
        let lower_key = if let Some(lower) = &query.lower {
            validate_key(&self.binding, &lower.key)?;
            Some(typed_key_from_transport(&self.binding, &lower.key)?)
        } else {
            None
        };
        let upper_key = if let Some(upper) = &query.upper {
            validate_key(&self.binding, &upper.key)?;
            Some(typed_key_from_transport(&self.binding, &upper.key)?)
        } else {
            None
        };
        if let (Some(lower), Some(upper)) = (&query.lower, &query.upper) {
            let ordering = compare_typed_keys(
                &self.binding,
                &lower_key.as_ref().unwrap().components,
                &upper_key.as_ref().unwrap().components,
            )?;
            if ordering == Ordering::Greater
                || (ordering == Ordering::Equal && (!lower.inclusive || !upper.inclusive))
            {
                return Ok(Vec::new());
            }
        }

        // Primary-profile last keys are monotone, so one binary search can
        // skip every fragment entirely before the lower bound.
        let start = self.typed_bounds.partition_point(|(_, last)| {
            query.lower.as_ref().is_some_and(|lower| {
                let ordering = compare_typed_keys(
                    &self.binding,
                    &last.components,
                    &lower_key.as_ref().unwrap().components,
                )
                .expect("typed interval keys match binding");
                ordering == Ordering::Less || (ordering == Ordering::Equal && !lower.inclusive)
            })
        });
        let mut selected = Vec::new();
        for (index, fragment) in self.fragments[start..].iter().enumerate() {
            if query.upper.as_ref().is_some_and(|upper| {
                let first = &self.typed_bounds[start + index].0;
                let ordering = compare_typed_keys(
                    &self.binding,
                    &first.components,
                    &upper_key.as_ref().unwrap().components,
                )
                .expect("typed interval keys match binding");
                ordering == Ordering::Greater || (ordering == Ordering::Equal && !upper.inclusive)
            }) {
                break;
            }
            selected.push(fragment);
        }
        Ok(selected)
    }

    /// Select the union of many full-key intervals with a single ordered
    /// sweep. Query endpoints are decoded and sorted once; work is linear in
    /// the fragment index plus the query list, regardless of how many queries
    /// overlap a broad file range.
    pub fn select_many<'a>(
        &'a self,
        expected_binding: &IndexBinding,
        queries: &[QueryInterval],
    ) -> Result<Vec<&'a FragmentInterval>> {
        if &self.binding != expected_binding {
            return Err(IndexError::BindingMismatch);
        }
        if let [query] = queries {
            return self.select(expected_binding, query);
        }
        #[derive(Clone)]
        struct TypedInterval {
            lower: Option<(TypedKey, bool)>,
            upper: Option<(TypedKey, bool)>,
        }
        let mut ranges = Vec::with_capacity(queries.len());
        for query in queries {
            let lower = query
                .lower
                .as_ref()
                .map(|endpoint| {
                    validate_key(&self.binding, &endpoint.key)?;
                    Ok((
                        typed_key_from_transport(&self.binding, &endpoint.key)?,
                        endpoint.inclusive,
                    ))
                })
                .transpose()?;
            let upper = query
                .upper
                .as_ref()
                .map(|endpoint| {
                    validate_key(&self.binding, &endpoint.key)?;
                    Ok((
                        typed_key_from_transport(&self.binding, &endpoint.key)?,
                        endpoint.inclusive,
                    ))
                })
                .transpose()?;
            if let (Some((lower_key, lower_inclusive)), Some((upper_key, upper_inclusive))) =
                (&lower, &upper)
            {
                let order = compare_typed_keys(
                    &self.binding,
                    &lower_key.components,
                    &upper_key.components,
                )?;
                if order == Ordering::Greater
                    || (order == Ordering::Equal && (!lower_inclusive || !upper_inclusive))
                {
                    continue;
                }
            }
            ranges.push(TypedInterval { lower, upper });
        }
        ranges.sort_by(|left, right| match (&left.lower, &right.lower) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Less,
            (Some(_), None) => Ordering::Greater,
            (Some((left, left_inclusive)), Some((right, right_inclusive))) => {
                compare_typed_keys(&self.binding, &left.components, &right.components)
                    .expect("validated interval endpoints share an index binding")
                    .then_with(|| right_inclusive.cmp(left_inclusive))
            }
        });

        // Union overlapping query intervals. Besides reducing duplicates,
        // this keeps a broad fragment from being tested against every query.
        let mut merged: Vec<TypedInterval> = Vec::with_capacity(ranges.len());
        for next in ranges {
            let Some(current) = merged.last_mut() else {
                merged.push(next);
                continue;
            };
            let overlaps = match (&current.upper, &next.lower) {
                (None, _) | (_, None) => true,
                (Some((upper, upper_inclusive)), Some((lower, lower_inclusive))) => {
                    let order =
                        compare_typed_keys(&self.binding, &upper.components, &lower.components)?;
                    order == Ordering::Greater
                        || (order == Ordering::Equal && (*upper_inclusive || *lower_inclusive))
                }
            };
            if !overlaps {
                merged.push(next);
                continue;
            }
            current.upper = match (&current.upper, next.upper) {
                (None, _) | (_, None) => None,
                (Some((left, left_inclusive)), Some((right, right_inclusive))) => {
                    match compare_typed_keys(&self.binding, &left.components, &right.components)? {
                        Ordering::Less => Some((right, right_inclusive)),
                        Ordering::Greater => Some((left.clone(), *left_inclusive)),
                        Ordering::Equal => Some((left.clone(), *left_inclusive || right_inclusive)),
                    }
                }
            };
        }

        let before = |upper: &Option<(TypedKey, bool)>, lower: &TypedKey| -> Result<bool> {
            let Some((upper, inclusive)) = upper else {
                return Ok(false);
            };
            let order = compare_typed_keys(&self.binding, &upper.components, &lower.components)?;
            Ok(order == Ordering::Less || (order == Ordering::Equal && !inclusive))
        };
        let reaches = |lower: &Option<(TypedKey, bool)>, upper: &TypedKey| -> Result<bool> {
            let Some((lower, inclusive)) = lower else {
                return Ok(true);
            };
            let order = compare_typed_keys(&self.binding, &lower.components, &upper.components)?;
            Ok(order == Ordering::Less || (order == Ordering::Equal && *inclusive))
        };
        let mut query_index = 0;
        let mut selected = Vec::new();
        for (fragment_index, fragment) in self.fragments.iter().enumerate() {
            let (first, last) = &self.typed_bounds[fragment_index];
            while query_index < merged.len() && before(&merged[query_index].upper, first)? {
                query_index += 1;
            }
            if query_index < merged.len() && reaches(&merged[query_index].lower, last)? {
                selected.push(fragment);
            }
        }
        Ok(selected)
    }
}

fn validate_binding(binding: &IndexBinding) -> Result<()> {
    if binding.table_uuid.is_nil()
        || binding.snapshot_id <= 0
        || binding.schema_id < 0
        || !matches!(
            binding.comparator_version.as_str(),
            PRIMITIVE_COMPARATOR_V1 | ARROW_TYPED_COMPARATOR_V4
        )
        || !is_sha256(&binding.live_files_sha256)
        || binding.key_fields.is_empty()
    {
        return Err(IndexError::Invalid(
            "unsupported or incomplete index binding".into(),
        ));
    }
    let mut field_ids = HashSet::new();
    if binding.key_fields.iter().any(|field| {
        field.field_id <= 0
            || !field_ids.insert(field.field_id)
            || (binding.comparator_version == ARROW_TYPED_COMPARATOR_V4)
                != matches!(field.data_type, KeyType::Arrow(_))
            || matches!(&field.data_type, KeyType::Arrow(logical) if !logical_type_matches_arrow(logical, &field.physical_type))
    }) {
        return Err(IndexError::Invalid(
            "duplicate or invalid key field ID".into(),
        ));
    }
    Ok(())
}

fn logical_type_matches_arrow(logical: &serde_json::Value, physical: &DataType) -> bool {
    let Ok(logical) = serde_json::from_value::<vail_execution::expressions::Type>(logical.clone())
    else {
        return false;
    };
    logical.ensure_keyable().is_ok()
        && logical
            .to_arrow()
            .is_ok_and(|expected| arrow_types_same_layout(&expected, physical))
        && make_comparator(
            new_null_array(physical, 1).as_ref(),
            new_null_array(physical, 1).as_ref(),
            SortOptions::default(),
        )
        .is_ok()
}

fn arrow_types_same_layout(left: &DataType, right: &DataType) -> bool {
    match (left, right) {
        (DataType::Struct(left), DataType::Struct(right)) => {
            left.len() == right.len()
                && left.iter().zip(right).all(|(left, right)| {
                    left.name() == right.name()
                        && arrow_types_same_layout(left.data_type(), right.data_type())
                })
        }
        (DataType::List(left), DataType::List(right))
        | (DataType::LargeList(left), DataType::LargeList(right)) => {
            arrow_types_same_layout(left.data_type(), right.data_type())
        }
        (DataType::FixedSizeList(left, left_size), DataType::FixedSizeList(right, right_size)) => {
            left_size == right_size && arrow_types_same_layout(left.data_type(), right.data_type())
        }
        _ => left == right,
    }
}

/// Normalize a logically typed key component to the canonical physical Arrow
/// type stored in the snapshot. Iceberg may drop Vail extension metadata and
/// rename list child fields while preserving the actual value layout.
pub fn normalize_typed_key_component(array: &ArrayRef, physical: &DataType) -> Result<ArrayRef> {
    if array.data_type() == physical {
        return Ok(array.clone());
    }
    if !arrow_types_same_layout(array.data_type(), physical) {
        return Err(IndexError::Invalid(format!(
            "key endpoint Arrow layout {:?} differs from stored layout {:?}",
            array.data_type(),
            physical
        )));
    }
    arrow_cast::cast(array, physical).map_err(|error| IndexError::Invalid(error.to_string()))
}

pub fn is_certifiable_key_type(logical: &serde_json::Value, physical: &DataType) -> bool {
    logical_type_matches_arrow(logical, physical)
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn validate_key(binding: &IndexBinding, key: &CompositeKey) -> Result<()> {
    if key.len() != binding.key_fields.len() {
        return Err(IndexError::Invalid(
            "composite key arity differs from binding".into(),
        ));
    }
    for (value, field) in key.iter().zip(&binding.key_fields) {
        let valid = matches!(
            (value, &field.data_type),
            (
                KeyValue::Missing,
                KeyType::Int32 | KeyType::Int64 | KeyType::Bool | KeyType::AsciiString
            ) | (KeyValue::Int32(_), KeyType::Int32)
                | (KeyValue::Int64(_), KeyType::Int64)
                | (KeyValue::Bool(_), KeyType::Bool)
        ) || matches!((value, &field.data_type), (KeyValue::AsciiString(s), KeyType::AsciiString) if s.is_ascii())
            || matches!((value, &field.data_type), (KeyValue::ArrowIpc(bytes), _) if binding.comparator_version == ARROW_TYPED_COMPARATOR_V4 && !bytes.is_empty());
        if !valid {
            return Err(IndexError::Invalid(format!(
                "field {} has mismatched or non-ASCII key value",
                field.field_id
            )));
        }
    }
    Ok(())
}

fn key_array_type_matches(data_type: &DataType, field: &KeyField) -> bool {
    if data_type != &field.physical_type {
        return false;
    }
    let key_type = &field.data_type;
    match key_type {
        KeyType::Int32 => *data_type == arrow_schema::DataType::Int32,
        KeyType::Int64 => *data_type == arrow_schema::DataType::Int64,
        KeyType::Bool => *data_type == arrow_schema::DataType::Boolean,
        KeyType::AsciiString => matches!(data_type, arrow_schema::DataType::Utf8),
        KeyType::Arrow(_) => true,
    }
}

pub fn compare_keys(
    binding: &IndexBinding,
    left: &CompositeKey,
    right: &CompositeKey,
) -> Result<Ordering> {
    if left.len() != binding.key_fields.len() || right.len() != binding.key_fields.len() {
        return Err(IndexError::Invalid(
            "composite key arity differs from binding".into(),
        ));
    }
    let left = left
        .iter()
        .zip(&binding.key_fields)
        .map(|(value, field)| key_value_array(value, field))
        .collect::<Result<Vec<_>>>()?;
    let right = right
        .iter()
        .zip(&binding.key_fields)
        .map(|(value, field)| key_value_array(value, field))
        .collect::<Result<Vec<_>>>()?;
    compare_typed_keys(binding, &left, &right)
}

fn key_value_array(value: &KeyValue, field: &KeyField) -> Result<ArrayRef> {
    let array = match value {
        KeyValue::Missing => {
            let data_type = match &field.data_type {
                KeyType::Int32 => arrow_schema::DataType::Int32,
                KeyType::Int64 => arrow_schema::DataType::Int64,
                KeyType::Bool => arrow_schema::DataType::Boolean,
                KeyType::AsciiString => arrow_schema::DataType::Utf8,
                KeyType::Arrow(_) => {
                    return Err(IndexError::Invalid(
                        "missing complex key must use typed Arrow IPC null".into(),
                    ));
                }
            };
            new_null_array(&data_type, 1)
        }
        KeyValue::Int32(value) if field.data_type == KeyType::Int32 => {
            Arc::new(arrow_array::Int32Array::from(vec![Some(*value)]))
        }
        KeyValue::Int64(value) if field.data_type == KeyType::Int64 => {
            Arc::new(arrow_array::Int64Array::from(vec![Some(*value)]))
        }
        KeyValue::Bool(value) if field.data_type == KeyType::Bool => {
            Arc::new(arrow_array::BooleanArray::from(vec![Some(*value)]))
        }
        KeyValue::AsciiString(value) if field.data_type == KeyType::AsciiString => {
            Arc::new(arrow_array::StringArray::from(vec![Some(value.as_str())]))
        }
        KeyValue::ArrowIpc(bytes) => {
            let array = decode_typed_key_component(bytes)?;
            if !key_array_type_matches(array.data_type(), field) {
                return Err(IndexError::Invalid(format!(
                    "typed key Arrow type {} differs from its index binding",
                    array.data_type()
                )));
            }
            array
        }
        _ => {
            return Err(IndexError::Invalid(
                "typed key component differs from its index binding".into(),
            ));
        }
    };
    Ok(array)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn field(field_id: i32, data_type: KeyType, descending: bool) -> KeyField {
        let physical_type = match &data_type {
            KeyType::Int32 => DataType::Int32,
            KeyType::Int64 => DataType::Int64,
            KeyType::Bool => DataType::Boolean,
            KeyType::AsciiString => DataType::Utf8,
            KeyType::Arrow(logical) => match logical.as_str() {
                Some("float64") => DataType::Float64,
                _ => DataType::Int32,
            },
        };
        KeyField {
            field_id,
            data_type,
            physical_type,
            descending,
            nulls_last: true,
        }
    }

    fn binding(fields: Vec<KeyField>) -> IndexBinding {
        IndexBinding {
            table_uuid: Uuid::from_u128(1),
            snapshot_id: 42,
            schema_id: 7,
            comparator_version: PRIMITIVE_COMPARATOR_V1.into(),
            live_files_sha256: "a".repeat(64),
            key_fields: fields,
        }
    }

    fn fragment(name: &str, first: CompositeKey, last: CompositeKey) -> FragmentInterval {
        FragmentInterval {
            identity: FragmentIdentity {
                file_path: format!("file:///table/{name}.vortex"),
                content_sha256: Some("b".repeat(64)),
            },
            row_count: 10,
            first_key: first,
            last_key: last,
        }
    }

    #[test]
    fn retired_hail_comparator_is_not_a_total_order_certificate() {
        let mut value = binding(vec![field(
            1,
            KeyType::Arrow(serde_json::json!("float64")),
            false,
        )]);
        value.comparator_version = HAIL_COMPARATOR_V2.into();
        assert!(FragmentIndex::new(value.clone(), vec![]).is_err());
        value.comparator_version = TOTAL_ORDER_COMPARATOR_V3.into();
        assert!(FragmentIndex::new(value.clone(), vec![]).is_err());
        value.comparator_version = ARROW_TYPED_COMPARATOR_V4.into();
        assert!(FragmentIndex::new(value, vec![]).is_ok());
    }

    #[test]
    fn typed_ipc_bounds_compare_as_arrow_values_and_validate_physical_type() {
        use arrow_array::Float64Array;

        let mut binding = binding(vec![field(
            1,
            KeyType::Arrow(serde_json::json!("float64")),
            false,
        )]);
        binding.comparator_version = ARROW_TYPED_COMPARATOR_V4.into();
        let bound = |value: f64| {
            let array = Arc::new(Float64Array::from(vec![Some(value)])) as ArrayRef;
            vec![KeyValue::ArrowIpc(
                encode_typed_key_component(&array).unwrap(),
            )]
        };
        let index = FragmentIndex::new(
            binding.clone(),
            vec![
                fragment("a", bound(-0.0), bound(1.0)),
                fragment("b", bound(2.0), bound(3.0)),
            ],
        )
        .unwrap();
        assert_eq!(index.typed_fragment_bounds().len(), 2);
        let encoded = serde_json::to_vec(&index).unwrap();
        let decoded: FragmentIndex = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded.typed_fragment_bounds().len(), 2);
        assert_eq!(
            decoded
                .select(
                    &binding,
                    &QueryInterval {
                        lower: Some(Endpoint {
                            key: bound(2.0),
                            inclusive: true
                        }),
                        upper: Some(Endpoint {
                            key: bound(2.0),
                            inclusive: true
                        }),
                    }
                )
                .unwrap()
                .len(),
            1,
        );

        let mut invalid: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
        invalid["binding"]["key_fields"][0]["physical_type"] = serde_json::json!("Utf8");
        assert!(serde_json::from_value::<FragmentIndex>(invalid).is_err());

        let wrong_type = Arc::new(arrow_array::Int32Array::from(vec![Some(1)])) as ArrayRef;
        let malformed = vec![KeyValue::ArrowIpc(
            encode_typed_key_component(&wrong_type).unwrap(),
        )];
        assert!(
            FragmentIndex::new(
                binding.clone(),
                vec![fragment("wrong", malformed.clone(), malformed)]
            )
            .is_err()
        );
        let malformed = vec![KeyValue::ArrowIpc(vec![0xff, 0x00])];
        assert!(
            FragmentIndex::new(
                binding,
                vec![fragment("bad-ipc", malformed.clone(), malformed)]
            )
            .is_err()
        );
    }

    #[test]
    fn certification_rejects_unordered_or_mismatched_logical_key_types() {
        let set = serde_json::json!({"set":"int32"});
        let set_type = serde_json::from_value::<vail_execution::expressions::Type>(set.clone())
            .unwrap()
            .to_arrow()
            .unwrap();
        assert!(!is_certifiable_key_type(&set, &set_type));
        assert!(!is_certifiable_key_type(
            &serde_json::json!({"dict":["int32","str"]}),
            &set_type
        ));
        assert!(!is_certifiable_key_type(
            &serde_json::json!({"unknown":"int32"}),
            &DataType::Int32
        ));
        assert!(!is_certifiable_key_type(
            &serde_json::json!("float64"),
            &DataType::Int32
        ));

        let reference = serde_json::json!({
            "name": "test",
            "contigs": ["chr1"],
            "lengths": {"chr1": 100},
            "x_contigs": [],
            "y_contigs": [],
            "mt_contigs": [],
            "par": []
        });
        let locus = serde_json::json!({"locus": reference});
        let locus_type = serde_json::from_value::<vail_execution::expressions::Type>(locus.clone())
            .unwrap()
            .to_arrow()
            .unwrap();
        let DataType::Struct(ref fields) = locus_type else {
            panic!("locus key type must be a struct")
        };
        let canonical_locus = DataType::Struct(
            fields
                .iter()
                .map(|field| {
                    Arc::new(Field::new(
                        field.name(),
                        field.data_type().clone(),
                        field.is_nullable(),
                    ))
                })
                .collect(),
        );
        assert!(is_certifiable_key_type(&locus, &canonical_locus));
        assert!(!is_certifiable_key_type(&locus, &DataType::Int32));
        let logical_array = new_null_array(&locus_type, 1);
        let normalized = normalize_typed_key_component(&logical_array, &canonical_locus).unwrap();
        assert_eq!(normalized.data_type(), &canonical_locus);
        assert!(normalize_typed_key_component(&logical_array, &DataType::Int32).is_err());

        for logical in [
            serde_json::json!("call"),
            serde_json::json!({"interval":"int32"}),
        ] {
            let arrow =
                serde_json::from_value::<vail_execution::expressions::Type>(logical.clone())
                    .unwrap()
                    .to_arrow()
                    .unwrap();
            assert!(is_certifiable_key_type(&logical, &arrow));
        }
    }

    fn i32_key(value: i32) -> CompositeKey {
        vec![KeyValue::Int32(value)]
    }
    fn endpoint(value: i32, inclusive: bool) -> Endpoint {
        Endpoint {
            key: i32_key(value),
            inclusive,
        }
    }
    fn names<'a>(selected: &[&'a FragmentInterval]) -> Vec<&'a str> {
        selected
            .iter()
            .map(|f| f.identity.file_path.as_str())
            .collect()
    }

    #[test]
    fn hail_indexed_read_boundary_shapes_retain_touching_duplicates() {
        let bound = binding(vec![field(1, KeyType::Int32, false)]);
        let index = FragmentIndex::new(
            bound.clone(),
            vec![
                fragment("c", i32_key(250), i32_key(499)),
                fragment("a", i32_key(0), i32_key(149)),
                fragment("d", i32_key(500), i32_key(700)),
                fragment("b", i32_key(150), i32_key(250)),
            ],
        )
        .unwrap();
        let query = QueryInterval {
            lower: Some(endpoint(150, true)),
            upper: Some(endpoint(250, false)),
        };
        assert_eq!(
            names(&index.select(&bound, &query).unwrap()),
            vec!["file:///table/b.vortex"]
        );
        let query = QueryInterval {
            lower: Some(endpoint(150, false)),
            upper: Some(endpoint(250, true)),
        };
        assert_eq!(
            names(&index.select(&bound, &query).unwrap()),
            vec!["file:///table/b.vortex", "file:///table/c.vortex",]
        );
        let query = QueryInterval {
            lower: Some(endpoint(250, true)),
            upper: Some(endpoint(250, true)),
        };
        assert_eq!(index.select(&bound, &query).unwrap().len(), 2);
        let query = QueryInterval {
            lower: Some(endpoint(250, false)),
            upper: Some(endpoint(250, true)),
        };
        assert!(index.select(&bound, &query).unwrap().is_empty());
    }

    #[test]
    fn hail_filtered_boundaries_are_candidate_only() {
        let bound = binding(vec![field(1, KeyType::Int32, false)]);
        let index = FragmentIndex::new(
            bound.clone(),
            vec![
                fragment("left", i32_key(100), i32_key(141)),
                fragment("middle", i32_key(141), i32_key(144)),
                fragment("right", i32_key(145), i32_key(200)),
            ],
        )
        .unwrap();
        let inclusive = QueryInterval {
            lower: Some(endpoint(140, true)),
            upper: Some(endpoint(145, true)),
        };
        assert_eq!(index.select(&bound, &inclusive).unwrap().len(), 3);
        let exclusive = QueryInterval {
            lower: Some(endpoint(141, false)),
            upper: Some(endpoint(145, false)),
        };
        assert_eq!(
            names(&index.select(&bound, &exclusive).unwrap()),
            vec!["file:///table/middle.vortex"]
        );
    }

    #[test]
    fn select_many_unions_sorted_intervals_with_one_candidate_sweep() {
        let bound = binding(vec![field(1, KeyType::Int32, false)]);
        let index = FragmentIndex::new(
            bound.clone(),
            vec![
                fragment("0", i32_key(0), i32_key(9)),
                fragment("1", i32_key(10), i32_key(19)),
                fragment("2", i32_key(20), i32_key(29)),
                fragment("3", i32_key(30), i32_key(39)),
                fragment("4", i32_key(40), i32_key(49)),
            ],
        )
        .unwrap();
        let queries = [
            QueryInterval {
                lower: Some(endpoint(14, true)),
                upper: Some(endpoint(35, true)),
            },
            QueryInterval {
                lower: Some(endpoint(5, true)),
                upper: Some(endpoint(10, false)),
            },
            QueryInterval {
                lower: Some(endpoint(20, true)),
                upper: Some(endpoint(20, true)),
            },
        ];
        let selected = index.select_many(&bound, &queries).unwrap();
        assert_eq!(
            names(&selected),
            vec![
                "file:///table/0.vortex",
                "file:///table/1.vortex",
                "file:///table/2.vortex",
                "file:///table/3.vortex",
            ]
        );
    }

    #[test]
    fn select_many_preserves_unbounded_and_exclusive_boundary_semantics() {
        let bound = binding(vec![field(1, KeyType::Int32, false)]);
        let index = FragmentIndex::new(
            bound.clone(),
            vec![
                fragment("0", i32_key(0), i32_key(9)),
                fragment("1", i32_key(10), i32_key(19)),
                fragment("2", i32_key(20), i32_key(29)),
                fragment("3", i32_key(30), i32_key(39)),
            ],
        )
        .unwrap();

        // An exclusive upper bound at a file's first key excludes that file;
        // the unbounded lower endpoint still selects every earlier file.
        let before_ten = QueryInterval {
            lower: None,
            upper: Some(endpoint(10, false)),
        };
        assert_eq!(
            names(&index.select_many(&bound, &[before_ten]).unwrap()),
            vec!["file:///table/0.vortex"]
        );

        // Equal exclusive endpoints leave the boundary key out, while an
        // unbounded upper endpoint selects the remainder of the index.
        let after_twenty = QueryInterval {
            lower: Some(endpoint(20, false)),
            upper: None,
        };
        assert_eq!(
            names(&index.select_many(&bound, &[after_twenty]).unwrap()),
            vec!["file:///table/2.vortex", "file:///table/3.vortex"]
        );

        let touching_but_disjoint = [
            QueryInterval {
                lower: Some(endpoint(0, true)),
                upper: Some(endpoint(10, false)),
            },
            QueryInterval {
                lower: Some(endpoint(10, false)),
                upper: Some(endpoint(20, true)),
            },
        ];
        assert_eq!(
            names(&index.select_many(&bound, &touching_but_disjoint).unwrap()),
            vec![
                "file:///table/0.vortex",
                "file:///table/1.vortex",
                "file:///table/2.vortex"
            ]
        );
    }

    #[test]
    fn compound_mixed_direction_missing_and_ascii_are_lexicographic() {
        let bound = binding(vec![
            field(1, KeyType::Int32, false),
            field(2, KeyType::AsciiString, true),
        ]);
        let key = |a, b| vec![KeyValue::Int32(a), b];
        let index = FragmentIndex::new(
            bound.clone(),
            vec![
                fragment(
                    "late",
                    key(1, KeyValue::AsciiString("a".into())),
                    key(1, KeyValue::Missing),
                ),
                fragment(
                    "early",
                    key(1, KeyValue::AsciiString("z".into())),
                    key(1, KeyValue::AsciiString("m".into())),
                ),
                fragment(
                    "middle",
                    key(1, KeyValue::AsciiString("m".into())),
                    key(1, KeyValue::AsciiString("a".into())),
                ),
                fragment(
                    "next",
                    key(2, KeyValue::AsciiString("z".into())),
                    key(2, KeyValue::Missing),
                ),
            ],
        )
        .unwrap();
        let query = QueryInterval {
            lower: Some(Endpoint {
                key: key(1, KeyValue::AsciiString("m".into())),
                inclusive: true,
            }),
            upper: Some(Endpoint {
                key: key(1, KeyValue::Missing),
                inclusive: false,
            }),
        };
        assert_eq!(
            names(&index.select(&bound, &query).unwrap()),
            vec![
                "file:///table/early.vortex",
                "file:///table/middle.vortex",
                "file:///table/late.vortex",
            ]
        );
        let query = QueryInterval {
            lower: Some(Endpoint {
                key: key(1, KeyValue::Missing),
                inclusive: true,
            }),
            upper: None,
        };
        assert_eq!(
            names(&index.select(&bound, &query).unwrap()),
            vec!["file:///table/late.vortex", "file:///table/next.vortex",]
        );
    }

    #[test]
    fn empty_unbounded_and_reversed_queries() {
        let bound = binding(vec![
            field(1, KeyType::Int64, true),
            field(2, KeyType::Bool, false),
        ]);
        let empty = FragmentIndex::new(bound.clone(), vec![]).unwrap();
        assert!(
            empty
                .select(&bound, &QueryInterval::default())
                .unwrap()
                .is_empty()
        );
        let index = FragmentIndex::new(
            bound.clone(),
            vec![
                fragment(
                    "b",
                    vec![KeyValue::Int64(2), KeyValue::Bool(false)],
                    vec![KeyValue::Int64(1), KeyValue::Bool(true)],
                ),
                fragment(
                    "a",
                    vec![KeyValue::Int64(4), KeyValue::Bool(false)],
                    vec![KeyValue::Int64(2), KeyValue::Bool(false)],
                ),
            ],
        )
        .unwrap();
        assert_eq!(
            names(&index.select(&bound, &QueryInterval::default()).unwrap()),
            vec!["file:///table/a.vortex", "file:///table/b.vortex",]
        );
        let query = QueryInterval {
            lower: Some(Endpoint {
                key: vec![KeyValue::Int64(1), KeyValue::Bool(false)],
                inclusive: true,
            }),
            upper: Some(Endpoint {
                key: vec![KeyValue::Int64(4), KeyValue::Bool(true)],
                inclusive: true,
            }),
        };
        assert!(index.select(&bound, &query).unwrap().is_empty());
    }

    #[test]
    fn same_start_sorts_point_run_before_wider_run_regardless_of_name() {
        let bound = binding(vec![field(1, KeyType::Int32, false)]);
        let index = FragmentIndex::new(
            bound.clone(),
            vec![
                fragment("a-wide", i32_key(1), i32_key(3)),
                fragment("z-point", i32_key(1), i32_key(1)),
                fragment("later", i32_key(3), i32_key(4)),
            ],
        )
        .unwrap();
        assert_eq!(
            names(&index.select(&bound, &QueryInterval::default()).unwrap()),
            vec![
                "file:///table/z-point.vortex",
                "file:///table/a-wide.vortex",
                "file:///table/later.vortex",
            ]
        );
    }

    #[test]
    fn missing_position_is_independent_of_descending_value_direction() {
        let mut order = field(1, KeyType::Bool, true);
        let bound = binding(vec![order.clone()]);
        let fragments = || {
            vec![
                fragment("missing", vec![KeyValue::Missing], vec![KeyValue::Missing]),
                fragment(
                    "false",
                    vec![KeyValue::Bool(false)],
                    vec![KeyValue::Bool(false)],
                ),
                fragment(
                    "true",
                    vec![KeyValue::Bool(true)],
                    vec![KeyValue::Bool(true)],
                ),
            ]
        };
        let index = FragmentIndex::new(bound.clone(), fragments()).unwrap();
        assert_eq!(
            names(&index.select(&bound, &QueryInterval::default()).unwrap()),
            vec![
                "file:///table/true.vortex",
                "file:///table/false.vortex",
                "file:///table/missing.vortex",
            ]
        );
        order.nulls_last = false;
        let bound = binding(vec![order]);
        let index = FragmentIndex::new(bound.clone(), fragments()).unwrap();
        assert_eq!(
            names(&index.select(&bound, &QueryInterval::default()).unwrap()),
            vec![
                "file:///table/missing.vortex",
                "file:///table/true.vortex",
                "file:///table/false.vortex",
            ]
        );
    }

    #[test]
    fn rejects_interior_overlap_duplicate_identity_and_invalid_keys() {
        let bound = binding(vec![field(1, KeyType::Int32, false)]);
        assert!(
            FragmentIndex::new(
                bound.clone(),
                vec![
                    fragment("a", i32_key(1), i32_key(5)),
                    fragment("b", i32_key(4), i32_key(9)),
                ]
            )
            .is_err()
        );
        assert!(
            FragmentIndex::new(
                bound.clone(),
                vec![
                    fragment("a", i32_key(1), i32_key(2)),
                    fragment("a", i32_key(3), i32_key(4)),
                ]
            )
            .is_err()
        );
        assert!(
            FragmentIndex::new(bound.clone(), vec![fragment("a", i32_key(2), i32_key(1))]).is_err()
        );
        let mut zero = fragment("zero", i32_key(1), i32_key(1));
        zero.row_count = 0;
        assert!(FragmentIndex::new(bound.clone(), vec![zero]).is_err());
        assert!(
            FragmentIndex::new(bound.clone(), vec![fragment("a", vec![], i32_key(1))]).is_err()
        );
        assert!(
            FragmentIndex::new(
                bound.clone(),
                vec![fragment(
                    "a",
                    vec![KeyValue::Int64(1)],
                    vec![KeyValue::Int64(2)]
                )]
            )
            .is_err()
        );
        let index =
            FragmentIndex::new(bound.clone(), vec![fragment("a", i32_key(1), i32_key(2))]).unwrap();
        assert!(
            index
                .select(
                    &bound,
                    &QueryInterval {
                        lower: Some(Endpoint {
                            key: vec![KeyValue::AsciiString("é".into())],
                            inclusive: true
                        }),
                        upper: None,
                    }
                )
                .is_err()
        );
        assert!(
            index
                .select(
                    &bound,
                    &QueryInterval {
                        lower: Some(Endpoint {
                            key: vec![],
                            inclusive: true
                        }),
                        upper: None,
                    }
                )
                .is_err()
        );
        let mut other = bound.clone();
        other.snapshot_id += 1;
        assert_eq!(
            index.select(&other, &QueryInterval::default()),
            Err(IndexError::BindingMismatch)
        );
        other = bound.clone();
        other.schema_id += 1;
        assert_eq!(
            index.select(&other, &QueryInterval::default()),
            Err(IndexError::BindingMismatch)
        );
        other = bound.clone();
        other.live_files_sha256 = "c".repeat(64);
        assert_eq!(
            index.select(&other, &QueryInterval::default()),
            Err(IndexError::BindingMismatch)
        );
        assert!(FragmentIndex::new(binding(vec![]), vec![]).is_err());
        let mut invalid_schema = bound.clone();
        invalid_schema.schema_id = -1;
        assert!(FragmentIndex::new(invalid_schema, vec![]).is_err());
        assert!(
            FragmentIndex::new(
                binding(vec![
                    field(1, KeyType::Int32, false),
                    field(1, KeyType::Int64, false),
                ]),
                vec![]
            )
            .is_err()
        );
        other = bound.clone();
        other.comparator_version = "hail-v1".into();
        assert_eq!(
            index.select(&other, &QueryInterval::default()),
            Err(IndexError::BindingMismatch)
        );
    }
}
