//! In-memory interval planning for certified, whole-file keyed fragments.
//!
//! This module compares metadata only. A caller must separately prove each
//! fragment's row order and endpoints, persist an index with a snapshot, and
//! reconcile selected identities with the exact Iceberg tasks it will read.
//! Candidate selection may overselect a file; row filtering is a later step.

use std::cmp::Ordering;
use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

/// Comparator implemented by this bounded primitive-key planner.
pub const PRIMITIVE_COMPARATOR_V1: &str = "vail-primitive-key-v1";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum KeyType {
    Int32,
    Int64,
    Bool,
    AsciiString,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum KeyValue {
    Missing,
    Int32(i32),
    Int64(i64),
    Bool(bool),
    AsciiString(String),
}

pub type CompositeKey = Vec<KeyValue>;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KeyField {
    pub field_id: i32,
    pub data_type: KeyType,
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
    pub content_sha256: String,
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
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FragmentIndex {
    binding: IndexBinding,
    fragments: Vec<FragmentInterval>,
}

impl FragmentIndex {
    pub fn new(binding: IndexBinding, mut fragments: Vec<FragmentInterval>) -> Result<Self> {
        validate_binding(&binding)?;
        let mut paths = HashSet::new();
        for fragment in &fragments {
            if fragment.identity.file_path.is_empty()
                || !is_sha256(&fragment.identity.content_sha256)
                || !paths.insert(fragment.identity.file_path.as_str())
                || fragment.row_count == 0
            {
                return Err(IndexError::Invalid(
                    "fragment path, checksum, identity, or row count is invalid".into(),
                ));
            }
            validate_key(&binding, &fragment.first_key)?;
            validate_key(&binding, &fragment.last_key)?;
            if compare_keys(&binding, &fragment.first_key, &fragment.last_key) == Ordering::Greater
            {
                return Err(IndexError::Invalid(
                    "fragment first key exceeds last key".into(),
                ));
            }
        }
        fragments.sort_by(|left, right| {
            compare_keys(&binding, &left.first_key, &right.first_key)
                .then_with(|| compare_keys(&binding, &left.last_key, &right.last_key))
                .then_with(|| left.identity.file_path.cmp(&right.identity.file_path))
        });
        for pair in fragments.windows(2) {
            if compare_keys(&binding, &pair[0].last_key, &pair[1].first_key) == Ordering::Greater {
                return Err(IndexError::Invalid(
                    "fragment intervals overlap in their interior".into(),
                ));
            }
        }
        Ok(Self { binding, fragments })
    }

    pub fn binding(&self) -> &IndexBinding {
        &self.binding
    }

    pub fn fragments(&self) -> &[FragmentInterval] {
        &self.fragments
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
        if let Some(lower) = &query.lower {
            validate_key(&self.binding, &lower.key)?;
        }
        if let Some(upper) = &query.upper {
            validate_key(&self.binding, &upper.key)?;
        }
        if let (Some(lower), Some(upper)) = (&query.lower, &query.upper) {
            let ordering = compare_keys(&self.binding, &lower.key, &upper.key);
            if ordering == Ordering::Greater
                || (ordering == Ordering::Equal && (!lower.inclusive || !upper.inclusive))
            {
                return Ok(Vec::new());
            }
        }

        // Primary-profile last keys are monotone, so one binary search can
        // skip every fragment entirely before the lower bound.
        let start = self.fragments.partition_point(|fragment| {
            query.lower.as_ref().is_some_and(|lower| {
                let ordering = compare_keys(&self.binding, &fragment.last_key, &lower.key);
                ordering == Ordering::Less || (ordering == Ordering::Equal && !lower.inclusive)
            })
        });
        let mut selected = Vec::new();
        for fragment in &self.fragments[start..] {
            if query.upper.as_ref().is_some_and(|upper| {
                let ordering = compare_keys(&self.binding, &fragment.first_key, &upper.key);
                ordering == Ordering::Greater || (ordering == Ordering::Equal && !upper.inclusive)
            }) {
                break;
            }
            selected.push(fragment);
        }
        Ok(selected)
    }
}

fn validate_binding(binding: &IndexBinding) -> Result<()> {
    if binding.table_uuid.is_nil()
        || binding.snapshot_id <= 0
        || binding.schema_id < 0
        || binding.comparator_version != PRIMITIVE_COMPARATOR_V1
        || !is_sha256(&binding.live_files_sha256)
        || binding.key_fields.is_empty()
    {
        return Err(IndexError::Invalid(
            "unsupported or incomplete index binding".into(),
        ));
    }
    let mut field_ids = HashSet::new();
    if binding
        .key_fields
        .iter()
        .any(|field| field.field_id <= 0 || !field_ids.insert(field.field_id))
    {
        return Err(IndexError::Invalid(
            "duplicate or invalid key field ID".into(),
        ));
    }
    Ok(())
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
            (value, field.data_type),
            (KeyValue::Missing, _)
                | (KeyValue::Int32(_), KeyType::Int32)
                | (KeyValue::Int64(_), KeyType::Int64)
                | (KeyValue::Bool(_), KeyType::Bool)
        ) || matches!((value, field.data_type), (KeyValue::AsciiString(s), KeyType::AsciiString) if s.is_ascii());
        if !valid {
            return Err(IndexError::Invalid(format!(
                "field {} has mismatched or non-ASCII key value",
                field.field_id
            )));
        }
    }
    Ok(())
}

pub(crate) fn compare_keys(
    binding: &IndexBinding,
    left: &CompositeKey,
    right: &CompositeKey,
) -> Ordering {
    for ((left, right), field) in left.iter().zip(right).zip(&binding.key_fields) {
        let ordering = match (left, right) {
            (KeyValue::Missing, KeyValue::Missing) => Ordering::Equal,
            (KeyValue::Missing, _) => {
                if field.nulls_last {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            (_, KeyValue::Missing) => {
                if field.nulls_last {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            (KeyValue::Int32(a), KeyValue::Int32(b)) => directional(a.cmp(b), field.descending),
            (KeyValue::Int64(a), KeyValue::Int64(b)) => directional(a.cmp(b), field.descending),
            (KeyValue::Bool(a), KeyValue::Bool(b)) => directional(a.cmp(b), field.descending),
            (KeyValue::AsciiString(a), KeyValue::AsciiString(b)) => {
                directional(a.cmp(b), field.descending)
            }
            _ => unreachable!("keys were validated against the binding"),
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

fn directional(ordering: Ordering, descending: bool) -> Ordering {
    if descending {
        ordering.reverse()
    } else {
        ordering
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn field(field_id: i32, data_type: KeyType, descending: bool) -> KeyField {
        KeyField {
            field_id,
            data_type,
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
                content_sha256: "b".repeat(64),
            },
            row_count: 10,
            first_key: first,
            last_key: last,
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
