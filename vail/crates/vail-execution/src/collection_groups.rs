//! Per-list grouping delegated to DataFusion's native GROUP BY implementation.
//! Only owner/selection indices are built here; Arrow takes copy the values.
use arrow_array::{Array, ArrayRef, ListArray, StructArray, UInt32Array};
use arrow_buffer::{BooleanBuffer, NullBuffer, OffsetBuffer};
use arrow_ord::sort::{SortColumn, lexsort_to_indices};
use arrow_schema::{DataType, Field, Schema, SortOptions};
use arrow_select::take::take;
use datafusion_common::{Result, exec_err, plan_err};
use datafusion_expr::{
    AggregateUDF, ColumnarValue, EmitTo, GroupsAccumulator, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, Volatility,
};
use datafusion_physical_expr::{
    aggregate::{AggregateExprBuilder, AggregateFunctionExpr},
    expressions::Column,
};
use datafusion_physical_plan::aggregates::{group_values::new_group_values, order::GroupOrdering};
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Hash)]
enum Operation {
    First,
    Last,
    Group,
    Aggregate(Vec<AggregateSpec>),
}
#[derive(Debug, PartialEq, Eq, Hash)]
struct AggregateSpec {
    function: String,
    filter_group: Option<usize>,
}
#[derive(Debug, PartialEq, Eq, Hash)]
struct CollectionGroups {
    name: String,
    operation: Operation,
    signature: Signature,
}

#[derive(Clone, Copy)]
struct ListInput<'a> {
    array: &'a ListArray,
    scalar: bool,
}

struct FilteredGroupInput {
    keys: ListArray,
    scalar: bool,
    values: std::collections::HashMap<usize, ListArray>,
}

fn list_struct_field(parent: &ListArray, name: &str) -> Result<ListArray> {
    let records = parent
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            datafusion_common::exec_datafusion_err!("filtered group inputs must be records")
        })?;
    for row in 0..parent.len() {
        if parent.is_null(row) {
            continue;
        }
        let start = parent.value_offsets()[row] as usize;
        let end = parent.value_offsets()[row + 1] as usize;
        if (start..end).any(|index| records.is_null(index)) {
            return exec_err!("filtered group records must be defined");
        }
    }
    let DataType::List(item) = parent.data_type() else {
        unreachable!()
    };
    let DataType::Struct(fields) = item.data_type() else {
        unreachable!()
    };
    let field = fields
        .iter()
        .find(|field| field.name() == name)
        .cloned()
        .ok_or_else(|| {
            datafusion_common::exec_datafusion_err!("filtered group input missing {name}")
        })?;
    let values = records
        .column_by_name(name)
        .ok_or_else(|| {
            datafusion_common::exec_datafusion_err!("filtered group input missing {name}")
        })?
        .clone();
    Ok(ListArray::try_new(
        field,
        parent.value_offsets().clone(),
        values,
        parent.nulls().cloned(),
    )?)
}

pub(crate) fn udf(name: &str) -> Option<Arc<ScalarUDF>> {
    let operation = match name {
        "__vail_dict_first" => Operation::First,
        "__vail_dict_last" => Operation::Last,
        "__vail_collection_group" => Operation::Group,
        _ => {
            let specs = if let Some(functions) =
                name.strip_prefix("__vail_collection_group_aggregate_multi__")
            {
                functions.split("__").collect::<Vec<_>>()
            } else {
                let spec = name.strip_prefix("__vail_collection_group_aggregate_")?;
                vec![spec]
            };
            let functions = specs
                .into_iter()
                .map(|spec| {
                    if let Some((function, group)) = spec.rsplit_once("_filtered") {
                        Some(AggregateSpec {
                            function: function.to_owned(),
                            filter_group: Some(group.parse().ok()?),
                        })
                    } else {
                        Some(AggregateSpec {
                            function: spec.to_owned(),
                            filter_group: None,
                        })
                    }
                })
                .collect::<Option<Vec<_>>>()?;
            if functions.is_empty()
                || functions.iter().any(|spec| {
                    !matches!(
                        spec.function.as_str(),
                        "count"
                            | "count_where"
                            | "sum"
                            | "product"
                            | "min"
                            | "max"
                            | "mean"
                            | "stddev_pop"
                            | "fraction"
                            | "any"
                            | "all"
                    )
                })
            {
                return None;
            }
            Operation::Aggregate(functions)
        }
    };
    let aggregate_arguments = match &operation {
        Operation::Aggregate(functions) => {
            let filter_groups = functions
                .iter()
                .filter_map(|spec| spec.filter_group)
                .collect::<std::collections::HashSet<_>>()
                .len();
            1 + filter_groups
                + functions
                    .iter()
                    .filter(|spec| spec.filter_group.is_none() && spec.function != "count")
                    .count()
        }
        _ => 1,
    };
    Some(Arc::new(ScalarUDF::from(CollectionGroups {
        name: name.to_owned(),
        signature: Signature::any(aggregate_arguments, Volatility::Immutable),
        operation,
    })))
}

fn entry_fields(dtype: &DataType) -> Result<(Arc<Field>, Arc<Field>)> {
    let DataType::List(item) = dtype else {
        return plan_err!("collection grouping requires a list");
    };
    let DataType::Struct(fields) = item.data_type() else {
        return plan_err!("collection grouping requires key/value records");
    };
    let field = |name: &str| {
        fields
            .iter()
            .find(|f| f.name() == name)
            .cloned()
            .ok_or_else(|| {
                datafusion_common::plan_datafusion_err!("collection records require {name}")
            })
    };
    Ok((field("key")?, field("value")?))
}

impl ScalarUDFImpl for CollectionGroups {
    fn name(&self) -> &str {
        &self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        if let Operation::Aggregate(functions) = &self.operation {
            let [DataType::List(key), remaining @ ..] = args else {
                return plan_err!("group aggregate requires key and value lists");
            };
            let group_count = functions
                .iter()
                .filter_map(|spec| spec.filter_group)
                .collect::<std::collections::HashSet<_>>()
                .len();
            let mut cursor = 0;
            for group in 0..group_count {
                let Some(DataType::List(record)) = remaining.get(cursor) else {
                    return plan_err!("filtered group aggregate requires record lists");
                };
                cursor += 1;
                let DataType::Struct(fields) = record.data_type() else {
                    return plan_err!("filtered group aggregate requires key/value records");
                };
                let Some(key_field) = fields.iter().find(|field| field.name() == "key") else {
                    return plan_err!("filtered group aggregate requires key fields");
                };
                if key_field.data_type() != key.data_type() {
                    return plan_err!("filtered group key type differs from base key type");
                }
                if !functions
                    .iter()
                    .any(|spec| spec.filter_group == Some(group))
                {
                    return plan_err!("filtered group indices must be contiguous");
                }
            }
            let mut result_fields = Vec::with_capacity(functions.len());
            for (index, spec) in functions.iter().enumerate() {
                let function = spec.function.as_str();
                let input_type = if function == "count" {
                    None
                } else if let Some(group) = spec.filter_group {
                    let Some(DataType::List(record)) = remaining.get(group) else {
                        return plan_err!("filtered group aggregate requires record lists");
                    };
                    let DataType::Struct(fields) = record.data_type() else {
                        unreachable!()
                    };
                    let name = format!("__value_{index}");
                    Some(
                        fields
                            .iter()
                            .find(|field| field.name() == name)
                            .ok_or_else(|| {
                                datafusion_common::plan_datafusion_err!(
                                    "filtered group aggregate requires {name}"
                                )
                            })?
                            .data_type(),
                    )
                } else {
                    let value = remaining.get(cursor);
                    cursor += 1;
                    Some(match value {
                        Some(DataType::List(value)) => value.data_type(),
                        _ => return plan_err!("group aggregate requires matching value lists"),
                    })
                };
                let result_type = if function == "count" || function == "count_where" {
                    if function == "count_where" && input_type != Some(&DataType::Boolean) {
                        return plan_err!("count_where group values must be boolean lists");
                    }
                    DataType::Int64
                } else {
                    let input_type = input_type.unwrap();
                    let aggregate = configured_group_aggregate(function, input_type)?;
                    AggregateExprBuilder::new(aggregate, vec![Arc::new(Column::new("value", 0))])
                        .schema(Arc::new(Schema::new(vec![Field::new(
                            "value",
                            input_type.clone(),
                            true,
                        )])))
                        .alias("group_aggregate")
                        .build()?
                        .field()
                        .data_type()
                        .clone()
                };
                result_fields.push(Field::new(
                    if functions.len() == 1 {
                        "value".to_owned()
                    } else {
                        format!("__agg_{index}")
                    },
                    result_type,
                    true,
                ));
            }
            if cursor != remaining.len() {
                return plan_err!("group aggregate received extra value lists");
            }
            let value_type = if functions.len() == 1 {
                result_fields.remove(0).data_type().clone()
            } else {
                DataType::Struct(result_fields.into())
            };
            return Ok(DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(
                    vec![
                        key.as_ref().clone().with_name("key"),
                        Field::new("value", value_type, true),
                    ]
                    .into(),
                ),
                true,
            ))));
        }
        let (key, value) = entry_fields(&args[0])?;
        if self.operation != Operation::Group {
            return Ok(args[0].clone());
        }
        let value = Field::new(
            "value",
            DataType::List(Arc::new(value.as_ref().clone().with_name("item"))),
            true,
        );
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(vec![key, Arc::new(value)].into()),
            true,
        ))))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if let Operation::Aggregate(functions) = &self.operation {
            let scalar = args
                .args
                .iter()
                .all(|value| matches!(value, ColumnarValue::Scalar(_)));
            let rows = if scalar { 1 } else { args.number_rows };
            let arrays = args
                .args
                .iter()
                .cloned()
                .map(|value| {
                    let is_scalar = matches!(value, ColumnarValue::Scalar(_));
                    value
                        .into_array(if is_scalar { 1 } else { rows })
                        .map(|array| (array, is_scalar))
                })
                .collect::<Result<Vec<_>>>()?;
            let keys = arrays[0]
                .0
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| {
                    datafusion_common::exec_datafusion_err!("group keys must be lists")
                })?;
            let keys = ListInput {
                array: keys,
                scalar: arrays[0].1,
            };
            let filter_group_count = functions
                .iter()
                .filter_map(|spec| spec.filter_group)
                .collect::<std::collections::HashSet<_>>()
                .len();
            let mut filtered_inputs = Vec::with_capacity(filter_group_count);
            for (array, scalar) in arrays.iter().skip(1).take(filter_group_count) {
                let records = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                    datafusion_common::exec_datafusion_err!("filtered group inputs must be lists")
                })?;
                let keys = list_struct_field(records, "key")?;
                let mut values = std::collections::HashMap::new();
                for (index, spec) in functions.iter().enumerate() {
                    if spec.filter_group.is_some()
                        && spec.filter_group == Some(filtered_inputs.len())
                        && spec.function != "count"
                    {
                        values.insert(
                            index,
                            list_struct_field(records, &format!("__value_{index}"))?,
                        );
                    }
                }
                filtered_inputs.push(FilteredGroupInput {
                    keys,
                    scalar: *scalar,
                    values,
                });
            }
            let mut inputs =
                arrays
                    .iter()
                    .skip(1 + filter_group_count)
                    .filter_map(|(array, scalar)| {
                        array
                            .as_any()
                            .downcast_ref::<ListArray>()
                            .map(|array| ListInput {
                                array,
                                scalar: *scalar,
                            })
                    });
            let mut specs = Vec::with_capacity(functions.len());
            for (index, spec) in functions.iter().enumerate() {
                let aggregate_keys = if let Some(group) = spec.filter_group {
                    let input = filtered_inputs.get(group).ok_or_else(|| {
                        datafusion_common::exec_datafusion_err!("missing filtered group input")
                    })?;
                    ListInput {
                        array: &input.keys,
                        scalar: input.scalar,
                    }
                } else {
                    keys
                };
                let value = if spec.function == "count" {
                    None
                } else if let Some(group) = spec.filter_group {
                    let input = filtered_inputs.get(group).ok_or_else(|| {
                        datafusion_common::exec_datafusion_err!("missing filtered group input")
                    })?;
                    Some(ListInput {
                        array: input.values.get(&index).ok_or_else(|| {
                            datafusion_common::exec_datafusion_err!(
                                "missing filtered group value input"
                            )
                        })?,
                        scalar: input.scalar,
                    })
                } else {
                    Some(inputs.next().ok_or_else(|| {
                        datafusion_common::exec_datafusion_err!("group values must be lists")
                    })?)
                };
                specs.push((
                    spec.function.as_str(),
                    spec.filter_group.is_some(),
                    aggregate_keys,
                    value,
                ));
            }
            let output_type = self.return_type(
                &arrays
                    .iter()
                    .map(|(array, _)| array.data_type().clone())
                    .collect::<Vec<_>>(),
            )?;
            let result = grouped_aggregate(keys, &specs, rows, &output_type)?;
            return if scalar {
                Ok(ColumnarValue::Scalar(
                    crate::expressions::scalar_at(&result, 0).map_err(|error| {
                        datafusion_common::DataFusionError::Execution(error.to_string())
                    })?,
                ))
            } else {
                Ok(ColumnarValue::Array(result))
            };
        }
        let scalar = matches!(args.args[0], ColumnarValue::Scalar(_));
        let input = args.args[0]
            .clone()
            .into_array(if scalar { 1 } else { args.number_rows })?;
        let entries = input.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
            datafusion_common::exec_datafusion_err!("collection grouping requires lists")
        })?;
        let output_type = self.return_type(&[input.data_type().clone()])?;
        let result = grouped(entries, &self.operation, &output_type)?;
        if scalar {
            Ok(ColumnarValue::Scalar(
                crate::expressions::scalar_at(&result, 0).map_err(|error| {
                    datafusion_common::DataFusionError::Execution(error.to_string())
                })?,
            ))
        } else {
            Ok(ColumnarValue::Array(result))
        }
    }
}

fn grouped(entries: &ListArray, operation: &Operation, output_type: &DataType) -> Result<ArrayRef> {
    let records = entries
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            datafusion_common::exec_datafusion_err!("collection entries must be structs")
        })?;
    let keys = records.column_by_name("key").unwrap();
    let values = records.column_by_name("value").unwrap();
    let mut sources = Vec::new();
    let mut owners = Vec::new();
    for row in 0..entries.len() {
        if entries.is_null(row) {
            continue;
        }
        let owner = u32::try_from(row).map_err(|_| {
            datafusion_common::exec_datafusion_err!(
                "collection owner count exceeds Arrow index capacity"
            )
        })?;
        let start = entries.value_offsets()[row] as usize;
        let end = entries.value_offsets()[row + 1] as usize;
        for entry in start..end {
            if records.is_null(entry) {
                return exec_err!("collection entry records must be defined");
            }
            sources.push(entry as u32);
            owners.push(owner);
        }
    }
    let sources = UInt32Array::from(sources);
    let owner_array: ArrayRef = Arc::new(UInt32Array::from(owners));
    let key_array = take(keys.as_ref(), &sources, None)?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("owner", DataType::UInt32, false),
        Field::new("key", key_array.data_type().clone(), true),
    ]));
    let mut grouping = new_group_values(schema, &GroupOrdering::None)?;
    let mut ids = Vec::new();
    grouping.intern(&[owner_array, key_array], &mut ids)?;
    let count = grouping.len();
    let mut representatives = vec![u32::MAX; count];
    let mut group_lengths = vec![0usize; count];
    for (input, &group) in ids.iter().enumerate() {
        if representatives[group] == u32::MAX || *operation == Operation::Last {
            representatives[group] = sources.value(input);
        }
        group_lengths[group] += 1;
    }
    let emitted = grouping.emit(EmitTo::All)?;
    let group_owners = emitted[0].as_any().downcast_ref::<UInt32Array>().unwrap();
    let mut owner_lengths = vec![0usize; entries.len()];
    for owner in group_owners.values() {
        owner_lengths[*owner as usize] += 1;
    }
    let representatives = UInt32Array::from(representatives);
    let representative_keys = take(keys.as_ref(), &representatives, None)?;
    let options = Some(SortOptions {
        descending: false,
        nulls_first: false,
    });
    let order = lexsort_to_indices(
        &[
            SortColumn {
                values: emitted[0].clone(),
                options,
            },
            SortColumn {
                values: representative_keys.clone(),
                options,
            },
        ],
        None,
    )?;
    let DataType::List(item) = output_type else {
        unreachable!()
    };
    let output_values: ArrayRef = if *operation != Operation::Group {
        let selected = take(&representatives, &order, None)?;
        take(
            records,
            selected.as_any().downcast_ref::<UInt32Array>().unwrap(),
            None,
        )?
    } else {
        let sorted_groups = order.values();
        let offsets = OffsetBuffer::<i32>::from_lengths(
            sorted_groups
                .iter()
                .map(|&group| group_lengths[group as usize]),
        );
        let mut cursors = vec![0usize; count];
        for (position, &group) in sorted_groups.iter().enumerate() {
            cursors[group as usize] = offsets[position] as usize;
        }
        let mut selections = vec![0u32; sources.len()];
        for (input, &group) in ids.iter().enumerate() {
            selections[cursors[group]] = sources.value(input);
            cursors[group] += 1;
        }
        let selected = take(values.as_ref(), &UInt32Array::from(selections), None)?;
        let DataType::Struct(fields) = item.data_type() else {
            unreachable!()
        };
        let DataType::List(value_field) = fields[1].data_type() else {
            unreachable!()
        };
        let grouped_values = Arc::new(ListArray::try_new(
            value_field.clone(),
            offsets,
            selected,
            None,
        )?);
        let records = StructArray::try_new(
            fields.clone(),
            vec![
                take(representative_keys.as_ref(), &order, None)?,
                grouped_values,
            ],
            None,
        )?;
        Arc::new(records)
    };
    Ok(Arc::new(ListArray::try_new(
        item.clone(),
        OffsetBuffer::from_lengths(owner_lengths),
        output_values,
        entries.nulls().cloned(),
    )?))
}

fn configured_group_aggregate(function: &str, input: &DataType) -> Result<Arc<AggregateUDF>> {
    crate::reducer::configured_udaf(function, Some(input), &serde_json::Value::Null)
        .map_err(|error| datafusion_common::plan_datafusion_err!("{error}"))
}

struct GroupSegment {
    keys: ArrayRef,
    owners: Vec<u32>,
}

fn selected_spans(
    keys: ListInput<'_>,
    values: Option<ListInput<'_>>,
    row_count: usize,
    include_owners: bool,
) -> Result<(Vec<(usize, usize)>, Option<Vec<(usize, usize)>>, Vec<u32>)> {
    let inputs = std::iter::once(keys).chain(values).collect::<Vec<_>>();
    if inputs
        .iter()
        .any(|input| input.array.len() != if input.scalar { 1 } else { row_count })
    {
        return exec_err!("group key/value row counts differ");
    }
    let offsets = inputs
        .iter()
        .map(|input| input.array.value_offsets())
        .collect::<Vec<_>>();
    let mut key_spans = Vec::new();
    let mut value_spans = values.map(|_| Vec::new());
    let mut owners = Vec::new();
    for row in 0..row_count {
        let key_row = if keys.scalar { 0 } else { row };
        let valid = !keys.array.is_null(key_row);
        if !valid && keys.scalar {
            continue;
        }
        if let Some(value_input) = values {
            let value_row = if value_input.scalar { 0 } else { row };
            if value_input.array.is_null(value_row) == valid {
                return exec_err!("group key/value list validity differs");
            }
        }
        if !valid {
            continue;
        }
        let start = offsets[0][key_row];
        let end = offsets[0][key_row + 1];
        if let Some(value_input) = values {
            let value_row = if value_input.scalar { 0 } else { row };
            if offsets[1][value_row + 1] - offsets[1][value_row] != end - start {
                return exec_err!("group key/value list lengths differ");
            }
            value_spans.as_mut().unwrap().push((
                offsets[1][value_row] as usize,
                offsets[1][value_row + 1] as usize,
            ));
        }
        key_spans.push((start as usize, end as usize));
        let owner = if include_owners {
            Some(u32::try_from(row).map_err(|_| {
                datafusion_common::exec_datafusion_err!(
                    "collection owner count exceeds Arrow index capacity"
                )
            })?)
        } else {
            None
        };
        for offset in 0..(end - start) as usize {
            if include_owners {
                owners.push(owner.unwrap());
            }
        }
    }
    Ok((key_spans, value_spans, owners))
}

fn flatten_spans(values: &ArrayRef, spans: &[(usize, usize)]) -> Result<ArrayRef> {
    let spans = spans
        .iter()
        .copied()
        .filter(|(start, end)| start != end)
        .collect::<Vec<_>>();
    if spans.is_empty() {
        return Ok(values.slice(0, 0));
    }
    if spans.windows(2).all(|pair| pair[0].1 == pair[1].0) {
        let start = spans[0].0;
        let end = spans.last().unwrap().1;
        return Ok(values.slice(start, end - start));
    }
    let chunks = spans
        .iter()
        .map(|(start, end)| values.slice(*start, end - start))
        .collect::<Vec<_>>();
    Ok(arrow_select::concat::concat(
        &chunks
            .iter()
            .map(|chunk| chunk.as_ref())
            .collect::<Vec<_>>(),
    )?)
}

fn flatten_segment(keys: ListInput<'_>, row_count: usize) -> Result<GroupSegment> {
    let (key_spans, _, owners) = selected_spans(keys, None, row_count, true)?;
    let key_data = flatten_spans(keys.array.values(), &key_spans)?;
    Ok(GroupSegment {
        keys: key_data,
        owners,
    })
}

fn flatten_values(
    keys: ListInput<'_>,
    values: ListInput<'_>,
    row_count: usize,
) -> Result<ArrayRef> {
    let (_, value_spans, _) = selected_spans(keys, Some(values), row_count, false)?;
    flatten_spans(values.array.values(), value_spans.as_ref().unwrap())
}

fn grouped_aggregate(
    keys: ListInput<'_>,
    specs: &[(&str, bool, ListInput<'_>, Option<ListInput<'_>>)],
    row_count: usize,
    output_type: &DataType,
) -> Result<ArrayRef> {
    let mut segments = vec![flatten_segment(keys, row_count)?];
    let mut segment_inputs = vec![keys];
    let mut spec_segments = Vec::with_capacity(specs.len());
    let mut spec_values = Vec::with_capacity(specs.len());
    for (function, filtered, aggregate_keys, values) in specs {
        let segment = if *filtered {
            if let Some(segment) = segment_inputs.iter().position(|input| {
                input.scalar == aggregate_keys.scalar
                    && std::ptr::eq(input.array, aggregate_keys.array)
            }) {
                segment
            } else {
                segments.push(flatten_segment(*aggregate_keys, row_count)?);
                segment_inputs.push(*aggregate_keys);
                segments.len() - 1
            }
        } else {
            0
        };
        let data = if *function == "count" {
            None
        } else {
            Some(flatten_values(*aggregate_keys, values.unwrap(), row_count)?)
        };
        spec_segments.push(segment);
        spec_values.push(data);
    }

    let mut owners = Vec::new();
    let mut segment_starts = Vec::with_capacity(segments.len());
    let mut key_arrays = Vec::with_capacity(segments.len());
    for segment in &segments {
        segment_starts.push(owners.len());
        owners.extend_from_slice(&segment.owners);
        key_arrays.push(segment.keys.clone());
    }
    let key_data = if key_arrays.len() == 1 {
        key_arrays[0].clone()
    } else {
        arrow_select::concat::concat(
            &key_arrays
                .iter()
                .map(|array| array.as_ref())
                .collect::<Vec<_>>(),
        )?
    };
    let owner_array: ArrayRef = Arc::new(UInt32Array::from(owners));
    let grouping_schema = Arc::new(Schema::new(vec![
        Field::new("owner", DataType::UInt32, false),
        Field::new("key", key_data.data_type().clone(), true),
    ]));
    let mut grouping = new_group_values(grouping_schema, &GroupOrdering::None)?;
    let mut group_ids = Vec::new();
    grouping.intern(&[owner_array.clone(), key_data], &mut group_ids)?;
    let group_count = grouping.len();

    let mut aggregates = Vec::with_capacity(specs.len());
    for (index, (function, _, _, _)) in specs.iter().enumerate() {
        let segment = spec_segments[index];
        let start = segment_starts[segment];
        let end = start + segments[segment].owners.len();
        let ids = &group_ids[start..end];
        let data = if *function == "count" {
            owner_array.slice(start, end - start)
        } else {
            spec_values[index].as_ref().unwrap().clone()
        };
        let aggregate_udf = if *function == "count" || *function == "count_where" {
            datafusion_functions_aggregate::count::count_udaf()
        } else {
            configured_group_aggregate(function, data.data_type())?
        };
        let aggregate =
            AggregateExprBuilder::new(aggregate_udf, vec![Arc::new(Column::new("value", 0))])
                .schema(Arc::new(Schema::new(vec![Field::new(
                    "value",
                    data.data_type().clone(),
                    true,
                )])))
                .alias("group_aggregate")
                .build()?;
        if !aggregate.groups_accumulator_supported() {
            return exec_err!("group aggregate {function} has no native grouped accumulator");
        }
        let mut accumulator = aggregate.create_groups_accumulator()?;
        let predicate = if *function == "count_where" {
            Some(
                data.as_any()
                    .downcast_ref::<arrow_array::BooleanArray>()
                    .ok_or_else(|| {
                        datafusion_common::exec_datafusion_err!("count_where values must be bool")
                    })?,
            )
        } else {
            None
        };
        accumulator.update_batch(std::slice::from_ref(&data), ids, predicate, group_count)?;
        aggregates.push(accumulator.evaluate(EmitTo::All)?);
    }

    let emitted = grouping.emit(EmitTo::All)?;
    let emitted_owners = emitted[0]
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| datafusion_common::exec_datafusion_err!("group owners must be uint32"))?;
    let options = Some(SortOptions {
        descending: false,
        nulls_first: false,
    });
    let order = lexsort_to_indices(
        &[
            SortColumn {
                values: emitted[0].clone(),
                options,
            },
            SortColumn {
                values: emitted[1].clone(),
                options,
            },
        ],
        None,
    )?;
    let sorted_keys = take(emitted[1].as_ref(), &order, None)?;
    let sorted_aggregates = aggregates
        .iter()
        .map(|aggregate| take(aggregate.as_ref(), &order, None))
        .collect::<Result<Vec<_>>>()?;
    let DataType::List(item) = output_type else {
        unreachable!()
    };
    let DataType::Struct(fields) = item.data_type() else {
        unreachable!()
    };
    let value = if specs.len() == 1 {
        sorted_aggregates[0].clone()
    } else {
        let DataType::Struct(value_fields) = fields[1].data_type() else {
            unreachable!()
        };
        Arc::new(StructArray::try_new(
            value_fields.clone(),
            sorted_aggregates,
            None,
        )?) as ArrayRef
    };
    let records = StructArray::try_new(fields.clone(), vec![sorted_keys, value], None)?;
    let mut owner_lengths = vec![0usize; row_count];
    for owner in emitted_owners.values() {
        owner_lengths[*owner as usize] += 1;
    }
    let nulls = if row_count == 0 {
        None
    } else if keys.scalar && row_count > 1 {
        keys.array.nulls().map(|_| {
            NullBuffer::new(BooleanBuffer::from(
                (0..row_count)
                    .map(|_| !keys.array.is_null(0))
                    .collect::<Vec<_>>(),
            ))
        })
    } else {
        keys.array.nulls().cloned()
    };
    Ok(Arc::new(ListArray::try_new(
        item.clone(),
        OffsetBuffer::from_lengths(owner_lengths),
        Arc::new(records),
        nulls,
    )?))
}
