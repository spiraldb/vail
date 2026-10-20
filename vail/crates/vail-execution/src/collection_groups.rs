//! Per-list grouping delegated to DataFusion's native GROUP BY implementation.
//! Only owner/selection indices are built here; Arrow takes copy the values.
use arrow_array::{Array, ArrayRef, ListArray, StructArray, UInt32Array};
use arrow_buffer::OffsetBuffer;
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
    Aggregate(String),
}
#[derive(Debug, PartialEq, Eq, Hash)]
struct CollectionGroups {
    name: String,
    operation: Operation,
    signature: Signature,
}

pub(crate) fn udf(name: &str) -> Option<Arc<ScalarUDF>> {
    let operation = match name {
        "__vail_dict_first" => Operation::First,
        "__vail_dict_last" => Operation::Last,
        "__vail_collection_group" => Operation::Group,
        _ => {
            let function = name.strip_prefix("__vail_collection_group_aggregate_")?;
            if !matches!(
                function,
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
            ) {
                return None;
            }
            Operation::Aggregate(function.to_owned())
        }
    };
    Some(Arc::new(ScalarUDF::from(CollectionGroups {
        name: name.to_owned(),
        signature: Signature::any(
            if matches!(&operation, Operation::Aggregate(_)) {
                2
            } else {
                1
            },
            Volatility::Immutable,
        ),
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
        if let Operation::Aggregate(function) = &self.operation {
            let [DataType::List(key), DataType::List(value)] = args else {
                return plan_err!("group aggregate requires key and value lists");
            };
            let result_type = if function == "count" {
                DataType::Int64
            } else {
                let aggregate = configured_group_aggregate(
                    if function == "count_where" {
                        "count"
                    } else {
                        function
                    },
                    value.data_type(),
                )?;
                AggregateExprBuilder::new(aggregate, vec![Arc::new(Column::new("value", 0))])
                    .schema(Arc::new(Schema::new(vec![Field::new(
                        "value",
                        value.data_type().clone(),
                        true,
                    )])))
                    .alias("group_aggregate")
                    .build()?
                    .field()
                    .data_type()
                    .clone()
            };
            return Ok(DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(
                    vec![
                        key.as_ref().clone().with_name("key"),
                        Field::new("value", result_type, true),
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
        if let Operation::Aggregate(function) = &self.operation {
            let scalar = args
                .args
                .iter()
                .all(|value| matches!(value, ColumnarValue::Scalar(_)));
            let rows = if scalar { 1 } else { args.number_rows };
            let arrays = args
                .args
                .iter()
                .cloned()
                .map(|value| value.into_array(rows))
                .collect::<Result<Vec<_>>>()?;
            let keys = arrays[0]
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| {
                    datafusion_common::exec_datafusion_err!("group keys must be lists")
                })?;
            let values = arrays[1]
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| {
                    datafusion_common::exec_datafusion_err!("group values must be lists")
                })?;
            let output_type =
                self.return_type(&[arrays[0].data_type().clone(), arrays[1].data_type().clone()])?;
            let result = grouped_aggregate(keys, values, function, &output_type)?;
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

fn grouped_aggregate(
    keys: &ListArray,
    values: &ListArray,
    function: &str,
    output_type: &DataType,
) -> Result<ArrayRef> {
    if keys.len() != values.len() {
        return exec_err!("group key/value row counts differ");
    }
    let key_offsets = keys.value_offsets();
    let value_offsets = values.value_offsets();
    let keys_contiguous = keys.null_count() == 0;
    let values_contiguous = values.null_count() == 0;
    let key_base = key_offsets[0] as usize;
    let value_base = value_offsets[0] as usize;
    let mut key_indices = Vec::new();
    let mut value_indices = Vec::new();
    let mut owners = Vec::new();
    for row in 0..keys.len() {
        if keys.is_null(row) != values.is_null(row) {
            return exec_err!("group key/value list validity differs");
        }
        if keys.is_null(row) {
            continue;
        }
        let key_start = key_offsets[row] as usize;
        let key_end = key_offsets[row + 1] as usize;
        let value_start = value_offsets[row] as usize;
        let value_end = value_offsets[row + 1] as usize;
        if key_end - key_start != value_end - value_start {
            return exec_err!("group key/value list lengths differ");
        }
        let owner = u32::try_from(row).map_err(|_| {
            datafusion_common::exec_datafusion_err!(
                "collection owner count exceeds Arrow index capacity"
            )
        })?;
        for offset in 0..key_end - key_start {
            key_indices.push(u32::try_from(key_start + offset).map_err(|_| {
                datafusion_common::exec_datafusion_err!(
                    "collection element offset exceeds Arrow index capacity"
                )
            })?);
            value_indices.push(u32::try_from(value_start + offset).map_err(|_| {
                datafusion_common::exec_datafusion_err!(
                    "collection element offset exceeds Arrow index capacity"
                )
            })?);
            owners.push(owner);
        }
    }
    let key_data = if keys_contiguous {
        let end = *key_offsets.last().unwrap() as usize;
        keys.values().slice(key_base, end - key_base)
    } else {
        take(
            keys.values().as_ref(),
            &UInt32Array::from(key_indices),
            None,
        )?
    };
    let value_data = if values_contiguous {
        let end = *value_offsets.last().unwrap() as usize;
        values.values().slice(value_base, end - value_base)
    } else {
        take(
            values.values().as_ref(),
            &UInt32Array::from(value_indices),
            None,
        )?
    };
    if keys_contiguous && values_contiguous && key_data.len() != owners.len() {
        return exec_err!("group key/value offsets do not cover their children");
    }
    let owner_array: ArrayRef = Arc::new(UInt32Array::from(owners));
    let grouping_schema = Arc::new(Schema::new(vec![
        Field::new("owner", DataType::UInt32, false),
        Field::new("key", key_data.data_type().clone(), true),
    ]));
    let mut grouping = new_group_values(grouping_schema, &GroupOrdering::None)?;
    let mut group_ids = Vec::new();
    grouping.intern(&[owner_array, key_data.clone()], &mut group_ids)?;
    let group_count = grouping.len();

    let aggregates = if function == "count" {
        let mut counts = vec![0i64; group_count];
        for group in &group_ids {
            counts[*group] += 1;
        }
        Arc::new(arrow_array::Int64Array::from(counts)) as ArrayRef
    } else {
        let aggregate_function = if function == "count_where" {
            "count"
        } else {
            function
        };
        let aggregate = AggregateExprBuilder::new(
            configured_group_aggregate(aggregate_function, value_data.data_type())?,
            vec![Arc::new(Column::new("value", 0))],
        )
        .schema(Arc::new(Schema::new(vec![Field::new(
            "value",
            value_data.data_type().clone(),
            true,
        )])))
        .alias("group_aggregate")
        .build()?;
        if !aggregate.groups_accumulator_supported() {
            return exec_err!("group aggregate {function} has no native grouped accumulator");
        }
        let mut accumulator = aggregate.create_groups_accumulator()?;
        if function == "count_where" {
            let predicates = value_data
                .as_any()
                .downcast_ref::<arrow_array::BooleanArray>()
                .ok_or_else(|| {
                    datafusion_common::exec_datafusion_err!("count_where values must be bool")
                })?;
            accumulator.update_batch(&[value_data], &group_ids, Some(predicates), group_count)?;
        } else {
            accumulator.update_batch(&[value_data], &group_ids, None, group_count)?;
        }
        accumulator.evaluate(EmitTo::All)?
    };

    let emitted = grouping.emit(EmitTo::All)?;
    let emitted_owners = emitted[0]
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| datafusion_common::exec_datafusion_err!("group owners must be uint32"))?;
    let emitted_keys = &emitted[1];
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
                values: emitted_keys.clone(),
                options,
            },
        ],
        None,
    )?;
    let sorted_keys = take(emitted_keys.as_ref(), &order, None)?;
    let sorted_values = take(aggregates.as_ref(), &order, None)?;
    let DataType::List(item) = output_type else {
        unreachable!()
    };
    let DataType::Struct(fields) = item.data_type() else {
        unreachable!()
    };
    let records = StructArray::try_new(fields.clone(), vec![sorted_keys, sorted_values], None)?;
    let mut owner_lengths = vec![0usize; keys.len()];
    for owner in emitted_owners.values() {
        owner_lengths[*owner as usize] += 1;
    }
    Ok(Arc::new(ListArray::try_new(
        item.clone(),
        OffsetBuffer::from_lengths(owner_lengths),
        Arc::new(records),
        keys.nulls().cloned(),
    )?))
}
