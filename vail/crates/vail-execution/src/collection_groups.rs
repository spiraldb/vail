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
    Aggregate(Vec<(String, bool)>),
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

pub(crate) fn udf(name: &str) -> Option<Arc<ScalarUDF>> {
    let operation = match name {
        "__vail_dict_first" => Operation::First,
        "__vail_dict_last" => Operation::Last,
        "__vail_collection_group" => Operation::Group,
        _ => {
            let functions = if let Some(functions) =
                name.strip_prefix("__vail_collection_group_aggregate_multi__")
            {
                functions
                    .split("__")
                    .map(|spec| {
                        spec.strip_suffix("_filtered")
                            .map_or((spec.to_owned(), false), |function| {
                                (function.to_owned(), true)
                            })
                    })
                    .collect::<Vec<_>>()
            } else {
                let spec = name.strip_prefix("__vail_collection_group_aggregate_")?;
                vec![
                    spec.strip_suffix("_filtered")
                        .map_or((spec.to_owned(), false), |function| {
                            (function.to_owned(), true)
                        }),
                ]
            };
            if functions.is_empty()
                || functions.iter().any(|(function, _)| {
                    !matches!(
                        function.as_str(),
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
            1 + functions
                .iter()
                .map(|(function, filtered)| {
                    (if function == "count" { 0 } else { 1 }) + (if *filtered { 1 } else { 0 })
                })
                .sum::<usize>()
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
            let [DataType::List(key), values @ ..] = args else {
                return plan_err!("group aggregate requires key and value lists");
            };
            let mut value_types = values.iter();
            let mut result_fields = Vec::with_capacity(functions.len());
            for (index, (function, filtered)) in functions.iter().enumerate() {
                let input_type = if function == "count" {
                    None
                } else {
                    Some(match value_types.next() {
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
                if *filtered {
                    match value_types.next() {
                        Some(DataType::List(filter))
                            if filter.data_type() == &DataType::Boolean => {}
                        _ => {
                            return plan_err!(
                                "filtered group aggregate requires boolean filter lists"
                            );
                        }
                    }
                }
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
            if value_types.next().is_some() {
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
            let mut values = arrays.iter().skip(1);
            let mut specs = Vec::with_capacity(functions.len());
            for (function, filtered) in functions {
                let value = if function == "count" {
                    None
                } else {
                    Some(
                        values
                            .next()
                            .and_then(|(array, scalar)| {
                                array
                                    .as_any()
                                    .downcast_ref::<ListArray>()
                                    .map(|array| ListInput {
                                        array,
                                        scalar: *scalar,
                                    })
                            })
                            .ok_or_else(|| {
                                datafusion_common::exec_datafusion_err!(
                                    "group values must be lists"
                                )
                            })?,
                    )
                };
                let filter = if *filtered {
                    Some(
                        values
                            .next()
                            .and_then(|(array, scalar)| {
                                array
                                    .as_any()
                                    .downcast_ref::<ListArray>()
                                    .map(|array| ListInput {
                                        array,
                                        scalar: *scalar,
                                    })
                            })
                            .ok_or_else(|| {
                                datafusion_common::exec_datafusion_err!(
                                    "group filters must be lists"
                                )
                            })?,
                    )
                } else {
                    None
                };
                specs.push((function.as_str(), value, filter));
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

fn grouped_aggregate(
    keys: ListInput<'_>,
    specs: &[(&str, Option<ListInput<'_>>, Option<ListInput<'_>>)],
    row_count: usize,
    output_type: &DataType,
) -> Result<ArrayRef> {
    let mut lists = vec![keys];
    for (_, values, filter) in specs {
        lists.extend(values.iter().copied());
        lists.extend(filter.iter().copied());
    }
    if lists
        .iter()
        .any(|input| input.array.len() != if input.scalar { 1 } else { row_count })
    {
        return exec_err!("group key/value row counts differ");
    }
    let selection_needed = (row_count > 1 && lists.iter().any(|input| input.scalar))
        || lists.iter().any(|input| input.array.null_count() != 0);
    let offsets = lists
        .iter()
        .map(|input| input.array.value_offsets())
        .collect::<Vec<_>>();
    let mut selections = if selection_needed {
        vec![Vec::new(); lists.len()]
    } else {
        Vec::new()
    };
    let mut owners = Vec::new();
    for row in 0..row_count {
        let key_row = if keys.scalar { 0 } else { row };
        let valid = !keys.array.is_null(key_row);
        if !valid && keys.scalar {
            continue;
        }
        if lists.iter().enumerate().skip(1).any(|(index, input)| {
            let input_row = if input.scalar { 0 } else { row };
            input.array.is_null(input_row) == valid
        }) {
            return exec_err!("group key/value/filter list validity differs");
        }
        if !valid {
            continue;
        }
        let key_start = offsets[0][key_row] as usize;
        let key_end = offsets[0][key_row + 1] as usize;
        for (index, list_offsets) in offsets.iter().enumerate().skip(1) {
            let input_row = if lists[index].scalar { 0 } else { row };
            if list_offsets[input_row + 1] - list_offsets[input_row] != (key_end - key_start) as i32
            {
                return exec_err!("group key/value/filter list lengths differ");
            }
        }
        let owner = u32::try_from(row).map_err(|_| {
            datafusion_common::exec_datafusion_err!(
                "collection owner count exceeds Arrow index capacity"
            )
        })?;
        for offset in 0..key_end - key_start {
            if selection_needed {
                for (index, list_offsets) in offsets.iter().enumerate() {
                    let input_row = if lists[index].scalar { 0 } else { row };
                    selections[index].push(
                        u32::try_from(list_offsets[input_row] as usize + offset).map_err(|_| {
                            datafusion_common::exec_datafusion_err!(
                                "collection element offset exceeds Arrow index capacity"
                            )
                        })?,
                    );
                }
            }
            owners.push(owner);
        }
    }
    let child_arrays = lists
        .iter()
        .enumerate()
        .map(|(index, input)| {
            if selection_needed {
                take(
                    input.array.values().as_ref(),
                    &UInt32Array::from(std::mem::take(&mut selections[index])),
                    None,
                )
            } else {
                let start = offsets[index][0] as usize;
                let end = offsets[index][row_count] as usize;
                Ok(input.array.values().slice(start, end - start))
            }
        })
        .collect::<Result<Vec<_>>>()?;
    if !selection_needed && child_arrays[0].len() != owners.len() {
        return exec_err!("group key offsets do not cover their children");
    }
    let key_data = child_arrays[0].clone();
    let owner_array: ArrayRef = Arc::new(UInt32Array::from(owners));
    let grouping_schema = Arc::new(Schema::new(vec![
        Field::new("owner", DataType::UInt32, false),
        Field::new("key", key_data.data_type().clone(), true),
    ]));
    let mut grouping = new_group_values(grouping_schema, &GroupOrdering::None)?;
    let mut group_ids = Vec::new();
    grouping.intern(&[owner_array.clone(), key_data.clone()], &mut group_ids)?;
    let group_count = grouping.len();

    let mut aggregates = Vec::with_capacity(specs.len());
    let mut child_index = 1;
    for (function, values, filter) in specs {
        let data = if *function == "count" {
            owner_array.clone()
        } else {
            let data = child_arrays[child_index].clone();
            child_index += 1;
            data
        };
        let filter_data = if filter.is_some() {
            let data = child_arrays[child_index].clone();
            child_index += 1;
            Some(data)
        } else {
            None
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
        let mut combined_count_where = None;
        let predicate = if *function == "count_where" {
            let input = data
                .as_any()
                .downcast_ref::<arrow_array::BooleanArray>()
                .ok_or_else(|| {
                    datafusion_common::exec_datafusion_err!("count_where values must be bool")
                })?;
            if let Some(filter) = &filter_data {
                let filter = filter
                    .as_any()
                    .downcast_ref::<arrow_array::BooleanArray>()
                    .ok_or_else(|| {
                        datafusion_common::exec_datafusion_err!("group filter must be bool")
                    })?;
                let payload = input.values().clone();
                let mut validity = &payload & filter.values();
                if let Some(nulls) = input.nulls() {
                    validity = &validity & nulls.inner();
                }
                if let Some(nulls) = filter.nulls() {
                    validity = &validity & nulls.inner();
                }
                combined_count_where = Some(arrow_array::BooleanArray::new(
                    payload,
                    Some(NullBuffer::new(validity)),
                ));
                Some(combined_count_where.as_ref().unwrap())
            } else {
                Some(input)
            }
        } else if filter_data.is_some() {
            Some(
                filter_data
                    .as_ref()
                    .unwrap()
                    .as_any()
                    .downcast_ref::<arrow_array::BooleanArray>()
                    .ok_or_else(|| {
                        datafusion_common::exec_datafusion_err!("group filter must be bool")
                    })?,
            )
        } else {
            None
        };
        accumulator.update_batch(&[data.clone()], &group_ids, predicate, group_count)?;
        aggregates.push(accumulator.evaluate(EmitTo::All)?);
    }

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
