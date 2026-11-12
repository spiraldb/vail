// Shared array-DAG execution and the ordinary packed Boolean reduction sink.
pub(super) fn reduce(
    bundle: &MultiReduce,
    batch: &RecordBatch,
    state_bytes: usize,
) -> Result<Vec<ArrayRef>> {
    let program = &bundle.program;
    let available = MAX_SCRATCH - state_bytes - bundle.kernels.fixed_bytes;
    let capacity = (available / bundle.workspace_bytes_per_lane).min(MAX_BLOCK);
    debug_assert!(capacity != 0);
    let reads = bind_reads(program, batch)?;
    let broadcasts = Broadcasts::new(&bundle.kernels.mod_constants, capacity)?;
    let mut slots = bundle.kernels.constants.clone();
    let lists = reads[program.root_read]
        .values
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let offsets = lists.value_offsets();
    let validity = (0..batch.num_rows())
        .map(|row| reads[program.root_read].valid(&[row, 0]))
        .collect::<Vec<_>>();
    let mut denominator = vec![0_u64; batch.num_rows()];
    let mut numerators = bundle
        .sinks
        .iter()
        .map(|_| vec![0_i64; batch.num_rows()])
        .collect::<Vec<_>>();
    let mut segments = Vec::with_capacity(capacity.min(batch.num_rows()));
    let mut row = 0;
    while row < validity.len() {
        if !validity[row] {
            row += 1;
            continue;
        }
        let first_row = row;
        while row < validity.len() && validity[row] {
            row += 1;
        }
        let end = offsets[row] as usize;
        let mut owner = first_row;
        for start in (offsets[first_row] as usize..end).step_by(capacity) {
            let count = (end - start).min(capacity);
            for (slot, constant) in slots.iter_mut().zip(&bundle.kernels.constants) {
                *slot = constant.clone();
            }
            let domain = ArrayDomain {
                start,
                len: count,
                selected: None,
            };
            evaluate_arrays(bundle, &reads, &mut slots, &broadcasts, domain, true, false)?;
            let mut selection: Option<BooleanBuffer> = None;
            let mut rejected = false;
            for &guard in &bundle.guards {
                let guard = slots[guard].as_ref().expect("guard was evaluated");
                let values = guard.array.as_any().downcast_ref::<BooleanArray>().unwrap();
                if guard.scalar {
                    if values.is_null(0) || !values.value(0) {
                        rejected = true;
                        break;
                    }
                    continue;
                }
                let bits = if let Some(nulls) = values.nulls() {
                    values.values() & nulls.inner()
                } else {
                    values.values().clone()
                };
                selection = Some(match selection {
                    None => bits,
                    Some(old) => &old & &bits,
                });
            }
            if rejected {
                continue;
            }
            let present = selection
                .as_ref()
                .map_or(count, BooleanBuffer::count_set_bits);
            if present == 0 {
                continue;
            }
            segments.clear();
            let mut position = start;
            let mut compacted_end = 0;
            while position < start + count {
                while position >= offsets[owner + 1] as usize {
                    owner += 1;
                }
                let stop = (offsets[owner + 1] as usize).min(start + count);
                let len = if present == count {
                    stop - position
                } else {
                    let bits = selection.as_ref().unwrap();
                    crate::list_aggregate::bitmap_reduce::<2, false>(
                        bits.values(),
                        bits.offset(),
                        None,
                        position - start,
                        stop - position,
                    )
                };
                if len != 0 {
                    compacted_end += len;
                    segments.push((owner, compacted_end));
                    denominator[owner] += len as u64;
                }
                position = stop;
            }
            let indices = (present != count).then(|| {
                UInt32Array::from_iter_values(selection.as_ref().unwrap().set_indices_u32())
            });
            if indices.is_some() {
                // Filtered coordinates define a new domain; only pure demanded
                // dependencies are evaluated again on those selected entries.
                for (slot, constant) in slots.iter_mut().zip(&bundle.kernels.constants) {
                    *slot = constant.clone();
                }
            }
            evaluate_arrays(
                bundle,
                &reads,
                &mut slots,
                &broadcasts,
                ArrayDomain {
                    selected: indices.as_ref(),
                    ..domain
                },
                false,
                indices.is_none(),
            )?;
            for (sink, output) in bundle.sinks.iter().zip(&mut numerators) {
                let result = slots[sink.value].as_ref().expect("sink was evaluated");
                let values = result
                    .array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap();
                let mut first = 0;
                for &(owner, stop) in &segments {
                    let count = if result.scalar {
                        usize::from(!values.is_null(0) && values.value(0)) * (stop - first)
                    } else if let Some(nulls) = values.nulls() {
                        crate::list_aggregate::bitmap_reduce::<2, true>(
                            values.values().values(),
                            values.values().offset(),
                            Some(nulls),
                            first,
                            stop - first,
                        )
                    } else {
                        crate::list_aggregate::bitmap_reduce::<2, false>(
                            values.values().values(),
                            values.values().offset(),
                            None,
                            first,
                            stop - first,
                        )
                    };
                    output[owner] += count as i64;
                    first = stop;
                }
            }
        }
    }
    let nulls = validity
        .iter()
        .any(|&v| !v)
        .then(|| NullBuffer::from(validity));
    bundle
        .sinks
        .iter()
        .zip(numerators)
        .map(|(sink, counts)| {
            if sink.fraction {
                let mut result = Float64Builder::with_capacity(batch.num_rows());
                for (row, (count, &total)) in counts.into_iter().zip(&denominator).enumerate() {
                    result.append_option(
                        (nulls.as_ref().is_none_or(|n| n.is_valid(row)) && total != 0)
                            .then(|| count as f64 / total as f64),
                    );
                }
                Ok(Arc::new(result.finish()) as ArrayRef)
            } else {
                Ok(Arc::new(Int64Array::new(counts.into(), nulls.clone())) as ArrayRef)
            }
        })
        .collect()
}
