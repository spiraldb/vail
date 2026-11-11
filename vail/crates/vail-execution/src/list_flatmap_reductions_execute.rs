// Multi-sink consumer for the same typed register block executor.
pub(super) fn reduce(
    bundle: &MultiReduce,
    batch: &RecordBatch,
    state_bytes: usize,
) -> Result<Vec<ArrayRef>> {
    let program = &bundle.program;
    let reads = bind_reads(program, batch)?;
    let bytes_per_lane = bundle.workspace_bytes_per_lane;
    let available = MAX_SCRATCH - state_bytes - 4096;
    if available < bytes_per_lane {
        return Err(DataFusionError::Internal(
            "multi reduction workspace estimate changed".into(),
        ));
    }
    let capacity = (available / bytes_per_lane).clamp(1, MAX_BLOCK);
    let mut execution = Execution {
        program,
        reads,
        positions: vec![0; 2],
        coordinates: vec![vec![], Vec::with_capacity(capacity)],
        leaves: vec![],
        workspace: program.workspace.lease(&program.registers, capacity),
        output: Output::Bool(BooleanBuilder::new()),
        output_len: 0,
        output_counts: vec![],
        guard_results: vec![],
        capacity,
        domains: vec![(false, false, 0); 2],
        span: None,
        emissions: vec![],
        fold_entries: vec![],
        output_validity: vec![],
        reuse_guards: false,
    };
    let lists = execution.reads[program.root_read]
        .values
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .clone();
    let offsets = lists.value_offsets();
    let validity = (0..batch.num_rows())
        .map(|row| {
            execution.positions[0] = row;
            execution.reads[program.root_read].valid(&execution.positions)
        })
        .collect::<Vec<_>>();
    let mut denominator = vec![0_u64; batch.num_rows()];
    let mut numerators = bundle
        .sinks
        .iter()
        .map(|_| vec![0_i64; batch.num_rows()])
        .collect::<Vec<_>>();
    let mut segments = Vec::with_capacity(capacity.min(batch.num_rows()));
    let mut selected = vec![0_u8; capacity];
    let mut packed = vec![0_u8; capacity.div_ceil(8)];
    let mask = if bundle.sinks.len() == 64 {
        u64::MAX
    } else {
        (1_u64 << bundle.sinks.len()) - 1
    };
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
        let end_row = row;
        let end = offsets[end_row] as usize;
        let mut owner = first_row;
        for start in (offsets[first_row] as usize..end).step_by(capacity) {
            let count = (end - start).min(capacity);
            execution.reuse_guards = false;
            execution.span = Some(SpanInput {
                coordinate: 1,
                leaves: 0,
                lanes: InputLanes::Contiguous(start),
            });
            if !bundle.guards.is_empty() {
                execution.compute(count, Some(1));
            }
            let present_count = if bundle.guards.is_empty() {
                selected[..count].fill(1);
                count
            } else {
                for (lane, selected) in selected[..count].iter_mut().enumerate() {
                    *selected = u8::from(bundle.guards.iter()
                        .all(|&guard| execution.workspace[guard].true_at(lane)));
                }
                selected[..count].iter().map(|&v| usize::from(v)).sum()
            };
            // The filter owns the value domain. Never evaluate an expensive
            // body on eliminated entries, including hidden null-owner spans.
            if present_count == 0 {
                continue;
            }
            // All sinks share this segmented compacted domain. Derive it once
            // from owner offsets, rather than scattering a counter per lane.
            segments.clear();
            let mut position = start;
            let mut compacted_end = 0;
            while position < start + count {
                while position >= offsets[owner + 1] as usize {
                    owner += 1;
                }
                let stop = (offsets[owner + 1] as usize).min(start + count);
                let len = if present_count == count {
                    stop - position
                } else {
                    selected[position - start..stop - start].iter()
                        .map(|&v| usize::from(v)).sum()
                };
                if len != 0 {
                    compacted_end += len;
                    segments.push((owner, compacted_end));
                    denominator[owner] += len as u64;
                }
                position = stop;
            }
            let active_count = if present_count == count {
                execution.reuse_guards = !bundle.guards.is_empty();
                execution.span = Some(SpanInput {
                    coordinate: 1,
                    leaves: mask,
                    lanes: InputLanes::Contiguous(start),
                });
                count
            } else {
                execution.coordinates[1].clear();
                for lane in 0..count {
                    if selected[lane] != 0 {
                        execution.coordinates[1].push(start + lane);
                    }
                }
                // Selection changes the domain; recompute cheap guard/leaf
                // reads when also demanded by a sink, not their old vectors.
                execution.span = Some(SpanInput {
                    coordinate: 1,
                    leaves: mask,
                    lanes: InputLanes::Selected,
                });
                present_count
            };
            execution.compute(active_count, None);
            for (sink, output) in bundle.sinks.iter().zip(&mut numerators) {
                let values = &execution.workspace[sink.value];
                if !values.scalar {
                    pack_boolean(values, active_count, &mut packed);
                }
                let mut first = 0;
                for &(owner, end) in &segments {
                    let count = if values.scalar {
                        usize::from(values.true_at(0)) * (end - first)
                    } else {
                        crate::list_aggregate::bitmap_reduce::<2, false>(
                            &packed, 0, None, first, end - first,
                        )
                    };
                    output[owner] += count as i64;
                    first = end;
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

// Convert one bounded predicate block to the packed view used by ordinary
// Boolean list reducers. Scratch is reused across sinks and blocks; null
// predicates contribute false without changing the shared denominator.
fn pack_boolean(buffer: &Buffer, len: usize, packed: &mut [u8]) {
    let values = buffer.boolean(len);
    if buffer.all_valid {
        for (byte, values) in packed.iter_mut().zip(values.chunks(8)) {
            *byte = values.iter().enumerate()
                .fold(0, |word, (bit, &value)| word | (u8::from(value != 0) << bit));
        }
    } else {
        for ((byte, values), valid) in packed.iter_mut().zip(values.chunks(8))
            .zip(buffer.valid[..len].chunks(8))
        {
            *byte = values.iter().zip(valid).enumerate()
                .fold(0, |word, (bit, (&value, &valid))| {
                    word | (u8::from(value != 0 && valid != 0) << bit)
                });
        }
    }
}
