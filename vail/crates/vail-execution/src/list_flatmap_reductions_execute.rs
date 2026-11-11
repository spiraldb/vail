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
    let mut owners = vec![0_usize; capacity];
    let mut selected = vec![0_u8; capacity];
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
            let mut present_count = 0;
            for lane in 0..count {
                while start + lane >= offsets[owner + 1] as usize {
                    owner += 1;
                }
                owners[lane] = owner;
                let present = bundle
                    .guards
                    .iter()
                    .all(|&guard| execution.workspace[guard].true_at(lane));
                selected[lane] = u8::from(present);
                denominator[owner] += u64::from(present);
                present_count += usize::from(present);
            }
            // The filter owns the value domain. Never evaluate an expensive
            // body on eliminated entries, including hidden null-owner spans.
            if present_count == 0 {
                continue;
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
                        let next = execution.coordinates[1].len();
                        execution.coordinates[1].push(start + lane);
                        owners[next] = owners[lane];
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
                for lane in 0..active_count {
                    output[owners[lane]] += i64::from(values.true_at(lane));
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
