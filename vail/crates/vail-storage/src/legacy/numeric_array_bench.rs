//! Isolated required-integer array experiment; never enabled in production.
use super::*;
use std::{hint::black_box, time::Instant};

type Decoder = fn(&Node, &mut Input, &mut Column) -> Result<()>;

#[inline(always)]
fn append_varint_values<const BITS: u32>(
    input: &mut Input,
    builder: &mut Int32Builder,
    n: usize,
) -> Result<()> {
    if n <= 8 {
        if let Some(bytes) = input.try_small_varints(n)? {
            let mut values = [0i32; 8];
            for (out, byte) in values.iter_mut().zip(bytes).take(n) {
                *out = i32::from(byte);
            }
            builder.append_slice(&values[..n]);
            return Ok(());
        }
    }
    for _ in 0..n {
        let value = input.varint(BITS)?;
        if BITS == 64 && value > u32::MAX as u64 {
            return Err("Hail int32 varint overflow".into());
        }
        builder.append_value(value as u32 as i32);
    }
    Ok(())
}

#[inline(never)]
fn grouped_required_i32_array(node: &Node, input: &mut Input, out: &mut Column) -> Result<()> {
    let n = node.length(input)?;
    let child_node = &node.children[0];
    let Column::List(_, values, _, _) = out else {
        unreachable!("benchmark list output")
    };
    let builder = values.int32();
    match child_node.wire {
        Wire::Varint => append_varint_values::<64>(input, builder, n)?,
        Wire::Int32 if input.leb => append_varint_values::<32>(input, builder, n)?,
        Wire::Int32 => {
            for _ in 0..n {
                builder.append_value(i32::from_le_bytes(input.fixed()?));
            }
        }
        _ => unreachable!("benchmark required int32 encoding"),
    }
    out.end_list()?;
    Ok(())
}

fn leb(mut value: u64, bytes: &mut Vec<u8>) {
    loop {
        let byte = (value & 127) as u8;
        value >>= 7;
        bytes.push(byte | if value == 0 { 0 } else { 128 });
        if value == 0 {
            break;
        }
    }
}

fn fixture(
    n: usize,
    arrays: usize,
    pattern: usize,
    encoding: usize,
) -> (Node, Arc<[u8]>, bool, DataType) {
    let ty = Type::Array {
        array: Box::new(Type::Primitive(P::Int32)),
    };
    let child = Encoding {
        kind: if encoding == 0 { "EVarint" } else { "EInt32" }.into(),
        required: true,
        fields: vec![],
        rank: 0,
    };
    let wire = Encoding {
        kind: "EArray2".into(),
        required: true,
        fields: vec![("element".into(), child)],
        rank: 0,
    };
    let mut bytes = Vec::new();
    for array in 0..arrays {
        leb(n as u64, &mut bytes);
        for i in 0..n {
            let value = match pattern {
                0 => ((array + i) % 64) as i32,
                1 => 129 * (i as i32 + 1),
                _ => -1 - (i % 11) as i32,
            };
            if encoding == 2 {
                bytes.extend(value.to_le_bytes());
            } else {
                leb(value as u32 as u64, &mut bytes);
            }
        }
    }
    (
        Node::new(&wire, &ty),
        bytes.into(),
        encoding == 1,
        ty.to_arrow().unwrap(),
    )
}

fn decode(
    node: &Node,
    bytes: Arc<[u8]>,
    leb: bool,
    dtype: &DataType,
    arrays: usize,
    decoder: Decoder,
) -> Result<ArrayRef> {
    let mut input = Input::from_memory(bytes.clone(), leb);
    let mut column = Column::new(dtype);
    for _ in 0..arrays {
        decoder(node, &mut input, &mut column)?;
    }
    assert_eq!(input.bytes, bytes.len() as u64);
    column.finish()
}

#[test]
fn grouped_numeric_array_experiment_matches_scalar_output() -> Result<()> {
    for n in 0..=9 {
        for pattern in 0..3 {
            for encoding in 0..3 {
                let (node, bytes, leb, dtype) = fixture(n, 257, pattern, encoding);
                let scalar = decode(&node, bytes.clone(), leb, &dtype, 257, Node::append_list)?;
                let grouped = decode(&node, bytes, leb, &dtype, 257, grouped_required_i32_array)?;
                assert_eq!(
                    scalar.as_ref(),
                    grouped.as_ref(),
                    "n={n} pattern={pattern} encoding={encoding}"
                );
            }
        }
    }
    // A larger fixture crosses the real 64 KiB input buffer while probing.
    for pattern in 0..3 {
        let (node, bytes, leb, dtype) = fixture(8, 8193, pattern, 0);
        let scalar = decode(&node, bytes.clone(), leb, &dtype, 8193, Node::append_list)?;
        let grouped = decode(&node, bytes, leb, &dtype, 8193, grouped_required_i32_array)?;
        assert_eq!(scalar.as_ref(), grouped.as_ref());
    }
    Ok(())
}

fn measure(
    node: &Node,
    bytes: Arc<[u8]>,
    leb: bool,
    dtype: &DataType,
    arrays: usize,
    decoder: Decoder,
) -> Result<Vec<f64>> {
    let mut samples = Vec::new();
    for repetition in 0..7 {
        let mut input = Input::from_memory(bytes.clone(), leb);
        let mut column = Column::new(dtype);
        let decoder = black_box(decoder);
        let start = Instant::now();
        for _ in 0..arrays {
            decoder(black_box(node), &mut input, &mut column)?;
        }
        let output = column.finish()?;
        let seconds = start.elapsed().as_secs_f64();
        assert_eq!(input.bytes, bytes.len() as u64);
        black_box(output);
        if repetition >= 2 {
            samples.push(seconds);
        }
    }
    Ok(samples)
}

#[test]
#[ignore = "manual isolated numeric array decoder comparison"]
fn benchmark_required_numeric_arrays_scalar_vs_grouped() -> Result<()> {
    let arrays = std::env::var("VAIL_NUMERIC_ARRAY_BENCH_ARRAYS")
        .ok()
        .map(|value| value.parse::<usize>())
        .transpose()?
        .unwrap_or(65536);
    for n in [2usize, 3, 8, 9] {
        for pattern in 0..3 {
            for encoding in 0..3 {
                let (node, bytes, leb, dtype) = fixture(n, arrays, pattern, encoding);
                let scalar = measure(&node, bytes.clone(), leb, &dtype, arrays, Node::append_list)?;
                let grouped = measure(
                    &node,
                    bytes.clone(),
                    leb,
                    &dtype,
                    arrays,
                    grouped_required_i32_array,
                )?;
                let median = |values: &[f64]| {
                    let mut sorted = values.to_vec();
                    sorted.sort_by(f64::total_cmp);
                    sorted[sorted.len() / 2]
                };
                let scalar_median = median(&scalar);
                let grouped_median = median(&grouped);
                println!(
                    "{}",
                    serde_json::json!({"benchmark":"required_i32_arrays", "length":n,
            "pattern":(["small", "continued", "negative"][pattern]), "encoding":(["varint", "leb_int32", "fixed_int32"][encoding]),
            "arrays":arrays, "input_bytes":bytes.len(), "scalar_seconds":scalar,
            "grouped_seconds":grouped, "scalar_ns_per_array":scalar_median * 1e9 / arrays as f64,
            "grouped_ns_per_array":grouped_median * 1e9 / arrays as f64,
            "speedup":scalar_median / grouped_median})
                );
            }
        }
    }
    Ok(())
}
