//! Opt-in resident-memory stage measurements. Stages overlap; timings are not additive.
use super::*;
use std::{hint::black_box, sync::Arc, time::Instant};

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

fn measure<S>(
    label: &str,
    values: usize,
    bytes: usize,
    mut setup: impl FnMut() -> S,
    mut run: impl FnMut(&mut S) -> Result<u64>,
) -> Result<f64> {
    let mut times = Vec::new();
    let mut expected = None;
    for repetition in 0..9 {
        let mut state = setup(); // allocation, input construction, and predecoded data are untimed
        let start = Instant::now();
        let checksum = run(black_box(&mut state))?;
        let elapsed = start.elapsed();
        black_box(&state);
        black_box(checksum);
        assert_eq!(*expected.get_or_insert(checksum), checksum);
        if repetition >= 2 {
            times.push(elapsed.as_secs_f64());
        }
    }
    times.sort_by(f64::total_cmp);
    let seconds = times[times.len() / 2];
    eprintln!(
        "{label:36} {:8.3} ns/value {:8.2} Mvalue/s {:8.2} MiB/s ({values} values, {bytes} input bytes)",
        seconds * 1e9 / values as f64,
        values as f64 / seconds / 1e6,
        bytes as f64 / seconds / 1048576.0
    );
    Ok(seconds)
}

fn projected_scalar_rows(input: &mut Input, output: &mut Column, n: usize) -> Result<u64> {
    let Column::Struct(_, children, validity) = output else {
        unreachable!()
    };
    let (first, rest) = children.split_at_mut(1);
    let x = first[0].int32();
    let y = rest[0].float64();
    for _ in 0..n {
        let bits = bitmap(input, 3)?;
        if bits.low_word() & 7 == 0 {
            x.append_value(input.int(32)? as i32);
            y.append_value(f64::from_le_bytes(input.fixed()?));
            input.int(64)?;
        } else {
            if absent(&bits, 0) {
                x.append_null();
            } else {
                x.append_value(input.int(32)? as i32);
            }
            if absent(&bits, 1) {
                y.append_null();
            } else {
                y.append_value(f64::from_le_bytes(input.fixed()?));
            }
            if !absent(&bits, 2) {
                input.int(64)?;
            }
        }
        validity.append_non_null();
    }
    Ok(n as u64)
}

#[test]
#[ignore = "opt-in resident-memory decoder stages; run release --ignored --nocapture"]
fn resident_decoder_stages() -> Result<()> {
    const N: usize = 262_144;
    let integers: Arc<[u32]> = (0..N)
        .map(|i| {
            if i % 4 == 0 {
                (i as u32 * 7919) & 0xffffff
            } else {
                (i % 127) as u32
            }
        })
        .collect();
    let floats: Arc<[f64]> = (0..N).map(|i| i as f64 * 0.25).collect();
    let mut encoded = Vec::new();
    for &value in integers.iter() {
        leb(value as u64, &mut encoded);
    }
    let encoded: Arc<[u8]> = encoded.into();
    let float_bytes: Arc<[u8]> = floats.iter().flat_map(|v| v.to_le_bytes()).collect();
    eprintln!(
        "Resident Cursor -> normal buffered Input; 2 warmups, 7 samples, median. No filesystem/decompression. Stages overlap and MUST NOT be added together."
    );
    measure(
        "parse varint32 + checksum",
        N,
        encoded.len(),
        || Input::from_memory(encoded.clone(), true),
        |input| {
            let mut sum = 0u64;
            for _ in 0..N {
                sum = sum.wrapping_add(input.varint(32)?);
            }
            assert_eq!(input.bytes as usize, encoded.len());
            Ok(sum)
        },
    )?;
    measure(
        "skip varints (no value decode)",
        N,
        encoded.len(),
        || Input::from_memory(encoded.clone(), true),
        |input| {
            input.skip_varints(N, 32)?;
            assert_eq!(input.bytes as usize, encoded.len());
            Ok(input.bytes)
        },
    )?;
    measure(
        "parse float64 + checksum",
        N,
        float_bytes.len(),
        || Input::from_memory(float_bytes.clone(), false),
        |input| {
            let mut sum = 0.0;
            for _ in 0..N {
                sum += f64::from_le_bytes(input.fixed()?);
            }
            Ok(sum.to_bits())
        },
    )?;
    for capacity in [N, 8192] {
        measure(
            if capacity == N {
                "Int32 append capacity=N"
            } else {
                "Int32 append capacity8192 + growth"
            },
            N,
            0,
            || Int32Builder::with_capacity(capacity),
            |builder| {
                for &value in integers.iter() {
                    builder.append_value(value as i32);
                }
                Ok(builder.len() as u64)
            },
        )?;
        measure(
            if capacity == N {
                "Float64 append capacity=N"
            } else {
                "Float64 append capacity8192 + growth"
            },
            N,
            0,
            || Float64Builder::with_capacity(capacity),
            |builder| {
                for &value in floats.iter() {
                    builder.append_value(value);
                }
                Ok(builder.len() as u64)
            },
        )?;
    }
    for missing in [false, true] {
        let bitmap_bytes: Arc<[u8]> = (0..N)
            .map(|i| 0xf8 | if missing && i % 5 == 0 { 1 } else { 0 })
            .collect();
        measure(
            if missing {
                "bitmap3 mixed + present count"
            } else {
                "bitmap3 present + present count"
            },
            N,
            N,
            || Input::from_memory(bitmap_bytes.clone(), false),
            |input| {
                let mut present = 0;
                for _ in 0..N {
                    present += bitmap(input, 3)?.present_count(3);
                }
                Ok(present as u64)
            },
        )?;
        let ty = Type::Struct {
            r#struct: vec![
                ("x".into(), Type::Primitive(P::Int32)),
                ("y".into(), Type::Primitive(P::Float64)),
                ("unused".into(), Type::Primitive(P::Int64)),
            ],
        };
        let scalar = |kind: &str| Encoding {
            kind: kind.into(),
            required: false,
            fields: vec![],
            rank: 0,
        };
        let encoding = Encoding {
            kind: "EBaseStruct".into(),
            required: true,
            fields: vec![
                ("x".into(), scalar("EInt32")),
                ("y".into(), scalar("EFloat64")),
                ("unused".into(), scalar("EInt64")),
            ],
            rank: 0,
        };
        let mut node = Node::new(&encoding, &ty);
        node.demand(&[vec!["x".into()], vec!["y".into()]]);
        let arrow_type = ty.to_arrow()?;
        let mut bytes = Vec::new();
        for i in 0..N {
            bytes.push(bitmap_bytes[i]);
            if !missing || i % 5 != 0 {
                leb(integers[i] as u64, &mut bytes);
            }
            bytes.extend_from_slice(&floats[i].to_le_bytes());
            leb(integers[i] as u64, &mut bytes);
        }
        let bytes: Arc<[u8]> = bytes.into();
        let values = N * 2 - if missing { N.div_ceil(5) } else { 0 };
        // Identical selected columns, initial capacities, validity, and bytes.
        // This synthetic fixture is not the real GT/AD/DP/GQ/PL entry schema.
        let mut expected = Column::projected(&node, &arrow_type);
        let mut actual = Column::projected(&node, &arrow_type);
        let mut input = Input::from_memory(bytes.clone(), true);
        for _ in 0..N {
            node.append(&mut input, &mut expected)?;
        }
        projected_scalar_rows(&mut Input::from_memory(bytes.clone(), true), &mut actual, N)?;
        assert_eq!(expected.finish()?.as_ref(), actual.finish()?.as_ref());
        drop((expected, actual, input));
        let seconds = measure(
            if missing {
                "Node selected struct mixed + append"
            } else {
                "Node selected struct present + append"
            },
            values,
            bytes.len(),
            || {
                (
                    Input::from_memory(bytes.clone(), true),
                    Column::projected(&node, &arrow_type),
                )
            },
            |(input, output)| {
                for _ in 0..N {
                    node.append(input, output)?;
                }
                assert_eq!(input.bytes as usize, bytes.len());
                Ok(output.len() as u64)
            },
        )?;
        eprintln!("  Node: {:.3} ns/row ({N} rows)", seconds * 1e9 / N as f64);
        let seconds = measure(
            if missing {
                "handwritten selected mixed + append"
            } else {
                "handwritten selected present + append"
            },
            values,
            bytes.len(),
            || {
                (
                    Input::from_memory(bytes.clone(), true),
                    Column::projected(&node, &arrow_type),
                )
            },
            |(input, output)| {
                let rows = projected_scalar_rows(input, output, N)?;
                assert_eq!(input.bytes as usize, bytes.len());
                Ok(rows)
            },
        )?;
        eprintln!(
            "  handwritten: {:.3} ns/row ({N} rows)",
            seconds * 1e9 / N as f64
        );
    }
    Ok(())
}
