//! Isolated schema-interpreter upper-bound experiment; never a production reader.
//! Register as a test-only child of decode.rs. Both paths use identical Input,
//! builders and wire checks. Only field/type/requiredness dispatch is removed.
use super::*;
use std::time::{Duration, Instant};

fn integer(input: &mut Input) -> Result<i32> {
    let value = input.varint(64)?;
    if value > u32::MAX as u64 {
        return Err("Hail int32 varint overflow".into());
    }
    Ok(value as u32 as i32)
}
fn length(input: &mut Input) -> Result<usize> {
    Ok(input.varint(32)? as usize)
}
fn scan(input: &mut Input, mut row: impl FnMut(&mut Input) -> Result<()>) -> Result<usize> {
    let mut rows = 0;
    loop {
        match input.byte()? {
            0 => {
                input.finish()?;
                return Ok(rows);
            }
            1 => {
                row(input)?;
                rows += 1;
            }
            _ => return Err("invalid Hail row marker".into()),
        }
    }
}
fn bind_list(column: &mut Column) -> BoundList<'_, Int32Builder> {
    let Column::List(_, child, offsets, validity) = column else {
        panic!("array output")
    };
    let Column::Int32(values) = child.as_mut() else {
        panic!("ordinary Int32 builder")
    };
    BoundList {
        values,
        offsets,
        validity,
        decode: decode_arrow_list::<2>,
    }
}
// Monomorphic list body, identical integer validation and builder writes to the
// bound production callback, called directly rather than through a function ptr.
fn list(input: &mut Input, output: &mut BoundList<'_, Int32Builder>) -> Result<()> {
    for _ in 0..length(input)? {
        output.values.append_value(integer(input)?);
    }
    output.offsets.push(i32::try_from(output.values.len())?);
    output.validity.append_non_null();
    Ok(())
}
fn skip_list(input: &mut Input) -> Result<()> {
    let count = length(input)?;
    input.skip_varints(count, 32)
}
// Same transactional borrowed-buffer strategy as BufferedScalars, with the
// five field operations written directly instead of a ProbeField interpreter.
fn probe_dp(bytes: &[u8], cursor: &mut usize) -> Option<Option<i32>> {
    let missing = *bytes.get(*cursor)?;
    *cursor += 1;
    if missing & 1 == 0 {
        let count = *bytes.get(*cursor)?;
        if count >= 128 {
            return None;
        }
        *cursor += 1;
        slice_skip_varints(bytes, cursor, count as usize)?;
    }
    let dp = if missing & 2 != 0 {
        None
    } else {
        let value = *bytes.get(*cursor)?;
        if value >= 128 {
            return None;
        }
        *cursor += 1;
        Some(i32::from(value))
    };
    for bit in [4, 8] {
        if missing & bit == 0 {
            if *bytes.get(*cursor)? >= 128 {
                return None;
            }
            *cursor += 1;
        }
    }
    if missing & 16 == 0 {
        let count = *bytes.get(*cursor)?;
        if count >= 128 {
            return None;
        }
        *cursor += 1;
        slice_skip_varints(bytes, cursor, count as usize)?;
    }
    Some(dp)
}
fn specialized<const FULL: bool>(input: &mut Input, column: &mut Column) -> Result<usize> {
    let Column::Struct(_, root_fields, root_validity) = column else {
        panic!("root struct")
    };
    let Column::List(_, entries, offsets, list_validity) = &mut root_fields[0] else {
        panic!("entries array")
    };
    let Column::Struct(_, fields, entry_validity) = entries.as_mut() else {
        panic!("entry struct")
    };
    let [ad, dp, gq, gt, pl] = fields.as_mut_slice() else {
        panic!("five fields")
    };
    let Column::Int32(dp) = dp else {
        panic!("Int32 destination")
    };
    if FULL {
        let mut ad = bind_list(ad);
        let mut pl = bind_list(pl);
        let Column::Int32(gq) = gq else {
            panic!("Int32 destination")
        };
        let Column::Call(gt) = gt else {
            panic!("packed Call destination")
        };
        scan(input, |input| {
            for _ in 0..length(input)? {
                let missing = input.byte()?;
                if missing & 1 != 0 {
                    ad.null();
                } else {
                    list(input, &mut ad)?;
                }
                if missing & 2 != 0 {
                    dp.append_null();
                } else {
                    dp.append_value(integer(input)?);
                }
                if missing & 4 != 0 {
                    gq.append_null();
                } else {
                    gq.append_value(integer(input)?);
                }
                if missing & 8 != 0 {
                    gt.null();
                } else {
                    let value = integer(input)? as u32;
                    validate_packed_call(value)?;
                    gt.packed.push(value);
                    gt.valid.append_non_null();
                }
                if missing & 16 != 0 {
                    pl.null();
                } else {
                    list(input, &mut pl)?;
                }
                entry_validity.append_non_null();
            }
            offsets.push(i32::try_from(entry_validity.len())?);
            list_validity.append_non_null();
            root_validity.append_non_null();
            Ok(())
        })
    } else {
        scan(input, |input| {
            let count = length(input)?;
            let mut done = 0;
            while done < count {
                let accepted = input.with_buffer(|bytes| {
                    let (mut cursor, mut accepted) = (0, 0);
                    while done + accepted < count {
                        let start = cursor;
                        let Some(value) = probe_dp(bytes, &mut cursor) else {
                            cursor = start;
                            break;
                        };
                        dp.append_option(value);
                        entry_validity.append_non_null();
                        accepted += 1;
                    }
                    (accepted, cursor)
                })?;
                if accepted != 0 {
                    done += accepted;
                    continue;
                }
                let missing = input.byte()?;
                if missing & 1 == 0 {
                    skip_list(input)?;
                }
                if missing & 2 != 0 {
                    dp.append_null();
                } else {
                    dp.append_value(integer(input)?);
                }
                if missing & 4 == 0 {
                    input.varint(64)?;
                }
                if missing & 8 == 0 {
                    input.varint(64)?;
                }
                if missing & 16 == 0 {
                    skip_list(input)?;
                }
                entry_validity.append_non_null();
                done += 1;
            }
            offsets.push(i32::try_from(entry_validity.len())?);
            list_validity.append_non_null();
            root_validity.append_non_null();
            Ok(())
        })
    }
}
fn check_shape(component: &Component) {
    let root = &component.encoding;
    assert_eq!(root.kind, "EBaseStruct");
    assert!(root.required);
    assert_eq!(root.fields.len(), 1);
    let array = &root.fields[0].1;
    assert_eq!(array.kind, "EArray2");
    assert!(array.required);
    let entry = &array.fields[0].1;
    assert_eq!(entry.kind, "EBaseStruct");
    assert!(entry.required);
    assert_eq!(entry.fields.len(), 5);
    for (i, (_, field)) in entry.fields.iter().enumerate() {
        assert!(!field.required);
        if i == 0 || i == 4 {
            assert_eq!(field.kind, "EArray2");
            assert_eq!(field.fields[0].1.kind, "EVarint");
            assert!(field.fields[0].1.required);
        } else {
            assert_eq!(field.kind, "EVarint");
        }
    }
    let Type::Struct {
        r#struct: root_types,
    } = &component.dtype
    else {
        panic!("root type")
    };
    let Type::Array { array } = &root_types[0].1 else {
        panic!("array type")
    };
    let Type::Struct { r#struct: types } = array.as_ref() else {
        panic!("entry type")
    };
    assert_eq!(
        types.iter().map(|(name, _)| name).collect::<Vec<_>>(),
        entry
            .fields
            .iter()
            .map(|(name, _)| name)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        types.iter().map(|(_, ty)| ty.clone()).collect::<Vec<_>>(),
        vec![
            Type::Array {
                array: Box::new(Type::Primitive(P::Int32))
            },
            Type::Primitive(P::Int32),
            Type::Primitive(P::Int32),
            Type::Primitive(P::Call),
            Type::Array {
                array: Box::new(Type::Primitive(P::Int32))
            },
        ]
    );
}
fn run(
    bytes: Arc<[u8]>,
    leb: bool,
    node: &Node,
    compiled: bool,
    full: bool,
) -> (Result<usize>, u64, Column, Duration) {
    let mut input = Input::from_memory(bytes, leb);
    let mut column = Column::projected(node, &node.ty.to_arrow().unwrap());
    let start = Instant::now();
    let result = if compiled {
        if full {
            specialized::<true>(&mut input, &mut column)
        } else {
            specialized::<false>(&mut input, &mut column)
        }
    } else {
        scan(&mut input, |input| node.append(input, &mut column))
    };
    let elapsed = start.elapsed();
    (result, input.bytes, column, elapsed)
}
fn differential(bytes: Arc<[u8]>, leb: bool, node: &Node, full: bool) -> Result<()> {
    let (generic, gb, mut gc, _) = run(bytes.clone(), leb, node, false, full);
    let (compiled, cb, mut cc, _) = run(bytes, leb, node, true, full);
    assert_eq!(generic.is_ok(), compiled.is_ok());
    assert_eq!(gb, cb);
    match (generic, compiled) {
        (Ok(g), Ok(c)) => {
            assert_eq!(g, c);
            assert_eq!(gc.finish()?.to_data(), cc.finish()?.to_data());
        }
        (Err(g), Err(c)) => assert_eq!(g.to_string(), c.to_string()),
        _ => unreachable!(),
    }
    Ok(())
}
#[test]
#[ignore = "manual real-fixture monomorphic decoder comparison"]
fn real_entries_schema_dispatch_cost() -> Result<()> {
    let path = std::env::var("VAIL_LEGACY_PROFILE_MATRIX").unwrap_or_else(|_| "/Users/danielking/projects/vail/vail/conformance/runs/20261016-hail-paired/data/profile25.mt".into());
    let component = Dataset::open(&path, "MatrixTableSpec")?.component("entries")?;
    check_shape(&component);
    let part = std::env::var("VAIL_LEGACY_PROFILE_PART")
        .ok()
        .map(|s| s.parse::<usize>())
        .transpose()?
        .unwrap_or(0);
    let mut input = Input::open(&component.paths[part], &component.buffer)?;
    let leb = input.leb;
    let mut bytes = Vec::new();
    loop {
        let copied = input.with_buffer(|available| {
            bytes.extend_from_slice(available);
            (available.len(), available.len())
        })?;
        if copied == 0 {
            break;
        }
    }
    let bytes: Arc<[u8]> = bytes.into();
    for full in [true, false] {
        let mut node = Node::new(&component.encoding, &component.dtype);
        if !full {
            node.demand(&[vec![
                component.encoding.fields[0].0.clone(),
                component.encoding.fields[0].1.fields[0].1.fields[1]
                    .0
                    .clone(),
            ]]);
        }
        differential(bytes.clone(), leb, &node, full)?;
        for cut in [1, 2, 7, 17, bytes.len() / 2] {
            differential(Arc::from(&bytes[..bytes.len() - cut]), leb, &node, full)?;
        }
        let mut samples = [Vec::new(), Vec::new()];
        for round in 0..6 {
            for compiled in if round % 2 == 0 {
                [false, true]
            } else {
                [true, false]
            } {
                let (result, consumed, column, elapsed) =
                    run(bytes.clone(), leb, &node, compiled, full);
                let rows = result?;
                assert_eq!(consumed as usize, bytes.len());
                std::hint::black_box(column);
                if round != 0 {
                    samples[usize::from(compiled)].push(elapsed.as_secs_f64());
                }
                eprintln!(
                    "schema_bench full={full} compiled={compiled} rows={rows} seconds={:.6}",
                    elapsed.as_secs_f64()
                );
            }
        }
        for values in &mut samples {
            values.sort_by(f64::total_cmp);
        }
        eprintln!(
            "schema_bench MEDIAN full={full} interpreted={:.6} monomorphic={:.6} ratio={:.3} bytes={} (decompression, setup, finish/drop excluded)",
            samples[0][2],
            samples[1][2],
            samples[0][2] / samples[1][2],
            bytes.len()
        );
    }
    Ok(())
}
