//! Black-box legacy reader regressions: serialized file bytes enter exclusively
//! through public Component/PartReader/layout APIs; expected values are independent.
use arrow_array::{
    Array, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, ListArray,
    RecordBatch, StringArray, StructArray,
};
use arrow_schema::{DataType, Schema};
use datafusion_common::utils::memory::{RecordBatchMemoryCounter, get_record_batch_memory_size};
use std::{collections::HashMap, sync::Arc};
use tempfile::TempDir;
use vail_storage::legacy::{Component, Parser, Result, layout::arrow_batch};
use vortex::VortexSessionDefault;

struct Fixture {
    _dir: TempDir,
    component: Component,
}
fn missing(array: &dyn Array, row: usize) -> bool {
    array
        .logical_nulls()
        .is_some_and(|nulls| nulls.is_null(row))
}
fn leb(mut value: u64, out: &mut Vec<u8>) {
    loop {
        let b = (value & 127) as u8;
        value >>= 7;
        out.push(b | if value == 0 { 0 } else { 128 });
        if value == 0 {
            break;
        }
    }
}
fn fixture(
    virtual_type: &str,
    encoded_type: &str,
    bytes: &[u8],
    compressed: Option<&str>,
    leb_codec: bool,
) -> Result<Fixture> {
    let refs = HashMap::new();
    let (dtype, _) = Parser::new(virtual_type, &refs).virtual_type()?;
    let encoding = Parser::new(encoded_type, &refs).encoded()?;
    let DataType::Struct(fields) = dtype.to_arrow()? else {
        panic!("root struct")
    };
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("part-0");
    let (wire, mut buffer) = if let Some(codec) = compressed {
        let mut wire = vec![];
        for block in bytes.chunks(65_536) {
            let payload = if codec == "ZstdBlockBufferSpec" {
                zstd::bulk::compress(block, 1)?
            } else {
                lz4_flex::block::compress(block)
            };
            let mut frame = (block.len() as i32).to_le_bytes().to_vec();
            frame.extend(payload);
            wire.extend_from_slice(&(frame.len() as i32).to_le_bytes());
            wire.extend(frame);
        }
        (
            wire,
            serde_json::json!({"name":"BlockingBufferSpec", "blockSize":65536,
            "child":{"name":codec,"blockSize":65536,"child":{"name":"StreamBlockBufferSpec"}}}),
        )
    } else {
        (
            bytes.to_vec(),
            serde_json::json!({"name":"StreamBufferSpec"}),
        )
    };
    if leb_codec {
        buffer = serde_json::json!({"name":"LEB128BufferSpec","child":buffer});
    }
    std::fs::write(&path, wire)?;
    Ok(Fixture {
        _dir: dir,
        component: Component {
            schema: Arc::new(Schema::new(fields)),
            dtype,
            encoding,
            buffer,
            paths: vec![path],
            int_key_bounds: vec![],
            index: None,
        },
    })
}
fn batches(
    f: &Fixture,
    size: usize,
    projection: &[usize],
    paths: Option<Vec<Vec<String>>>,
) -> Result<Vec<RecordBatch>> {
    let mut reader = f.component.reader(0, projection)?.with_decode_paths(paths);
    let mut result = vec![];
    while let Some(batch) = reader.next_batch(size)? {
        result.push(batch);
    }
    Ok(result)
}
fn combined(batches: &[RecordBatch]) -> RecordBatch {
    arrow_select::concat::concat_batches(&batches[0].schema(), batches.iter()).unwrap()
}
fn scalar_bytes() -> Vec<u8> {
    let mut b = vec![1, 0];
    leb(u32::MAX as u64, &mut b);
    leb(u64::MAX, &mut b);
    b.extend(0.5f32.to_le_bytes());
    b.extend((-0.0f64).to_le_bytes());
    b.push(1);
    leb(2, &mut b);
    b.extend("é".as_bytes());
    leb(12, &mut b);
    b.extend([1, 65]);
    leb(129, &mut b);
    b.extend(f32::from_bits(0x7fc00042).to_le_bytes());
    b.extend(f64::INFINITY.to_le_bytes());
    b.push(0);
    leb(70_000, &mut b);
    b.extend(std::iter::repeat_n(b'x', 70_000));
    b.push(0);
    b
}
const SCALAR_V: &str = "Struct{i:Int32,j:Int64,f:Float32,g:Float64,b:Boolean,s:String,gt:Call}";
const SCALAR_E: &str =
    "+EBaseStruct{i:EVarint,j:EVarint,f:EFloat32,g:EFloat64,b:EBoolean,s:EBinary2,gt:EVarint}";
#[test]
fn public_scalar_widths_strings_calls_compression_and_reordered_projection() -> Result<()> {
    for codec in [
        None,
        Some("ZstdBlockBufferSpec"),
        Some("LZ4BlockBufferSpec"),
    ] {
        let f = fixture(SCALAR_V, SCALAR_E, &scalar_bytes(), codec, false)?;
        for size in [1, 16] {
            let all = combined(&batches(&f, size, &[0, 1, 2, 3, 4, 5, 6], None)?);
            let i = all.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
            assert_eq!(i.iter().collect::<Vec<_>>(), vec![Some(-1), None]);
            assert_eq!(
                all.column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .as_ref(),
                &[-1, 129]
            );
            let fv = all
                .column(2)
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap();
            assert_eq!(fv.value(1).to_bits(), 0x7fc00042);
            let gv = all
                .column(3)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            assert_eq!(gv.value(0).to_bits(), (-0.0f64).to_bits());
            assert_eq!(
                all.column(4)
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                vec![Some(true), Some(false)]
            );
            let strings = all
                .column(5)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(strings.value(0), "é");
            assert_eq!(strings.value(1), "x".repeat(70_000));
            let calls = all
                .column(6)
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();
            assert!(!missing(calls, 0));
            assert!(missing(calls, 1));
            let projected = combined(&batches(&f, size, &[5, 0], None)?);
            assert_eq!(projected.column(0).to_data(), all.column(5).to_data());
            assert_eq!(projected.column(1).to_data(), all.column(0).to_data());
        }
    }
    Ok(())
}
#[test]
fn public_vortex_call_slice_memory_and_ipc_preserve_arrow_contract() -> Result<()> {
    let f = fixture(SCALAR_V, SCALAR_E, &scalar_bytes(), None, false)?;
    let session = vortex::session::VortexSession::default();
    let mut reader = f.component.reader(0, &[0, 1, 2, 3, 4, 5, 6])?;
    let array = reader.next_vortex_batch(16, &session)?.unwrap();
    let slice = array.slice(0..2)?;
    let batch = arrow_batch(slice, f.component.schema.clone(), &session)?;
    let bytes = get_record_batch_memory_size(&batch);
    assert!(bytes > 0);
    let mut counter = RecordBatchMemoryCounter::new();
    assert_eq!(counter.count_batch(&batch), bytes);
    assert_eq!(counter.count_batch(&batch.slice(1, 1)), 0);
    let mut wire = vec![];
    {
        let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut wire, &batch.schema())?;
        writer.write(&batch)?;
        writer.finish()?;
    }
    let restored = arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(wire), None)?
        .next()
        .unwrap()?;
    assert_eq!(restored.schema(), batch.schema());
    for (a, b) in restored.columns().iter().zip(batch.columns()) {
        assert_eq!(a.to_data(), b.to_data());
    }
    let gt = restored
        .column(6)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert_eq!(
        gt.column(0)
            .as_any()
            .downcast_ref::<arrow_array::UInt16Array>()
            .unwrap()
            .value(0),
        0
    );
    assert_eq!(
        gt.column(1)
            .as_any()
            .downcast_ref::<arrow_array::UInt16Array>()
            .unwrap()
            .value(0),
        1
    );
    assert!(missing(gt, 1));
    Ok(())
}
fn array_length(n: usize, mode: u8, b: &mut Vec<u8>) {
    if mode == 0 {
        b.extend((n as i32).to_le_bytes());
    } else {
        leb(n as u64, b);
    }
}
#[test]
fn public_mixed_entries_cover_chunk_caps_missingness_lengths_and_batches() -> Result<()> {
    for mode in 0..3 {
        let kind = if mode == 2 { "EArray2" } else { "EArray" };
        let e = format!(
            "+EBaseStruct{{entries:{kind}[EBaseStruct{{AD:{kind}[+EVarint],dp:EVarint,gt:EVarint,PL:{kind}[+EVarint]}}]}}"
        );
        let v = "Struct{entries:Array[Struct{dp:Int32,PL:Array[Int32],gt:Call,AD:Array[Int32]}]}";
        let counts: [usize; 6] = [0, 1, 15, 16, 17, 33];
        let mut wire = vec![];
        for n in counts {
            wire.extend([1, 0]);
            array_length(n, mode, &mut wire);
            let start = wire.len();
            wire.resize(start + n.div_ceil(8), 0);
            for i in 0..n {
                if i % 11 == 0 {
                    wire[start + i / 8] |= 1 << (i % 8);
                    continue;
                }
                let mask = u8::from(i % 7 == 0)
                    | (u8::from(i % 3 == 0) << 1)
                    | (u8::from(i % 5 == 0) << 2);
                wire.push(mask);
                let ad = [0, 1, 31, 33][i % 4];
                if mask & 1 == 0 {
                    array_length(ad, mode, &mut wire);
                    for k in 0..ad {
                        leb((k * 129) as u64, &mut wire);
                    }
                }
                if mask & 2 == 0 {
                    leb((200 + i) as u64, &mut wire);
                }
                if mask & 4 == 0 {
                    leb(12, &mut wire);
                }
                let pl = if i % 2 == 0 { 17 } else { 2 };
                array_length(pl, mode, &mut wire);
                for k in 0..pl {
                    leb((k * 191) as u64, &mut wire);
                }
            }
        }
        wire.extend([1, 1, 0]);
        let f = fixture(v, &e, &wire, None, mode == 1)?;
        for size in [1, 2, 16] {
            let all = combined(&batches(&f, size, &[0], None)?);
            let lists = all.column(0).as_any().downcast_ref::<ListArray>().unwrap();
            assert_eq!(lists.len(), 7);
            assert!(missing(lists, 6));
            for (row, n) in counts.into_iter().enumerate() {
                let values = lists.value(row);
                let entries = values.as_any().downcast_ref::<StructArray>().unwrap();
                assert_eq!(entries.len(), n);
                let dp = entries
                    .column_by_name("dp")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                let ad = entries
                    .column_by_name("AD")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .unwrap();
                for i in 0..n {
                    if i % 11 == 0 {
                        assert!(missing(entries, i));
                        continue;
                    }
                    assert!(!missing(entries, i));
                    assert_eq!(missing(dp, i), i % 3 == 0);
                    if i % 3 != 0 {
                        assert_eq!(dp.value(i), (200 + i) as i32);
                    }
                    assert_eq!(missing(ad, i), i % 7 == 0);
                    if i % 7 != 0 {
                        let a = ad.value(i);
                        let a = a.as_any().downcast_ref::<Int32Array>().unwrap();
                        assert_eq!(
                            a.values().as_ref(),
                            &(0..[0, 1, 31, 33][i % 4])
                                .map(|k| k as i32 * 129)
                                .collect::<Vec<_>>()
                        );
                    }
                }
            }
            let pruned = combined(&batches(
                &f,
                size,
                &[0],
                Some(vec![vec!["entries".into(), "dp".into()]]),
            )?);
            let projected = pruned
                .column(0)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            for row in 0..6 {
                let a = lists.value(row);
                let b = projected.value(row);
                let a = a.as_any().downcast_ref::<StructArray>().unwrap();
                let b = b.as_any().downcast_ref::<StructArray>().unwrap();
                assert_eq!(
                    a.column_by_name("dp").unwrap().to_data(),
                    b.column_by_name("dp").unwrap().to_data()
                );
            }
        }
    }
    Ok(())
}
#[test]
fn public_required_array_and_string_boundaries_and_errors() -> Result<()> {
    let v = "Struct{pad:String,values:Array[Int32],tail:Int32}";
    let e = "+EBaseStruct{pad:+EBinary2,values:+EArray2[+EVarint],tail:+EVarint}";
    for prefix in [0, 65_520, 65_535, 70_000] {
        let mut good = vec![1];
        leb(prefix, &mut good);
        good.extend(std::iter::repeat_n(b'x', prefix as usize));
        leb(4, &mut good);
        for value in [0, 127, 128, u32::MAX as u64] {
            leb(value, &mut good);
        }
        leb(42, &mut good);
        good.push(0);
        let f = fixture(v, e, &good, None, false)?;
        let b = combined(&batches(&f, 1, &[0, 1, 2], None)?);
        let l = b.column(1).as_any().downcast_ref::<ListArray>().unwrap();
        let a = l.value(0);
        assert_eq!(
            a.as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .as_ref(),
            &[0, 127, 128, -1]
        );
        assert_eq!(
            b.column(2)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            42
        );
        for cut in [1, 2, 3, 5] {
            let f = fixture(v, e, &good[..good.len() - cut], None, false)?;
            assert!(batches(&f, 16, &[0, 1, 2], None).is_err());
        }
    }
    for payload in [vec![128], vec![128; 10], vec![255, 255, 255, 255, 31]] {
        let mut b = vec![1, 1];
        b.extend(payload);
        b.push(0);
        let f = fixture(
            "Struct{v:Array[Int32]}",
            "+EBaseStruct{v:+EArray2[+EVarint]}",
            &b,
            None,
            false,
        )?;
        assert!(batches(&f, 16, &[0], None).is_err());
    }
    for string in [vec![255], vec![195], vec![195, 169]] {
        let mut b = vec![1];
        leb(string.len() as u64, &mut b);
        b.extend(&string);
        b.push(0);
        let f = fixture(
            "Struct{s:String}",
            "+EBaseStruct{s:+EBinary2}",
            &b,
            None,
            false,
        )?;
        assert_eq!(
            batches(&f, 1, &[0], None).is_ok(),
            std::str::from_utf8(&string).is_ok()
        );
    }
    Ok(())
}
#[test]
fn public_skipped_scalar_framing_is_independent_of_numeric_width() -> Result<()> {
    let mut b = vec![1];
    b.extend(std::iter::repeat_n(128, 37));
    b.extend([1, 42, 0]);
    let f = fixture(
        "Struct{unused:Int32,value:Int32}",
        "+EBaseStruct{unused:+EVarint,value:+EVarint}",
        &b,
        None,
        false,
    )?;
    assert!(batches(&f, 16, &[0, 1], None).is_err());
    let b = combined(&batches(&f, 16, &[1], None)?);
    assert_eq!(
        b.column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        42
    );
    Ok(())
}

#[test]
fn public_vortex_nested_entry_validity_distinguishes_absent_from_all_null() -> Result<()> {
    let f = fixture(
        "Struct{entries:Array[Struct{x:Int32}]}",
        "+EBaseStruct{entries:+EArray2[EBaseStruct{x:EVarint}]}",
        &[1, 3, 2, 1, 0, 8, 0],
        None,
        false,
    )?;
    let session = vortex::session::VortexSession::default();
    let mut reader = f.component.reader(0, &[0])?;
    let array = reader.next_vortex_batch(1, &session)?.unwrap();
    let batch = arrow_batch(array, f.component.schema.clone(), &session)?;
    let lists = batch
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let entries = lists.value(0);
    let entries = entries.as_any().downcast_ref::<StructArray>().unwrap();
    assert!(!missing(entries, 0));
    assert!(missing(entries, 1));
    assert!(!missing(entries, 2));
    let x = entries
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert!(missing(x, 0));
    assert_eq!(x.value(2), 8);
    Ok(())
}

#[test]
fn public_nested_struct_lists_reuse_builders_across_rows_and_batch_splits() -> Result<()> {
    let f = fixture(
        "Struct{wrapper:Struct{items:Array[Struct{numbers:Array[Int32],flag:Int32}]}}",
        "+EBaseStruct{wrapper:EBaseStruct{items:+EArray2[EBaseStruct{numbers:EArray2[+EVarint],flag:EVarint}]}}",
        &[1, 0, 2, 0, 0, 2, 1, 128, 1, 9, 1, 10, 1, 1, 1, 0, 0, 0],
        None,
        false,
    )?;
    let one = combined(&batches(&f, 1, &[0], None)?);
    let many = combined(&batches(&f, 16, &[0], None)?);
    assert_eq!(one.column(0).to_data(), many.column(0).to_data());
    let wrapper = many
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert_eq!(wrapper.len(), 3);
    assert!(missing(wrapper, 1));
    let items = wrapper
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(items.value_length(0), 2);
    assert_eq!(items.value_length(2), 0);
    assert!(!missing(items, 2));
    let entries = items.value(0);
    let entries = entries.as_any().downcast_ref::<StructArray>().unwrap();
    assert!((0..entries.len()).all(|row| !missing(entries, row)));
    let numbers = entries
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(numbers.value_length(0), 2);
    assert!(missing(numbers, 1));
    let flags = entries
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(flags.values().as_ref(), &[9, 10]);
    Ok(())
}

#[test]
fn public_mixed_errors_after_staged_prefix_and_second_outer_row() -> Result<()> {
    let v = "Struct{entries:Array[Struct{xs:Array[Int32],v:Int32}]}";
    let e = "+EBaseStruct{entries:+EArray2[+EBaseStruct{xs:+EArray2[+EVarint],v:+EVarint}]}";
    for failure in [0, 1, 15, 16, 17] {
        let mut wire = vec![1, 1, 1, 7, 9, 1, 20];
        for index in 0..=failure {
            wire.extend([2, 1, 128, 1]);
            leb(
                if index == failure {
                    u32::MAX as u64 + 1
                } else {
                    42
                },
                &mut wire,
            );
        }
        let f = fixture(v, e, &wire, None, false)?;
        let mut reader = f.component.reader(0, &[0])?;
        assert_eq!(reader.next_batch(1)?.unwrap().num_rows(), 1);
        let narrow = reader.next_batch(1).unwrap_err().to_string();
        let wide = batches(&f, 16, &[0], None).unwrap_err().to_string();
        assert!(narrow.contains("overflow"));
        assert_eq!(narrow, wide);
    }
    Ok(())
}

#[test]
fn public_calls_cover_phase_ploidy_allele_width_nulls_and_invalid_words() -> Result<()> {
    let words = [
        Some(0),
        Some(4),
        Some(12),
        Some(20),
        Some(13),
        Some(10),
        Some((65535u64 << 3) | 2),
        None,
    ];
    let expected = [
        (0, 0, 0, false),
        (0, 0, 2, false),
        (0, 1, 2, false),
        (1, 1, 2, false),
        (0, 1, 2, true),
        (1, 0, 1, false),
        (65535, 0, 1, false),
    ];
    let mut wire = vec![];
    for value in words {
        wire.extend([1, u8::from(value.is_none())]);
        if let Some(value) = value {
            leb(value, &mut wire);
        }
    }
    wire.push(0);
    let f = fixture(
        "Struct{gt:Call}",
        "+EBaseStruct{gt:EVarint}",
        &wire,
        None,
        false,
    )?;
    let session = vortex::session::VortexSession::default();
    let mut reader = f.component.reader(0, &[0])?;
    let mut rows = 0;
    while let Some(array) = reader.next_vortex_batch(3, &session)? {
        let batch = arrow_batch(array, f.component.schema.clone(), &session)?;
        let gt = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        for row in 0..gt.len() {
            if rows == 7 {
                assert!(missing(gt, row));
            } else {
                let (a, b, p, phase) = expected[rows];
                assert!(!missing(gt, row));
                assert_eq!(
                    gt.column(0)
                        .as_any()
                        .downcast_ref::<arrow_array::UInt16Array>()
                        .unwrap()
                        .value(row),
                    a
                );
                assert_eq!(
                    gt.column(1)
                        .as_any()
                        .downcast_ref::<arrow_array::UInt16Array>()
                        .unwrap()
                        .value(row),
                    b
                );
                assert_eq!(
                    gt.column(2)
                        .as_any()
                        .downcast_ref::<arrow_array::UInt8Array>()
                        .unwrap()
                        .value(row),
                    p
                );
                assert_eq!(
                    gt.column(3)
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .unwrap()
                        .value(row),
                    phase
                );
            }
            rows += 1;
        }
    }
    assert_eq!(rows, 8);
    for invalid in [6, (65536u64 << 3) | 2] {
        let mut wire = vec![1, 0];
        leb(invalid, &mut wire);
        wire.push(0);
        let f = fixture(
            "Struct{gt:Call}",
            "+EBaseStruct{gt:EVarint}",
            &wire,
            None,
            false,
        )?;
        assert!(batches(&f, 16, &[0], None).is_err());
    }
    Ok(())
}

#[test]
fn public_compressed_reader_rejects_truncated_and_invalid_block_frames() -> Result<()> {
    for codec in ["ZstdBlockBufferSpec", "LZ4BlockBufferSpec"] {
        for invalid in [0, 1, 2] {
            let f = fixture(SCALAR_V, SCALAR_E, &scalar_bytes(), Some(codec), false)?;
            let path = &f.component.paths[0];
            let mut wire = std::fs::read(path)?;
            match invalid {
                0 => {
                    wire.pop();
                }
                1 => wire[..4].copy_from_slice(&(-1_i32).to_le_bytes()),
                _ => wire[..4].copy_from_slice(&i32::MAX.to_le_bytes()),
            }
            std::fs::write(path, wire)?;
            assert!(batches(&f, 16, &[0, 1, 2, 3, 4, 5, 6], None).is_err());
        }
    }
    Ok(())
}

#[test]
fn public_wide_missingness_bitmap_and_invalid_boolean() -> Result<()> {
    let fields = (0..65)
        .map(|i| format!("x{i}:Int32"))
        .collect::<Vec<_>>()
        .join(",");
    let encoding = (0..65)
        .map(|i| format!("x{i}:EVarint"))
        .collect::<Vec<_>>()
        .join(",");
    let mut wire = vec![1];
    let mut bitmap = [255_u8; 9];
    bitmap[7] &= !128;
    bitmap[8] &= !1;
    wire.extend(bitmap);
    wire.extend([63, 64, 0]);
    let f = fixture(
        &format!("Struct{{{fields}}}"),
        &format!("+EBaseStruct{{{encoding}}}"),
        &wire,
        None,
        false,
    )?;
    let batch = combined(&batches(&f, 1, &[64, 63, 0], None)?);
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        64
    );
    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        63
    );
    assert!(missing(batch.column(2).as_ref(), 0));
    let f = fixture(
        "Struct{b:Boolean}",
        "+EBaseStruct{b:+EBoolean}",
        &[1, 2, 0],
        None,
        false,
    )?;
    assert!(batches(&f, 1, &[0], None).is_err());
    Ok(())
}
