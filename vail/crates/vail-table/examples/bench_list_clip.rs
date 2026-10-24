//! Public-API resource experiment: fixed visible rows, growing retained owner.
use std::time::Instant;
use vortex::array::arrays::{DictArray, ListViewArray, PrimitiveArray, StructArray};
use vortex::array::validity::Validity;
use vortex::array::{IntoArray, VortexSessionExecute};
use vortex::dtype::FieldNames;
use vortex::expr::{fill_null, is_not_null, list_sum, lit, mask, root};
use vortex::{VortexSessionDefault, session::VortexSession};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let session = VortexSession::default();
    let profile = std::env::var_os("VAIL_CLIP_PROFILE").is_some();
    for (owner, width, distant, large_dictionary, compressed_dictionary) in [
        (10_000usize, 128usize, false, false, false),
        (1_000_000, 128, false, false, false),
        (40_000_000, 128, false, false, false),
        (40_000_000, 8192, false, false, false),
        (10_000, 64, true, false, false),
        (1_000_000, 64, true, false, false),
        (40_000_000, 64, true, false, false),
        (10_000, 64, true, true, false),
        (1_000_000, 64, true, true, false),
        (40_000_000, 64, true, true, false),
        (10_000, 64, true, true, true),
        (1_000_000, 64, true, true, true),
        (40_000_000, 64, true, true, true),
    ] {
        if profile && !(owner == 40_000_000 && distant && !large_dictionary) { continue; }
        let begin = Instant::now();
        let cardinality = if large_dictionary { owner } else { 102 };
        let first = if distant { 0 } else { owner - 2 * width };
        let second = owner - width;
        let shift = if compressed_dictionary { 0 } else { 37 };
        let dictionary = PrimitiveArray::from_option_iter(
            (0..cardinality).map(|i| (i % 13 != 0).then_some((i % 102) as i32 - shift)),
        );
        let dictionary = if compressed_dictionary {
            vortex::encodings::fastlanes::bitpack_compress::bitpack_encode(
                &dictionary,
                7,
                None,
                &mut session.create_execution_ctx(),
            )?
            .into_array()
        } else {
            dictionary.into_array()
        };
        let values = DictArray::try_new(
            PrimitiveArray::from_iter((0..owner).map(|i| (i % cardinality) as u32)).into_array(),
            dictionary,
        )?
        .into_array();
        let entry = StructArray::try_new(
            FieldNames::from(["DP"]),
            vec![values],
            owner,
            Validity::from_iter((0..owner).map(|i| i % 11 != 0)),
        )?
        .into_array();
        let elements = StructArray::try_new(
            FieldNames::from(["value"]),
            vec![entry],
            owner,
            Validity::from_iter((0..owner).map(|i| i % 17 != 0)),
        )?
        .into_array();
        let list = ListViewArray::try_new(
            elements,
            PrimitiveArray::from_iter([first as u32, owner as u32 - width as u32, 0, 0])
                .into_array(),
            PrimitiveArray::from_iter([width as u32, width as u32, 0, owner as u32 - 1])
                .into_array(),
            Validity::from_iter([true, true, true, false]),
        )?
        .into_array();
        let leaf = vortex::array::scalar_fn::fns::list_field::list_field(
            root(),
            list.dtype(),
            &["value".into(), "DP".into()],
        )?;
        let expr =
            mask(fill_null(list_sum(leaf), lit(0_i64)), is_not_null(root())).bind(list.dtype())?;
        let expected = [first, second].map(|start| {
            Some(
                (start..start + width)
                    .filter(|i| i % 11 != 0 && i % 17 != 0 && (i % cardinality) % 13 != 0)
                    .map(|i| (i % 102) as i64 - i64::from(shift))
                    .sum::<i64>(),
            )
        });
        let expected = [expected[0], expected[1], Some(0), None];
        let prepare_wall = begin.elapsed().as_secs_f64();
        let mut ctx = session.create_execution_ctx();
        let mut times = Vec::new();
        for run in 0..if profile { 2505 } else { 45 } {
            let begin = Instant::now();
            let result = list
                .clone()
                .apply_bound(&expr)?
                .execute::<PrimitiveArray>(&mut ctx)?;
            let wall = begin.elapsed().as_secs_f64();
            let valid = result.validity()?.execute_mask(result.len(), &mut ctx)?;
            for i in 0..4 {
                assert_eq!(
                    valid.value(i).then_some(result.as_slice::<i64>()[i]),
                    expected[i]
                );
            }
            if run >= 5 {
                times.push(wall);
            }
        }
        times.sort_by(f64::total_cmp);
        println!(
            "{}",
            serde_json::json!({"owner":owner,"distant":distant,"large_dictionary":large_dictionary,"compressed_dictionary":compressed_dictionary,"visible_per_row":width,"rows":4,"repeat":40,
            "prepare_wall":prepare_wall,"median_query_wall":times[20],"min_query_wall":times[0],"max_query_wall":times[39]})
        );
    }
    Ok(())
}
