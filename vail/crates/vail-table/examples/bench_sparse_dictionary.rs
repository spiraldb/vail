//! Public-API probe for retained dictionary selection across encodings.
use std::time::Instant;
use vortex::array::aggregate_fn::fns::sum_v2::SumV2;
use vortex::array::aggregate_fn::{DynGroupedAccumulator, GroupedAccumulator, NumericalAggregateOpts};
use vortex::array::arrays::{DictArray, ListViewArray, PrimitiveArray};
use vortex::array::validity::Validity;
use vortex::array::{IntoArray, VortexSessionExecute};
use vortex::buffer::buffer;
use vortex::{VortexSessionDefault, session::VortexSession};
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let session = VortexSession::default();
    for owner in [10_000usize, 1_000_000, 10_000_000] {
        let mut ctx = session.create_execution_ctx();
        let values = PrimitiveArray::from_option_iter(
            (0..owner).map(|i| (i % 13 != 0).then_some((i % 101) as i32)),
        );
        let values =
            vortex::encodings::pco::Pco::from_primitive(values.as_view(), 0, 1024, &mut ctx)?
                .into_array();
        let codes = PrimitiveArray::from_option_iter((0..128).map(|i| {
            (i % 11 != 0).then_some(if i < 64 {
                i as u32
            } else {
                (owner - 128 + i) as u32
            })
        }))
        .into_array();
        let dict = DictArray::try_new(codes, values)?.into_array();
        let dtype = dict.dtype().clone();
        let groups = ListViewArray::try_new(
            dict,
            buffer![0u32, 64].into_array(),
            buffer![64u32, 64].into_array(),
            Validity::NonNullable,
        )?
        .into_array();
        let expected = (0..128)
            .filter(|i| i % 11 != 0)
            .map(|i| if i < 64 { i } else { owner - 128 + i })
            .filter(|i| i % 13 != 0)
            .map(|i| (i % 101) as i64)
            .sum::<i64>();
        let mut times = Vec::new();
        for run in 0..25 {
            let begin = Instant::now();
            let mut accumulator = GroupedAccumulator::try_new(
                SumV2,
                NumericalAggregateOpts::default(),
                dtype.clone(),
            )?;
            accumulator.accumulate_list(&groups, &mut ctx)?;
            let result = accumulator.finish()?.execute::<PrimitiveArray>(&mut ctx)?;
            let wall = begin.elapsed().as_secs_f64();
            assert_eq!(result.as_slice::<i64>().iter().sum::<i64>(), expected);
            if run >= 5 {
                times.push(wall);
            }
        }
        times.sort_by(f64::total_cmp);
        println!(
            "{}",
            serde_json::json!({"encoding":"pco","owner":owner,"visible":128,"median_query_wall":times[10]})
        );
    }
    Ok(())
}
