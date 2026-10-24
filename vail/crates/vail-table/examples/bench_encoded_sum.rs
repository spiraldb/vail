//! Public-API CPU experiment for dictionary cardinality and logical-mask density.
use std::time::Instant;
use vortex::array::aggregate_fn::fns::sum_v2::SumV2;
use vortex::array::aggregate_fn::{
    DynGroupedAccumulator, GroupedAccumulator, NumericalAggregateOpts,
};
use vortex::array::arrays::{DictArray, ListViewArray, PrimitiveArray};
use vortex::array::validity::Validity;
use vortex::array::{IntoArray, VortexSessionExecute};
use vortex::dtype::{DType, Nullability::Nullable, PType};
use vortex::{VortexSessionDefault, session::VortexSession};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    let cardinality = args
        .get(1)
        .map(|x| x.parse())
        .transpose()?
        .unwrap_or(102usize);
    let density = args
        .get(2)
        .map(|x| x.parse())
        .transpose()?
        .unwrap_or(900u64);
    let rows = 16_384usize;
    let cols = 2500usize;
    let dictionary = (0..cardinality)
        .map(|i| (i % 13 != 0).then_some(i as i32 - 37))
        .collect::<Vec<_>>();
    let mut expected = 0_i64;
    let codes = PrimitiveArray::from_option_iter((0..rows * cols).map(|i| {
        let mut hash = (i as u64).wrapping_add(0x9e3779b97f4a7c15);
        hash = (hash ^ (hash >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        hash = (hash ^ (hash >> 27)).wrapping_mul(0x94d049bb133111eb);
        hash ^= hash >> 31;
        if hash % 1000 >= density {
            return None;
        }
        let code = (i * 13 + 7) % cardinality;
        if let Some(value) = dictionary[code] {
            expected += i64::from(value);
        }
        Some(code as u16)
    }))
    .into_array();
    let elements = DictArray::try_new(
        codes,
        PrimitiveArray::from_option_iter(dictionary).into_array(),
    )?
    .into_array();
    let groups = ListViewArray::try_new(
        elements,
        PrimitiveArray::from_iter((0..rows).map(|r| (r * cols) as u32)).into_array(),
        PrimitiveArray::from_iter(std::iter::repeat_n(cols as u32, rows)).into_array(),
        Validity::NonNullable,
    )?
    .into_array();
    let session = VortexSession::default();
    for run in 0..7 {
        let mut ctx = session.create_execution_ctx();
        let mut acc = GroupedAccumulator::try_new(
            SumV2,
            NumericalAggregateOpts::default(),
            DType::Primitive(PType::I32, Nullable),
        )?;
        let start = Instant::now();
        acc.accumulate_list(&groups, &mut ctx)?;
        let result = acc.finish()?.execute::<PrimitiveArray>(&mut ctx)?;
        let seconds = start.elapsed().as_secs_f64();
        let mask = result.validity()?.execute_mask(result.len(), &mut ctx)?;
        let got = result
            .as_slice::<i64>()
            .iter()
            .zip(mask.iter())
            .filter_map(|(&v, valid)| valid.then_some(v))
            .sum::<i64>();
        assert_eq!(got, expected);
        if run >= 2 {
            println!(
                "{}",
                serde_json::json!({"cardinality":cardinality,"density_permille":density,"run":run-2,"entries":rows*cols,"wall":seconds,"sum":got})
            );
        }
    }
    Ok(())
}
