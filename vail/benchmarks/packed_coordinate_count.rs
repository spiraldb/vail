//! Standalone production-kernel experiment. Compile with:
//! kache rustc -O --edition=2024 benchmarks/packed_coordinate_count.rs -o /tmp/packed-coordinate-count
//! Run serially without competing builds; outputs CSV, verifies an independent scalar oracle.
#[path = "../crates/vail-execution/src/packed_count.rs"]
mod packed_count;
use packed_count::{BitSlice, accumulate};
use std::{hint::black_box, time::Instant};
fn main() {
    println!("rows,width,masks,density_percent,repeats,ns_per_input_bit");
    for width in [1, 7, 31, 32, 63, 64, 65, 1000, 2535] {
        for rows in [1, 25, 263, 1024] {
            for density in [0, 1, 50, 99, 100] {
                for masks in [1, 4] {
                    let n = rows * width;
                    let buffers: Vec<Vec<u8>> = (0..masks).map(|m| {
                        let offset = 3 + m * 2;
                        let mut data = vec![0u8; (n + offset + 7) / 8];
                        for i in 0..n {
                            let hash = (i as u64).wrapping_mul(6364136223846793005).wrapping_add(m as u64 * 1442695040888963407);
                            if hash % 100 < density { data[(i + offset) / 8] |= 1 << ((i + offset) % 8); }
                        }
                        data
                    }).collect();
                    let slices: Vec<_> = buffers.iter().enumerate().map(|(m,b)| BitSlice { data:b, offset:3+m*2 }).collect();
                    let mut expected = vec![17i64; width];
                    for i in 0..n {
                        if slices.iter().all(|b| b.data[(i+b.offset)/8] & (1 << ((i+b.offset)%8)) != 0) { expected[i%width] += 1; }
                    }
                    let mut actual = vec![17i64; width];
                    accumulate(&slices, rows, &mut actual);
                    assert_eq!(actual, expected, "rows{rows} width{width} masks{masks} density{density}");
                    let repeats = (2_000_000 / n).clamp(2, 10000);
                    let mut counters = vec![0i64; width];
                    let start = Instant::now();
                    for _ in 0..repeats { accumulate(black_box(&slices), rows, black_box(&mut counters)); }
                    let ns = start.elapsed().as_nanos() as f64 / (n * repeats) as f64;
                    black_box(counters);
                    println!("{rows},{width},{masks},{density},{repeats},{ns:.6}");
                }
            }
        }
    }
}
