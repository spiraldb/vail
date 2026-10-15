use std::time::Instant;
use vail_execution::reference::{ParInterval, ReferenceDefinition};
fn main() {
    for n in [256, 1024, 4096] {
        let contigs = (0..n).map(|i| format!("c{i}")).collect::<Vec<_>>();
        let reference = ReferenceDefinition {
            name: "cost".into(),
            lengths: contigs.iter().map(|c| (c.clone(), 7)).collect(),
            x_contigs: contigs[..n / 2].to_vec(),
            y_contigs: contigs[n / 2..].to_vec(),
            mt_contigs: vec![],
            par: contigs
                .iter()
                .map(|c| ParInterval {
                    contig: c.clone(),
                    start: 1,
                    end: 6,
                })
                .collect(),
            contigs,
        };
        let start = Instant::now();
        for _ in 0..100 {
            std::hint::black_box(&reference).validate().unwrap();
        }
        println!(
            "{{\"contigs\":{n},\"iterations\":100,\"seconds\":{}}}",
            start.elapsed().as_secs_f64()
        );
    }
}
