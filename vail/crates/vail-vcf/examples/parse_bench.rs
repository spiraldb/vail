//! Streaming decode throughput, including Arrow construction, without DataFusion.
use std::{io::BufRead, time::Instant};
fn main() {
    for path in std::env::args().skip(1) {
        let options = vail_vcf::Options {
            array_elements_required: false,
            ..Default::default()
        };
        let header = vail_vcf::read_header(&path, &options).unwrap();
        let samples = header.samples.len();
        let batch_rows = 8192usize.min((65536 / samples.max(1)).max(1));
        let mut parser = vail_vcf::Parser::new(header, options).unwrap();
        let mut batch = parser.batch().unwrap();
        let mut reader = vail_vcf::open(&path).unwrap();
        let mut line = String::new();
        let mut bytes = 0;
        let mut rows = 0;
        let started = Instant::now();
        loop {
            line.clear();
            if reader.read_line(&mut line).unwrap() == 0 {
                break;
            }
            bytes += line.len();
            if parser
                .append(&mut batch, line.trim_end_matches(['\r', '\n']))
                .unwrap()
            {
                rows += 1;
            }
            if batch.rows == batch_rows {
                std::hint::black_box(batch.finish().unwrap());
            }
        }
        if batch.rows > 0 {
            std::hint::black_box(batch.finish().unwrap());
        }
        let seconds = started.elapsed().as_secs_f64();
        println!(
            "{path}: rows={rows} samples={samples} formats={} decoded_bytes={bytes} seconds={seconds:.6} MiB_s={:.1} cells_s={:.0}",
            parser.cached_formats(),
            bytes as f64 / 1048576.0 / seconds,
            (rows * samples) as f64 / seconds
        );
    }
}
