//! Compare the proposed giant FORMAT regexp with shared delimiter discovery.
use std::{hint::black_box, time::Instant};
fn main() {
    for samples in [10, 100, 1000] {
        let repeats = 20000 / (samples / 10).max(1);
        let tail = vec!["0/1:12,8:20:99:200,0,300"; samples].join("\t");
        let sample_pattern =
            r"([^:\t]+)(?::([^:\t]+))?(?::([^:\t]+))?(?::([^:\t]+))?(?::([^:\t]+))?";
        let pattern = format!("^{}$", vec![sample_pattern; samples].join("\t"));
        let started = Instant::now();
        let compiled = regex::Regex::new(&pattern);
        let compile_ms = started.elapsed().as_secs_f64() * 1000.0;
        let Ok(regex) = compiled else {
            println!("samples={samples} regex_compile_error={:?}", compiled.err());
            continue;
        };
        let mut captures = regex.capture_locations();
        let mut expected = Vec::new();
        let mut sample_start = 0;
        for sample_end in
            memchr::memchr_iter(b'\t', tail.as_bytes()).chain(std::iter::once(tail.len()))
        {
            let mut start = sample_start;
            for end in memchr::memchr_iter(b':', &tail.as_bytes()[sample_start..sample_end])
                .map(|i| i + sample_start)
                .chain(std::iter::once(sample_end))
            {
                expected.push((start, end));
                start = end + 1;
            }
            sample_start = sample_end + 1;
        }
        regex.captures_read(&mut captures, &tail).unwrap();
        assert_eq!(
            expected,
            (1..captures.len())
                .map(|i| captures.get(i).unwrap())
                .collect::<Vec<_>>()
        );
        let started = Instant::now();
        for _ in 0..repeats {
            regex
                .captures_read(&mut captures, black_box(&tail))
                .unwrap();
            black_box(&captures);
        }
        let regex_s = started.elapsed().as_secs_f64();
        let mut spans = Vec::with_capacity(samples * 5);
        let started = Instant::now();
        for _ in 0..repeats {
            spans.clear();
            let mut sample_start = 0;
            for sample_end in memchr::memchr_iter(b'\t', black_box(tail.as_bytes()))
                .chain(std::iter::once(tail.len()))
            {
                let mut start = sample_start;
                for end in memchr::memchr_iter(b':', &tail.as_bytes()[sample_start..sample_end])
                    .map(|i| i + sample_start)
                    .chain(std::iter::once(sample_end))
                {
                    spans.push((start, end));
                    start = end + 1;
                }
                sample_start = sample_end + 1;
            }
            black_box(&spans);
        }
        let scan_s = started.elapsed().as_secs_f64();
        let mib = (tail.len() * repeats) as f64 / (1024.0 * 1024.0);
        println!(
            "samples={samples} bytes={} repeats={repeats} regex_compile_ms={compile_ms:.2} regex_MiB_s={:.1} delimiter_MiB_s={:.1} ratio={:.1}",
            tail.len(),
            mib / regex_s,
            mib / scan_s,
            regex_s / scan_s
        );
    }
}
