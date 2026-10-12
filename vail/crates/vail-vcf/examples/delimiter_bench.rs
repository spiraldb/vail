//! Isolate scan overhead, offset traffic, and SIMD block processing.
use std::{hint::black_box, time::Instant};
#[inline(never)]
fn nested(bytes: &[u8], out: &mut Vec<(usize, usize)>) {
    out.clear();
    let mut sample_start = 0;
    for sample_end in memchr::memchr_iter(b'\t', bytes).chain(std::iter::once(bytes.len())) {
        let mut start = sample_start;
        for end in memchr::memchr_iter(b':', &bytes[sample_start..sample_end])
            .map(|i| i + sample_start)
            .chain(std::iter::once(sample_end))
        {
            out.push((start, end));
            start = end + 1;
        }
        sample_start = sample_end + 1;
    }
}
#[inline(never)]
fn flat(bytes: &[u8], out: &mut Vec<(usize, usize)>) {
    out.clear();
    let mut start = 0;
    for end in memchr::memchr2_iter(b':', b'\t', bytes).chain(std::iter::once(bytes.len())) {
        out.push((start, end));
        start = end + 1;
    }
}
#[inline(never)]
fn scalar(bytes: &[u8], out: &mut Vec<(usize, usize)>) {
    out.clear();
    let mut start = 0;
    for (i, &b) in bytes.iter().enumerate() {
        if b == b':' || b == b'\t' {
            out.push((start, i));
            start = i + 1;
        }
    }
    out.push((start, bytes.len()));
}
#[cfg(all(target_arch = "aarch64", target_endian = "little"))]
#[inline(never)]
fn neon(bytes: &[u8], out: &mut Vec<(usize, usize)>) {
    use std::arch::aarch64::*;
    out.clear();
    let mut start = 0;
    let mut offset = 0;
    // aarch64 guarantees NEON. Only full 16-byte blocks are loaded.
    unsafe {
        let colon = vdupq_n_u8(b':');
        let tab = vdupq_n_u8(b'\t');
        while offset + 16 <= bytes.len() {
            let input = vld1q_u8(bytes.as_ptr().add(offset));
            let matches = vorrq_u8(vceqq_u8(input, colon), vceqq_u8(input, tab));
            // Four bits per input byte; enumerate every match from one vector.
            let nibbles = vshrn_n_u16::<4>(vreinterpretq_u16_u8(matches));
            let mut mask = vget_lane_u64::<0>(vreinterpret_u64_u8(nibbles));
            while mask != 0 {
                let lane = mask.trailing_zeros() as usize / 4;
                let end = offset + lane;
                out.push((start, end));
                start = end + 1;
                mask &= !(0xf_u64 << (lane * 4));
            }
            offset += 16;
        }
    }
    for (i, &b) in bytes[offset..].iter().enumerate() {
        if b == b':' || b == b'\t' {
            let end = offset + i;
            out.push((start, end));
            start = end + 1;
        }
    }
    out.push((start, bytes.len()));
}
#[inline(never)]
fn count(bytes: &[u8]) -> usize {
    bytes.iter().filter(|&&b| b == b':' || b == b'\t').count()
}
fn main() {
    for samples in [10, 100, 1000, 10000] {
        let bytes = vec!["0/1:12,8:20:99:200,0,300"; samples]
            .join("\t")
            .into_bytes();
        let repeats = (100_000_000 / bytes.len()).max(10);
        let mut expected = Vec::new();
        nested(&bytes, &mut expected);
        let mut out = Vec::with_capacity(samples * 5);
        let mut kernels: Vec<(&str, fn(&[u8], &mut Vec<(usize, usize)>))> =
            vec![("nested", nested), ("flat", flat), ("scalar", scalar)];
        #[cfg(all(target_arch = "aarch64", target_endian = "little"))]
        kernels.push(("neon_blocks", neon));
        print!(
            "samples={samples} input_bytes={} offset_bytes={}",
            bytes.len(),
            expected.len() * 16
        );
        for (name, kernel) in kernels {
            kernel(&bytes, &mut out);
            assert_eq!(out, expected);
            let begin = Instant::now();
            for _ in 0..repeats {
                kernel(black_box(&bytes), &mut out);
                black_box(&out);
            }
            let seconds = begin.elapsed().as_secs_f64();
            print!(
                " {name}_MiB_s={:.1}",
                (bytes.len() * repeats) as f64 / 1048576.0 / seconds
            );
        }
        assert_eq!(count(&bytes), expected.len() - 1);
        let begin = Instant::now();
        for _ in 0..repeats {
            black_box(count(black_box(&bytes)));
        }
        let seconds = begin.elapsed().as_secs_f64();
        println!(
            " count_only_MiB_s={:.1}",
            (bytes.len() * repeats) as f64 / 1048576.0 / seconds
        );
    }
}
