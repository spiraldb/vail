//! Positional population counts over row-major packed bitmaps. References:
//! Klarqvist/Muła/Lemire, arXiv:1911.02696; Clausecker/Lemire/Schintke,
//! arXiv:2412.16370. Arbitrary matrix widths require offset-aware row gathers;
//! masks are intersected in registers, never expanded into a numeric array.

#[derive(Clone, Copy)]
pub(crate) struct BitSlice<'a> {
    pub data: &'a [u8],
    pub offset: usize,
}
impl BitSlice<'_> {
    #[inline(always)]
    fn word(self, index: usize, count: usize) -> u64 {
        let bit = index + self.offset;
        let start = bit / 8;
        let shift = bit % 8;
        let mut bytes = [0_u8; 8];
        let available = 8.min(self.data.len() - start);
        bytes[..available].copy_from_slice(&self.data[start..start + available]);
        let mut word = u64::from_le_bytes(bytes) >> shift;
        if shift != 0 && start + 8 < self.data.len() {
            word |= u64::from(self.data[start + 8]) << (64 - shift);
        }
        if count != 64 {
            word &= (1_u64 << count) - 1;
        }
        word
    }
}

pub(crate) trait Counter: Copy {
    fn increment(&mut self, value: u64);
}
impl Counter for i64 {
    #[inline(always)]
    fn increment(&mut self, value: u64) {
        *self = self.wrapping_add(value as i64);
    }
}
impl Counter for u64 {
    #[inline(always)]
    fn increment(&mut self, value: u64) {
        *self = self.wrapping_add(value);
    }
}

struct Reader<'a, const MASKS: usize> {
    inputs: [BitSlice<'a>; 4],
}
impl<const MASKS: usize> Reader<'_, MASKS> {
    #[inline(always)]
    fn word(&self, index: usize, count: usize) -> u64 {
        let mut bits = self.inputs[0].word(index, count);
        for input in &self.inputs[1..MASKS] {
            bits &= input.word(index, count);
        }
        bits
    }
}

#[inline(always)]
fn add_word<C: Counter>(output: &mut [C], mut bits: u64, weight: u64) {
    if bits == u64::MAX >> (64 - output.len()) {
        for counter in output {
            counter.increment(weight);
        }
    } else {
        while bits != 0 {
            let position = bits.trailing_zeros() as usize;
            output[position].increment(weight);
            bits &= bits - 1;
        }
    }
}

fn direct<C: Counter, const MASKS: usize>(
    input: &Reader<'_, MASKS>,
    rows: usize,
    output: &mut [C],
) {
    let width = output.len();
    for row in 0..rows {
        for (block, target) in output.chunks_mut(64).enumerate() {
            let mut bits = input.word(row * width + block * 64, target.len());
            if bits.count_ones() < 8 || bits == u64::MAX >> (64 - target.len()) {
                add_word(target, bits, 1);
            } else {
                for counter in target {
                    counter.increment(bits & 1);
                    bits >>= 1;
                }
            }
        }
    }
}

#[inline(always)]
fn full_adder(a: u64, b: u64, c: u64) -> (u64, u64) {
    let sum = a ^ b;
    ((a & b) | (sum & c), sum ^ c)
}

// Portable Harley-Seal bit-sliced counting: fifteen full adders compress16
// incoming rows into four retained planes and one weight16 carry plane.
fn carry_save<C: Counter, const MASKS: usize>(
    input: &Reader<'_, MASKS>,
    rows: usize,
    output: &mut [C],
) {
    let width = output.len();
    for (block, target) in output.chunks_mut(64).enumerate() {
        let column = block * 64;
        let n = target.len();
        let (mut one, mut two, mut four, mut eight) = (0, 0, 0, 0);
        let mut row = 0;
        while row + 16 <= rows {
            let mut words = [0_u64; 16];
            for (i, word) in words.iter_mut().enumerate() {
                *word = input.word((row + i) * width + column, n);
            }
            let (a, x) = full_adder(one, words[0], words[1]);
            let (b, y) = full_adder(x, words[2], words[3]);
            one = y;
            let (c, x) = full_adder(two, a, b);
            two = x;
            let (a, x) = full_adder(one, words[4], words[5]);
            let (b, y) = full_adder(x, words[6], words[7]);
            one = y;
            let (d, x) = full_adder(two, a, b);
            two = x;
            let (e, x) = full_adder(four, c, d);
            four = x;
            let (a, x) = full_adder(one, words[8], words[9]);
            let (b, y) = full_adder(x, words[10], words[11]);
            one = y;
            let (c, x) = full_adder(two, a, b);
            two = x;
            let (a, x) = full_adder(one, words[12], words[13]);
            let (b, y) = full_adder(x, words[14], words[15]);
            one = y;
            let (d, x) = full_adder(two, a, b);
            two = x;
            let (f, x) = full_adder(four, c, d);
            four = x;
            let (sixteen, x) = full_adder(eight, e, f);
            eight = x;
            add_word(target, sixteen, 16);
            row += 16;
        }
        for r in row..rows {
            add_word(target, input.word(r * width + column, n), 1);
        }
        for (bits, weight) in [(one, 1), (two, 2), (four, 4), (eight, 8)] {
            add_word(target, bits, weight);
        }
    }
}

// A64 NEON is baseline on our supported aarch64 targets. Each SIMD lane holds
// one column's partial count for at most255 rows, so u8 cannot overflow. Four
// registers cover64 columns. Widen into persistent64-bit counters once/block;
// the only scratch is64 bytes, not an entry-sized byte array or transpose.
#[cfg(all(target_arch = "aarch64", target_endian = "little"))]
fn neon<C: Counter, const MASKS: usize>(input: &Reader<'_, MASKS>, rows: usize, output: &mut [C]) {
    use std::arch::aarch64::*;
    // SAFETY: all vector loads/stores below address fixed16-byte arrays or
    // four16-byte segments of the64-byte scratch. Bitmap loads remain checked.
    unsafe {
        let lookup = [0_u8, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1];
        let masks = [1_u8, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128];
        let lookup = vld1q_u8(lookup.as_ptr());
        let masks = vld1q_u8(masks.as_ptr());
        let width = output.len();
        for (block, target) in output.chunks_mut(64).enumerate() {
            let column = block * 64;
            for start in (0..rows).step_by(255) {
                let mut partial = [vdupq_n_u8(0); 4];
                let end = (start + 255).min(rows);
                let mut row = start;
                // CSA15: eleven full adders compress fifteen rows into four
                // bit planes. Decode the planes once, rather than unpacking
                // every incoming row. The same fixed registers/scratch serve
                // both this network and its short-input remainder.
                while row + 15 <= end {
                    let word = |i| input.word((row + i) * width + column, target.len());
                    let (a0, b0) = full_adder(word(0), word(1), word(2));
                    let (a1, b1) = full_adder(word(3), word(4), word(5));
                    let (a2, b2) = full_adder(word(6), word(7), word(8));
                    let (a3, b3) = full_adder(word(9), word(10), word(11));
                    let (a4, b4) = full_adder(word(12), word(13), word(14));
                    let (a5, b5) = full_adder(b0, b1, b2);
                    let (a6, one) = full_adder(b3, b4, b5);
                    let (a7, b6) = full_adder(a0, a1, a2);
                    let (a8, b7) = full_adder(a3, a4, a5);
                    let (a9, two) = full_adder(b6, b7, a6);
                    let (eight, four) = full_adder(a7, a8, a9);
                    for (i, count) in partial.iter_mut().enumerate() {
                        let lane = |bits: u64| {
                            vtstq_u8(
                                vqtbl1q_u8(
                                    vreinterpretq_u8_u16(vdupq_n_u16((bits >> (16 * i)) as u16)),
                                    lookup,
                                ),
                                masks,
                            )
                        };
                        let bit1 = vandq_u8(lane(one), vdupq_n_u8(1));
                        let bit2 = vandq_u8(lane(two), vdupq_n_u8(2));
                        let bit4 = vandq_u8(lane(four), vdupq_n_u8(4));
                        let bit8 = vandq_u8(lane(eight), vdupq_n_u8(8));
                        *count =
                            vaddq_u8(*count, vorrq_u8(vorrq_u8(bit1, bit2), vorrq_u8(bit4, bit8)));
                    }
                    row += 15;
                }
                for row in row..end {
                    let bits = input.word(row * width + column, target.len());
                    for (i, count) in partial.iter_mut().enumerate() {
                        let bytes = vreinterpretq_u8_u16(vdupq_n_u16((bits >> (16 * i)) as u16));
                        let repeated = vqtbl1q_u8(bytes, lookup);
                        *count = vsubq_u8(*count, vtstq_u8(repeated, masks));
                    }
                }
                let mut scratch = [0_u8; 64];
                for (i, count) in partial.iter().enumerate() {
                    vst1q_u8(scratch.as_mut_ptr().add(i * 16), *count);
                }
                for (counter, count) in target.iter_mut().zip(scratch) {
                    counter.increment(u64::from(count));
                }
            }
        }
    }
}

pub(crate) fn accumulate<C: Counter>(inputs: &[BitSlice<'_>], rows: usize, output: &mut [C]) {
    if rows == 0 || output.is_empty() {
        return;
    }
    if inputs.is_empty() {
        for counter in output {
            counter.increment(rows as u64);
        }
        return;
    }
    let mut masks = [inputs[0]; 4];
    masks[..inputs.len()].copy_from_slice(inputs);
    fn run<C: Counter, const MASKS: usize>(
        masks: [BitSlice<'_>; 4],
        rows: usize,
        output: &mut [C],
    ) {
        let input = Reader::<MASKS> { inputs: masks };
        if output.len() == 1 {
            let mut count = 0_u64;
            for index in (0..rows).step_by(64) {
                count += u64::from(input.word(index, (rows - index).min(64)).count_ones());
            }
            output[0].increment(count);
            return;
        }
        #[cfg(all(target_arch = "aarch64", target_endian = "little"))]
        if rows >= 4 && output.len() >= 32 && std::arch::is_aarch64_feature_detected!("neon") {
            return neon(&input, rows, output);
        }
        if rows >= 16 {
            carry_save(&input, rows, output);
        } else {
            direct(&input, rows, output);
        }
    }
    match inputs.len() {
        1 => run::<C, 1>(masks, rows, output),
        2 => run::<C, 2>(masks, rows, output),
        3 => run::<C, 3>(masks, rows, output),
        4 => run::<C, 4>(masks, rows, output),
        _ => unreachable!("at most predicate, validity, filter and filter validity"),
    }
}
