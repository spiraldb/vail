//! Hail's packed signed Int32 Call storage, shared by ingestion and localization.
pub fn pack(alleles: &[i32], phased: bool) -> Result<i32, String> {
    if alleles.len() > 2 || alleles.iter().any(|a| *a < 0) {
        return Err("Call requires at most two nonnegative alleles".into());
    }
    let repr = match alleles {
        [] => 0,
        [a] => i64::from(*a),
        [a, b] => {
            let (a, b) = (i64::from(*a), i64::from(*b));
            let (j, k) = if phased {
                (a, a + b)
            } else {
                (a.min(b), a.max(b))
            };
            // Use i128 to validate extreme int32 alleles without overflow.
            let repr = i128::from(k) * (i128::from(k) + 1) / 2 + i128::from(j);
            i64::try_from(repr).map_err(|_| "Call allele representation overflow")?
        }
        _ => unreachable!(),
    };
    if !(0..(1 << 29)).contains(&repr) {
        return Err("Call allele representation exceeds 29 bits".into());
    }
    Ok((((repr as u32) << 3) | ((alleles.len() as u32) << 1) | u32::from(phased)) as i32)
}
/// Checked stack-backed packed Call view for hot serializers/adapters.
pub struct DecodedCall {
    alleles: [i32; 2],
    ploidy: usize,
    phased: bool,
}
impl DecodedCall {
    pub fn alleles(&self) -> &[i32] {
        &self.alleles[..self.ploidy]
    }
    pub fn phased(&self) -> bool {
        self.phased
    }
}
pub fn decode(word: i32) -> Result<DecodedCall, String> {
    let bits = word as u32;
    let phased = bits & 1 != 0;
    let ploidy = ((bits >> 1) & 3) as usize;
    let repr = i64::from(bits >> 3);
    let alleles = match ploidy {
        0 if repr == 0 => [0, 0],
        1 => [repr as i32, 0],
        2 => {
            let k = ((8 * repr + 1).isqrt() - 1) / 2;
            let j = repr - k * (k + 1) / 2;
            [j as i32, (if phased { k - j } else { k }) as i32]
        }
        _ => return Err("invalid packed Call ploidy or allele representation".into()),
    };
    let call = DecodedCall {
        alleles,
        ploidy,
        phased,
    };
    if pack(call.alleles(), phased)? != word {
        return Err("invalid packed Call".into());
    }
    Ok(call)
}
pub fn unpack(word: i32) -> Result<(Vec<i32>, bool), String> {
    let call = decode(word)?;
    Ok((call.alleles().to_vec(), call.phased()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hail_packed_words_and_roundtrips() {
        for (alleles, phased, expected) in [
            (vec![], false, 0),
            (vec![], true, 1),
            (vec![7], false, 58),
            (vec![7], true, 59),
            (vec![0, 2], false, 28),
            (vec![1, 1], false, 20),
            (vec![0, 1], true, 13),
            (vec![1, 0], true, 21),
            (vec![(1 << 29) - 1], false, -6),
        ] {
            assert_eq!(pack(&alleles, phased).unwrap(), expected);
            let decoded=decode(expected).unwrap();
            assert_eq!(decoded.alleles(),alleles.as_slice());
            assert_eq!(decoded.phased(),phased);
            assert_eq!(unpack(expected).unwrap(), (alleles, phased));
        }
        for phased in [false, true] {
            for a in [0, 1, 100, 16000] {
                for b in [0, 1, 100, 16000] {
                    let alleles = if phased {
                        vec![a, b]
                    } else {
                        vec![a.min(b), a.max(b)]
                    };
                    assert_eq!(
                        unpack(pack(&alleles, phased).unwrap()).unwrap(),
                        (alleles, phased)
                    );
                }
            }
        }
    }

    #[test]
    fn invalid_calls_are_rejected() {
        for alleles in [
            vec![-1],
            vec![0, 1, 2],
            vec![1 << 29],
            vec![i32::MAX, i32::MAX],
        ] {
            assert!(pack(&alleles, false).is_err());
        }
        for word in [6, 7, 8, 9] {
            assert!(unpack(word).is_err());
        }
    }
}
