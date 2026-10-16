//! Canonical diploid-or-smaller Calls with actual UInt16 allele values.
//! No packed word, triangular encoding, or overflow representation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Call {
    alleles: [u16; 2],
    ploidy: u8,
    phased: bool,
}
impl Call {
    pub fn new(alleles: &[i32], phased: bool) -> Result<Self, String> {
        if alleles.len() > 2 {
            return Err("Call requires at most two alleles".into());
        }
        let mut values = [0u16; 2];
        for (slot, &allele) in values.iter_mut().zip(alleles) {
            *slot = u16::try_from(allele)
                .map_err(|_| "Call allele index must be between 0 and 65535")?;
        }
        if !phased && alleles.len() == 2 && values[0] > values[1] {
            values.swap(0, 1);
        }
        Ok(Self {
            alleles: values,
            ploidy: alleles.len() as u8,
            phased,
        })
    }
    /// Validate physical fields, including zero padding and unphased order.
    pub fn from_fields(first: u16, second: u16, ploidy: u8, phased: bool) -> Result<Self, String> {
        if ploidy > 2
            || (ploidy == 0 && first != 0)
            || (ploidy < 2 && second != 0)
            || (ploidy == 2 && !phased && first > second)
        {
            return Err("invalid Call ploidy, padding, or allele order".into());
        }
        Ok(Self {
            alleles: [first, second],
            ploidy,
            phased,
        })
    }
    pub fn alleles(&self) -> &[u16] {
        &self.alleles[..usize::from(self.ploidy)]
    }
    pub fn first(self) -> u16 {
        self.alleles[0]
    }
    pub fn second(self) -> u16 {
        self.alleles[1]
    }
    pub fn ploidy(self) -> u8 {
        self.ploidy
    }
    pub fn phased(self) -> bool {
        self.phased
    }
}
