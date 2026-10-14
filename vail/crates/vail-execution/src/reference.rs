//! Immutable coordinate metadata for a reference genome.
use rangemap::RangeSet;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ParInterval {
    pub contig: String,
    pub start: i32,
    pub end: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReferenceDefinition {
    pub name: String,
    pub contigs: Vec<String>,
    pub lengths: BTreeMap<String, i32>,
    #[serde(default)]
    pub x_contigs: Vec<String>,
    #[serde(default)]
    pub y_contigs: Vec<String>,
    #[serde(default)]
    pub mt_contigs: Vec<String>,
    #[serde(default)]
    pub par: Vec<ParInterval>,
}

impl ReferenceDefinition {
    pub fn validate(&self) -> Result<(), String> {
        if self.name.is_empty() {
            return Err("reference name cannot be empty".into());
        }
        if self.contigs.is_empty() {
            return Err("reference must declare at least one contig".into());
        }
        let names: HashSet<_> = self.contigs.iter().collect();
        if names.len() != self.contigs.len() || self.contigs.iter().any(String::is_empty) {
            return Err("contig names must be non-empty and unique".into());
        }
        if self.lengths.len() != self.contigs.len()
            || self
                .contigs
                .iter()
                .any(|c| self.lengths.get(c).is_none_or(|n| *n <= 0))
        {
            return Err("each contig must have a positive int32 length".into());
        }
        for group in [&self.x_contigs, &self.y_contigs, &self.mt_contigs] {
            let mut seen = HashSet::new();
            if group.iter().any(|c| !names.contains(c) || !seen.insert(c)) {
                return Err("classification contains unknown or duplicate contig".into());
            }
        }
        if self
            .x_contigs
            .iter()
            .any(|c| self.y_contigs.contains(c) || self.mt_contigs.contains(c))
            || self.y_contigs.iter().any(|c| self.mt_contigs.contains(c))
        {
            return Err("a contig cannot have more than one chromosome classification".into());
        }
        let mut seen_par = HashSet::new();
        for p in &self.par {
            let Some(length) = self.lengths.get(&p.contig) else {
                return Err(format!("unknown PAR contig {:?}", p.contig));
            };
            if !self.x_contigs.contains(&p.contig) && !self.y_contigs.contains(&p.contig) {
                return Err("PAR intervals must be on X or Y contigs".into());
            }
            if p.start < 1 || p.start >= p.end || p.end > *length || !seen_par.insert(p) {
                return Err(
                    "PAR interval must be a valid half-open interval within its contig".into(),
                );
            }
        }
        Ok(())
    }
    pub fn ordinal(&self, contig: &str) -> Option<i32> {
        self.contigs
            .iter()
            .position(|c| c == contig)
            .and_then(|i| i32::try_from(i).ok())
    }

    /// Build immutable lookup metadata for repeated locus evaluation.
    pub(crate) fn prepare(&self) -> Result<PreparedReference, String> {
        self.validate()?;
        let mut ordinals = HashMap::with_capacity(self.contigs.len());
        let mut ends = Vec::with_capacity(self.contigs.len());
        let mut total = 0_i64;
        for (index, contig) in self.contigs.iter().enumerate() {
            i32::try_from(index).map_err(|_| "reference has too many contigs")?;
            ordinals.insert(contig.clone(), index);
            total = total
                .checked_add(i64::from(self.lengths[contig]))
                .ok_or("total reference length exceeds int64")?;
            ends.push(total);
        }
        let par_intervals = |names: &[String]| {
            let mut intervals = vec![RangeSet::<i32>::new(); self.contigs.len()];
            for interval in &self.par {
                if names.iter().any(|name| name == &interval.contig) {
                    let ordinal = ordinals[&interval.contig];
                    intervals[ordinal].insert(interval.start..interval.end);
                }
            }
            intervals
        };
        let x_contigs = self.x_contigs.iter().map(|c| ordinals[c]).collect();
        let y_contigs = self.y_contigs.iter().map(|c| ordinals[c]).collect();
        let mt_contigs = self.mt_contigs.iter().map(|c| ordinals[c]).collect();
        let x_par = par_intervals(&self.x_contigs);
        let y_par = par_intervals(&self.y_contigs);
        Ok(PreparedReference {
            ordinals,
            cumulative_ends: ends,
            total_length: total,
            x_contigs,
            y_contigs,
            mt_contigs,
            x_par,
            y_par,
        })
    }
    pub fn validate_coordinate(
        &self,
        ordinal: i32,
        contig: &str,
        position: i32,
    ) -> Result<(), String> {
        let index = usize::try_from(ordinal).ok();
        if index.is_none_or(|i| self.contigs.get(i).is_none_or(|name| name != contig)) {
            return Err("locus ordinal does not match contig".into());
        }
        let length = self
            .lengths
            .get(contig)
            .ok_or_else(|| "unknown locus contig".to_string())?;
        if position < 1 || position > *length {
            return Err(format!(
                "locus position {position} is outside contig {contig:?} (1..={length})"
            ));
        }
        Ok(())
    }
}

/// Derived lookup tables. Kept outside the serialized, publicly mutable
/// definition so callers cannot observe stale cached metadata.
#[derive(Debug)]
pub(crate) struct PreparedReference {
    ordinals: HashMap<String, usize>,
    pub(crate) cumulative_ends: Vec<i64>,
    total_length: i64,
    x_contigs: HashSet<usize>,
    y_contigs: HashSet<usize>,
    mt_contigs: HashSet<usize>,
    x_par: Vec<RangeSet<i32>>,
    y_par: Vec<RangeSet<i32>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reference() -> ReferenceDefinition {
        ReferenceDefinition {
            name: "prepared".into(),
            contigs: vec!["a".into(), "b".into(), "x".into(), "y".into()],
            lengths: [
                ("a".into(), 2),
                ("b".into(), 3),
                ("x".into(), 5),
                ("y".into(), 4),
            ]
            .into(),
            x_contigs: vec!["x".into()],
            y_contigs: vec!["y".into()],
            mt_contigs: vec![],
            par: vec![
                ParInterval {
                    contig: "x".into(),
                    start: 2,
                    end: 4,
                },
                ParInterval {
                    contig: "x".into(),
                    start: 3,
                    end: 5,
                },
                ParInterval {
                    contig: "y".into(),
                    start: 1,
                    end: 2,
                },
            ],
        }
    }

    #[test]
    fn prepared_reference_indexes_contigs_boundaries_and_intervals() {
        let reference = reference();
        let prepared = reference.prepare().unwrap();
        assert_eq!(prepared.ordinal("b"), Some(1));
        assert_eq!(prepared.ordinal("missing"), None);
        for (global, expected) in [
            (0, (0, 1)),
            (1, (0, 2)),
            (2, (1, 1)),
            (4, (1, 3)),
            (5, (2, 1)),
            (13, (3, 4)),
        ] {
            assert_eq!(prepared.from_global(global), Some(expected));
        }
        assert_eq!(prepared.from_global(14), None);
        assert!(prepared.validate_coordinate(&reference, 1, "b", 3).is_ok());
        assert!(prepared.validate_coordinate(&reference, 0, "b", 1).is_err());
        assert!(prepared.is_x(2));
        assert!(prepared.is_y(3));
        assert!(prepared.in_par(2, 2, true));
        assert!(prepared.in_par(2, 4, true));
        assert!(!prepared.in_par(2, 5, true));
        assert!(prepared.in_par(3, 1, false));
        assert!(!prepared.in_par(3, 2, false));
    }

    #[test]
    fn prepared_reference_binary_searches_many_contig_boundaries() {
        let contigs = (0..256).map(|i| format!("c{i}")).collect::<Vec<_>>();
        let lengths = contigs.iter().map(|c| (c.clone(), 7)).collect();
        let reference = ReferenceDefinition {
            name: "many".into(),
            contigs,
            lengths,
            x_contigs: vec![],
            y_contigs: vec![],
            mt_contigs: vec![],
            par: vec![],
        };
        let prepared = reference.prepare().unwrap();
        for ordinal in 0..256_usize {
            let first_global = ordinal as i64 * 7;
            assert_eq!(prepared.from_global(first_global), Some((ordinal, 1)));
            assert_eq!(prepared.from_global(first_global + 6), Some((ordinal, 7)));
            assert_eq!(
                prepared.ordinal(&reference.contigs[ordinal]),
                Some(ordinal as i32)
            );
        }
    }
}

impl PreparedReference {
    pub(crate) fn ordinal(&self, contig: &str) -> Option<i32> {
        self.ordinals
            .get(contig)
            .and_then(|&i| i32::try_from(i).ok())
    }

    pub(crate) fn validate_coordinate(
        &self,
        reference: &ReferenceDefinition,
        ordinal: i32,
        contig: &str,
        position: i32,
    ) -> Result<(), String> {
        let index = usize::try_from(ordinal).ok();
        if index.is_none_or(|i| i >= reference.contigs.len() || reference.contigs[i] != contig) {
            return Err("locus ordinal does not match contig".into());
        }
        let length = reference
            .lengths
            .get(contig)
            .ok_or("unknown locus contig")?;
        if position < 1 || position > *length {
            return Err(format!(
                "locus position {position} is outside contig {contig:?} (1..={length})"
            ));
        }
        Ok(())
    }

    pub(crate) fn from_global(&self, position: i64) -> Option<(usize, i32)> {
        if position < 0 || position >= self.total_length {
            return None;
        }
        let ordinal = self.cumulative_ends.partition_point(|&end| end <= position);
        let start = if ordinal == 0 {
            0
        } else {
            self.cumulative_ends[ordinal - 1]
        };
        Some((ordinal, i32::try_from(position - start + 1).ok()?))
    }

    pub(crate) fn is_x(&self, ordinal: usize) -> bool {
        self.x_contigs.contains(&ordinal)
    }
    pub(crate) fn is_y(&self, ordinal: usize) -> bool {
        self.y_contigs.contains(&ordinal)
    }
    pub(crate) fn is_mt(&self, ordinal: usize) -> bool {
        self.mt_contigs.contains(&ordinal)
    }
    pub(crate) fn in_par(&self, ordinal: usize, position: i32, is_x: bool) -> bool {
        let intervals = if is_x { &self.x_par } else { &self.y_par };
        intervals
            .get(ordinal)
            .is_some_and(|ranges| ranges.contains(&position))
    }
}
