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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
        self.validated_classifications().map(|_| ())
    }

    fn validated_classifications(&self) -> Result<Classifications<'_>, String> {
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
        fn group<'a>(
            contigs: &'a [String],
            names: &HashSet<&String>,
        ) -> Result<HashSet<&'a str>, String> {
            let mut seen = HashSet::with_capacity(contigs.len());
            for contig in contigs {
                if !names.contains(contig) || !seen.insert(contig.as_str()) {
                    return Err("classification contains unknown or duplicate contig".into());
                }
            }
            Ok(seen)
        }
        let groups = Classifications {
            x: group(&self.x_contigs, &names)?,
            y: group(&self.y_contigs, &names)?,
            mt: group(&self.mt_contigs, &names)?,
        };
        if !groups.x.is_disjoint(&groups.y)
            || !groups.x.is_disjoint(&groups.mt)
            || !groups.y.is_disjoint(&groups.mt)
        {
            return Err("a contig cannot have more than one chromosome classification".into());
        }
        let mut seen_par = HashSet::new();
        for p in &self.par {
            let Some(length) = self.lengths.get(&p.contig) else {
                return Err(format!("unknown PAR contig {:?}", p.contig));
            };
            if !groups.x.contains(p.contig.as_str()) && !groups.y.contains(p.contig.as_str()) {
                return Err("PAR intervals must be on X or Y contigs".into());
            }
            if p.start < 1 || p.start >= p.end || p.end > *length || !seen_par.insert(p) {
                return Err(
                    "PAR interval must be a valid half-open interval within its contig".into(),
                );
            }
        }
        Ok(groups)
    }
    pub fn ordinal(&self, contig: &str) -> Option<i32> {
        self.contigs
            .iter()
            .position(|c| c == contig)
            .and_then(|i| i32::try_from(i).ok())
    }

    /// Build immutable lookup metadata for repeated locus evaluation.
    pub(crate) fn prepare(&self) -> Result<PreparedReference, String> {
        let groups = self.validated_classifications()?;
        let mut ordinals = HashMap::with_capacity(self.contigs.len());
        let mut ends = Vec::with_capacity(self.contigs.len());
        let mut lengths = Vec::with_capacity(self.contigs.len());
        let mut total = 0_i64;
        for (index, contig) in self.contigs.iter().enumerate() {
            i32::try_from(index).map_err(|_| "reference has too many contigs")?;
            ordinals.insert(contig.clone(), index);
            lengths.push(self.lengths[contig]);
            total = total
                .checked_add(i64::from(self.lengths[contig]))
                .ok_or("total reference length exceeds int64")?;
            ends.push(total);
        }
        let x_contigs = groups.x.iter().map(|c| ordinals[*c]).collect();
        let y_contigs = groups.y.iter().map(|c| ordinals[*c]).collect();
        let mt_contigs = groups.mt.iter().map(|c| ordinals[*c]).collect();
        let mut x_par = vec![RangeSet::<i32>::new(); self.contigs.len()];
        let mut y_par = vec![RangeSet::<i32>::new(); self.contigs.len()];
        for interval in &self.par {
            let intervals = if groups.x.contains(interval.contig.as_str()) {
                &mut x_par
            } else {
                &mut y_par
            };
            intervals[ordinals[&interval.contig]].insert(interval.start..interval.end);
        }
        Ok(PreparedReference {
            contigs: self.contigs.clone(),
            lengths,
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

struct Classifications<'a> {
    x: HashSet<&'a str>,
    y: HashSet<&'a str>,
    mt: HashSet<&'a str>,
}

/// Immutable validated snapshot for repeated evaluation. Contig names, lengths,
/// cumulative ends, and PAR tables share ordinal indices. Positive lengths make
/// cumulative ends strictly increasing, permitting binary search.
#[derive(Debug)]
pub(crate) struct PreparedReference {
    contigs: Vec<String>,
    lengths: Vec<i32>,
    ordinals: HashMap<String, usize>,
    cumulative_ends: Vec<i64>,
    total_length: i64,
    x_contigs: HashSet<usize>,
    y_contigs: HashSet<usize>,
    mt_contigs: HashSet<usize>,
    x_par: Vec<RangeSet<i32>>,
    y_par: Vec<RangeSet<i32>>,
}

impl PreparedReference {
    pub(crate) fn ordinal(&self, contig: &str) -> Option<i32> {
        self.ordinals
            .get(contig)
            .and_then(|&i| i32::try_from(i).ok())
    }

    pub(crate) fn validate_coordinate(
        &self,
        ordinal: i32,
        contig: &str,
        position: i32,
    ) -> Result<(), String> {
        let index = usize::try_from(ordinal).ok();
        if index.is_none_or(|i| self.contigs.get(i).is_none_or(|name| name != contig)) {
            return Err("locus ordinal does not match contig".into());
        }
        let length = self.lengths[index.expect("validated ordinal")];
        if position < 1 || position > length {
            return Err(format!(
                "locus position {position} is outside contig {contig:?} (1..={length})"
            ));
        }
        Ok(())
    }

    pub(crate) fn from_global(&self, position: i64) -> Option<(&str, i32)> {
        if position < 0 || position >= self.total_length {
            return None;
        }
        let ordinal = self.cumulative_ends.partition_point(|&end| end <= position);
        let start = if ordinal == 0 {
            0
        } else {
            self.cumulative_ends[ordinal - 1]
        };
        Some((
            self.contigs[ordinal].as_str(),
            i32::try_from(position - start + 1).ok()?,
        ))
    }

    pub(crate) fn global_position(&self, ordinal: usize, position: i32) -> Option<i64> {
        let length = *self.lengths.get(ordinal)?;
        if position < 1 || position > length {
            return None;
        }
        let start = ordinal
            .checked_sub(1)
            .map_or(0, |i| self.cumulative_ends[i]);
        Some(start + i64::from(position - 1))
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
