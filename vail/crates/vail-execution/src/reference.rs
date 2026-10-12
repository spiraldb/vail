//! Immutable coordinate metadata for a reference genome.
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};

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
    pub fn validate_coordinate(
        &self,
        ordinal: i32,
        contig: &str,
        position: i32,
    ) -> Result<(), String> {
        if self.ordinal(contig) != Some(ordinal) {
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
