//! Engine-independent semantic workloads. Unknown fields and versions are rejected.
use crate::{Result, require};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fs, path::Path};
/// Explicit semantic operation; SQL translation belongs to the adapter.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Query {
    Scan,
    ScanFilterProject { threshold: u64 },
    GroupBySum,
    TpchQ1,
}
/// Required dataset schema, independent of engine configuration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DatasetRequirement {
    pub required_schema: String,
}
/// Canonical validation method, mandatory for every case.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ValidationSpec {
    pub kind: String,
}
/// Versioned semantic case, suitable for future ADE adapters.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct BenchmarkCase {
    pub schema_version: u32,
    pub id: String,
    pub version: u32,
    pub description: String,
    pub dataset: DatasetRequirement,
    pub query: Query,
    pub validation: ValidationSpec,
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
}

/// Fields that define whether two recorded workloads have the same semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkloadSemanticIdentity<'a> {
    pub schema_version: u32,
    pub id: &'a str,
    pub version: u32,
    pub dataset: &'a DatasetRequirement,
    pub query: &'a Query,
    pub validation: &'a ValidationSpec,
}

impl BenchmarkCase {
    /// Loads and validates a strict TOML semantic specification.
    ///
    /// # Errors
    /// Returns an error for unsupported semantics, malformed TOML or unreadable input.
    pub fn load(path: &Path) -> Result<Self> {
        let s: Self = toml::from_str(&fs::read_to_string(path)?)?;
        s.validate()?;
        Ok(s)
    }
    /// Checks supported semantic versions, dataset requirements and validation method.
    ///
    /// # Errors
    /// Returns an error for unsupported semantics, malformed TOML or unreadable input.
    pub fn validate(&self) -> Result<()> {
        require(
            self.schema_version == 1 && self.version == 1,
            "unsupported workload schema/semantic version",
        )?;
        require(!self.id.is_empty(), "workload id cannot be empty")?;
        let tpch = self.query == Query::TpchQ1;
        require(
            self.dataset.required_schema
                == if tpch {
                    "tpch-v1"
                } else {
                    "synthetic-events-v1"
                },
            "query/dataset schema mismatch",
        )?;
        require(
            self.validation.kind
                == if tpch {
                    "q1_tolerance_v1"
                } else {
                    "canonical_integer_rows_v1"
                },
            "unsupported required validation",
        )
    }

    /// Returns the stable identity used to determine semantic comparability.
    #[must_use]
    pub fn semantic_identity(&self) -> WorkloadSemanticIdentity<'_> {
        WorkloadSemanticIdentity {
            schema_version: self.schema_version,
            id: &self.id,
            version: self.version,
            dataset: &self.dataset,
            query: &self.query,
            validation: &self.validation,
        }
    }

    /// Tests semantic compatibility while ignoring description and tags.
    #[must_use]
    pub fn semantically_equivalent(&self, other: &Self) -> bool {
        self.semantic_identity() == other.semantic_identity()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn specs_strict() {
        let source = include_str!("../../../benchmarks/specs/aggregate-skew.toml");
        let c: BenchmarkCase = toml::from_str(source).unwrap();
        c.validate().unwrap();
        assert_eq!(
            toml::from_str::<BenchmarkCase>(&toml::to_string(&c).unwrap()).unwrap(),
            c
        );
        assert!(toml::from_str::<BenchmarkCase>(&source.replace("group_by_sum", "magic")).is_err());
        assert!(toml::from_str::<BenchmarkCase>(&format!("engine = 'df'\n{source}")).is_err());
        let mut bad = c;
        bad.schema_version = 99;
        assert!(bad.validate().is_err());
    }

    #[test]
    fn semantic_identity_includes_contract_and_excludes_description_and_tags() {
        let source = include_str!("../../../benchmarks/specs/aggregate-skew.toml");
        let case: BenchmarkCase = toml::from_str(source).unwrap();
        let mut changed = case.clone();
        changed.description.push_str(" (clarified)");
        changed.tags.insert("note".into(), "informational".into());
        assert!(case.semantically_equivalent(&changed));

        changed = case.clone();
        changed.query = Query::Scan;
        assert!(!case.semantically_equivalent(&changed));
        changed = case.clone();
        changed.validation.kind = "different-validation".into();
        assert!(!case.semantically_equivalent(&changed));
        changed = case.clone();
        changed.dataset.required_schema = "different-schema".into();
        assert!(!case.semantically_equivalent(&changed));
        changed = case.clone();
        changed.version += 1;
        assert!(!case.semantically_equivalent(&changed));
        changed = case;
        changed.id.push_str("-different");
        assert!(!changed.semantically_equivalent(&BenchmarkCase {
            id: "aggregate-skew".into(),
            ..changed.clone()
        }));
    }
}
