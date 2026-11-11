//! A closed, typed cyclic region. The acyclic `PlanV1` has its own wire contract.
use crate::{Field, Result, Schema, Type, require};
use serde::{Deserialize, Serialize};

pub const MAX_VERTICES: usize = 4_096;
pub const MAX_EDGES: usize = 65_536;
pub const MAX_ROUNDS: u32 = 4_096;
pub const MAX_CREDITS: usize = 1_024;
pub const MAX_PROPOSALS: usize = 2 * MAX_EDGES;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Algorithm {
    ReachabilityBfsV1 { seed: u64 },
    WeakComponentsMinLabelV1 {},
}

impl Algorithm {
    #[must_use]
    pub fn output_schema(self) -> Schema {
        let (name, ty) = match self {
            Self::ReachabilityBfsV1 { .. } => ("distance", Type::U32),
            Self::WeakComponentsMinLabelV1 {} => ("component_min_vertex", Type::U64),
        };
        vec![
            Field::new("vertex", Type::U64, false),
            Field::new(name, ty, false),
        ]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Vertices,
    Edges,
}
impl Role {
    #[must_use]
    pub fn schema(self) -> Schema {
        let names: &[_] = match self {
            Self::Vertices => &["vertex"],
            Self::Edges => &["src", "dst"],
        };
        names
            .iter()
            .map(|name| Field::new(name, Type::U64, false))
            .collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Operator {
    Expand,
    Reduce,
    Delay,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PortType {
    BfsFrontier,
    BfsCandidate,
    LabelFrontier,
    LabelCandidate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Arc {
    from: u8,
    to: u8,
    schema: PortType,
    delay: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CyclicPlanV1 {
    version: u8,
    algorithm: Algorithm,
    nodes: [Operator; 3],
    arcs: [Arc; 3],
}

impl CyclicPlanV1 {
    #[must_use]
    pub fn new(algorithm: Algorithm) -> Self {
        let (frontier, candidate) = match algorithm {
            Algorithm::ReachabilityBfsV1 { .. } => (PortType::BfsFrontier, PortType::BfsCandidate),
            Algorithm::WeakComponentsMinLabelV1 {} => {
                (PortType::LabelFrontier, PortType::LabelCandidate)
            }
        };
        Self {
            version: 1,
            algorithm,
            nodes: [Operator::Expand, Operator::Reduce, Operator::Delay],
            arcs: [
                Arc {
                    from: 0,
                    to: 1,
                    schema: candidate,
                    delay: 0,
                },
                Arc {
                    from: 1,
                    to: 2,
                    schema: frontier,
                    delay: 0,
                },
                Arc {
                    from: 2,
                    to: 0,
                    schema: frontier,
                    delay: 1,
                },
            ],
        }
    }

    /// # Errors
    /// Refuses unsupported operators, arcs, schemas and versions. The only
    /// admitted topology has a cycle and a DAG when the delayed arc is removed.
    pub fn bind(&self) -> Result<BoundCycle> {
        require(
            *self == Self::new(self.algorithm),
            "unsupported cyclic topology, version, delay or port",
        )?;
        Ok(BoundCycle {
            algorithm: self.algorithm,
            backedge: self.arcs[2],
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct BoundCycle {
    algorithm: Algorithm,
    backedge: Arc,
}

impl BoundCycle {
    #[must_use]
    pub fn algorithm(self) -> Algorithm {
        self.algorithm
    }

    /// # Errors
    /// Refuses round overflow.
    pub fn feedback_round(self, from_round: u32) -> Result<u32> {
        from_round
            .checked_add(self.backedge.delay)
            .ok_or_else(|| crate::Error("feedback round overflow".into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn admits_only_the_typed_delayed_cycle() {
        let plan = CyclicPlanV1::new(Algorithm::ReachabilityBfsV1 { seed: 0 });
        let value = serde_json::to_value(plan).unwrap();
        let bound = serde_json::from_value::<CyclicPlanV1>(value.clone())
            .unwrap()
            .bind()
            .unwrap();
        assert_eq!(bound.feedback_round(0).unwrap(), 1);
        assert!(bound.feedback_round(u32::MAX).is_err());
        for (arc, key, replacement) in [
            (2, "delay", json!(0)),
            (0, "to", json!(0)),
            (2, "schema", json!("label_frontier")),
            (2, "to", json!(9)),
            (1, "delay", json!(1)),
        ] {
            let mut bad = value.clone();
            bad["arcs"][arc][key] = replacement;
            assert!(
                serde_json::from_value::<CyclicPlanV1>(bad)
                    .unwrap()
                    .bind()
                    .is_err()
            );
        }
        let mut bad = value.clone();
        bad["nodes"].as_array_mut().unwrap().push(json!("delay"));
        assert!(serde_json::from_value::<CyclicPlanV1>(bad).is_err());
        let encoded = serde_json::to_string(&value).unwrap();
        assert!(
            serde_json::from_str::<CyclicPlanV1>(&encoded.replacen('{', "{\"version\":1,", 1))
                .is_err()
        );
        assert!(
            serde_json::from_str::<CyclicPlanV1>(&encoded.replacen('{', "{\"unknown\":0,", 1))
                .is_err()
        );
    }
}
