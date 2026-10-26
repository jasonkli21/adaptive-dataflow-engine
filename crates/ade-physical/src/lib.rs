//! Versioned, frontend-independent, bounded native execution contract.
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub type NodeId = u32;
pub type SourceId = u32;
pub type Schema = Vec<Field>;
pub const MAX_NODES: usize = 64;
pub const MAX_PLAN_BYTES: usize = 256 * 1024;
pub const MAX_SOURCE_ROWS: u64 = 1_000_000;
pub const MAX_SOURCE_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
#[error("physical v1 admission: {0}")]
pub struct Error(pub String);
pub type Result<T> = std::result::Result<T, Error>;
fn require(ok: bool, message: &str) -> Result<()> {
    if ok {
        Ok(())
    } else {
        Err(Error(message.into()))
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Type {
    U64,
    I64,
    F64,
    U32,
    Utf8,
    TimestampMs,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Field {
    pub name: String,
    pub data_type: Type,
    pub nullable: bool,
}
impl Field {
    #[must_use]
    pub fn new(name: &str, data_type: Type, nullable: bool) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Target {
    Local,
    DistributedAggregate,
    DistributedJoin,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceFile {
    pub name: String,
    pub bytes: u64,
    pub rows: u64,
    pub fingerprint: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Source {
    pub id: SourceId,
    pub table: String,
    pub manifest: String,
    pub logical: String,
    pub schema_identity: String,
    pub schema: Schema,
    pub files: Vec<SourceFile>,
    pub replayable: bool,
}
impl Source {
    /// # Errors
    /// Rejects malformed identities, schemas and excessive source inventories.
    pub fn validate(&self) -> Result<()> {
        require(
            matches!(self.table.as_str(), "input" | "ade_left" | "ade_right"),
            "table allowlist",
        )?;
        require(
            [&self.manifest, &self.logical, &self.schema_identity]
                .iter()
                .all(|s| hash(s)),
            "source identity must be BLAKE3 hex",
        )?;
        validate_schema(&self.schema)?;
        require(
            self.schema.iter().all(|f| !f.nullable),
            "nullable source unsupported",
        )?;
        require(
            !self.files.is_empty() && self.files.len() <= 64,
            "source file cap 1..64",
        )?;
        let mut names = BTreeSet::new();
        let (mut rows, mut bytes) = (0u64, 0u64);
        for f in &self.files {
            require(
                !f.name.is_empty()
                    && f.name.len() <= 256
                    && f.name.ends_with(".parquet")
                    && f.name
                        .split('/')
                        .all(|s| !s.is_empty() && s != "." && s != ".." && !s.contains('\\'))
                    && names.insert(&f.name)
                    && hash(&f.fingerprint),
                "invalid source file",
            )?;
            rows = rows
                .checked_add(f.rows)
                .ok_or_else(|| Error("row overflow".into()))?;
            bytes = bytes
                .checked_add(f.bytes)
                .ok_or_else(|| Error("byte overflow".into()))?;
        }
        require(
            rows <= MAX_SOURCE_ROWS && bytes <= MAX_SOURCE_BYTES,
            "source cap: 1m rows / 64MiB per table",
        )
    }
}
fn hash(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
}
fn validate_schema(s: &Schema) -> Result<()> {
    let mut names = BTreeSet::new();
    require(
        !s.is_empty()
            && s.len() <= 16
            && s.iter()
                .all(|f| !f.name.is_empty() && f.name.len() <= 128 && names.insert(&f.name)),
        "schema columns must be unique, named, bounded",
    )
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Distribution {
    Single,
    Broadcast,
    Hash(Vec<usize>),
    HashOrBroadcast(usize),
    Gather,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Properties {
    pub bounded: bool,
    pub deterministic: bool,
    pub replayable: bool,
    /// Ascending, non-null ordering column indexes. Empty means bag semantics.
    pub ordering: Vec<usize>,
    /// Semantic stage requirement; distributed query APIs gather final output.
    pub distribution: Distribution,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Selection {
    pub column: usize,
    pub alias: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Operator {
    Scan {
        source: SourceId,
    },
    Filter {
        input: NodeId,
        column: usize,
        less_than: u64,
    },
    Project {
        input: NodeId,
        columns: Vec<Selection>,
    },
    GroupSumCount {
        input: NodeId,
        key: usize,
        value: usize,
    },
    Sort {
        input: NodeId,
        key: usize,
        row_id: usize,
        value: usize,
    },
    Limit {
        input: NodeId,
        rows: u64,
    },
    InnerJoin {
        left: NodeId,
        right: NodeId,
        left_key: usize,
        right_key: usize,
        left_value: usize,
        right_value: usize,
    },
}
impl Operator {
    #[must_use]
    pub fn inputs(&self) -> Vec<NodeId> {
        match self {
            Self::Scan { .. } => vec![],
            Self::InnerJoin { left, right, .. } => vec![*left, *right],
            Self::Filter { input, .. }
            | Self::Project { input, .. }
            | Self::GroupSumCount { input, .. }
            | Self::Sort { input, .. }
            | Self::Limit { input, .. } => vec![*input],
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Node {
    pub id: NodeId,
    pub operator: Operator,
    pub schema: Schema,
    pub properties: Properties,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanV1 {
    pub version: u32,
    pub target: Target,
    pub sources: Vec<Source>,
    /// Canonical topological order. `NodeId` equals ordinal; one bounded tree.
    pub nodes: Vec<Node>,
    pub output: NodeId,
    pub max_output_rows: u64,
    pub max_output_bytes: u64,
}
impl PlanV1 {
    #[must_use]
    pub fn new(target: Target, sources: Vec<Source>) -> Self {
        Self {
            version: 1,
            target,
            sources,
            nodes: vec![],
            output: 0,
            max_output_rows: 1_000_000,
            max_output_bytes: 64 * 1024 * 1024,
        }
    }
    /// Append an operator with derived, never caller-guessed, schema/properties.
    /// # Errors
    /// Rejects type, column, dependency or graph-cap violations.
    pub fn push(&mut self, operator: Operator) -> Result<NodeId> {
        require(self.nodes.len() < MAX_NODES, "node cap 64")?;
        let id = u32::try_from(self.nodes.len()).map_err(|_| Error("node ID".into()))?;
        let (schema, properties) = self.infer(id, &operator)?;
        self.nodes.push(Node {
            id,
            operator,
            schema,
            properties,
        });
        self.output = id;
        Ok(id)
    }
    #[must_use]
    pub fn node(&self, id: NodeId) -> Option<&Node> {
        self.nodes.get(id as usize)
    }
    #[allow(
        clippy::too_many_lines,
        reason = "Exhaustive schema/property inference stays beside the operator contract."
    )]
    fn infer(&self, id: NodeId, op: &Operator) -> Result<(Schema, Properties)> {
        let inputs = op.inputs();
        require(
            inputs.iter().all(|i| *i < id),
            "cycle/dangling/noncanonical input",
        )?;
        let get = |i| self.node(i).ok_or_else(|| Error("dangling input".into()));
        let mut props = Properties {
            bounded: true,
            deterministic: true,
            replayable: true,
            ordering: vec![],
            distribution: Distribution::Single,
        };
        if let Some(i) = inputs.first() {
            props = get(*i)?.properties.clone();
        }
        let check = |schema: &Schema, i: usize, ty: Type| -> Result<()> {
            require(
                schema
                    .get(i)
                    .is_some_and(|f| f.data_type == ty && !f.nullable),
                "operator column type/nullability mismatch",
            )
        };
        let schema = match op {
            Operator::Scan { source } => {
                let s = self
                    .sources
                    .get(*source as usize)
                    .ok_or_else(|| Error("missing source".into()))?;
                props.replayable = s.replayable;
                s.schema.clone()
            }
            Operator::Filter { input, column, .. } => {
                let s = &get(*input)?.schema;
                check(s, *column, Type::U64)?;
                s.clone()
            }
            Operator::Project { input, columns } => {
                let s = &get(*input)?.schema;
                props.ordering = props
                    .ordering
                    .iter()
                    .map(|i| columns.iter().position(|c| c.column == *i))
                    .collect::<Option<Vec<_>>>()
                    .unwrap_or_default();
                if self.target != Target::Local {
                    props.distribution = Distribution::Gather;
                }
                columns
                    .iter()
                    .map(|c| {
                        let mut f = s
                            .get(c.column)
                            .ok_or_else(|| Error("project column".into()))?
                            .clone();
                        f.name.clone_from(&c.alias);
                        Ok(f)
                    })
                    .collect::<Result<Schema>>()?
            }
            Operator::GroupSumCount { input, key, value } => {
                let s = &get(*input)?.schema;
                check(s, *key, Type::U64)?;
                check(s, *value, Type::I64)?;
                props.ordering.clear();
                if self.target == Target::DistributedAggregate {
                    props.distribution = Distribution::Hash(vec![0]);
                }
                vec![
                    Field::new("key", Type::U64, false),
                    Field::new("sum", Type::I64, true),
                    Field::new("count", Type::I64, false),
                ]
            }
            Operator::Sort {
                input,
                key,
                row_id,
                value,
            } => {
                let s = &get(*input)?.schema;
                check(s, *key, Type::U64)?;
                check(s, *row_id, Type::U64)?;
                check(s, *value, Type::I64)?;
                require(
                    s[*key].name == "key"
                        && s[*row_id].name == "row_id"
                        && s[*value].name == "value_i64",
                    "sort requires original synthetic key,row_id,value_i64",
                )?;
                props.ordering = vec![0, 1];
                vec![s[*key].clone(), s[*row_id].clone(), s[*value].clone()]
            }
            Operator::Limit { input, rows } => {
                require(
                    props.ordering.len() == 2 && *rows <= self.max_output_rows,
                    "LIMIT needs key,row_id ordering and output cap",
                )?;
                get(*input)?.schema.clone()
            }
            Operator::InnerJoin {
                left,
                right,
                left_key,
                right_key,
                left_value,
                right_value,
            } => {
                let l = get(*left)?;
                let r = get(*right)?;
                check(&l.schema, *left_key, Type::U64)?;
                check(&r.schema, *right_key, Type::U64)?;
                check(&l.schema, *left_value, Type::I64)?;
                check(&r.schema, *right_value, Type::I64)?;
                props.replayable = false;
                props.ordering.clear();
                props.distribution = Distribution::HashOrBroadcast(0);
                vec![
                    Field::new("key", Type::U64, false),
                    Field::new("left_value", Type::I64, false),
                    Field::new("right_value", Type::I64, false),
                ]
            }
        };
        validate_schema(&schema)?;
        Ok((schema, props))
    }
    /// # Errors
    /// Rejects forged properties, malformed graphs and unsupported native targets.
    pub fn validate(&self) -> Result<()> {
        require(self.version == 1, "unsupported version")?;
        require(
            !self.nodes.is_empty()
                && self.nodes.len() <= MAX_NODES
                && self.output as usize == self.nodes.len() - 1,
            "graph size/output",
        )?;
        require(
            self.max_output_rows <= 1_000_000
                && self.max_output_bytes > 0
                && self.max_output_bytes <= 64 * 1024 * 1024,
            "output cap",
        )?;
        require(
            !self.sources.is_empty() && self.sources.len() <= 2,
            "source count",
        )?;
        let mut tables = BTreeSet::new();
        for (i, s) in self.sources.iter().enumerate() {
            require(
                s.id as usize == i && tables.insert(&s.table),
                "source ID/table",
            )?;
            s.validate()?;
        }
        let mut uses = vec![0usize; self.nodes.len()];
        let mut source_uses = vec![0usize; self.sources.len()];
        for (i, n) in self.nodes.iter().enumerate() {
            require(n.id as usize == i, "noncanonical/duplicate node ID")?;
            let (s, p) = self.infer(n.id, &n.operator)?;
            require(
                s == n.schema && p == n.properties,
                "forged schema/properties",
            )?;
            for input in n.operator.inputs() {
                uses[input as usize] += 1;
            }
            if let Operator::Scan { source } = n.operator {
                source_uses[source as usize] += 1;
            }
        }
        require(
            uses[..uses.len() - 1].iter().all(|u| *u == 1) && source_uses.iter().all(|u| *u == 1),
            "unreachable/shared graph or source",
        )?;
        self.validate_target()?;
        require(
            serde_json::to_vec(self)
                .map_err(|e| Error(e.to_string()))?
                .len()
                <= MAX_PLAN_BYTES,
            "serialized plan cap",
        )
    }
    fn validate_target(&self) -> Result<()> {
        let output = self
            .node(self.output)
            .ok_or_else(|| Error("output".into()))?;
        let mut base = output;
        while let Operator::Project { input, .. } = base.operator {
            base = self.node(input).ok_or_else(|| Error("input".into()))?;
        }
        match self.target {
            Target::Local => require(
                self.nodes
                    .iter()
                    .all(|n| !matches!(n.operator, Operator::InnerJoin { .. }))
                    && self.sources.len() == 1
                    && self.sources[0].table == "input",
                "local supports input unary operators only",
            ),
            Target::DistributedAggregate => {
                let Operator::GroupSumCount { input, key, value } = base.operator else {
                    return Err(Error(
                        "distributed aggregate requires group sum/count".into(),
                    ));
                };
                let scan = self
                    .node(input)
                    .ok_or_else(|| Error("aggregate input".into()))?;
                require(
                    matches!(scan.operator, Operator::Scan { source: 0 })
                        && self.sources.len() == 1
                        && self.sources[0].table == "input"
                        && scan.schema[key].name == "key"
                        && scan.schema[value].name == "value_i64"
                        && self.nodes[..base.id as usize]
                            .iter()
                            .all(|n| matches!(n.operator, Operator::Scan { .. })),
                    "distributed aggregate requires unfiltered original key/value scan",
                )
            }
            Target::DistributedJoin => {
                let Operator::InnerJoin {
                    left,
                    right,
                    left_key,
                    right_key,
                    left_value,
                    right_value,
                } = base.operator
                else {
                    return Err(Error("distributed join requires inner equijoin".into()));
                };
                let l = self.node(left).ok_or_else(|| Error("join left".into()))?;
                let r = self.node(right).ok_or_else(|| Error("join right".into()))?;
                require(
                    matches!(l.operator, Operator::Scan { source: 0 })
                        && matches!(r.operator, Operator::Scan { source: 1 })
                        && self.sources.len() == 2
                        && self.sources[0].table == "ade_left"
                        && self.sources[1].table == "ade_right"
                        && l.schema[left_key].name == "key"
                        && r.schema[right_key].name == "key"
                        && l.schema[left_value].name == "value_i64"
                        && r.schema[right_value].name == "value_i64"
                        && self.nodes[..base.id as usize]
                            .iter()
                            .all(|n| matches!(n.operator, Operator::Scan { .. })),
                    "join requires original left/right scans and key/value columns",
                )
            }
        }
    }
    /// # Errors
    /// Strict bounded deserialization followed by full validation.
    pub fn from_json(bytes: &[u8]) -> Result<Self> {
        require(bytes.len() <= MAX_PLAN_BYTES, "plan byte cap")?;
        let p: Self = serde_json::from_slice(bytes).map_err(|e| Error(e.to_string()))?;
        p.validate()?;
        Ok(p)
    }
    /// Canonical JSON is the versioned deterministic explain format.
    /// # Errors
    /// Rejects invalid plans.
    pub fn explain(&self) -> Result<String> {
        self.validate()?;
        serde_json::to_string_pretty(self).map_err(|e| Error(e.to_string()))
    }
    /// # Errors
    /// Rejects invalid plans.
    pub fn digest(&self) -> Result<String> {
        self.validate()?;
        Ok(
            blake3::hash(&serde_json::to_vec(self).map_err(|e| Error(e.to_string()))?)
                .to_hex()
                .to_string(),
        )
    }
}

#[must_use]
pub fn fingerprint(bytes: &[u8]) -> String {
    blake3::hash(bytes).to_hex().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    fn plan() -> PlanV1 {
        let mut p = PlanV1::new(
            Target::Local,
            vec![Source {
                id: 0,
                table: "input".into(),
                manifest: fingerprint(b"m"),
                logical: fingerprint(b"l"),
                schema_identity: fingerprint(b"s"),
                schema: vec![
                    Field::new("key", Type::U64, false),
                    Field::new("value_i64", Type::I64, false),
                ],
                files: vec![SourceFile {
                    name: "part.parquet".into(),
                    bytes: 10,
                    rows: 1,
                    fingerprint: fingerprint(b"file"),
                }],
                replayable: true,
            }],
        );
        p.push(Operator::Scan { source: 0 }).unwrap();
        p
    }
    #[test]
    fn canonical_roundtrip_and_digest() {
        let p = plan();
        let bytes = serde_json::to_vec(&p).unwrap();
        assert_eq!(p, PlanV1::from_json(&bytes).unwrap());
        assert_eq!(
            p.digest().unwrap(),
            PlanV1::from_json(&bytes).unwrap().digest().unwrap()
        );
        assert!(p.explain().unwrap().contains("manifest"));
        let mut q = p.clone();
        q.max_output_rows = 1;
        assert_ne!(p.digest().unwrap(), q.digest().unwrap());
    }
    #[test]
    fn version_unknown_fields_and_size_refuse() {
        let p = plan();
        let mut v = serde_json::to_value(p).unwrap();
        v["version"] = 2.into();
        assert!(PlanV1::from_json(&serde_json::to_vec(&v).unwrap()).is_err());
        v["version"] = 1.into();
        v["future"] = true.into();
        assert!(PlanV1::from_json(&serde_json::to_vec(&v).unwrap()).is_err());
        assert!(PlanV1::from_json(&vec![b' '; MAX_PLAN_BYTES + 1]).is_err());
    }
    #[test]
    fn forged_graph_properties_types_and_sources_refuse() {
        let p = plan();
        let mutations: [fn(&mut PlanV1); 8] = [
            |p| {
                p.nodes[0].operator = Operator::Filter {
                    input: 0,
                    column: 0,
                    less_than: 1,
                }
            },
            |p| p.nodes[0].id = 2,
            |p| p.nodes[0].schema[0].data_type = Type::I64,
            |p| p.nodes[0].properties.ordering = vec![0],
            |p| p.sources[0].files[0].name = "../escape.parquet".into(),
            |p| p.sources[0].schema[0].nullable = true,
            |p| p.sources[0].files[0].rows = MAX_SOURCE_ROWS + 1,
            |p| p.nodes[0].properties.distribution = Distribution::Broadcast,
        ];
        for mutate in mutations {
            let mut q = p.clone();
            mutate(&mut q);
            assert!(q.validate().is_err());
        }
    }
    #[test]
    fn columns_caps_unordered_limit_and_dead_nodes_refuse() {
        let mut p = plan();
        assert!(
            p.push(Operator::Filter {
                input: 0,
                column: 1,
                less_than: 2
            })
            .is_err()
        );
        assert!(
            p.push(Operator::Project {
                input: 0,
                columns: vec![Selection {
                    column: 99,
                    alias: "x".into()
                }]
            })
            .is_err()
        );
        assert!(p.push(Operator::Limit { input: 0, rows: 1 }).is_err());
        p.push(Operator::Scan { source: 0 }).unwrap();
        assert!(p.validate().is_err());
        let mut p = plan();
        for _ in 1..MAX_NODES {
            p.push(Operator::Filter {
                input: p.output,
                column: 0,
                less_than: 1,
            })
            .unwrap();
        }
        p.validate().unwrap();
        assert!(
            p.push(Operator::Filter {
                input: p.output,
                column: 0,
                less_than: 1
            })
            .is_err()
        );
    }
    #[test]
    fn distributed_upstream_filter_refuses() {
        let mut p = plan();
        p.target = Target::DistributedAggregate;
        let input = p
            .push(Operator::Filter {
                input: 0,
                column: 0,
                less_than: 2,
            })
            .unwrap();
        p.push(Operator::GroupSumCount {
            input,
            key: 0,
            value: 1,
        })
        .unwrap();
        assert!(p.validate().is_err());
    }
}
