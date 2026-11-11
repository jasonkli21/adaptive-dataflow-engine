use crate::{Algorithm, Error, Guard, Result, require};
use ade_exec::iterative::{Cut, Graph};
use serde::{Deserialize, Serialize};
use std::io::Write;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Footer {
    pub kind: String,
    pub rows: usize,
    pub rounds: u32,
    pub result: String,
}

#[derive(Serialize)]
#[serde(untagged)]
enum Row {
    Distance {
        vertex: u64,
        distance: u32,
    },
    Component {
        vertex: u64,
        component_min_vertex: u64,
    },
}

fn rows(graph: &Graph, cut: &Cut, algorithm: Algorithm) -> impl Iterator<Item = Result<Row>> {
    graph
        .vertices()
        .iter()
        .copied()
        .zip(cut.values.iter().copied())
        .filter_map(move |(vertex, value)| {
            value.map(|value| match algorithm {
                Algorithm::ReachabilityBfsV1 { .. } => u32::try_from(value)
                    .map(|distance| Row::Distance { vertex, distance })
                    .map_err(|_| Error::Refused("BFS output distance")),
                Algorithm::WeakComponentsMinLabelV1 {} => Ok(Row::Component {
                    vertex,
                    component_min_vertex: value,
                }),
            })
        })
}

pub(crate) fn line(value: &impl Serialize) -> Result<Vec<u8>> {
    let mut data = serde_json::to_vec(value)?;
    data.push(b'\n');
    Ok(data)
}

pub(crate) struct Terminal<'a> {
    pub graph: &'a Graph,
    pub cut: &'a Cut,
    pub algorithm: Algorithm,
}

impl Terminal<'_> {
    pub(crate) fn inspect(&self, guard: &Guard) -> Result<(Footer, usize)> {
        require(
            self.cut.in_flight == 0 && self.cut.next_round > 0 && self.cut.frontier.is_empty(),
            "cut is not terminal",
        )?;
        let mut hash = blake3::Hasher::new();
        let shape = match self.algorithm {
            Algorithm::ReachabilityBfsV1 { .. } => b"ade-cycle20-bfs-result-v1\0".as_slice(),
            Algorithm::WeakComponentsMinLabelV1 {} => {
                b"ade-cycle20-components-result-v1\0".as_slice()
            }
        };
        hash.update(shape);
        let mut count = 0;
        let mut bytes = 0;
        for row in rows(self.graph, self.cut, self.algorithm) {
            guard.check()?;
            let encoded = line(&row?)?;
            hash.update(&encoded);
            count += 1;
            bytes += encoded.len();
        }
        let footer = Footer {
            kind: "complete".into(),
            rows: count,
            rounds: self.cut.next_round,
            result: hash.finalize().to_hex().to_string(),
        };
        bytes += line(&footer)?.len();
        Ok((footer, bytes))
    }

    pub(crate) fn write(
        &self,
        footer: &Footer,
        full_bytes: usize,
        verify: bool,
        output: &mut impl Write,
        cap: usize,
        guard: &Guard,
    ) -> Result<()> {
        let trailer = line(footer)?;
        let required = if verify { trailer.len() } else { full_bytes };
        if required > cap {
            return Err(Error::Limit("serialized output bytes including footer"));
        }
        if !verify {
            for row in rows(self.graph, self.cut, self.algorithm) {
                guard.check()?;
                output.write_all(&line(&row?)?)?;
            }
        }
        guard.check()?;
        output.write_all(&trailer)?;
        output.flush()?;
        Ok(())
    }
}
