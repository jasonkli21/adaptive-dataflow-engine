//! Public `DataFusion` adapter and independent correctness oracles.
use crate::{
    Error, Result,
    dataset::{self, DatasetManifest},
    require,
    spec::{BenchmarkCase, Query},
};
use arrow::{
    array::{Array, Float64Array, Int64Array, UInt64Array},
    compute::cast,
    datatypes::DataType,
    record_batch::RecordBatch,
    util::display::array_value_to_string,
};
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fs::File, path::Path, time::Instant};

/// Explicit baseline configuration, separate from workload semantics.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct EngineConfig {
    pub target_partitions: usize,
    pub batch_size: usize,
    pub collect_statistics: bool,
}
impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            target_partitions: 2,
            batch_size: 8192,
            collect_statistics: false,
        }
    }
}
impl EngineConfig {
    /// Parses a whitelist of explicit engine options.
    ///
    /// # Errors
    /// Returns an error for invalid configuration/schema, missing input, engine setup/query failures or incorrect output.
    pub fn from_options(options: &[String]) -> Result<Self> {
        let mut c = Self::default();
        for option in options {
            let (k, v) = option
                .split_once('=')
                .ok_or_else(|| Error::Invalid("engine option must be key=value".into()))?;
            match k {
                "target_partitions" => {
                    c.target_partitions = v
                        .parse()
                        .map_err(|_| Error::Invalid("invalid target_partitions".into()))?;
                }
                "batch_size" => {
                    c.batch_size = v
                        .parse()
                        .map_err(|_| Error::Invalid("invalid batch_size".into()))?;
                }
                "collect_statistics" => {
                    c.collect_statistics = v
                        .parse()
                        .map_err(|_| Error::Invalid("invalid collect_statistics".into()))?;
                }
                _ => return Err(Error::Invalid(format!("unsupported engine option: {k}"))),
            }
        }
        c.validate()?;
        Ok(c)
    }
    /// Rejects invalid baseline configuration.
    ///
    /// # Errors
    /// Returns an error for invalid configuration/schema, missing input, engine setup/query failures or incorrect output.
    pub fn validate(&self) -> Result<()> {
        require(
            self.target_partitions > 0 && self.batch_size > 0,
            "partitions and batch size must be positive",
        )
    }
    /// Describes the baseline without creating a session.
    #[must_use]
    pub fn info(&self) -> EngineInfo {
        EngineInfo {
            name: "datafusion".into(),
            version: datafusion::DATAFUSION_VERSION.into(),
            configuration: BTreeMap::from([
                (
                    "target_partitions".into(),
                    self.target_partitions.to_string(),
                ),
                ("batch_size".into(), self.batch_size.to_string()),
                (
                    "collect_statistics".into(),
                    self.collect_statistics.to_string(),
                ),
            ]),
        }
    }
}
/// Metadata accepts future engine names and configuration keys.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct EngineInfo {
    pub name: String,
    pub version: String,
    pub configuration: BTreeMap<String, String>,
}
/// Small adapter boundary. Canonical validation remains owned by the harness.
#[allow(async_fn_in_trait)]
pub trait BenchmarkEngine: Sized {
    /// Prepares exactly one engine session for the semantic case.
    ///
    /// # Errors
    /// Rejects unsupported inputs/configuration or engine setup failures.
    async fn prepare(
        root: &Path,
        manifest: &DatasetManifest,
        case: &BenchmarkCase,
        config: &EngineConfig,
    ) -> Result<Self>;
    /// Executes and collects the prepared query.
    ///
    /// # Errors
    /// Returns query planning or execution failures.
    async fn execute(&self) -> Result<Vec<RecordBatch>>;
    fn info(&self) -> EngineInfo;
}
/// A single session belongs to a single child execution.
pub struct DataFusionEngine {
    ctx: SessionContext,
    sql: String,
    config: EngineConfig,
}
fn is_lineitem(path: &str) -> bool {
    let p = Path::new(path);
    p.components().any(|c| c.as_os_str() == "lineitem")
        || p.file_stem().is_some_and(|s| s == "lineitem")
}
impl BenchmarkEngine for DataFusionEngine {
    async fn prepare(
        root: &Path,
        m: &DatasetManifest,
        case: &BenchmarkCase,
        config: &EngineConfig,
    ) -> Result<Self> {
        case.validate()?;
        config.validate()?;
        m.validate()?;
        require(
            m.required_schema == case.dataset.required_schema,
            "workload requires a different dataset schema",
        )?;
        let cfg = SessionConfig::new()
            .with_target_partitions(config.target_partitions)
            .with_batch_size(config.batch_size)
            .with_collect_statistics(config.collect_statistics);
        let ctx = SessionContext::new_with_config(cfg);
        // Only manifest-listed files participate. Extra directory files cannot silently change input.
        let files = m
            .files
            .iter()
            .filter(|f| case.query != Query::TpchQ1 || is_lineitem(&f.relative_path))
            .map(|f| root.join(&f.relative_path).to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        require(!files.is_empty(), "no query input files")?;
        let table = ctx
            .read_parquet(files, ParquetReadOptions::default())
            .await
            .map_err(Error::EngineSetup)?
            .into_view();
        ctx.register_table("input", table)
            .map_err(Error::EngineSetup)?;
        let sql = match case.query {
            Query::Scan => "SELECT row_id, key, value_i64 FROM input".into(),
            Query::ScanFilterProject { threshold } => format!("SELECT key, value_i64 FROM input WHERE secondary < {threshold}"),
            Query::GroupBySum => "SELECT key, CAST(SUM(value_i64) AS BIGINT), COUNT(*) FROM input GROUP BY key".into(),
            Query::TpchQ1 => "WITH t AS (SELECT l_returnflag, l_linestatus, CAST(l_quantity AS DOUBLE) q, CAST(l_extendedprice AS DOUBLE) p, CAST(l_discount AS DOUBLE) d, CAST(l_tax AS DOUBLE) t FROM input WHERE l_shipdate <= DATE '1998-09-02') SELECT l_returnflag, l_linestatus, SUM(q), SUM(p), SUM(p*(1-d)), SUM(p*(1-d)*(1+t)), AVG(q), AVG(p), AVG(d), COUNT(*) FROM t GROUP BY l_returnflag,l_linestatus".into(),
        };
        Ok(Self {
            ctx,
            sql,
            config: config.clone(),
        })
    }
    async fn execute(&self) -> Result<Vec<RecordBatch>> {
        self.ctx
            .sql(&self.sql)
            .await
            .map_err(Error::Query)?
            .collect()
            .await
            .map_err(Error::Query)
    }
    fn info(&self) -> EngineInfo {
        self.config.info()
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Q1Row {
    key: Vec<String>,
    values: Vec<f64>,
}
#[derive(Debug, Serialize)]
enum Canonical {
    Integer(Vec<Vec<i128>>),
    Q1(Vec<Q1Row>),
}
impl Canonical {
    fn len(&self) -> usize {
        match self {
            Self::Integer(v) => v.len(),
            Self::Q1(v) => v.len(),
        }
    }
    fn fingerprint(&self) -> Result<String> {
        Ok(blake3::hash(&serde_json::to_vec(self)?)
            .to_hex()
            .to_string())
    }
}
fn integer(a: &dyn Array, row: usize) -> Result<i128> {
    require(!a.is_null(row), "unexpected null output")?;
    if let Some(a) = a.as_any().downcast_ref::<UInt64Array>() {
        return Ok(i128::from(a.value(row)));
    }
    if let Some(a) = a.as_any().downcast_ref::<Int64Array>() {
        return Ok(i128::from(a.value(row)));
    }
    Err(Error::Validation(format!(
        "unexpected integer output type: {:?}",
        a.data_type()
    )))
}
fn canonical(case: &BenchmarkCase, batches: &[RecordBatch]) -> Result<Canonical> {
    if case.query == Query::TpchQ1 {
        let mut rows = Vec::new();
        for b in batches {
            require(b.num_columns() == 10, "Q1 output width mismatch")?;
            let nums = (2..10)
                .map(|i| cast(b.column(i), &DataType::Float64))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            for r in 0..b.num_rows() {
                let key = (0..2)
                    .map(|i| array_value_to_string(b.column(i).as_ref(), r))
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                let values = nums
                    .iter()
                    .map(|a| {
                        require(!a.is_null(r), "unexpected Q1 null")?;
                        let v = a
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .ok_or_else(|| Error::Validation("Q1 numeric cast".into()))?
                            .value(r);
                        require(v.is_finite(), "non-finite Q1 output")?;
                        Ok(v)
                    })
                    .collect::<Result<Vec<_>>>()?;
                rows.push(Q1Row { key, values });
            }
        }
        rows.sort_by(|a, b| a.key.cmp(&b.key));
        return Ok(Canonical::Q1(rows));
    }
    let width = if matches!(case.query, Query::ScanFilterProject { .. }) {
        2
    } else {
        3
    };
    let mut rows = Vec::new();
    for b in batches {
        require(b.num_columns() == width, "output width mismatch")?;
        for r in 0..b.num_rows() {
            rows.push(
                b.columns()
                    .iter()
                    .map(|a| integer(a.as_ref(), r))
                    .collect::<Result<Vec<_>>>()?,
            );
        }
    }
    rows.sort();
    Ok(Canonical::Integer(rows))
}
fn expected_synthetic(case: &BenchmarkCase, c: &dataset::SyntheticConfig) -> Canonical {
    let mut rows = Vec::new();
    let mut groups: BTreeMap<u64, (i128, i128)> = BTreeMap::new();
    for id in 0..c.rows {
        let (key, second, value) = dataset::row(c, id);
        match case.query {
            Query::Scan => rows.push(vec![i128::from(id), i128::from(key), i128::from(value)]),
            Query::ScanFilterProject { threshold } => {
                if second < threshold {
                    rows.push(vec![i128::from(key), i128::from(value)]);
                }
            }
            Query::GroupBySum => {
                let group = groups.entry(key).or_default();
                group.0 += i128::from(value);
                group.1 += 1;
            }
            Query::TpchQ1 => unreachable!("schema checked before oracle"),
        }
    }
    if case.query == Query::GroupBySum {
        rows = groups
            .into_iter()
            .map(|(k, (s, n))| vec![i128::from(k), s, n])
            .collect();
    }
    rows.sort();
    Canonical::Integer(rows)
}
fn expected_q1(root: &Path, m: &DatasetManifest) -> Result<Canonical> {
    let mut groups: BTreeMap<Vec<String>, [f64; 8]> = BTreeMap::new();
    for file in m.files.iter().filter(|f| is_lineitem(&f.relative_path)) {
        let reader =
            ParquetRecordBatchReaderBuilder::try_new(File::open(root.join(&file.relative_path))?)?
                .build()?;
        for b in reader {
            let b = b?;
            let column = |name: &str| {
                b.column_by_name(name)
                    .ok_or_else(|| Error::Invalid(format!("missing lineitem column {name}")))
            };
            let nums = ["l_quantity", "l_extendedprice", "l_discount", "l_tax"]
                .iter()
                .map(|n| Ok(cast(column(n)?, &DataType::Float64)?))
                .collect::<Result<Vec<_>>>()?;
            for r in 0..b.num_rows() {
                let date = array_value_to_string(column("l_shipdate")?.as_ref(), r)?;
                if date.as_str() > "1998-09-02" {
                    continue;
                }
                let key = ["l_returnflag", "l_linestatus"]
                    .iter()
                    .map(|n| Ok(array_value_to_string(column(n)?.as_ref(), r)?))
                    .collect::<Result<Vec<_>>>()?;
                let mut n = [0.; 4];
                for (i, a) in nums.iter().enumerate() {
                    require(!a.is_null(r), "TPC-H reference null")?;
                    n[i] = a
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .ok_or_else(|| Error::Validation("reference cast".into()))?
                        .value(r);
                }
                let [quantity, price, discount, tax] = n;
                let g = groups.entry(key).or_insert([0.; 8]);
                for (sum, v) in g.iter_mut().zip([
                    quantity,
                    price,
                    price * (1. - discount),
                    price * (1. - discount) * (1. + tax),
                    quantity,
                    price,
                    discount,
                    1.,
                ]) {
                    *sum += v;
                }
            }
        }
    }
    Ok(Canonical::Q1(
        groups
            .into_iter()
            .map(|(key, mut values)| {
                for i in 4..7 {
                    values[i] /= values[7];
                }
                Q1Row {
                    key,
                    values: values.to_vec(),
                }
            })
            .collect(),
    ))
}
fn validate_equal(actual: &Canonical, expected: &Canonical) -> Result<()> {
    let equal = match (actual, expected) {
        (Canonical::Integer(a), Canonical::Integer(e)) => a == e,
        (Canonical::Q1(a), Canonical::Q1(e)) => {
            a.len() == e.len()
                && a.iter().zip(e).all(|(a, e)| {
                    a.key == e.key
                        && a.values
                            .iter()
                            .zip(&e.values)
                            .enumerate()
                            .all(|(i, (a, e))| {
                                a.is_finite()
                                    && e.is_finite()
                                    && if i == 7 {
                                        a.to_bits() == e.to_bits()
                                    } else {
                                        (a - e).abs() <= 1e-6 + 1e-9 * e.abs()
                                    }
                            })
                })
        }
        _ => false,
    };
    if equal {
        Ok(())
    } else {
        Err(Error::Validation(
            "canonical output differs from independent reference".into(),
        ))
    }
}
/// Child protocol v1. Timings exclude oracle/canonicalization, which are mandatory afterward.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionOutcome {
    pub schema_version: u32,
    pub validated: bool,
    pub engine: EngineInfo,
    pub setup_time_ns: u64,
    pub query_time_ns: u64,
    pub rows_output: u64,
    pub result_fingerprint: String,
}
/// Executes the baseline and checks canonical output against an independent oracle.
///
/// # Errors
/// Returns an error for invalid configuration/schema, missing input, engine setup/query failures or incorrect output.
pub async fn execute_validated(
    root: &Path,
    m: &DatasetManifest,
    case: &BenchmarkCase,
    config: &EngineConfig,
) -> Result<ExecutionOutcome> {
    execute_validated_with::<DataFusionEngine>(root, m, case, config).await
}

/// Applies the same timing and correctness contract to any baseline adapter.
///
/// # Errors
/// Returns configuration, setup, query, canonicalization or required validation failures.
pub async fn execute_validated_with<E: BenchmarkEngine>(
    root: &Path,
    m: &DatasetManifest,
    case: &BenchmarkCase,
    config: &EngineConfig,
) -> Result<ExecutionOutcome> {
    case.validate()?;
    m.validate()?;
    require(
        case.dataset.required_schema == m.required_schema,
        "dataset/spec mismatch",
    )?;
    let setup = Instant::now();
    let engine = E::prepare(root, m, case, config).await?;
    let setup_time_ns = u64::try_from(setup.elapsed().as_nanos()).unwrap_or(u64::MAX);
    let query = Instant::now();
    let batches = engine.execute().await?;
    let query_time_ns = u64::try_from(query.elapsed().as_nanos()).unwrap_or(u64::MAX);
    let actual = canonical(case, &batches)?;
    let expected = if let Some(c) = &m.synthetic {
        expected_synthetic(case, c)
    } else {
        expected_q1(root, m)?
    };
    validate_equal(&actual, &expected)?;
    Ok(ExecutionOutcome {
        schema_version: 1,
        validated: true,
        engine: engine.info(),
        setup_time_ns,
        query_time_ns,
        rows_output: actual.len() as u64,
        // Q1 uses the reference fingerprint: floating reduction order is tolerance-checked, never binary-compared.
        result_fingerprint: expected.fingerprint()?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn q1_tolerance_requires_exact_counts_and_finite_values() {
        let expected = Canonical::Q1(vec![Q1Row {
            key: vec!["A".into(), "F".into()],
            values: vec![1e9, 1., 1., 1., 1., 1., 1., 6e9],
        }]);
        let make = |sum, count| {
            Canonical::Q1(vec![Q1Row {
                key: vec!["A".into(), "F".into()],
                values: vec![sum, 1., 1., 1., 1., 1., 1., count],
            }])
        };
        assert!(validate_equal(&make(1e9 + 0.1, 6e9), &expected).is_ok());
        assert!(validate_equal(&make(1e9 + 10., 6e9), &expected).is_err());
        assert!(validate_equal(&make(1e9, 6e9 + 1.), &expected).is_err());
        assert!(validate_equal(&make(f64::NAN, 6e9), &expected).is_err());
    }
}
