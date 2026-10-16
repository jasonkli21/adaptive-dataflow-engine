//! Engine dispatch, public adapters, and independent correctness oracles.
use crate::{
    Error, Result,
    dataset::{self, DatasetManifest},
    require,
    spec::{BenchmarkCase, Query},
};
use arrow::{
    array::{Array, Date32Array, Float64Array, Int64Array, UInt64Array},
    compute::cast,
    datatypes::DataType,
    record_batch::RecordBatch,
    util::display::array_value_to_string,
};
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fs::File, path::Path, time::Instant};

/// DataFusion-owned configuration, separate from workload semantics.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DataFusionConfig {
    pub target_partitions: usize,
    pub batch_size: usize,
    pub collect_statistics: bool,
}
impl Default for DataFusionConfig {
    fn default() -> Self {
        Self {
            target_partitions: 2,
            batch_size: 8192,
            collect_statistics: false,
        }
    }
}
impl DataFusionConfig {
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

/// Compatibility alias for callers that used the Phase 0 `DataFusion` baseline type.
pub type EngineConfig = DataFusionConfig;

/// Engine selection and raw adapter options carried unchanged through the child protocol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineRequest {
    pub name: String,
    pub options: Vec<String>,
}

/// The small, explicit set of adapters currently compiled into the benchmark binary.
#[derive(Debug)]
pub enum ResolvedEngine {
    DataFusion(DataFusionConfig),
    AdeLocal(crate::ade_local::AdeLocalConfig),
    AdeDistributed(crate::ade_distributed::AdeDistributedConfig),
}

impl EngineRequest {
    /// Resolves a known adapter and lets that adapter parse its own options.
    ///
    /// # Errors
    /// Returns an actionable unsupported-engine or adapter-configuration error.
    pub fn resolve(&self) -> Result<ResolvedEngine> {
        match self.name.as_str() {
            "datafusion" => Ok(ResolvedEngine::DataFusion(DataFusionEngine::parse_config(
                &self.options,
            )?)),
            "ade-distributed" => Ok(ResolvedEngine::AdeDistributed(
                crate::ade_distributed::AdeDistributedEngine::parse_config(&self.options)?,
            )),
            "ade-local" => Ok(ResolvedEngine::AdeLocal(
                crate::ade_local::AdeLocalEngine::parse_config(&self.options)?,
            )),
            name => Err(Error::UnsupportedEngine(name.into())),
        }
    }
}

impl ResolvedEngine {
    /// Returns generic result metadata for the selected adapter.
    #[must_use]
    pub fn info(&self) -> EngineInfo {
        match self {
            Self::DataFusion(config) => config.info(),
            Self::AdeLocal(config) => config.info(),
            Self::AdeDistributed(config) => config.info(),
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
    /// Configuration owned and interpreted by this engine adapter.
    type Config;

    /// Parses adapter-specific raw CLI options.
    ///
    /// # Errors
    /// Rejects options not supported by this adapter.
    fn parse_config(options: &[String]) -> Result<Self::Config>;

    /// Prepares exactly one engine session for the semantic case.
    ///
    /// # Errors
    /// Rejects unsupported inputs/configuration or engine setup failures.
    async fn prepare(
        root: &Path,
        manifest: &DatasetManifest,
        case: &BenchmarkCase,
        config: &Self::Config,
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
    type Config = DataFusionConfig;

    fn parse_config(options: &[String]) -> Result<Self::Config> {
        DataFusionConfig::from_options(options)
    }

    async fn prepare(
        root: &Path,
        m: &DatasetManifest,
        case: &BenchmarkCase,
        config: &Self::Config,
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
            Query::SortKeyRowid => "SELECT key, row_id, value_i64 FROM input ORDER BY key ASC, row_id ASC".into(),
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
    if case.query == Query::SortKeyRowid {
        require(
            rows.windows(2)
                .all(|w| (w[0][0], w[0][1]) <= (w[1][0], w[1][1])),
            "sort output is out of order",
        )?;
    } else {
        rows.sort();
    }
    Ok(Canonical::Integer(rows))
}
fn expected_synthetic(case: &BenchmarkCase, c: &dataset::SyntheticConfig) -> Canonical {
    let mut rows = Vec::new();
    let mut groups: BTreeMap<u64, (i128, i128)> = BTreeMap::new();
    for id in 0..c.rows {
        let (key, second, value) = dataset::row(c, id);
        match case.query {
            Query::SortKeyRowid => {
                rows.push(vec![i128::from(key), i128::from(id), i128::from(value)]);
            }
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
    let cutoff_days = q1_cutoff_date32_days()?;
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
            let ship_dates = cast(column("l_shipdate")?, &DataType::Date32)?;
            let ship_dates = ship_dates
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| Error::Validation("TPC-H ship date cast to Date32".into()))?;
            for r in 0..b.num_rows() {
                require(!ship_dates.is_null(r), "TPC-H reference null ship date")?;
                if ship_dates.value(r) > cutoff_days {
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

fn q1_cutoff_date32_days() -> Result<i32> {
    let cutoff = chrono::NaiveDate::from_ymd_opt(1998, 9, 2)
        .ok_or_else(|| Error::Invalid("invalid Q1 cutoff date".into()))?;
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
        .ok_or_else(|| Error::Invalid("invalid Unix epoch date".into()))?;
    i32::try_from(cutoff.signed_duration_since(epoch).num_days())
        .map_err(|_| Error::Invalid("Q1 cutoff date is outside Date32 range".into()))
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
    config: &DataFusionConfig,
) -> Result<ExecutionOutcome> {
    execute_validated_with::<DataFusionEngine>(root, m, case, config).await
}

/// Resolves and executes the explicitly selected engine adapter.
///
/// # Errors
/// Returns unsupported engine, adapter option, setup, query, canonicalization or validation errors.
pub async fn execute_validated_request(
    root: &Path,
    m: &DatasetManifest,
    case: &BenchmarkCase,
    request: &EngineRequest,
) -> Result<ExecutionOutcome> {
    let resolved = request.resolve()?;
    match resolved {
        ResolvedEngine::AdeDistributed(config) => {
            execute_validated_with::<crate::ade_distributed::AdeDistributedEngine>(
                root, m, case, &config,
            )
            .await
        }
        ResolvedEngine::AdeLocal(config) => {
            execute_validated_with::<crate::ade_local::AdeLocalEngine>(root, m, case, &config).await
        }
        ResolvedEngine::DataFusion(config) => {
            execute_validated_with::<DataFusionEngine>(root, m, case, &config).await
        }
    }
}

/// Applies the same timing and correctness contract to any baseline adapter.
///
/// # Errors
/// Returns configuration, setup, query, canonicalization or required validation failures.
pub async fn execute_validated_with<E: BenchmarkEngine>(
    root: &Path,
    m: &DatasetManifest,
    case: &BenchmarkCase,
    config: &E::Config,
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
    use arrow::{
        array::{ArrayRef, Int64Array, UInt64Array},
        datatypes::{Field, Schema},
    };
    use std::sync::Arc;

    #[derive(Debug)]
    struct StubConfig {
        label: String,
    }

    struct StubEngine {
        rows: u64,
        seed: u64,
        label: String,
    }

    #[allow(
        clippy::unused_async_trait_impl,
        reason = "The test adapter intentionally implements the async contract synchronously."
    )]
    impl BenchmarkEngine for StubEngine {
        type Config = StubConfig;

        fn parse_config(options: &[String]) -> Result<Self::Config> {
            let mut label = None;
            for option in options {
                let (key, value) = option
                    .split_once('=')
                    .ok_or_else(|| Error::Invalid("stub option must be key=value".into()))?;
                if key != "stub_label" || value.is_empty() {
                    return Err(Error::Invalid(format!("unsupported stub option: {key}")));
                }
                label = Some(value.to_owned());
            }
            Ok(StubConfig {
                label: label.unwrap_or_else(|| "default".into()),
            })
        }

        async fn prepare(
            _root: &Path,
            manifest: &DatasetManifest,
            case: &BenchmarkCase,
            config: &Self::Config,
        ) -> Result<Self> {
            require(case.query == Query::Scan, "stub supports scan only")?;
            let synthetic = manifest
                .synthetic
                .as_ref()
                .ok_or_else(|| Error::Invalid("stub requires synthetic input".into()))?;
            Ok(Self {
                rows: synthetic.rows,
                seed: synthetic.seed,
                label: config.label.clone(),
            })
        }

        async fn execute(&self) -> Result<Vec<RecordBatch>> {
            let config = dataset::SyntheticConfig {
                rows: self.rows,
                seed: self.seed,
                ..Default::default()
            };
            let mut ids = Vec::new();
            let mut keys = Vec::new();
            let mut values = Vec::new();
            for id in 0..self.rows {
                let (key, _, value) = dataset::row(&config, id);
                ids.push(id);
                keys.push(key);
                values.push(value);
            }
            let schema = Arc::new(Schema::new(vec![
                Field::new("row_id", DataType::UInt64, false),
                Field::new("key", DataType::UInt64, false),
                Field::new("value_i64", DataType::Int64, false),
            ]));
            let columns: Vec<ArrayRef> = vec![
                Arc::new(UInt64Array::from(ids)),
                Arc::new(UInt64Array::from(keys)),
                Arc::new(Int64Array::from(values)),
            ];
            Ok(vec![RecordBatch::try_new(schema, columns)?])
        }

        fn info(&self) -> EngineInfo {
            EngineInfo {
                name: "test-stub".into(),
                version: "1".into(),
                configuration: BTreeMap::from([("stub_label".into(), self.label.clone())]),
            }
        }
    }

    #[tokio::test]
    async fn generic_harness_accepts_adapter_owned_configuration() {
        let directory = tempfile::tempdir().unwrap();
        let config = dataset::SyntheticConfig {
            rows: 7,
            files: 1,
            ..Default::default()
        };
        let manifest = dataset::generate(&directory.path().join("data"), "stub", &config).unwrap();
        let case = BenchmarkCase::load(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../benchmarks/specs/scan.toml")
                .as_path(),
        )
        .unwrap();
        let options = ["stub_label=isolated-config".to_owned()];
        let stub_config = StubEngine::parse_config(&options).unwrap();
        let outcome = execute_validated_with::<StubEngine>(
            &directory.path().join("data"),
            &manifest,
            &case,
            &stub_config,
        )
        .await
        .unwrap();
        assert_eq!(outcome.engine.name, "test-stub");
        assert_eq!(
            outcome.engine.configuration["stub_label"],
            "isolated-config"
        );
        assert!(DataFusionConfig::from_options(&options).is_err());
        assert!(StubEngine::parse_config(&["target_partitions=4".into()]).is_err());
    }

    #[test]
    fn sort_validation_rejects_wrong_order_across_batches() {
        let case = BenchmarkCase::load(
            &Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../benchmarks/specs/sort-key-rowid.toml"),
        )
        .unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::UInt64, false),
            Field::new("row_id", DataType::UInt64, false),
            Field::new("value_i64", DataType::Int64, false),
        ]));
        let batch = |id| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(UInt64Array::from(vec![1])),
                    Arc::new(UInt64Array::from(vec![id])),
                    Arc::new(Int64Array::from(vec![0])),
                ],
            )
            .unwrap()
        };
        assert!(canonical(&case, &[batch(1), batch(2)]).is_ok());
        assert!(canonical(&case, &[batch(2), batch(1)]).is_err());
    }

    #[test]
    fn engine_dispatch_rejects_unsupported_names() {
        let error = EngineRequest {
            name: "missing-engine".into(),
            options: Vec::new(),
        }
        .resolve()
        .unwrap_err();
        assert!(matches!(error, Error::UnsupportedEngine(name) if name == "missing-engine"));
    }

    #[test]
    fn q1_cutoff_is_a_date32_day_value() {
        let cutoff = q1_cutoff_date32_days().unwrap();
        let date = chrono::NaiveDate::from_ymd_opt(1998, 9, 2).unwrap();
        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        assert_eq!(
            cutoff,
            i32::try_from(date.signed_duration_since(epoch).num_days()).unwrap()
        );
        assert_eq!(cutoff, 10_471);
    }

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
