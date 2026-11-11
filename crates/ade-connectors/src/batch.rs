//! Native SQL execution and a local atomic sink for verified connector inputs.
#[cfg(any(feature = "flight", feature = "postgres"))]
pub(crate) mod capture;
pub(crate) mod parquet_stage;
pub mod sink;

use crate::{Context, Error, Result, Stage};
use ade_frontend::{
    Catalog, Target,
    execution::{Options, Output},
};

impl Stage {
    /// Compile a bounded SELECT over the physical table `input` and execute it
    /// through ADE. The catalog and plan never escape the stage lifetime.
    ///
    /// # Errors
    /// Refuses schema or row drift, inadmissible SQL and native execution errors.
    pub async fn query(&self, sql: &str, context: &Context) -> Result<Output> {
        let started = std::time::Instant::now();
        context.check()?;
        if !self.normalized {
            return Err(Error::Admission("Iceberg field ID normalization required"));
        }
        for file in &self.files {
            context.check()?;
            parquet_stage::open(file, false)?;
        }
        let mut catalog = Catalog::default();
        let admission = catalog.register_staged_input(
            ade_exec::physical::Binding {
                source: self.physical.clone(),
                files: self.files.clone(),
            },
            context.deadline(),
            &context.native,
        );
        context.check()?;
        admission.map_err(|_| Error::Native("staged source preflight"))?;
        let plan = catalog
            .plan_sql(sql, Target::Local)
            .await
            .map_err(|_| Error::Native("SQL binding"))?;
        context.check()?;
        let output = catalog.execute_staged_until(
            &plan,
            Options::default(),
            context.deadline(),
            context.native.clone(),
        );
        context.check()?;
        let mut output = output.map_err(|_| Error::Native("local execution"))?;
        output.diagnostics["connector"] = serde_json::json!({
            "entry": self.snapshot().entry,
            "snapshot": self.physical.logical,
            "staged_bytes": self.staged_bytes(),
            "download_bytes": self.download_bytes(),
            "final_stage_bytes": self.staged_bytes(),
            "normalization_write_bytes": self.normalization_write_bytes(),
            "peak_owned_stage_bytes": self.peak_owned_stage_bytes(),
            "stage_ns": self.stage_elapsed().as_nanos(),
            "native_query_ns": started.elapsed().as_nanos(),
            "native_input_bytes": self.physical.files.iter().map(|f| f.bytes).sum::<u64>(),
            "replay": self.snapshot().replay,
            "capabilities": self.snapshot().capabilities(),
            "splits": self.file_count(),
            "pushdown": "none",
        });
        Ok(output)
    }
}
