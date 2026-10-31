//! Opt-in semantic compilation. Native execution consumes the optimizer's target.
use crate::{
    Catalog, Result,
    execution::{Options, Output},
};
use ade_logical::{Controls, Decision, LogicalPlanV1};
use ade_physical::PlanV1;
use std::time::Instant;

fn logical_error(e: ade_logical::Error) -> crate::Error {
    crate::Error::Unsupported(e.0)
}
/// Bound SQL or Rust `DataFrame` operators use this common extraction seam.
/// # Errors
/// Refuses invalid legacy templates and semantic plans.
pub fn from_bound(plan: &PlanV1) -> Result<LogicalPlanV1> {
    LogicalPlanV1::from_physical(plan).map_err(logical_error)
}
impl Catalog {
    /// Execute shared semantic admission and chosen native lowering with one deadline.
    /// # Errors
    /// Refuses inconsistent capabilities, source drift, deadlines and native failure.
    pub fn execute_unified_until(
        &self,
        plan: &LogicalPlanV1,
        controls: &Controls,
        options: Options,
        deadline: Instant,
    ) -> Result<(Output, Decision)> {
        crate::require(
            controls.workers == options.workers
                && controls.distributed_available == options.worker_binary.is_some(),
            "logical/native capability mismatch",
        )?;
        let (physical, decision) = plan.lower_batch(controls).map_err(logical_error)?;
        let output = self.execute_until(&physical, options, deadline)?;
        Ok((output, decision))
    }
}

impl Catalog {
    /// Bind SQL semantics then discard physical facts before common optimization.
    /// # Errors
    /// Refuses unsupported SQL using the established public `DataFusion` binder.
    pub async fn plan_sql_unified(
        &self,
        sql: &str,
        binding_target: ade_physical::Target,
    ) -> Result<LogicalPlanV1> {
        from_bound(&self.plan_sql(sql, binding_target).await?)
    }
}
