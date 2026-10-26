//! Typed builder; it never generates or reparses SQL.
use crate::{Catalog, Result, require};
use ade_physical::{Operator, PlanV1, Selection, Target};
#[derive(Debug, Clone)]
pub struct DataFrame {
    plan: PlanV1,
}
impl DataFrame {
    /// # Errors
    /// Rejects an unregistered table.
    pub fn scan(catalog: &Catalog, table: &str, target: Target) -> Result<Self> {
        let (mut plan, _) = catalog.plan(target, &[table])?;
        plan.push(Operator::Scan { source: 0 })?;
        Ok(Self { plan })
    }
    fn column(&self, name: &str) -> Result<usize> {
        self.plan.nodes[self.plan.output as usize]
            .schema
            .iter()
            .position(|f| f.name == name)
            .ok_or_else(|| crate::Error::Unsupported(format!("unknown column {name}")))
    }
    /// # Errors
    /// Requires a non-null `UInt64` column.
    pub fn filter_lt(mut self, column: &str, threshold: u64) -> Result<Self> {
        let column = self.column(column)?;
        self.plan.push(Operator::Filter {
            input: self.plan.output,
            column,
            less_than: threshold,
        })?;
        Ok(self)
    }
    /// # Errors
    /// Requires existing columns and unique bounded aliases.
    pub fn select(mut self, columns: &[(&str, &str)]) -> Result<Self> {
        let columns = columns
            .iter()
            .map(|(name, alias)| {
                Ok(Selection {
                    column: self.column(name)?,
                    alias: (*alias).into(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        self.plan.push(Operator::Project {
            input: self.plan.output,
            columns,
        })?;
        Ok(self)
    }
    /// # Errors
    /// Requires `UInt64` key and `Int64` value columns.
    pub fn group_sum_count(mut self, key: &str, value: &str) -> Result<Self> {
        let (key, value) = (self.column(key)?, self.column(value)?);
        self.plan.push(Operator::GroupSumCount {
            input: self.plan.output,
            key,
            value,
        })?;
        Ok(self)
    }
    /// # Errors
    /// Only the native `key,row_id` ascending sort is admitted.
    pub fn sort_key_row_id(mut self) -> Result<Self> {
        let (key, row_id, value) = (
            self.column("key")?,
            self.column("row_id")?,
            self.column("value_i64")?,
        );
        self.plan.push(Operator::Sort {
            input: self.plan.output,
            key,
            row_id,
            value,
        })?;
        Ok(self)
    }
    /// # Errors
    /// Refuses unordered LIMIT and values above the plan output cap.
    pub fn limit(mut self, rows: u64) -> Result<Self> {
        self.plan.push(Operator::Limit {
            input: self.plan.output,
            rows,
        })?;
        Ok(self)
    }
    /// # Errors
    /// Output bounds must fit the fixed v1 ceilings.
    pub fn output_cap(mut self, rows: u64, bytes: u64) -> Result<Self> {
        self.plan.max_output_rows = rows;
        self.plan.max_output_bytes = bytes;
        require(
            rows <= 1_000_000 && bytes <= 64 * 1024 * 1024 && bytes > 0,
            "output cap",
        )?;
        Ok(self)
    }
    /// # Errors
    /// Final target validation refuses unsupported native graph shapes.
    pub fn build(self) -> Result<PlanV1> {
        self.plan.validate()?;
        Ok(self.plan)
    }
}
