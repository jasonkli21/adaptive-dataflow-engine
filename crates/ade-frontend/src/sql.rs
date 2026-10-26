//! Public `DataFusion` logical planning only; no `DataFusion` physical execution.
use crate::{Catalog, Error, Result, require};
use ade_physical::{Operator as Op, PlanV1, Selection, Target};
use datafusion::{
    common::{Column, ScalarValue, config::Dialect},
    datasource::empty::EmptyTable,
    logical_expr::{Expr, JoinType, LogicalPlan, Operator},
    prelude::SessionContext,
    sql::{parser::Statement, sqlparser::ast::Statement as AstStatement},
};
use std::sync::Arc;

impl Catalog {
    /// Bind a single SELECT and translate its resolved logical operators.
    /// # Errors
    /// Unsupported syntax, expressions, types and target combinations fail closed.
    pub async fn plan_sql(&self, sql: &str, target: Target) -> Result<PlanV1> {
        require(
            !sql.is_empty() && sql.len() <= 16 * 1024,
            "SQL byte cap 16KiB",
        )?;
        let ctx = SessionContext::new();
        // Schema-only registration is the sole catalog entrypoint. Remove default
        // table functions before binding; URL table auto-discovery stays disabled.
        let table_functions = ctx
            .state()
            .table_functions()
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for name in table_functions {
            ctx.deregister_udtf(&name);
        }
        for b in &self.native.bindings {
            ctx.register_table(
                &b.source.table,
                Arc::new(EmptyTable::new(ade_exec::physical::arrow_schema(
                    &b.source.schema,
                ))),
            )?;
        }
        let state = ctx.state();
        let statement = state.sql_to_statement(sql, &Dialect::Generic)?;
        match &statement {
            Statement::Statement(s) => match s.as_ref() {
                AstStatement::Query(q) => require(q.with.is_none(), "CTEs unsupported")?,
                _ => {
                    return Err(unsupported(
                        "only SELECT queries are admitted; DDL/DML refused",
                    ));
                }
            },
            _ => return Err(unsupported("external tables/COPY/DDL refused")),
        }
        let logical = state.statement_to_plan(statement).await?;
        let mut tables = vec![];
        gather_tables(&logical, &mut tables, 0)?;
        tables.sort();
        tables.dedup();
        let refs = tables.iter().map(String::as_str).collect::<Vec<_>>();
        let (mut plan, _) = self.plan(target, &refs)?;
        translate(&logical, &mut plan, 0)?;
        plan.validate()?;
        Ok(plan)
    }
}
fn unsupported(s: impl Into<String>) -> Error {
    Error::Unsupported(s.into())
}
fn gather_tables(p: &LogicalPlan, tables: &mut Vec<String>, depth: usize) -> Result<()> {
    require(depth < 32, "logical depth cap 32")?;
    if let LogicalPlan::TableScan(s) = p {
        tables.push(s.table_name.to_string());
    }
    for child in p.inputs() {
        gather_tables(child, tables, depth + 1)?;
    }
    Ok(())
}
fn column(e: &Expr, schema: &datafusion::common::DFSchema) -> Result<usize> {
    let Expr::Column(c) = e else {
        return Err(unsupported(format!(
            "only resolved columns supported, got {e}"
        )));
    };
    Ok(schema.index_of_column(c)?)
}
fn unalias(e: &Expr) -> &Expr {
    if let Expr::Alias(a) = e {
        unalias(&a.expr)
    } else {
        e
    }
}
fn literal(e: &Expr) -> Result<u64> {
    match e {
        Expr::Literal(ScalarValue::UInt64(Some(n)), _) => Ok(*n),
        Expr::Literal(ScalarValue::Int64(Some(n)), _) => {
            u64::try_from(*n).map_err(|_| unsupported("literal must be nonnegative UInt64"))
        }
        _ => Err(unsupported(
            "literal must be an exact nonnegative integer, no casts/expressions",
        )),
    }
}
fn translate(p: &LogicalPlan, plan: &mut PlanV1, depth: usize) -> Result<u32> {
    require(depth < 32, "logical depth cap 32")?;
    let child = |p: &LogicalPlan, plan: &mut PlanV1| translate(p, plan, depth + 1);
    match p {
        LogicalPlan::TableScan(s) => {
            require(
                s.projection.is_none() && s.filters.is_empty() && s.fetch.is_none(),
                "pushed scan operations unsupported",
            )?;
            let id = plan
                .sources
                .iter()
                .position(|source| source.table == s.table_name.to_string())
                .ok_or_else(|| unsupported("unregistered table"))?;
            plan.push(Op::Scan {
                source: u32::try_from(id).map_err(|_| unsupported("source ID overflow"))?,
            })
            .map_err(Into::into)
        }
        LogicalPlan::SubqueryAlias(s) => child(&s.input, plan),
        LogicalPlan::Filter(f) => {
            let Expr::BinaryExpr(b) = &f.predicate else {
                return Err(unsupported("filter requires column < integer"));
            };
            require(
                b.op == Operator::Lt,
                "only UInt64 < integer filter supported",
            )?;
            let col = column(&b.left, f.input.schema())?;
            require(
                f.input.schema().field(col).name() == "secondary",
                "SQL filter requires secondary",
            )?;
            let threshold = literal(&b.right)?;
            let input = child(&f.input, plan)?;
            plan.push(Op::Filter {
                input,
                column: col,
                less_than: threshold,
            })
            .map_err(Into::into)
        }
        LogicalPlan::Projection(proj) => projection(proj, plan, depth),
        LogicalPlan::Aggregate(a) => aggregate(a, plan, depth),
        LogicalPlan::Sort(s) => sort(s, plan, depth),
        LogicalPlan::Limit(l) => {
            require(
                l.skip.as_deref().is_none_or(|e| literal(e).ok() == Some(0)),
                "OFFSET unsupported",
            )?;
            let rows = literal(
                l.fetch
                    .as_deref()
                    .ok_or_else(|| unsupported("LIMIT needs count"))?,
            )?;
            let input = child(&l.input, plan)?;
            plan.push(Op::Limit { input, rows }).map_err(Into::into)
        }
        LogicalPlan::Join(j) => join(j, plan, depth),
        _ => Err(unsupported(format!(
            "logical operator {} is outside physical v1",
            p.display()
        ))),
    }
}

fn projection(
    proj: &datafusion::logical_expr::logical_plan::Projection,
    plan: &mut PlanV1,
    depth: usize,
) -> Result<u32> {
    let child = |p: &LogicalPlan, plan: &mut PlanV1| translate(p, plan, depth + 1);

    // A join's logical schema contains both source inventories; native output
    // retains only key/left_value/right_value. Map resolved column ordinals.
    let input = child(&proj.input, plan)?;
    let join = matches!(plan.nodes[input as usize].operator, Op::InnerJoin { .. });
    let mut columns = vec![];
    for (i, e) in proj.expr.iter().enumerate() {
        let original = column(unalias(e), proj.input.schema())?;
        let native = if join {
            let LogicalPlan::Join(j) = proj.input.as_ref() else {
                return Err(unsupported("post-join alias/DAG unsupported"));
            };
            let left_len = j.left.schema().fields().len();
            let name = proj.input.schema().field(original).name();
            match (original < left_len, name.as_str()) {
                (_, "key") => 0,
                (true, "value_i64") => 1,
                (false, "value_i64") => 2,
                _ => {
                    return Err(unsupported(
                        "join projection supports only key and left/right values",
                    ));
                }
            }
        } else {
            original
        };
        columns.push(Selection {
            column: native,
            alias: proj.schema.field(i).name().clone(),
        });
    }
    // Match resolved `DataFusion` output types as well as exact alias/order.
    for (c, f) in columns.iter().zip(proj.schema.fields()) {
        require(
            ade_exec::physical::arrow_type(plan.nodes[input as usize].schema[c.column].data_type)
                == *f.data_type(),
            "resolved SQL/native output type mismatch",
        )?;
    }
    plan.push(Op::Project { input, columns })
        .map_err(Into::into)
}

fn aggregate(
    a: &datafusion::logical_expr::logical_plan::Aggregate,
    plan: &mut PlanV1,
    depth: usize,
) -> Result<u32> {
    let child = |p: &LogicalPlan, plan: &mut PlanV1| translate(p, plan, depth + 1);

    require(
        a.group_expr.len() == 1 && a.aggr_expr.len() == 2,
        "requires one key, SUM(value_i64), COUNT(*)",
    )?;
    let key = column(&a.group_expr[0], a.input.schema())?;
    let mut kinds = vec![];
    let mut value = None;
    for e in &a.aggr_expr {
        let Expr::AggregateFunction(f) = unalias(e) else {
            return Err(unsupported("aggregate expression unsupported"));
        };
        let params = &f.params;
        require(
            !params.distinct
                && params.filter.is_none()
                && params.order_by.is_empty()
                && params.null_treatment.is_none(),
            "DISTINCT/aggregate filters/order/null treatment unsupported",
        )?;
        match f.func.name() {
            "sum" => {
                require(params.args.len() == 1, "SUM needs value column")?;
                let v = column(&params.args[0], a.input.schema())?;
                require(
                    a.input.schema().field(v).name() == "value_i64",
                    "SUM requires value_i64",
                )?;
                value = Some(v);
                kinds.push(1);
            }
            "count" => {
                require(
                    params.args.len() == 1
                        && matches!(
                            &params.args[0],
                            Expr::Literal(
                                ScalarValue::Int64(Some(1)) | ScalarValue::UInt8(Some(1)),
                                _
                            )
                        ),
                    "only COUNT(*) supported",
                )?;
                kinds.push(2);
            }
            _ => return Err(unsupported("only SUM and COUNT supported")),
        }
    }
    require(
        kinds.contains(&1) && kinds.contains(&2),
        "requires SUM and COUNT exactly once",
    )?;
    let input = child(&a.input, plan)?;
    let group = plan.push(Op::GroupSumCount {
        input,
        key,
        value: value.ok_or_else(|| unsupported("missing sum"))?,
    })?;
    // Normalize aggregate logical slot ordering/names for outer projections.
    let columns = std::iter::once(0)
        .chain(kinds)
        .enumerate()
        .map(|(i, col)| Selection {
            column: col,
            alias: a.schema.field(i).name().clone(),
        })
        .collect();
    // Avoid an extra node for the canonical native names (distributed shape).
    let canonical = a
        .schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .eq(["key", "sum", "count"]);
    if canonical {
        Ok(group)
    } else {
        plan.push(Op::Project {
            input: group,
            columns,
        })
        .map_err(Into::into)
    }
}

fn sort(
    s: &datafusion::logical_expr::logical_plan::Sort,
    plan: &mut PlanV1,
    depth: usize,
) -> Result<u32> {
    let child = |p: &LogicalPlan, plan: &mut PlanV1| translate(p, plan, depth + 1);

    require(
        s.expr.len() == 2 && s.expr.iter().all(|e| e.asc),
        "sort requires key ASC,row_id ASC",
    )?;
    let key = column(&s.expr[0].expr, s.input.schema())?;
    let row_id = column(&s.expr[1].expr, s.input.schema())?;
    let value = s
        .input
        .schema()
        .index_of_column(&Column::from_name("value_i64"))?;
    let input = child(&s.input, plan)?;
    let sorted = plan.push(Op::Sort {
        input,
        key,
        row_id,
        value,
    })?;
    // The native sort emits `key,row_id`,value; preserve exact projected order.
    require(
        plan.nodes[input as usize].schema.len() == 3,
        "sort SELECT must contain exactly key,row_id,value_i64",
    )?;
    let columns = plan.nodes[input as usize]
        .schema
        .iter()
        .map(|f| {
            let column = plan.nodes[sorted as usize]
                .schema
                .iter()
                .position(|v| v.name == f.name)
                .ok_or_else(|| unsupported("sort output column"))?;
            Ok(Selection {
                column,
                alias: f.name.clone(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let output = plan.push(Op::Project {
        input: sorted,
        columns,
    })?;
    if let Some(rows) = s.fetch {
        plan.push(Op::Limit {
            input: output,
            rows: rows as u64,
        })
        .map_err(Into::into)
    } else {
        Ok(output)
    }
}

fn join(
    j: &datafusion::logical_expr::logical_plan::Join,
    plan: &mut PlanV1,
    depth: usize,
) -> Result<u32> {
    let child = |p: &LogicalPlan, plan: &mut PlanV1| translate(p, plan, depth + 1);

    require(
        j.join_type == JoinType::Inner
            && !j.null_aware
            && j.join_constraint == datafusion::common::JoinConstraint::On,
        "only INNER JOIN ON supported",
    )?;
    // The public unoptimized SQL planner retains ON equality as a join
    // filter; already-extracted typed equijoin keys are also supported.
    let (left_key, right_key) = if j.on.len() == 1 && j.filter.is_none() {
        (
            column(&j.on[0].0, j.left.schema())?,
            column(&j.on[0].1, j.right.schema())?,
        )
    } else {
        require(j.on.is_empty(), "additional join predicates unsupported")?;
        let Some(Expr::BinaryExpr(eq)) = &j.filter else {
            return Err(unsupported("one column equality required in JOIN ON"));
        };
        require(eq.op == Operator::Eq, "join predicate must be equality")?;
        let first = column(&eq.left, &j.schema)?;
        let second = column(&eq.right, &j.schema)?;
        let left_width = j.left.schema().fields().len();
        if first < left_width && second >= left_width {
            (first, second - left_width)
        } else if second < left_width && first >= left_width {
            (second, first - left_width)
        } else {
            return Err(unsupported("join equality must relate opposite sources"));
        }
    };
    let left_value = j
        .left
        .schema()
        .index_of_column(&Column::from_name("value_i64"))?;
    let right_value = j
        .right
        .schema()
        .index_of_column(&Column::from_name("value_i64"))?;
    let left = child(&j.left, plan)?;
    let right = child(&j.right, plan)?;
    plan.push(Op::InnerJoin {
        left,
        right,
        left_key,
        right_key,
        left_value,
        right_value,
    })
    .map_err(Into::into)
}
