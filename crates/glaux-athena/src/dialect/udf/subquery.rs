//! Correlated scalar subqueries that are not written as an aggregate.
//!
//! Trino runs `SELECT (SELECT c.id FROM customers c WHERE c.id =
//! o.customer_id) FROM orders o`: the subquery yields one value per outer
//! row, `NULL` when nothing matches, and `SUBQUERY_MULTIPLE_ROWS` at run
//! time only when *some outer row* really does match more than one inner
//! row.
//!
//! DataFusion refuses the shape at planning instead — `Correlated scalar
//! subquery must be aggregated to return at most one row` — because its
//! decorrelation turns the subquery into a left join and needs an aggregate
//! to collapse the right side. The expression is therefore rewritten into
//! two aggregated subqueries over the same plan plus a guard *outside*
//! them:
//!
//! ```text
//! trino_scalar_subquery(
//!     (SELECT trino_single_value(x) FROM …),
//!     (SELECT trino_group_rows(x)   FROM …))
//! ```
//!
//! The row count is checked outside the subqueries, not inside the
//! aggregate, because DataFusion's decorrelation groups the *inner*
//! relation by the correlation columns and left-joins the outer one to it:
//! a group of several rows that no outer row matches must not raise, and it
//! would if the aggregate refused by itself. The guard sits in the outer
//! projection (or filter), so it sees one row per *outer* row — Trino's
//! rule — and an outer row with no match arrives as a `NULL` count and
//! stays `NULL`.
//!
//! Aggregated subqueries (`(SELECT count(*) …)`) are left alone.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, UInt64Array};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Column, DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, ColumnarValue, Expr, LogicalPlan,
    LogicalPlanBuilder, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Subquery,
    TypeSignature, Volatility,
};

use super::data_error;

/// The aggregate UDFs of this module.
pub fn aggregate_udfs() -> Vec<AggregateUDF> {
    vec![
        AggregateUDF::new_from_impl(TrinoGroupAgg::new(GroupAgg::Value)),
        AggregateUDF::new_from_impl(TrinoGroupAgg::new(GroupAgg::Rows)),
    ]
}

/// The scalar UDFs of this module.
pub fn all() -> Vec<ScalarUDF> {
    vec![ScalarUDF::new_from_impl(TrinoScalarSubquery::new())]
}

/// Wrap a correlated scalar subquery that is not already an aggregate so
/// DataFusion's decorrelation accepts it. Returns `None` when the subquery
/// needs no help (uncorrelated, or already aggregated) — see the module
/// docs.
pub fn aggregate_correlated_scalar_subquery(subquery: &Subquery) -> Result<Option<Expr>> {
    if subquery.outer_ref_columns.is_empty() || !needs_wrapping(&subquery.subquery) {
        return Ok(None);
    }
    let plan = subquery.subquery.as_ref();
    // A scalar subquery has exactly one output column; DataFusion's own
    // invariant check reports the multi-column case by name, so leave it.
    if plan.schema().fields().len() != 1 {
        return Ok(None);
    }
    let (qualifier, field) = plan.schema().qualified_field(0);
    let column = Expr::Column(Column::from((qualifier, field)));
    let aggregated = |which: GroupAgg| -> Result<Expr> {
        let inner = LogicalPlanBuilder::from(plan.clone())
            .aggregate(Vec::<Expr>::new(), vec![group_agg(which, column.clone())])?
            .build()?;
        Ok(Expr::ScalarSubquery(Subquery {
            subquery: Arc::new(inner),
            outer_ref_columns: subquery.outer_ref_columns.clone(),
            spans: subquery.spans.clone(),
        }))
    };
    Ok(Some(Expr::ScalarFunction(
        datafusion::logical_expr::expr::ScalarFunction::new_udf(
            Arc::new(ScalarUDF::new_from_impl(TrinoScalarSubquery::new())),
            vec![aggregated(GroupAgg::Value)?, aggregated(GroupAgg::Rows)?],
        ),
    )))
}

fn group_agg(which: GroupAgg, arg: Expr) -> Expr {
    Expr::AggregateFunction(datafusion::logical_expr::expr::AggregateFunction::new_udf(
        Arc::new(AggregateUDF::new_from_impl(TrinoGroupAgg::new(which))),
        vec![arg],
        false,
        None,
        vec![],
        None,
    ))
}

/// The same test DataFusion's plan invariant applies (`check_subquery_expr`
/// in `datafusion-expr`): projections are transparent and an `Aggregate`
/// (or a `Filter` over one — a `HAVING`) is accepted.
fn needs_wrapping(plan: &LogicalPlan) -> bool {
    !matches!(strip_projections(plan), LogicalPlan::Aggregate(_))
}

fn strip_projections(plan: &LogicalPlan) -> &LogicalPlan {
    match plan {
        LogicalPlan::Projection(projection) => strip_projections(&projection.input),
        LogicalPlan::Filter(filter) => match filter.input.as_ref() {
            input @ LogicalPlan::Aggregate(_) => input,
            _ => plan,
        },
        other => other,
    }
}

// ---------------------------------------------------------------------------
// The guard
// ---------------------------------------------------------------------------

/// `trino_scalar_subquery(value, rows)`: `value` when the subquery matched
/// at most one row, Trino's `SUBQUERY_MULTIPLE_ROWS` when it matched more.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TrinoScalarSubquery {
    signature: Signature,
}

impl Default for TrinoScalarSubquery {
    fn default() -> Self {
        Self::new()
    }
}

impl TrinoScalarSubquery {
    /// New instance.
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for TrinoScalarSubquery {
    fn name(&self) -> &str {
        "trino_scalar_subquery"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let rows = args.number_rows;
        let counts = arrow::compute::cast(&args.args[1].to_array(rows)?, &DataType::UInt64)?;
        let counts = counts
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| {
                DataFusionError::Internal("trino_scalar_subquery: row count is not UInt64".into())
            })?;
        if (0..counts.len()).any(|i| !counts.is_null(i) && counts.value(i) > 1) {
            return Err(data_error(
                "SUBQUERY_MULTIPLE_ROWS",
                "Scalar sub-query has returned multiple rows",
            ));
        }
        Ok(args.args[0].clone())
    }
}

// ---------------------------------------------------------------------------
// The aggregates
// ---------------------------------------------------------------------------

/// Which half of the wrapper's aggregate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GroupAgg {
    /// The group's first value (all its rows hold the same one when the
    /// subquery is well-formed).
    Value,
    /// How many rows the group holds — rows, not non-NULL values: two rows
    /// whose value is `NULL` are still two rows to Trino.
    Rows,
}

impl GroupAgg {
    fn name(self) -> &'static str {
        match self {
            Self::Value => "trino_single_value",
            Self::Rows => "trino_group_rows",
        }
    }
}

/// `trino_single_value(x)` / `trino_group_rows(x)`: see [`GroupAgg`]. Both
/// are internal to the correlated-scalar-subquery rewrite; the registry
/// refuses the names in user SQL.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TrinoGroupAgg {
    signature: Signature,
    which: GroupAgg,
}

impl TrinoGroupAgg {
    /// New instance.
    pub fn new(which: GroupAgg) -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
            which,
        }
    }
}

impl AggregateUDFImpl for TrinoGroupAgg {
    fn name(&self) -> &str {
        self.which.name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(match self.which {
            GroupAgg::Value => arg_types[0].clone(),
            GroupAgg::Rows => DataType::UInt64,
        })
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(GroupAccumulator {
            which: self.which,
            value: match self.which {
                GroupAgg::Value => ScalarValue::try_from(args.return_field.data_type())?,
                GroupAgg::Rows => ScalarValue::Null,
            },
            rows: 0,
        }))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let value = match self.which {
            GroupAgg::Value => args.return_field.data_type().clone(),
            GroupAgg::Rows => DataType::Null,
        };
        Ok(vec![
            Arc::new(Field::new(format!("{}[value]", args.name), value, true)),
            Arc::new(Field::new(
                format!("{}[rows]", args.name),
                DataType::UInt64,
                false,
            )),
        ])
    }
}

/// The first value seen plus how many rows the group has held so far.
#[derive(Debug)]
struct GroupAccumulator {
    which: GroupAgg,
    value: ScalarValue,
    rows: u64,
}

fn row_counts(states: &ArrayRef) -> Result<&UInt64Array> {
    states
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| {
            DataFusionError::Internal("trino_group_rows: row-count state is not UInt64".into())
        })
}

impl Accumulator for GroupAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let column = &values[0];
        if column.is_empty() {
            return Ok(());
        }
        if self.rows == 0 && self.which == GroupAgg::Value {
            self.value = ScalarValue::try_from_array(column, 0)?;
        }
        self.rows += column.len() as u64;
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let counts = row_counts(&states[1])?;
        for i in 0..counts.len() {
            let rows = if counts.is_null(i) {
                0
            } else {
                counts.value(i)
            };
            if rows == 0 {
                continue;
            }
            if self.rows == 0 && self.which == GroupAgg::Value {
                self.value = ScalarValue::try_from_array(&states[0], i)?;
            }
            self.rows += rows;
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            self.value.clone(),
            ScalarValue::UInt64(Some(self.rows)),
        ])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(match self.which {
            GroupAgg::Value => self.value.clone(),
            GroupAgg::Rows => ScalarValue::UInt64(Some(self.rows)),
        })
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.value.size()
    }
}
