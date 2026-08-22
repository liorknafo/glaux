//! `trino_nullable(x)`: the identity function with a nullable result.
//!
//! The rewriter wraps every scalar subquery in it. A scalar subquery that
//! returns no rows is NULL in Trino (and DataFusion), but DataFusion types
//! the subquery's output with the nullability of its source column, so
//! `(SELECT x FROM (VALUES (1)) t(x) WHERE x = 2)` — a non-nullable
//! `VALUES` column — is declared non-nullable and fails at execution with
//! `Column ... is declared as non-nullable but contains null values`. The
//! wrapper's default field is nullable, which is what Trino reports.

use datafusion::common::Result;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};

/// The UDF.
pub fn all() -> Vec<ScalarUDF> {
    vec![ScalarUDF::new_from_impl(TrinoNullable::new())]
}

/// `trino_nullable(x)`: see the module docs.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TrinoNullable {
    signature: Signature,
}

impl Default for TrinoNullable {
    fn default() -> Self {
        Self::new()
    }
}

impl TrinoNullable {
    /// New instance.
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for TrinoNullable {
    fn name(&self) -> &str {
        "trino_nullable"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(
        &self,
        arg_types: &[arrow::datatypes::DataType],
    ) -> Result<arrow::datatypes::DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(args.args.remove(0))
    }
}
