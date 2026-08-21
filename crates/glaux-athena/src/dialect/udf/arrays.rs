//! Array element access with Trino's index rules.
//!
//! - `element_at(array, i)`: 1-based; negative `i` counts from the end; an
//!   index beyond either end is `NULL`; `i = 0` is an error.
//! - `array[i]` (subscript): 1-based; `i < 1` and `i > cardinality` are
//!   errors, never `NULL` (DataFusion's `array_element` returns `NULL`).

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Int64Array};
use arrow::compute::{cast, take};
use arrow::datatypes::{DataType, Field};
use datafusion::common::{Result, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};

use super::{data_error, int64_array};
use crate::dialect::udf::casts::trino_type_name;

/// The array UDFs.
pub fn all() -> Vec<ScalarUDF> {
    vec![
        ScalarUDF::new_from_impl(TrinoElementAt::new(false)),
        ScalarUDF::new_from_impl(TrinoElementAt::new(true)),
    ]
}

/// `trino_element_at(array, i)` (lenient) and `trino_subscript(array, i)`
/// (strict). See the module docs.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TrinoElementAt {
    signature: Signature,
    strict: bool,
}

impl TrinoElementAt {
    /// New instance; `strict` selects subscript semantics.
    pub fn new(strict: bool) -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
            strict,
        }
    }

    fn function(&self) -> &'static str {
        if self.strict {
            "array subscript"
        } else {
            "element_at"
        }
    }
}

/// The element type of an Arrow list type, or `None` for non-lists.
fn element_type(data_type: &DataType) -> Option<&DataType> {
    match data_type {
        DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => {
            Some(f.data_type())
        }
        _ => None,
    }
}

/// Resolve a Trino index against a list of `len` elements to a 0-based
/// offset, or `None` for "no element" (lenient mode only).
fn resolve_index(function: &str, index: i64, len: i64, strict: bool) -> Result<Option<i64>> {
    if index == 0 {
        return Err(data_error(
            "INVALID_FUNCTION_ARGUMENT",
            format!("{function}: SQL array indices start at 1"),
        ));
    }
    if strict {
        if index < 0 {
            return Err(data_error(
                "INVALID_FUNCTION_ARGUMENT",
                format!("{function}: array subscript is negative: {index}"),
            ));
        }
        if index > len {
            return Err(data_error(
                "INVALID_FUNCTION_ARGUMENT",
                format!(
                    "{function}: array subscript must be less than or equal to array length: \
                     {index} > {len}"
                ),
            ));
        }
        return Ok(Some(index - 1));
    }
    Ok(if index > 0 {
        (index <= len).then_some(index - 1)
    } else {
        (-index <= len).then_some(len + index)
    })
}

impl ScalarUDFImpl for TrinoElementAt {
    fn name(&self) -> &str {
        if self.strict {
            "trino_subscript"
        } else {
            "trino_element_at"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match element_type(&arg_types[0]) {
            Some(t) => Ok(t.clone()),
            None => plan_err!(
                "{}: expected an array, got {}",
                self.function(),
                trino_type_name(&arg_types[0])
            ),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let function = self.function();
        let rows = args.number_rows;
        let input = args.args[0].to_array(rows)?;
        let indexes = int64_array(function, "index", &args.args[1], rows)?;
        // Normalise every list flavour to `List<i32>` so one code path
        // handles offsets.
        let list = match input.data_type() {
            DataType::List(_) => input,
            DataType::LargeList(f) | DataType::FixedSizeList(f, _) => {
                let target =
                    DataType::List(Arc::new(Field::new("item", f.data_type().clone(), true)));
                cast(&input, &target)?
            }
            other => {
                return plan_err!(
                    "{function}: expected an array, got {}",
                    trino_type_name(other)
                );
            }
        };
        let list = list.as_list::<i32>();
        let offsets = list.value_offsets();
        let mut positions: Vec<Option<i64>> = Vec::with_capacity(rows);
        for i in 0..rows {
            if list.is_null(i) || indexes.is_null(i) {
                positions.push(None);
                continue;
            }
            let start = i64::from(offsets[i]);
            let len = i64::from(offsets[i + 1]) - start;
            positions.push(
                resolve_index(function, indexes.value(i), len, self.strict)?.map(|p| start + p),
            );
        }
        let taken: ArrayRef = take(list.values().as_ref(), &Int64Array::from(positions), None)?;
        Ok(ColumnarValue::Array(taken))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn element_at_indexes_from_both_ends_and_is_null_out_of_range() {
        assert_eq!(resolve_index("element_at", 1, 3, false).unwrap(), Some(0));
        assert_eq!(resolve_index("element_at", 3, 3, false).unwrap(), Some(2));
        assert_eq!(resolve_index("element_at", 4, 3, false).unwrap(), None);
        assert_eq!(resolve_index("element_at", -1, 3, false).unwrap(), Some(2));
        assert_eq!(resolve_index("element_at", -3, 3, false).unwrap(), Some(0));
        assert_eq!(resolve_index("element_at", -4, 3, false).unwrap(), None);
        let err = resolve_index("element_at", 0, 3, false)
            .unwrap_err()
            .to_string();
        assert!(err.contains("start at 1"), "{err}");
    }

    #[test]
    fn subscripts_error_out_of_range() {
        assert_eq!(
            resolve_index("array subscript", 2, 3, true).unwrap(),
            Some(1)
        );
        for (index, needle) in [(0, "start at 1"), (-1, "negative"), (4, "4 > 3")] {
            let err = resolve_index("array subscript", index, 3, true)
                .unwrap_err()
                .to_string();
            assert!(err.contains(needle), "{index}: {err}");
        }
    }
}
