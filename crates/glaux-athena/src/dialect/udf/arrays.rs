//! Array functions with Trino's semantics where DataFusion's namesakes
//! differ.
//!
//! - `element_at(array, i)`: 1-based; negative `i` counts from the end; an
//!   index beyond either end is `NULL`; `i = 0` is an error.
//! - `array[i]` (subscript): 1-based; `i < 1` and `i > cardinality` are
//!   errors, never `NULL` (DataFusion's `array_element` returns `NULL`).
//! - `reverse(x)`: reverses a string *or* an array (DataFusion's `reverse`
//!   is string-only and would stringify an array).
//! - `contains(array, x)` / `arrays_overlap(a, b)`: `NULL`, not `false`,
//!   when no match is found but a `NULL` element could have been one.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanBuilder, Int64Array, ListArray, StringBuilder,
};
use arrow::compute::{cast, take};
use arrow::datatypes::{DataType, Field};
use datafusion::common::{Result, ScalarValue, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};

use super::{data_error, int64_array, string_array, type_mismatch};
use crate::dialect::strict::comparable;
use crate::dialect::udf::casts::trino_type_name;

/// The array UDFs.
pub fn all() -> Vec<ScalarUDF> {
    vec![
        ScalarUDF::new_from_impl(TrinoElementAt::new(false)),
        ScalarUDF::new_from_impl(TrinoElementAt::new(true)),
        ScalarUDF::new_from_impl(TrinoReverse::new()),
        ScalarUDF::new_from_impl(TrinoContains::new()),
        ScalarUDF::new_from_impl(TrinoArraysOverlap::new()),
    ]
}

/// Normalise every Arrow list flavour to `List<i32>` so one code path
/// handles offsets; errors for non-lists.
fn as_list(function: &str, input: ArrayRef) -> Result<ListArray> {
    let list = match input.data_type() {
        DataType::List(_) => input,
        DataType::LargeList(f) | DataType::FixedSizeList(f, _) => {
            let target = DataType::List(Arc::new(Field::new("item", f.data_type().clone(), true)));
            cast(&input, &target)?
        }
        other => {
            return plan_err!(
                "{function}: expected an array, got {}",
                trino_type_name(other)
            );
        }
    };
    Ok(list.as_list::<i32>().clone())
}

/// Whether the element type of `array` and `value` are comparable under
/// Trino's rules; otherwise the equality the function needs is a
/// `TYPE_MISMATCH` on Athena.
fn check_element_comparable(function: &str, array: &DataType, value: &DataType) -> Result<()> {
    let Some(element) = element_type(array) else {
        return plan_err!(
            "{function}: expected an array, got {}",
            trino_type_name(array)
        );
    };
    if comparable(element, value) {
        Ok(())
    } else {
        Err(type_mismatch(format!(
            "Unexpected parameters ({}, {}) for function {function}: cannot compare {} with {}",
            trino_type_name(array),
            trino_type_name(value),
            trino_type_name(element),
            trino_type_name(value)
        )))
    }
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

// ---------------------------------------------------------------------------
// reverse(varchar | array)
// ---------------------------------------------------------------------------

/// `trino_reverse(x)`: `reverse` for strings and arrays alike.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TrinoReverse {
    signature: Signature,
}

impl Default for TrinoReverse {
    fn default() -> Self {
        Self::new()
    }
}

impl TrinoReverse {
    /// New instance.
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for TrinoReverse {
    fn name(&self) -> &str {
        "trino_reverse"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null => {
                Ok(DataType::Utf8)
            }
            DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => Ok(
                DataType::List(Arc::new(Field::new("item", f.data_type().clone(), true))),
            ),
            other => Err(type_mismatch(format!(
                "Unexpected parameters ({}) for function reverse. Expected: reverse(varchar), \
                 reverse(array(T))",
                trino_type_name(other)
            ))),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let rows = args.number_rows;
        let input = args.args[0].to_array(rows)?;
        if element_type(input.data_type()).is_none() {
            let strings = string_array("reverse", &args.args[0], rows)?;
            let mut out = StringBuilder::new();
            for i in 0..rows {
                if strings.is_null(i) {
                    out.append_null();
                } else {
                    out.append_value(strings.value(i).chars().rev().collect::<String>());
                }
            }
            return Ok(ColumnarValue::Array(Arc::new(out.finish())));
        }
        let list = as_list("reverse", input)?;
        let offsets = list.value_offsets();
        let mut indices: Vec<i64> = Vec::with_capacity(list.values().len());
        for i in 0..list.len() {
            let (start, end) = (i64::from(offsets[i]), i64::from(offsets[i + 1]));
            indices.extend((start..end).rev());
        }
        let values = take(list.values().as_ref(), &Int64Array::from(indices), None)?;
        let reversed = ListArray::try_new(
            Arc::new(Field::new("item", values.data_type().clone(), true)),
            list.offsets().clone(),
            values,
            list.nulls().cloned(),
        )?;
        Ok(ColumnarValue::Array(Arc::new(reversed)))
    }
}

// ---------------------------------------------------------------------------
// contains(array, x) / arrays_overlap(a, b)
// ---------------------------------------------------------------------------

/// Three-valued result of a membership search.
fn membership(found: bool, saw_null: bool) -> Option<bool> {
    if found {
        Some(true)
    } else if saw_null {
        None
    } else {
        Some(false)
    }
}

/// The non-null elements of row `i` of `list` as scalars, plus whether the
/// row held a `NULL` element.
fn row_scalars(list: &ListArray, i: usize) -> Result<(Vec<ScalarValue>, bool)> {
    let offsets = list.value_offsets();
    let (start, end) = (offsets[i] as usize, offsets[i + 1] as usize);
    let values = list.values();
    let mut out = Vec::with_capacity(end - start);
    let mut saw_null = false;
    for j in start..end {
        if values.is_null(j) {
            saw_null = true;
        } else {
            out.push(ScalarValue::try_from_array(values.as_ref(), j)?);
        }
    }
    Ok((out, saw_null))
}

/// `trino_contains(array, x)`: `true` when `x` is an element, `NULL` when
/// it is not but the array has a `NULL` element (or `x` is `NULL`), else
/// `false`. DataFusion's `array_has` returns `false` in the `NULL` cases.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TrinoContains {
    signature: Signature,
}

impl Default for TrinoContains {
    fn default() -> Self {
        Self::new()
    }
}

impl TrinoContains {
    /// New instance.
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for TrinoContains {
    fn name(&self) -> &str {
        "trino_contains"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        check_element_comparable("contains", &arg_types[0], &arg_types[1])?;
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let rows = args.number_rows;
        let list = as_list("contains", args.args[0].to_array(rows)?)?;
        let element = list.values().data_type().clone();
        let needle = args.args[1].to_array(rows)?;
        let needle = if needle.data_type() == &element {
            needle
        } else {
            cast(&needle, &element)?
        };
        let mut out = BooleanBuilder::with_capacity(rows);
        for i in 0..rows {
            if list.is_null(i) || needle.is_null(i) {
                out.append_null();
                continue;
            }
            let wanted = ScalarValue::try_from_array(needle.as_ref(), i)?;
            let (elements, saw_null) = row_scalars(&list, i)?;
            out.append_option(membership(elements.contains(&wanted), saw_null));
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// `trino_arrays_overlap(a, b)`: `true` when the arrays share an element,
/// `NULL` when they do not but either has a `NULL` element, else `false`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TrinoArraysOverlap {
    signature: Signature,
}

impl Default for TrinoArraysOverlap {
    fn default() -> Self {
        Self::new()
    }
}

impl TrinoArraysOverlap {
    /// New instance.
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for TrinoArraysOverlap {
    fn name(&self) -> &str {
        "trino_arrays_overlap"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let (Some(left), Some(right)) = (element_type(&arg_types[0]), element_type(&arg_types[1]))
        else {
            return Err(type_mismatch(format!(
                "Unexpected parameters ({}, {}) for function arrays_overlap. Expected: \
                 arrays_overlap(array(T), array(T))",
                trino_type_name(&arg_types[0]),
                trino_type_name(&arg_types[1])
            )));
        };
        if !comparable(left, right) {
            return Err(type_mismatch(format!(
                "Unexpected parameters ({}, {}) for function arrays_overlap: cannot compare {} \
                 with {}",
                trino_type_name(&arg_types[0]),
                trino_type_name(&arg_types[1]),
                trino_type_name(left),
                trino_type_name(right)
            )));
        }
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let rows = args.number_rows;
        let left = as_list("arrays_overlap", args.args[0].to_array(rows)?)?;
        let right = as_list("arrays_overlap", args.args[1].to_array(rows)?)?;
        let right = if right.values().data_type() == left.values().data_type() {
            right
        } else {
            as_list("arrays_overlap", cast(&right, left.data_type())?)?
        };
        let mut out = BooleanBuilder::with_capacity(rows);
        for i in 0..rows {
            if left.is_null(i) || right.is_null(i) {
                out.append_null();
                continue;
            }
            let (a, a_null) = row_scalars(&left, i)?;
            let (b, b_null) = row_scalars(&right, i)?;
            let a: HashSet<ScalarValue> = a.into_iter().collect();
            let found = b.iter().any(|v| a.contains(v));
            out.append_option(membership(found, a_null || b_null));
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn membership_is_three_valued() {
        assert_eq!(membership(true, true), Some(true));
        assert_eq!(membership(false, true), None);
        assert_eq!(membership(false, false), Some(false));
    }

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
