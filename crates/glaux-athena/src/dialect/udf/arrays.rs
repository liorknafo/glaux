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
//! - `array_max` / `array_min`: `NULL` when any element is `NULL`
//!   (DataFusion skips NULL elements).
//! - `array_remove(array, x)`: keeps `NULL` elements (DataFusion drops them)
//!   and is `NULL` for a `NULL` `x`.
//! - `array_join(array, sep[, null_replacement])`: elements rendered in
//!   Trino's text forms (`1.0`, `2024-01-05 10:00:00.000`).
//! - `=` / `<>` / `<` … between arrays ([`ArrayCmp`]): three-valued
//!   equality over NULL elements and an error when ordering arrays with
//!   NULL elements, as in Trino (the analyzer routes the operators here).

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanBuilder, Int64Array, ListArray, StringBuilder, new_null_array,
};
use arrow::buffer::OffsetBuffer;
use arrow::compute::{SortOptions, cast, sort_to_indices, take};
use arrow::datatypes::{DataType, Field};
use datafusion::common::{DataFusionError, Result, ScalarValue, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};

use super::{data_error, int64_array, string_array, type_mismatch};
use crate::dialect::strict::comparable;
use crate::dialect::udf::casts::{to_varchar, trino_type_name};

/// The array UDFs.
pub fn all() -> Vec<ScalarUDF> {
    vec![
        ScalarUDF::new_from_impl(TrinoElementAt::new(false)),
        ScalarUDF::new_from_impl(TrinoElementAt::new(true)),
        ScalarUDF::new_from_impl(TrinoReverse::new()),
        ScalarUDF::new_from_impl(TrinoContains::new()),
        ScalarUDF::new_from_impl(TrinoArraysOverlap::new()),
        ScalarUDF::new_from_impl(TrinoArrayExtreme::new(true)),
        ScalarUDF::new_from_impl(TrinoArrayExtreme::new(false)),
        ScalarUDF::new_from_impl(TrinoArrayRemove::new()),
        ScalarUDF::new_from_impl(TrinoArrayPosition::new()),
        ScalarUDF::new_from_impl(TrinoArrayJoin::new()),
        ScalarUDF::new_from_impl(TrinoArrayCompare::new(ArrayCmp::Eq)),
        ScalarUDF::new_from_impl(TrinoArrayCompare::new(ArrayCmp::NotEq)),
        ScalarUDF::new_from_impl(TrinoArrayCompare::new(ArrayCmp::Lt)),
        ScalarUDF::new_from_impl(TrinoArrayCompare::new(ArrayCmp::LtEq)),
        ScalarUDF::new_from_impl(TrinoArrayCompare::new(ArrayCmp::Gt)),
        ScalarUDF::new_from_impl(TrinoArrayCompare::new(ArrayCmp::GtEq)),
        ScalarUDF::new_from_impl(TrinoArraySortKey::new()),
    ]
}

/// Whether element `i` of `array` is NULL, also for `NullArray` values
/// (whose physical null buffer is absent).
fn is_null_at(array: &dyn Array, i: usize) -> bool {
    array.data_type() == &DataType::Null || array.is_null(i)
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

/// Element equality with Trino's EQUAL semantics: floats compare with
/// Java's `==` (`NaN` equals nothing, `-0.0` equals `0.0`); everything
/// else uses `ScalarValue` equality (whose float rules would be Arrow's
/// total order: `NaN = NaN`, `-0.0 ≠ 0.0`).
fn scalar_equal_ieee(a: &ScalarValue, b: &ScalarValue) -> bool {
    match (a, b) {
        (ScalarValue::Float64(Some(x)), ScalarValue::Float64(Some(y))) => x == y,
        (ScalarValue::Float32(Some(x)), ScalarValue::Float32(Some(y))) => x == y,
        _ => a == b,
    }
}

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
        if is_null_at(values.as_ref(), j) {
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
            if list.is_null(i) || is_null_at(needle.as_ref(), i) {
                out.append_null();
                continue;
            }
            let wanted = ScalarValue::try_from_array(needle.as_ref(), i)?;
            let (elements, saw_null) = row_scalars(&list, i)?;
            let found = elements.iter().any(|e| scalar_equal_ieee(e, &wanted));
            out.append_option(membership(found, saw_null));
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

// ---------------------------------------------------------------------------
// array_max / array_min / array_remove / array_join
// ---------------------------------------------------------------------------

/// `trino_array_max(x)` / `trino_array_min(x)`: `NULL` for an empty array
/// or when any element is `NULL`, as in Trino.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TrinoArrayExtreme {
    signature: Signature,
    max: bool,
}

impl TrinoArrayExtreme {
    /// New instance.
    pub fn new(max: bool) -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
            max,
        }
    }

    fn function(&self) -> &'static str {
        if self.max { "array_max" } else { "array_min" }
    }
}

impl ScalarUDFImpl for TrinoArrayExtreme {
    fn name(&self) -> &str {
        if self.max {
            "trino_array_max"
        } else {
            "trino_array_min"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match element_type(&arg_types[0]) {
            Some(t) => Ok(t.clone()),
            None => Err(type_mismatch(format!(
                "Unexpected parameters ({}) for function {}. Expected: {}(array(T))",
                trino_type_name(&arg_types[0]),
                self.function(),
                self.function()
            ))),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let rows = args.number_rows;
        let list = as_list(self.function(), args.args[0].to_array(rows)?)?;
        let values = list.values();
        let offsets = list.value_offsets();
        let options = SortOptions {
            descending: self.max,
            nulls_first: false,
        };
        let mut positions: Vec<Option<i64>> = Vec::with_capacity(rows);
        for i in 0..rows {
            let (start, end) = (offsets[i] as usize, offsets[i + 1] as usize);
            if list.is_null(i) || start == end {
                positions.push(None);
                continue;
            }
            let slice = values.slice(start, end - start);
            if (0..slice.len()).any(|j| is_null_at(slice.as_ref(), j)) {
                positions.push(None);
                continue;
            }
            // Trino's `array_max` ranks NaN smallest (`COMPARISON_UNORDERED_
            // FIRST`), so it is the result only when every element is NaN;
            // Arrow's sort ranks NaN largest. (`array_min` agrees between
            // the two: both rank NaN largest there.)
            if self.max
                && let Some(best) = float_max_index(slice.as_ref())
            {
                positions.push(Some(start as i64 + best as i64));
                continue;
            }
            let order = sort_to_indices(slice.as_ref(), Some(options), Some(1))?;
            positions.push(Some(start as i64 + i64::from(order.value(0))));
        }
        let taken = take(values.as_ref(), &Int64Array::from(positions), None)?;
        Ok(ColumnarValue::Array(taken))
    }
}

/// The index of the Trino-max element of a float array with no nulls (NaN
/// ranked smallest, `-0.0 < 0.0`), or `None` when the array is not a float
/// array (empty slices never reach this: callers skip them).
fn float_max_index(values: &dyn Array) -> Option<usize> {
    let floats: Vec<f64> = match values.data_type() {
        DataType::Float64 => values
            .as_primitive::<arrow::datatypes::Float64Type>()
            .values()
            .to_vec(),
        DataType::Float32 => values
            .as_primitive::<arrow::datatypes::Float32Type>()
            .values()
            .iter()
            .map(|v| f64::from(*v))
            .collect(),
        _ => return None,
    };
    let mut best = 0usize;
    for (i, v) in floats.iter().enumerate().skip(1) {
        let b = floats[best];
        let prefer = if v.is_nan() {
            false
        } else if b.is_nan() {
            true
        } else {
            v.total_cmp(&b) == std::cmp::Ordering::Greater
        };
        if prefer {
            best = i;
        }
    }
    Some(best)
}

/// `trino_array_remove(array, x)`: every element equal to `x` removed,
/// `NULL` elements kept, `NULL` when `x` is `NULL`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TrinoArrayRemove {
    signature: Signature,
}

impl Default for TrinoArrayRemove {
    fn default() -> Self {
        Self::new()
    }
}

impl TrinoArrayRemove {
    /// New instance.
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for TrinoArrayRemove {
    fn name(&self) -> &str {
        "trino_array_remove"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        check_element_comparable("array_remove", &arg_types[0], &arg_types[1])?;
        let element = element_type(&arg_types[0]).expect("checked above");
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            element.clone(),
            true,
        ))))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let rows = args.number_rows;
        let list = as_list("array_remove", args.args[0].to_array(rows)?)?;
        let element = list.values().data_type().clone();
        let needle = args.args[1].to_array(rows)?;
        let needle = if needle.data_type() == &element || element == DataType::Null {
            needle
        } else {
            cast(&needle, &element)?
        };
        let values = list.values();
        let offsets = list.value_offsets();
        let mut kept: Vec<i64> = Vec::with_capacity(values.len());
        let mut new_offsets: Vec<i32> = Vec::with_capacity(rows + 1);
        let mut validity: Vec<bool> = Vec::with_capacity(rows);
        new_offsets.push(0);
        for i in 0..rows {
            if list.is_null(i) || is_null_at(needle.as_ref(), i) {
                validity.push(false);
                new_offsets.push(kept.len() as i32);
                continue;
            }
            let wanted = ScalarValue::try_from_array(needle.as_ref(), i)?;
            for j in offsets[i] as usize..offsets[i + 1] as usize {
                let keep = is_null_at(values.as_ref(), j)
                    || !scalar_equal_ieee(
                        &ScalarValue::try_from_array(values.as_ref(), j)?,
                        &wanted,
                    );
                if keep {
                    kept.push(j as i64);
                }
            }
            validity.push(true);
            new_offsets.push(kept.len() as i32);
        }
        let taken = take(values.as_ref(), &Int64Array::from(kept), None)?;
        let result = ListArray::try_new(
            Arc::new(Field::new("item", taken.data_type().clone(), true)),
            OffsetBuffer::new(new_offsets.into()),
            taken,
            Some(validity.into()),
        )?;
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

/// `trino_array_position(array, x)`: the 1-based position of the first
/// element equal to `x` (Trino's EQUAL operator, so floats compare IEEE:
/// `array_position(ARRAY[NaN], NaN)` is 0), `0` when there is none, `NULL`
/// for a `NULL` array or a `NULL` `x`; `NULL` elements never match.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TrinoArrayPosition {
    signature: Signature,
}

impl Default for TrinoArrayPosition {
    fn default() -> Self {
        Self::new()
    }
}

impl TrinoArrayPosition {
    /// New instance.
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for TrinoArrayPosition {
    fn name(&self) -> &str {
        "trino_array_position"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        check_element_comparable("array_position", &arg_types[0], &arg_types[1])?;
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let rows = args.number_rows;
        let list = as_list("array_position", args.args[0].to_array(rows)?)?;
        let element = list.values().data_type().clone();
        let needle = args.args[1].to_array(rows)?;
        let needle = if needle.data_type() == &element || element == DataType::Null {
            needle
        } else {
            cast(&needle, &element)?
        };
        let values = list.values();
        let offsets = list.value_offsets();
        let mut out = arrow::array::Int64Builder::with_capacity(rows);
        for i in 0..rows {
            if list.is_null(i) || is_null_at(needle.as_ref(), i) {
                out.append_null();
                continue;
            }
            let wanted = ScalarValue::try_from_array(needle.as_ref(), i)?;
            let (start, end) = (offsets[i] as usize, offsets[i + 1] as usize);
            let mut position = 0i64;
            for j in start..end {
                if !is_null_at(values.as_ref(), j)
                    && scalar_equal_ieee(&ScalarValue::try_from_array(values.as_ref(), j)?, &wanted)
                {
                    position = (j - start) as i64 + 1;
                    break;
                }
            }
            out.append_value(position);
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// `trino_array_join(array, separator[, null_replacement])`: elements in
/// Trino's text forms; `NULL` elements are skipped unless a replacement is
/// given.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TrinoArrayJoin {
    signature: Signature,
}

impl Default for TrinoArrayJoin {
    fn default() -> Self {
        Self::new()
    }
}

impl TrinoArrayJoin {
    /// New instance.
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Any(2), TypeSignature::Any(3)],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for TrinoArrayJoin {
    fn name(&self) -> &str {
        "trino_array_join"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let Some(element) = element_type(&arg_types[0]) else {
            return Err(type_mismatch(format!(
                "Unexpected parameters ({}) for function array_join. Expected: \
                 array_join(array(T), varchar)",
                trino_type_name(&arg_types[0])
            )));
        };
        if matches!(
            element,
            DataType::List(_) | DataType::LargeList(_) | DataType::Struct(_) | DataType::Map(..)
        ) {
            return Err(type_mismatch(format!(
                "Unexpected parameters ({}) for function array_join: elements must be scalar",
                trino_type_name(&arg_types[0])
            )));
        }
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let rows = args.number_rows;
        let list = as_list("array_join", args.args[0].to_array(rows)?)?;
        let separators = string_array("array_join", &args.args[1], rows)?;
        let replacements = match args.args.get(2) {
            Some(r) => Some(string_array("array_join", r, rows)?),
            None => None,
        };
        let values = list.values();
        let texts = if values.data_type() == &DataType::Null {
            to_varchar(&new_null_array(&DataType::Utf8, values.len()))?
        } else {
            to_varchar(values)?
        };
        let offsets = list.value_offsets();
        let mut out = StringBuilder::new();
        for i in 0..rows {
            let replacement_null = replacements.as_ref().is_some_and(|r| r.is_null(i));
            if list.is_null(i) || separators.is_null(i) || replacement_null {
                out.append_null();
                continue;
            }
            let mut parts: Vec<&str> = Vec::new();
            for j in offsets[i] as usize..offsets[i + 1] as usize {
                if is_null_at(values.as_ref(), j) {
                    if let Some(r) = &replacements {
                        parts.push(r.value(i));
                    }
                } else {
                    parts.push(texts.value(j));
                }
            }
            out.append_value(parts.join(separators.value(i)));
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

// ---------------------------------------------------------------------------
// Array comparison
// ---------------------------------------------------------------------------

/// Trino's array operators. `=` / `<>` use three-valued element logic: a
/// length mismatch or an element pair that definitely differs is `false`
/// (`true` for `<>`); otherwise a NULL element anywhere makes the result
/// NULL (`ARRAY[1, NULL] = ARRAY[1, NULL]` is NULL, not true as DataFusion
/// says). The ordering operators compare lexicographically and raise `ARRAY
/// comparison not supported for arrays with null elements` when a NULL
/// element is reached (DataFusion sorts NULL elements).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ArrayCmp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

impl ArrayCmp {
    fn symbol(self) -> &'static str {
        match self {
            Self::Eq => "=",
            Self::NotEq => "<>",
            Self::Lt => "<",
            Self::LtEq => "<=",
            Self::Gt => ">",
            Self::GtEq => ">=",
        }
    }
}

fn null_element_error() -> DataFusionError {
    data_error(
        "NOT_SUPPORTED",
        "ARRAY comparison not supported for arrays with null elements",
    )
}

/// The float element values at `i` / `j`, when both arrays are float
/// arrays (which [`unify_lists`] guarantees come unified).
fn float_pair(left: &dyn Array, i: usize, right: &dyn Array, j: usize) -> Option<(f64, f64)> {
    match (left.data_type(), right.data_type()) {
        (DataType::Float64, DataType::Float64) => Some((
            left.as_primitive::<arrow::datatypes::Float64Type>()
                .value(i),
            right
                .as_primitive::<arrow::datatypes::Float64Type>()
                .value(j),
        )),
        (DataType::Float32, DataType::Float32) => Some((
            f64::from(
                left.as_primitive::<arrow::datatypes::Float32Type>()
                    .value(i),
            ),
            f64::from(
                right
                    .as_primitive::<arrow::datatypes::Float32Type>()
                    .value(j),
            ),
        )),
        _ => None,
    }
}

/// Three-valued equality of elements `i` of `left` and `j` of `right`,
/// recursing into nested arrays. Float elements compare with Trino's EQUAL
/// operator (IEEE: `NaN` equals nothing, `-0.0` equals `0.0`); Arrow's
/// comparator would use its total order.
fn element_equal(
    left: &dyn Array,
    i: usize,
    right: &dyn Array,
    j: usize,
    comparator: &arrow::array::DynComparator,
) -> Result<Option<bool>> {
    if is_null_at(left, i) || is_null_at(right, j) {
        return Ok(None);
    }
    if let Some((a, b)) = float_pair(left, i, right, j) {
        return Ok(Some(a == b));
    }
    match (left.data_type(), right.data_type()) {
        (DataType::List(_), DataType::List(_)) => {
            let (l, r) = (left.as_list::<i32>(), right.as_list::<i32>());
            array_equal(l.value(i).as_ref(), r.value(j).as_ref())
        }
        _ => Ok(Some(comparator(i, j) == std::cmp::Ordering::Equal)),
    }
}

/// Trino's `ArrayEqualOperator` over two element arrays.
fn array_equal(left: &dyn Array, right: &dyn Array) -> Result<Option<bool>> {
    if left.len() != right.len() {
        return Ok(Some(false));
    }
    let comparator = arrow::array::make_comparator(left, right, SortOptions::default())?;
    let mut unknown = false;
    for k in 0..left.len() {
        match element_equal(left, k, right, k, &comparator)? {
            Some(false) => return Ok(Some(false)),
            Some(true) => {}
            None => unknown = true,
        }
    }
    Ok(if unknown { None } else { Some(true) })
}

/// Whether any element (at any depth) of `array` is NULL.
fn has_null_element(array: &dyn Array) -> bool {
    if array.null_count() > 0 || array.data_type() == &DataType::Null {
        return !array.is_empty();
    }
    if let DataType::List(_) = array.data_type() {
        let list = array.as_list::<i32>();
        return (0..list.len()).any(|i| has_null_element(list.value(i).as_ref()));
    }
    false
}

/// Trino's lexicographic array ordering; an error on NULL elements.
fn array_ordering(left: &dyn Array, right: &dyn Array) -> Result<std::cmp::Ordering> {
    use std::cmp::Ordering;
    let comparator = arrow::array::make_comparator(left, right, SortOptions::default())?;
    for k in 0..left.len().min(right.len()) {
        if is_null_at(left, k) || is_null_at(right, k) {
            return Err(null_element_error());
        }
        let ordering = match (left.data_type(), right.data_type()) {
            (DataType::List(_), DataType::List(_)) => array_ordering(
                left.as_list::<i32>().value(k).as_ref(),
                right.as_list::<i32>().value(k).as_ref(),
            )?,
            // Trino orders float elements with the IEEE operators (`-0.0 =
            // 0.0` ties; a NaN makes both `<` and `>` false, an outcome the
            // Ordering result cannot express), so NaN elements are refused
            // rather than ordered by Arrow's total order.
            _ => match float_pair(left, k, right, k) {
                Some((a, b)) if a.is_nan() || b.is_nan() => {
                    return Err(data_error(
                        "NOT_SUPPORTED",
                        "ARRAY comparison not supported for arrays with NaN elements",
                    ));
                }
                Some((a, b)) if a == b => std::cmp::Ordering::Equal,
                Some((a, b)) => a.total_cmp(&b),
                None => comparator(k, k),
            },
        };
        if ordering != Ordering::Equal {
            return Ok(ordering);
        }
    }
    Ok(left.len().cmp(&right.len()))
}

/// Cast both lists to `List<common element type>` so the element arrays
/// are directly comparable (`ARRAY[1] = ARRAY[1.0]`).
fn unify_lists(function: &str, left: ArrayRef, right: ArrayRef) -> Result<(ListArray, ListArray)> {
    let left = as_list(function, left)?;
    let right = as_list(function, right)?;
    let (l, r) = (left.value_type(), right.value_type());
    if l == r {
        return Ok((left, right));
    }
    let Some(common) = datafusion::logical_expr::type_coercion::binary::comparison_coercion(&l, &r)
    else {
        return Err(type_mismatch(format!(
            "Cannot apply operator: {} {function} {}",
            trino_type_name(left.data_type()),
            trino_type_name(right.data_type())
        )));
    };
    let target = DataType::List(Arc::new(Field::new("item", common, true)));
    let left = cast(&left, &target)?.as_list::<i32>().clone();
    let right = cast(&right, &target)?.as_list::<i32>().clone();
    Ok((left, right))
}

/// `trino_array_eq(a, b)` and friends: see [`ArrayCmp`].
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TrinoArrayCompare {
    signature: Signature,
    op: ArrayCmp,
}

impl TrinoArrayCompare {
    /// New instance.
    pub fn new(op: ArrayCmp) -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
            op,
        }
    }
}

impl ScalarUDFImpl for TrinoArrayCompare {
    fn name(&self) -> &str {
        match self.op {
            ArrayCmp::Eq => "trino_array_eq",
            ArrayCmp::NotEq => "trino_array_neq",
            ArrayCmp::Lt => "trino_array_lt",
            ArrayCmp::LtEq => "trino_array_lte",
            ArrayCmp::Gt => "trino_array_gt",
            ArrayCmp::GtEq => "trino_array_gte",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match (element_type(&arg_types[0]), element_type(&arg_types[1])) {
            (Some(l), Some(r)) if comparable(l, r) => Ok(DataType::Boolean),
            _ => Err(type_mismatch(format!(
                "Cannot apply operator: {} {} {}",
                trino_type_name(&arg_types[0]),
                self.op.symbol(),
                trino_type_name(&arg_types[1])
            ))),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        use std::cmp::Ordering;
        let rows = args.number_rows;
        let (left, right) = unify_lists(
            self.op.symbol(),
            args.args[0].to_array(rows)?,
            args.args[1].to_array(rows)?,
        )?;
        let mut out = BooleanBuilder::with_capacity(rows);
        for i in 0..rows {
            if left.is_null(i) || right.is_null(i) {
                out.append_null();
                continue;
            }
            let (l, r) = (left.value(i), right.value(i));
            match self.op {
                ArrayCmp::Eq => out.append_option(array_equal(l.as_ref(), r.as_ref())?),
                ArrayCmp::NotEq => {
                    out.append_option(array_equal(l.as_ref(), r.as_ref())?.map(|b| !b))
                }
                ordering_op => {
                    let ordering = array_ordering(l.as_ref(), r.as_ref())?;
                    out.append_value(match ordering_op {
                        ArrayCmp::Lt => ordering == Ordering::Less,
                        ArrayCmp::LtEq => ordering != Ordering::Greater,
                        ArrayCmp::Gt => ordering == Ordering::Greater,
                        _ => ordering != Ordering::Less,
                    });
                }
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// `trino_array_sort_key(arr)`: the array unchanged, after checking that no
/// element is NULL — `ORDER BY` on an array sorts by Trino's ordering
/// operator, which raises on NULL elements where DataFusion would sort them.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TrinoArraySortKey {
    signature: Signature,
}

impl Default for TrinoArraySortKey {
    fn default() -> Self {
        Self::new()
    }
}

impl TrinoArraySortKey {
    /// New instance.
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for TrinoArraySortKey {
    fn name(&self) -> &str {
        "trino_array_sort_key"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let input = args.args[0].to_array(args.number_rows)?;
        let list = as_list("ORDER BY", Arc::clone(&input))?;
        for i in 0..list.len() {
            if !list.is_null(i) && has_null_element(list.value(i).as_ref()) {
                return Err(null_element_error());
            }
        }
        Ok(ColumnarValue::Array(input))
    }
}

#[cfg(test)]
mod comparison_tests {
    use arrow::array::Int64Array;

    use super::*;

    fn ints(values: &[Option<i64>]) -> ArrayRef {
        Arc::new(Int64Array::from(values.to_vec()))
    }

    #[test]
    fn equality_is_three_valued_and_ordering_refuses_nulls() {
        let eq = |a: &[Option<i64>], b: &[Option<i64>]| {
            array_equal(ints(a).as_ref(), ints(b).as_ref()).unwrap()
        };
        assert_eq!(eq(&[Some(1), None], &[Some(1), None]), None);
        assert_eq!(eq(&[Some(1), None], &[Some(1), Some(2)]), None);
        assert_eq!(eq(&[Some(1), None], &[Some(2), None]), Some(false));
        assert_eq!(eq(&[Some(1)], &[Some(1), Some(2)]), Some(false));
        assert_eq!(eq(&[Some(1), Some(2)], &[Some(1), Some(2)]), Some(true));
        assert_eq!(eq(&[], &[]), Some(true));
        let ord = |a: &[Option<i64>], b: &[Option<i64>]| {
            array_ordering(ints(a).as_ref(), ints(b).as_ref())
        };
        assert_eq!(
            ord(&[Some(1), Some(5)], &[Some(1), Some(7)]).unwrap(),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            ord(&[Some(1)], &[Some(1), Some(7)]).unwrap(),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            ord(&[Some(2)], &[Some(1), Some(7)]).unwrap(),
            std::cmp::Ordering::Greater
        );
        let err = ord(&[Some(1), None], &[Some(1), Some(2)])
            .unwrap_err()
            .to_string();
        assert!(err.contains("null elements"), "{err}");
        // A NULL after the deciding element is never reached, as in Trino.
        assert_eq!(
            ord(&[Some(0), None], &[Some(1), Some(2)]).unwrap(),
            std::cmp::Ordering::Less
        );
        assert!(has_null_element(ints(&[Some(1), None]).as_ref()));
        assert!(!has_null_element(ints(&[Some(1)]).as_ref()));
    }
}
