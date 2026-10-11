//! `approx_percentile` on a port of airlift's `TDigest`, the structure Trino
//! uses for it.
//!
//! DataFusion's `approx_percentile_cont` is also a t-digest, but a
//! different one: its interpolation differs from airlift's `valuesAt`
//! (which returns the maximum for any offset at or beyond `n - 1` and
//! snaps to single-sample centroids instead of interpolating), so
//! `approx_percentile(amount, 0.9)` over eight values gives `216.1` there
//! and `240.0` on Trino. This module ports airlift's digest — the same
//! merge rule (`k`-scale normaliser `c / (4 ln(n/c) + 24)`, alternating
//! direction, compression 100 for serialised states and 200 internally)
//! and the same `valuesAt` — so the answer is Trino's whenever the data is
//! small enough that no centroids merge (a few dozen values), and the same
//! approximation scheme beyond that, where Trino's own answer already
//! depends on how the input was split across workers.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, ListArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, FieldRef, Float64Type};
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Signature, TypeSignature, Volatility,
};

use super::casts::trino_type_name;
use super::{data_error, is_integer, type_mismatch};

/// The aggregate UDFs of this module.
pub fn aggregate_udfs() -> Vec<AggregateUDF> {
    vec![AggregateUDF::new_from_impl(TrinoApproxPercentile::new())]
}

/// airlift's default compression.
const DEFAULT_COMPRESSION: f64 = 100.0;
const FUDGE_FACTOR: f64 = 10.0;

/// A port of `io.airlift.stats.TDigest`.
#[derive(Debug, Clone)]
pub struct TDigest {
    compression: f64,
    max_size: usize,
    means: Vec<f64>,
    weights: Vec<f64>,
    total_weight: f64,
    min: f64,
    max: f64,
    backwards: bool,
    needs_merge: bool,
}

impl Default for TDigest {
    fn default() -> Self {
        Self::new()
    }
}

impl TDigest {
    /// An empty digest with the default compression.
    pub fn new() -> Self {
        let compression = DEFAULT_COMPRESSION;
        Self {
            compression,
            max_size: (6.0 * (internal_compression_factor(compression) + FUDGE_FACTOR)) as usize,
            means: Vec::new(),
            weights: Vec::new(),
            total_weight: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            backwards: false,
            needs_merge: false,
        }
    }

    /// Number of values added.
    pub fn count(&self) -> f64 {
        self.total_weight
    }

    /// Add one value with weight 1.
    pub fn add(&mut self, value: f64) {
        // airlift merges when the buffer is full; the buffer grows by
        // doubling up to `max_size` first, which only affects allocation.
        if self.means.len() >= self.max_size {
            self.merge(internal_compression_factor(self.compression));
        }
        self.means.push(value);
        self.weights.push(1.0);
        self.total_weight += 1.0;
        if value < self.min {
            self.min = value;
        }
        if value > self.max {
            self.max = value;
        }
        self.needs_merge = true;
    }

    /// `mergeWith`: append the other digest's centroids.
    pub fn merge_with(&mut self, means: &[f64], weights: &[f64], min: f64, max: f64) {
        if means.is_empty() {
            return;
        }
        if self.means.len() + means.len() > self.max_size {
            self.merge_if_needed(internal_compression_factor(self.compression));
        }
        self.means.extend_from_slice(means);
        self.weights.extend_from_slice(weights);
        self.total_weight += weights.iter().sum::<f64>();
        self.min = self.min.min(min);
        self.max = self.max.max(max);
        self.needs_merge = true;
    }

    /// The centroids as airlift serialises them (`serialize()` merges with
    /// the public compression first): `(means, weights, min, max)`.
    pub fn serialized(&mut self) -> (&[f64], &[f64], f64, f64) {
        self.merge(self.compression);
        (&self.means, &self.weights, self.min, self.max)
    }

    /// `valueAt(quantile)`; NaN for an empty digest.
    pub fn value_at(&mut self, quantile: f64) -> f64 {
        let n = self.means.len();
        if n == 0 {
            return f64::NAN;
        }
        self.merge_if_needed(internal_compression_factor(self.compression));
        let n = self.means.len();
        if n == 1 {
            return self.means[0];
        }
        let (means, weights) = (&self.means, &self.weights);
        let total = self.total_weight;
        let (min, max) = (self.min, self.max);
        let offset = quantile * total;
        // lowest value
        if offset < 1.0 {
            return min;
        }
        // between bottom and first centroid
        if offset < weights[0] / 2.0 {
            return min + interpolate(offset, 1.0, min, weights[0] / 2.0, means[0]);
        }
        // between last centroid and top, but not the greatest value
        if offset <= total - 1.0
            && total - offset <= weights[n - 1] / 2.0
            && weights[n - 1] / 2.0 > 1.0
        {
            return max + interpolate(total - offset, 1.0, max, weights[n - 1] / 2.0, means[n - 1]);
        }
        // greatest value
        if offset >= total - 1.0 {
            return max;
        }
        let mut weight_so_far = weights[0] / 2.0;
        let mut current = 0usize;
        let mut delta = (weights[current] + weights[current + 1]) / 2.0;
        while current < n - 1 && weight_so_far + delta <= offset {
            weight_so_far += delta;
            current += 1;
            if current < n - 1 {
                delta = (weights[current] + weights[current + 1]) / 2.0;
            }
        }
        // past the last centroid
        if current == n - 1 {
            if offset <= total - 1.0 && weights[n - 1] / 2.0 > 1.0 {
                return max
                    + interpolate(total - offset, 1.0, max, weights[n - 1] / 2.0, means[n - 1]);
            }
            return max;
        }
        // single-sample cluster on the left and the quantile falls within it
        if weights[current] == 1.0 && offset - weight_so_far < weights[current] / 2.0 {
            return means[current];
        }
        // single-sample cluster on the right and the quantile falls within it
        if weights[current + 1] == 1.0 && offset - weight_so_far >= weights[current] / 2.0 {
            return means[current + 1];
        }
        // within a multi-sample cluster; a single-sample neighbour is
        // excluded from the interpolation
        let mut interpolation_offset = offset - weight_so_far;
        let mut section_length = delta;
        if weights[current] == 1.0 {
            interpolation_offset -= weights[current] / 2.0;
            section_length = weights[current + 1] / 2.0;
        } else if weights[current + 1] == 1.0 {
            section_length = weights[current] / 2.0;
        }
        means[current]
            + interpolate(
                interpolation_offset,
                0.0,
                means[current],
                section_length,
                means[current + 1],
            )
    }

    fn merge_if_needed(&mut self, compression: f64) {
        if self.needs_merge {
            self.merge(compression);
        }
    }

    fn merge(&mut self, compression: f64) {
        let n = self.means.len();
        if n == 0 {
            return;
        }
        // Sort centroids by mean (a stable sort, like airlift's
        // `DoubleArrays.sort` on a sorted prefix plus a sorted tail merge,
        // keeps equal means in insertion order; their weighted average is
        // the same whichever order they merge in).
        let mut order: Vec<usize> = (0..n).collect();
        order.sort_by(|&a, &b| self.means[a].total_cmp(&self.means[b]));
        let means: Vec<f64> = order.iter().map(|&i| self.means[i]).collect();
        let weights: Vec<f64> = order.iter().map(|&i| self.weights[i]).collect();

        let indexes: Vec<usize> = if self.backwards {
            (0..n).rev().collect()
        } else {
            (0..n).collect()
        };
        let total = self.total_weight;
        let mut out_means = Vec::with_capacity(n);
        let mut out_weights = Vec::with_capacity(n);
        let mut centroid_mean = means[indexes[0]];
        let mut centroid_weight = weights[indexes[0]];
        let mut weight_so_far = 0.0;
        let normalizer = normalizer(compression, total);
        let mut current_quantile: f64 = 0.0;
        let mut current_max_cluster_size = max_relative_cluster_size(current_quantile, normalizer);
        for &i in &indexes[1..] {
            let entry_weight = weights[i];
            let entry_mean = means[i];
            let tentative_weight = centroid_weight + entry_weight;
            let tentative_quantile = ((weight_so_far + tentative_weight) / total).min(1.0);
            let max_cluster_weight = total
                * current_max_cluster_size
                    .min(max_relative_cluster_size(tentative_quantile, normalizer));
            if tentative_weight <= max_cluster_weight {
                centroid_mean += (entry_mean - centroid_mean) * entry_weight / tentative_weight;
                centroid_weight = tentative_weight;
            } else {
                out_means.push(centroid_mean);
                out_weights.push(centroid_weight);
                weight_so_far += centroid_weight;
                current_quantile = weight_so_far / total;
                current_max_cluster_size = max_relative_cluster_size(current_quantile, normalizer);
                centroid_weight = entry_weight;
                centroid_mean = entry_mean;
            }
        }
        out_means.push(centroid_mean);
        out_weights.push(centroid_weight);
        if self.backwards {
            out_means.reverse();
            out_weights.reverse();
        }
        self.backwards = !self.backwards;
        self.means = out_means;
        self.weights = out_weights;
        self.needs_merge = false;
    }
}

fn interpolate(x: f64, x0: f64, y0: f64, x1: f64, y1: f64) -> f64 {
    (x - x0) / (x1 - x0) * (y1 - y0)
}

fn max_relative_cluster_size(quantile: f64, normalizer: f64) -> f64 {
    quantile * (1.0 - quantile) / normalizer
}

fn normalizer(compression: f64, weight: f64) -> f64 {
    compression / (4.0 * (weight / compression).ln() + 24.0)
}

fn internal_compression_factor(compression: f64) -> f64 {
    2.0 * compression
}

/// Java's `Math.round(double)`: the closest `long`, ties toward positive
/// infinity (`floor(x + 0.5)` computed exactly).
fn java_round(x: f64) -> i64 {
    if x.is_nan() {
        return 0;
    }
    let floor = x.floor();
    let rounded = if x - floor >= 0.5 { floor + 1.0 } else { floor };
    rounded as i64
}

/// `trino_approx_percentile(x, percentage)`: Trino's `approx_percentile`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TrinoApproxPercentile {
    signature: Signature,
}

impl Default for TrinoApproxPercentile {
    fn default() -> Self {
        Self::new()
    }
}

impl TrinoApproxPercentile {
    /// New instance.
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
        }
    }
}

/// Trino's overloads: `bigint` (every integer width widens to it), `real`,
/// `double`. A decimal argument is refused: Trino would pick one of its
/// floating overloads by its own resolution rules, and glaux does not
/// reproduce that choice.
fn result_type(arg: &DataType) -> Result<DataType> {
    match arg {
        t if is_integer(t) => Ok(DataType::Int64),
        DataType::Float32 => Ok(DataType::Float32),
        DataType::Float64 | DataType::Null => Ok(DataType::Float64),
        other => Err(type_mismatch(format!(
            "Unexpected parameters ({}, double) for function approx_percentile. Expected: \
             approx_percentile(bigint, double), approx_percentile(double, double), \
             approx_percentile(real, double); cast the argument explicitly",
            trino_type_name(other)
        ))),
    }
}

impl AggregateUDFImpl for TrinoApproxPercentile {
    fn name(&self) -> &str {
        "trino_approx_percentile"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        // Trino has an array-of-percentages overload returning an array;
        // glaux refuses it (the rewrite layer cannot see types, so the
        // refusal lives here).
        if matches!(
            arg_types[1],
            DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _)
        ) {
            return Err(type_mismatch(
                "approx_percentile(x, percentages) with an array of percentages is not \
                 supported; call approx_percentile once per percentage",
            ));
        }
        result_type(&arg_types[0])
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let input = args.exprs[0].data_type(args.schema)?;
        Ok(Box::new(ApproxPercentileAccumulator {
            digest: TDigest::new(),
            percentile: None,
            output: result_type(&input)?,
        }))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Arc::new(Field::new_list(
                format!("{}[tdigest means]", args.name),
                Field::new_list_field(DataType::Float64, true),
                true,
            )),
            Arc::new(Field::new_list(
                format!("{}[tdigest weights]", args.name),
                Field::new_list_field(DataType::Float64, true),
                true,
            )),
            Arc::new(Field::new(
                format!("{}[tdigest min]", args.name),
                DataType::Float64,
                true,
            )),
            Arc::new(Field::new(
                format!("{}[tdigest max]", args.name),
                DataType::Float64,
                true,
            )),
            Arc::new(Field::new(
                format!("{}[tdigest percentile]", args.name),
                DataType::Float64,
                true,
            )),
        ])
    }
}

#[derive(Debug)]
struct ApproxPercentileAccumulator {
    digest: TDigest,
    percentile: Option<f64>,
    output: DataType,
}

fn float_list(values: &[f64]) -> ScalarValue {
    let items: Vec<ScalarValue> = values
        .iter()
        .map(|v| ScalarValue::Float64(Some(*v)))
        .collect();
    ScalarValue::List(ScalarValue::new_list_nullable(&items, &DataType::Float64))
}

fn list_values(list: &ListArray, row: usize) -> Vec<f64> {
    if list.is_null(row) {
        return Vec::new();
    }
    list.value(row)
        .as_primitive::<Float64Type>()
        .iter()
        .flatten()
        .collect()
}

impl ApproxPercentileAccumulator {
    fn set_percentile(&mut self, values: &ArrayRef) -> Result<()> {
        let floats = cast(values, &DataType::Float64)?;
        let floats = floats.as_primitive::<Float64Type>();
        for p in floats.iter().flatten() {
            if !(0.0..=1.0).contains(&p) {
                return Err(data_error(
                    "INVALID_FUNCTION_ARGUMENT",
                    "Percentile must be between 0 and 1",
                ));
            }
            self.percentile = Some(p);
        }
        Ok(())
    }
}

impl Accumulator for ApproxPercentileAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.set_percentile(&values[1])?;
        let floats = cast(&values[0], &DataType::Float64)?;
        for v in floats.as_primitive::<Float64Type>().iter().flatten() {
            if !v.is_finite() {
                // airlift refuses NaN and infinite values.
                return Err(data_error(
                    "INVALID_FUNCTION_ARGUMENT",
                    if v.is_nan() {
                        "value is NaN"
                    } else {
                        "value must be finite"
                    },
                ));
            }
            self.digest.add(v);
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.digest.count() == 0.0 {
            return ScalarValue::try_from(&self.output);
        }
        let Some(percentile) = self.percentile else {
            return ScalarValue::try_from(&self.output);
        };
        let value = self.digest.value_at(percentile);
        Ok(match self.output {
            DataType::Int64 => ScalarValue::Int64(Some(java_round(value))),
            DataType::Float32 => ScalarValue::Float32(Some(value as f32)),
            _ => ScalarValue::Float64(Some(value)),
        })
    }

    fn size(&self) -> usize {
        size_of_val(self) + (self.digest.means.capacity() + self.digest.weights.capacity()) * 8
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let (means, weights, min, max) = self.digest.serialized();
        Ok(vec![
            float_list(means),
            float_list(weights),
            ScalarValue::Float64(Some(min)),
            ScalarValue::Float64(Some(max)),
            ScalarValue::Float64(self.percentile),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let means = states[0].as_list::<i32>();
        let weights = states[1].as_list::<i32>();
        let mins = states[2].as_primitive::<Float64Type>();
        let maxs = states[3].as_primitive::<Float64Type>();
        let percentiles = states[4].as_primitive::<Float64Type>();
        for row in 0..means.len() {
            if !percentiles.is_null(row) {
                self.percentile = Some(percentiles.value(row));
            }
            let m = list_values(means, row);
            let w = list_values(weights, row);
            if m.is_empty() {
                continue;
            }
            self.digest
                .merge_with(&m, &w, mins.value(row), maxs.value(row));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn digest_of(values: &[f64]) -> TDigest {
        let mut d = TDigest::new();
        for v in values {
            d.add(*v);
        }
        d
    }

    #[test]
    fn values_at_follows_airlift_for_singleton_centroids() {
        // offset 2.0 with four singletons: weightSoFar 1.5, the right
        // neighbour is a singleton and 2.0 - 1.5 >= 0.5 → means[2].
        assert_eq!(digest_of(&[1.0, 2.0, 3.0, 4.0]).value_at(0.5), 3.0);
        // offset 6.3 >= n - 1 → max.
        let orders = [120.5, 35.0, 80.25, 15.0, 240.0, 60.0, 10.0];
        assert_eq!(digest_of(&orders).value_at(0.9), 240.0);
        assert_eq!(digest_of(&orders).value_at(0.5), 60.0);
        assert_eq!(digest_of(&orders).value_at(0.0), 10.0);
        assert_eq!(digest_of(&orders).value_at(1.0), 240.0);
        assert_eq!(digest_of(&[5.0]).value_at(0.3), 5.0);
        assert!(TDigest::new().value_at(0.5).is_nan());
    }

    #[test]
    fn large_inputs_compress_and_stay_monotonic() {
        let values: Vec<f64> = (0..10_000).map(f64::from).collect();
        let mut d = digest_of(&values);
        assert!(d.means.len() < 1000);
        let mut last = f64::NEG_INFINITY;
        for q in [0.0, 0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99, 1.0] {
            let v = d.value_at(q);
            assert!(v >= last, "{q}: {v} < {last}");
            assert!((v - q * 9_999.0).abs() <= 60.0, "{q}: {v}");
            last = v;
        }
        assert_eq!(d.value_at(1.0), 9_999.0);
    }

    #[test]
    fn merged_partial_digests_agree_with_a_single_digest() {
        // The distributed path: two partial digests serialised and merged
        // must answer like one digest over all the values (exactly, while
        // nothing compresses).
        let all = [120.5, 35.0, 80.25, 15.0, 240.0, 60.0, 10.0];
        let mut left = digest_of(&all[..4]);
        let mut right = digest_of(&all[4..]);
        let mut merged = TDigest::new();
        let (m, w, min, max) = left.serialized();
        let (m, w, min, max) = (m.to_vec(), w.to_vec(), min, max);
        merged.merge_with(&m, &w, min, max);
        let (m, w, min, max) = right.serialized();
        let (m, w, min, max) = (m.to_vec(), w.to_vec(), min, max);
        merged.merge_with(&m, &w, min, max);
        for q in [0.0, 0.25, 0.5, 0.75, 0.9, 1.0] {
            assert_eq!(merged.value_at(q), digest_of(&all).value_at(q), "{q}");
        }
    }

    #[test]
    fn java_round_ties_toward_positive_infinity() {
        assert_eq!(java_round(2.5), 3);
        assert_eq!(java_round(-2.5), -2);
        assert_eq!(java_round(-0.5), 0);
        assert_eq!(java_round(0.49999999999999994), 0);
        assert_eq!(java_round(-1e-20), 0);
    }
}
