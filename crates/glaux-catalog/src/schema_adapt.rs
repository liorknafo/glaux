//! Matching file columns to Glue columns the way Athena does.
//!
//! Glue stores column names in lowercase; data files do not have to. Athena's
//! readers bridge that gap **by name, case-insensitively**:
//!
//! - `ParquetHiveSerDe` (with its default `parquet.column.index.access =
//!   false`) matches Parquet columns to table columns by lowercased name. A
//!   table column absent from a file reads as NULL (Athena's documented
//!   schema-evolution behavior for Parquet); a Parquet type the table type
//!   cannot hold is `HIVE_BAD_DATA`.
//! - The OpenX `JsonSerDe` (with its default `case.insensitive = true`)
//!   lowercases every JSON key before matching it to a column; keys with no
//!   column are ignored and columns with no key are NULL.
//!
//! DataFusion's stock readers are case-sensitive and fill anything they do
//! not find with NULL, which under a Glue schema turns a spelling difference
//! into silently all-NULL columns. This module supplies the Athena semantics:
//!
//! - [`GlueExprAdapterFactory`] — a [`PhysicalExprAdapterFactory`] for the
//!   Parquet scan that resolves Glue columns against the file schema
//!   case-insensitively and only inserts casts the table type can hold
//!   losslessly; anything else (ambiguous names, narrowing or unrelated
//!   types, reordered struct fields) is an explicit error naming the column.
//! - [`GlueJsonSource`] — a [`FileSource`] for NDJSON that renames each
//!   record's keys to the Glue column spelling before Arrow's JSON decoder
//!   sees them, so matching is case-insensitive at every struct level.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, SchemaRef, TimeUnit};
use arrow::json::ReaderBuilder;
use bytes::Bytes;
use datafusion::common::metadata::FieldMetadata;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::datasource::file_format::json::JsonDecoder;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{
    FileOpenFuture, FileOpener, FileScanConfig, FileSource,
};
use datafusion::datasource::projection::{ProjectionOpener, SplitProjection};
use datafusion::datasource::table_schema::TableSchema;
use datafusion::physical_expr::expressions::{CastExpr, Column, Literal};
use datafusion::physical_expr_adapter::{PhysicalExprAdapter, PhysicalExprAdapterFactory};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::projection::ProjectionExprs;
use datafusion::physical_plan::{PhysicalExpr, apply_expression_roots};
use datafusion_datasource::decoder::{DecoderDeserializer, deserialize_stream};
use futures::{StreamExt, TryStreamExt};
use object_store::{ObjectStore, ObjectStoreExt as _};
use serde_json::{Map, Value};

// ---------------------------------------------------------------------------
// Parquet: case-insensitive, type-checked column resolution
// ---------------------------------------------------------------------------

/// Creates [`GlueExprAdapter`]s for each Parquet file of one Glue table.
#[derive(Debug, Clone)]
pub struct GlueExprAdapterFactory {
    /// `database.table`, for error messages.
    table: Arc<str>,
}

impl GlueExprAdapterFactory {
    /// Adapter factory for the table `database.table`.
    pub fn new(database: &str, table: &str) -> Self {
        Self {
            table: Arc::from(format!("{database}.{table}")),
        }
    }
}

impl PhysicalExprAdapterFactory for GlueExprAdapterFactory {
    fn create(
        &self,
        logical_file_schema: SchemaRef,
        physical_file_schema: SchemaRef,
    ) -> Result<Arc<dyn PhysicalExprAdapter>> {
        let mut by_folded_name: HashMap<String, Vec<usize>> = HashMap::new();
        for (index, field) in physical_file_schema.fields().iter().enumerate() {
            by_folded_name
                .entry(field.name().to_lowercase())
                .or_default()
                .push(index);
        }
        Ok(Arc::new(GlueExprAdapter {
            table: Arc::clone(&self.table),
            logical_file_schema,
            physical_file_schema,
            by_folded_name,
        }))
    }
}

/// Rewrites column references in a scan's projection and predicate from the
/// Glue (logical) schema onto one file's (physical) schema.
#[derive(Debug)]
pub struct GlueExprAdapter {
    table: Arc<str>,
    logical_file_schema: SchemaRef,
    physical_file_schema: SchemaRef,
    /// Lowercased physical column name → physical column indices.
    by_folded_name: HashMap<String, Vec<usize>>,
}

impl PhysicalExprAdapter for GlueExprAdapter {
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        expr.transform(|expr| match expr.downcast_ref::<Column>() {
            Some(column) => self.rewrite_column(column),
            None => Ok(Transformed::no(expr)),
        })
        .data()
    }
}

impl GlueExprAdapter {
    fn exec_err(&self, message: String) -> DataFusionError {
        DataFusionError::Execution(format!("table {}: {message}", self.table))
    }

    /// Find the physical column for a Glue column: an exact-name match wins,
    /// otherwise a unique case-insensitive match; two file columns that
    /// differ only by case are ambiguous and rejected.
    fn resolve_physical(&self, name: &str) -> Result<Option<usize>> {
        if let Ok(index) = self.physical_file_schema.index_of(name) {
            return Ok(Some(index));
        }
        match self
            .by_folded_name
            .get(&name.to_lowercase())
            .map(Vec::as_slice)
        {
            None | Some([]) => Ok(None),
            Some([index]) => Ok(Some(*index)),
            Some(indices) => {
                let names: Vec<&str> = indices
                    .iter()
                    .map(|i| self.physical_file_schema.field(*i).name().as_str())
                    .collect();
                Err(self.exec_err(format!(
                    "column {name:?} matches several Parquet columns that differ only by \
                     case ({names:?}); column names are matched case-insensitively, so \
                     this file is ambiguous"
                )))
            }
        }
    }

    fn rewrite_column(&self, column: &Column) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
        let Ok(logical_field) = self.logical_file_schema.field_with_name(column.name()) else {
            // Not a Glue column. DataFusion may inject references to
            // columns that exist only in the file (e.g. virtual columns);
            // those pass through by exact name. Anything else is a planner
            // bug, surfaced rather than guessed at.
            return match self.physical_file_schema.index_of(column.name()) {
                Ok(index) if index == column.index() => {
                    Ok(Transformed::no(Arc::new(column.clone())))
                }
                Ok(index) => Ok(Transformed::yes(Arc::new(Column::new(
                    column.name(),
                    index,
                )))),
                Err(_) => Err(self.exec_err(format!(
                    "column {:?} exists in neither the Glue schema nor the Parquet file",
                    column.name()
                ))),
            };
        };

        let Some(physical_index) = self.resolve_physical(column.name())? else {
            // Athena semantics: a table column missing from a Parquet file
            // reads as NULL (schema evolution by adding columns).
            let null = ScalarValue::Null.cast_to(logical_field.data_type())?;
            return Ok(Transformed::yes(Arc::new(Literal::new_with_metadata(
                null,
                Some(FieldMetadata::from(logical_field)),
            ))));
        };
        let physical_field = self.physical_file_schema.field(physical_index);
        let resolved: Arc<dyn PhysicalExpr> =
            Arc::new(Column::new(physical_field.name(), physical_index));

        if physical_field.data_type() == logical_field.data_type() {
            return Ok(Transformed::yes(resolved));
        }
        if !parquet_type_fits(physical_field.data_type(), logical_field.data_type()) {
            return Err(self.exec_err(format!(
                "column {:?} is {} in the Parquet file but {} in the Glue schema; the file \
                 type cannot be read as the table type without loss, so this file is rejected \
                 (matches Athena HIVE_BAD_DATA)",
                column.name(),
                physical_field.data_type(),
                logical_field.data_type()
            )));
        }
        Ok(Transformed::yes(Arc::new(CastExpr::new_with_target_field(
            resolved,
            Arc::new(logical_field.clone()),
            None,
        ))))
    }
}

/// Whether a Parquet (physical) Arrow type can be cast to the Glue (logical)
/// type without loss: identical types, integer/float widening, any string or
/// binary encoding into a string column, timestamp/date representation
/// changes, decimal precision widening at the same scale, and the same rules
/// applied element-wise to lists, maps, and (position-by-position) structs.
fn parquet_type_fits(physical: &DataType, logical: &DataType) -> bool {
    use DataType::*;
    if physical == logical {
        return true;
    }
    match (physical, logical) {
        (Dictionary(_, value), _) => parquet_type_fits(value, logical),
        (Int8, Int16 | Int32 | Int64) | (Int16, Int32 | Int64) | (Int32, Int64) => true,
        (UInt8, Int16 | Int32 | Int64) | (UInt16, Int32 | Int64) | (UInt32, Int64) => true,
        (Float16, Float32 | Float64) | (Float32, Float64) => true,
        (
            Utf8 | LargeUtf8 | Utf8View | Binary | LargeBinary | BinaryView,
            Utf8 | LargeUtf8 | Utf8View,
        ) => true,
        (Binary | LargeBinary | BinaryView, Binary | LargeBinary | BinaryView) => true,
        // Unit widening never changes the instant (Arrow multiplies; an
        // out-of-range value is a cast error, not a wrong value), narrowing
        // would silently truncate. A zoned file column read as Glue's naive
        // `timestamp` keeps its UTC epoch value, which is exactly how Athena
        // reads it; any other zone change would shift the wall-clock value.
        (Timestamp(pu, ptz), Timestamp(lu, ltz)) => {
            let unit_rank = |u: &TimeUnit| match u {
                TimeUnit::Second => 0,
                TimeUnit::Millisecond => 1,
                TimeUnit::Microsecond => 2,
                TimeUnit::Nanosecond => 3,
            };
            unit_rank(pu) <= unit_rank(lu) && (ltz.is_none() || ptz == ltz)
        }
        (Date32 | Date64, Date32 | Date64) => true,
        (Decimal128(p1, s1), Decimal128(p2, s2)) => s1 == s2 && p1 <= p2,
        (List(p) | LargeList(p), List(l) | LargeList(l)) => {
            parquet_type_fits(p.data_type(), l.data_type())
        }
        // Map entries are casted positionally (key, value); entry field
        // names vary by writer and carry no meaning.
        (Map(p, _), Map(l, _)) => match (p.data_type(), l.data_type()) {
            (Struct(pf), Struct(lf)) => struct_fields_fit(pf, lf, false),
            _ => false,
        },
        (Struct(pf), Struct(lf)) => struct_fields_fit(pf, lf, true),
        _ => false,
    }
}

/// Struct casts in Arrow are positional, so a struct fits only when the
/// field count matches and every position matches by (case-insensitive)
/// name and by type.
fn struct_fields_fit(physical: &Fields, logical: &Fields, check_names: bool) -> bool {
    physical.len() == logical.len()
        && physical.iter().zip(logical.iter()).all(|(p, l)| {
            (!check_names || p.name().eq_ignore_ascii_case(l.name()))
                && parquet_type_fits(p.data_type(), l.data_type())
        })
}

// ---------------------------------------------------------------------------
// JSON: key renaming to Glue column spelling
// ---------------------------------------------------------------------------

/// NDJSON [`FileSource`] that matches JSON keys to Glue columns
/// case-insensitively (OpenX `JsonSerDe` default), at every struct level.
///
/// Files are always read whole (no byte-range splitting), because key
/// rewriting changes record lengths.
#[derive(Clone)]
pub struct GlueJsonSource {
    table: Arc<str>,
    table_schema: TableSchema,
    batch_size: Option<usize>,
    metrics: ExecutionPlanMetricsSet,
    projection: SplitProjection,
}

impl fmt::Debug for GlueJsonSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GlueJsonSource")
            .field("table", &self.table)
            .finish()
    }
}

impl GlueJsonSource {
    /// Source for the table `database.table` with the given table schema
    /// (file columns plus partition columns).
    pub fn new(database: &str, table: &str, table_schema: TableSchema) -> Self {
        Self {
            table: Arc::from(format!("{database}.{table}")),
            projection: SplitProjection::unprojected(&table_schema),
            table_schema,
            batch_size: None,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl FileSource for GlueJsonSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        _base_config: &FileScanConfig,
        _partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        let file_schema = self.table_schema.file_schema();
        let projected_schema = Arc::new(file_schema.project(&self.projection.file_indices)?);
        let opener: Arc<dyn FileOpener> = Arc::new(GlueJsonOpener {
            table: Arc::clone(&self.table),
            batch_size: self.batch_size.ok_or_else(|| {
                DataFusionError::Internal("batch size must be set before opening files".into())
            })?,
            projected_schema,
            object_store,
        });
        ProjectionOpener::try_new(self.projection.clone(), opener, file_schema)
    }

    fn table_schema(&self) -> &TableSchema {
        &self.table_schema
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        let mut source = self.clone();
        source.batch_size = Some(batch_size);
        Arc::new(source)
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        Some(&self.projection.source)
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
        let mut source = self.clone();
        let merged = self.projection.source.try_merge(projection)?;
        source.projection = SplitProjection::new(self.table_schema.file_schema(), &merged);
        Ok(Some(Arc::new(source)))
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn file_type(&self) -> &str {
        "json"
    }

    fn supports_repartitioning(&self) -> bool {
        false
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        apply_expression_roots(self.projection.source.iter(), f)
    }
}

/// Opens one NDJSON object, rewriting keys line by line before decoding.
struct GlueJsonOpener {
    table: Arc<str>,
    batch_size: usize,
    projected_schema: SchemaRef,
    object_store: Arc<dyn ObjectStore>,
}

impl FileOpener for GlueJsonOpener {
    fn open(&self, file: PartitionedFile) -> Result<FileOpenFuture> {
        if file.range.is_some() {
            return Err(DataFusionError::Internal(
                "GlueJsonSource does not support byte-range file splits".into(),
            ));
        }
        let store = Arc::clone(&self.object_store);
        let schema = Arc::clone(&self.projected_schema);
        let batch_size = self.batch_size;
        let table = Arc::clone(&self.table);

        Ok(Box::pin(async move {
            let location = file.object_meta.location.clone();
            let raw = store.get(&location).await?.into_stream();
            let rewriter = Arc::new(KeyRewriter {
                table,
                location: location.to_string(),
                schema: Arc::clone(&schema),
            });
            let rewritten = rewrite_lines(raw.map_err(DataFusionError::from).boxed(), rewriter);
            let decoder = ReaderBuilder::new(schema)
                .with_batch_size(batch_size)
                .build_decoder()?;
            let stream = deserialize_stream(
                rewritten.fuse(),
                DecoderDeserializer::new(JsonDecoder::new(decoder)),
            );
            Ok(stream.map_err(Into::into).boxed())
        }))
    }
}

/// Split a byte stream into lines and rewrite each complete line's keys.
fn rewrite_lines(
    input: futures::stream::BoxStream<'static, Result<Bytes>>,
    rewriter: Arc<KeyRewriter>,
) -> futures::stream::BoxStream<'static, Result<Bytes>> {
    struct State {
        input: futures::stream::BoxStream<'static, Result<Bytes>>,
        pending: Vec<u8>,
        line_number: usize,
        done: bool,
    }
    let state = State {
        input,
        pending: Vec::new(),
        line_number: 0,
        done: false,
    };
    futures::stream::try_unfold(state, move |mut state| {
        let rewriter = Arc::clone(&rewriter);
        async move {
            loop {
                if state.done {
                    return Ok(None);
                }
                match state.input.next().await {
                    Some(chunk) => {
                        state.pending.extend_from_slice(&chunk?);
                        let Some(last_newline) = state.pending.iter().rposition(|b| *b == b'\n')
                        else {
                            continue;
                        };
                        let rest = state.pending.split_off(last_newline + 1);
                        let complete = std::mem::replace(&mut state.pending, rest);
                        let mut out = Vec::with_capacity(complete.len());
                        for line in complete.split_inclusive(|b| *b == b'\n') {
                            state.line_number += 1;
                            rewriter.rewrite_line(line, state.line_number, &mut out)?;
                        }
                        return Ok(Some((Bytes::from(out), state)));
                    }
                    None => {
                        state.done = true;
                        if state.pending.iter().all(u8::is_ascii_whitespace) {
                            return Ok(None);
                        }
                        let last = std::mem::take(&mut state.pending);
                        state.line_number += 1;
                        let mut out = Vec::with_capacity(last.len() + 1);
                        rewriter.rewrite_line(&last, state.line_number, &mut out)?;
                        return Ok(Some((Bytes::from(out), state)));
                    }
                }
            }
        }
    })
    .boxed()
}

/// Renames JSON object keys to the matching Glue column spelling, guided by
/// the (projected) file schema.
struct KeyRewriter {
    table: Arc<str>,
    location: String,
    schema: SchemaRef,
}

impl KeyRewriter {
    fn rewrite_line(&self, line: &[u8], line_number: usize, out: &mut Vec<u8>) -> Result<()> {
        if line.iter().all(u8::is_ascii_whitespace) {
            out.extend_from_slice(line);
            return Ok(());
        }
        let mut value: Value = serde_json::from_slice(line).map_err(|e| {
            DataFusionError::Execution(format!(
                "table {}: object {} line {line_number} is not valid JSON: {e}",
                self.table, self.location
            ))
        })?;
        self.rewrite_value(&mut value, self.schema.fields(), "")
            .map_err(|message| {
                DataFusionError::Execution(format!(
                    "table {}: object {} line {line_number}: {message}",
                    self.table, self.location
                ))
            })?;
        serde_json::to_writer(&mut *out, &value)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        out.push(b'\n');
        Ok(())
    }

    /// Rename the keys of `value` (if it is an object) to the field names in
    /// `fields`, recursing into nested structs, list elements, and map
    /// values. Keys with no matching field are dropped (ignored by Athena);
    /// two keys that fold to the same field are an error.
    fn rewrite_value(
        &self,
        value: &mut Value,
        fields: &Fields,
        path: &str,
    ) -> std::result::Result<(), String> {
        let Value::Object(object) = value else {
            // Not an object (null, scalar, array): Arrow's decoder reports a
            // type mismatch itself, with the column name.
            return Ok(());
        };
        let mut renamed = Map::with_capacity(object.len());
        for (key, mut child) in std::mem::take(object) {
            let Some(field) = find_field(fields, &key) else {
                continue;
            };
            let child_path = if path.is_empty() {
                field.name().clone()
            } else {
                format!("{path}.{}", field.name())
            };
            self.rewrite_nested(&mut child, field.data_type(), &child_path)?;
            if renamed.insert(field.name().clone(), child).is_some() {
                return Err(format!(
                    "several keys differ only by case and all map to column {child_path:?} \
                     (keys are matched case-insensitively), so the record is ambiguous"
                ));
            }
        }
        *object = renamed;
        Ok(())
    }

    fn rewrite_nested(
        &self,
        value: &mut Value,
        data_type: &DataType,
        path: &str,
    ) -> std::result::Result<(), String> {
        match data_type {
            DataType::Struct(fields) => self.rewrite_value(value, fields, path),
            DataType::List(item) | DataType::LargeList(item) => {
                if let Value::Array(items) = value {
                    for item_value in items {
                        self.rewrite_nested(item_value, item.data_type(), path)?;
                    }
                }
                Ok(())
            }
            DataType::Map(entries, _) => {
                let DataType::Struct(entry_fields) = entries.data_type() else {
                    return Ok(());
                };
                let Some(value_field) = entry_fields.get(1) else {
                    return Ok(());
                };
                if let Value::Object(map) = value {
                    for map_value in map.values_mut() {
                        self.rewrite_nested(map_value, value_field.data_type(), path)?;
                    }
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

/// Exact-name match first, then a case-insensitive one. Glue column names
/// are unique case-insensitively (Athena lowercases them), so at most one
/// field can match.
fn find_field<'a>(fields: &'a Fields, key: &str) -> Option<&'a Arc<Field>> {
    fields
        .iter()
        .find(|f| f.name() == key)
        .or_else(|| fields.iter().find(|f| f.name().eq_ignore_ascii_case(key)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Schema;

    #[test]
    fn type_fit_rules() {
        use DataType::*;
        assert!(parquet_type_fits(&Int32, &Int64));
        assert!(parquet_type_fits(&Float32, &Float64));
        assert!(parquet_type_fits(&Utf8View, &Utf8));
        assert!(parquet_type_fits(&Binary, &Utf8));
        assert!(parquet_type_fits(
            &Timestamp(TimeUnit::Millisecond, None),
            &Timestamp(TimeUnit::Nanosecond, None)
        ));
        // Narrowing the unit truncates; rejected.
        assert!(!parquet_type_fits(
            &Timestamp(TimeUnit::Nanosecond, None),
            &Timestamp(TimeUnit::Millisecond, None)
        ));
        // Zoned file column into Glue's naive timestamp keeps the UTC value.
        assert!(parquet_type_fits(
            &Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            &Timestamp(TimeUnit::Nanosecond, None)
        ));
        assert!(parquet_type_fits(
            &Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            &Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        ));
        // Naive -> zoned and zone changes shift wall-clock values; rejected.
        assert!(!parquet_type_fits(
            &Timestamp(TimeUnit::Microsecond, None),
            &Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        ));
        assert!(!parquet_type_fits(
            &Timestamp(TimeUnit::Microsecond, Some("+02:00".into())),
            &Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        ));
        assert!(parquet_type_fits(&Decimal128(10, 2), &Decimal128(18, 2)));
        assert!(!parquet_type_fits(&Decimal128(10, 3), &Decimal128(18, 2)));
        assert!(!parquet_type_fits(&Int64, &Int32));
        assert!(!parquet_type_fits(&Float64, &Float32));
        assert!(!parquet_type_fits(&Utf8, &Int64));
        assert!(!parquet_type_fits(&Int64, &Utf8));
        assert!(!parquet_type_fits(&Boolean, &Int64));

        let physical = Struct(Fields::from(vec![
            Field::new("A", Int32, true),
            Field::new("b", Utf8, true),
        ]));
        let logical = Struct(Fields::from(vec![
            Field::new("a", Int64, true),
            Field::new("b", Utf8, true),
        ]));
        assert!(parquet_type_fits(&physical, &logical));
        let reordered = Struct(Fields::from(vec![
            Field::new("b", Utf8, true),
            Field::new("a", Int64, true),
        ]));
        assert!(!parquet_type_fits(&physical, &reordered));
    }

    #[test]
    fn json_keys_are_renamed_recursively() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("userid", DataType::Int64, true),
            Field::new(
                "profile",
                DataType::Struct(Fields::from(vec![
                    Field::new("firstname", DataType::Utf8, true),
                    Field::new(
                        "tags",
                        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                        true,
                    ),
                ])),
                true,
            ),
        ]));
        let rewriter = KeyRewriter {
            table: Arc::from("db.t"),
            location: "k".into(),
            schema,
        };
        let mut out = Vec::new();
        rewriter
            .rewrite_line(
                br#"{"userId": 7, "Profile": {"FirstName": "a", "tags": ["x"]}, "extra": 1}"#,
                1,
                &mut out,
            )
            .unwrap();
        let value: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(value["userid"], 7);
        assert_eq!(value["profile"]["firstname"], "a");
        assert!(value.get("extra").is_none());

        // High-precision decimals must survive the rewrite byte-for-byte
        // (serde_json `arbitrary_precision`), or decimal(38,9) data would
        // be rounded through f64.
        let mut out = Vec::new();
        rewriter
            .rewrite_line(
                br#"{"userId": 123456789012345678901.123456789}"#,
                1,
                &mut out,
            )
            .unwrap();
        assert_eq!(
            std::str::from_utf8(&out).unwrap().trim_end(),
            r#"{"userid":123456789012345678901.123456789}"#
        );

        let err = rewriter
            .rewrite_line(br#"{"userId": 7, "USERID": 8}"#, 2, &mut Vec::new())
            .unwrap_err()
            .to_string();
        assert!(err.contains("userid") && err.contains("line 2"), "{err}");

        let err = rewriter
            .rewrite_line(b"{not json", 3, &mut Vec::new())
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("line 3") && err.contains("not valid JSON"),
            "{err}"
        );
    }
}
