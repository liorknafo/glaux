//! The corpus fixture tables as real files behind Glue table definitions.
//!
//! The row data is the single source of truth shared with glaux-athena's
//! own corpus tests (included by path, so the two can never drift). Each
//! table is materialized in a different format so the corpus exercises all
//! three readers the catalog supports:
//!
//! | Table | Format | SerDe |
//! |---|---|---|
//! | `customers` | Parquet | `ParquetHiveSerDe` |
//! | `orders` | NDJSON | OpenX `JsonSerDe` |
//! | `countries` | delimited text (`,`) | `LazySimpleSerDe` |
//! | `events` | Parquet | `ParquetHiveSerDe` |
//!
//! [`FixtureTable::glue_table`] yields the Glue definition used offline;
//! the record path builds the identical `TableInput` for real Glue from
//! the same fields, so both sides read the same bytes through the same
//! metadata.

#[path = "../../glaux-athena/tests/common/fixtures.rs"]
#[allow(dead_code)]
mod data;

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, RecordBatch};
use arrow::datatypes::{DataType, Schema};
use arrow::util::display::ArrayFormatter;
use bytes::Bytes;
use glaux_athena::results::format_options;
use glaux_catalog::{GlueColumn, GlueSerDeInfo, GlueStorageDescriptor, GlueTable};
use parquet::arrow::ArrowWriter;
use serde_json::{Map, Value};

use crate::{HarnessError, Result};

pub use data::{countries, customers, events, orders};

/// Hive SerDe class for Parquet.
pub const PARQUET_SERDE: &str = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";
/// OpenX JSON SerDe class.
pub const OPENX_JSON_SERDE: &str = "org.openx.data.jsonserde.JsonSerDe";
/// LazySimpleSerDe class (delimited text).
pub const LAZY_SIMPLE_SERDE: &str = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";

/// File format a fixture table is materialized in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    /// Apache Parquet.
    Parquet,
    /// Newline-delimited JSON.
    Json,
    /// Comma-delimited text without a header.
    Csv,
}

impl Format {
    /// Short label for docs and reports.
    pub fn label(self) -> &'static str {
        match self {
            Format::Parquet => "Parquet",
            Format::Json => "JSON",
            Format::Csv => "CSV",
        }
    }

    /// Glue `SerdeInfo.SerializationLibrary`.
    pub fn serde(self) -> &'static str {
        match self {
            Format::Parquet => PARQUET_SERDE,
            Format::Json => OPENX_JSON_SERDE,
            Format::Csv => LAZY_SIMPLE_SERDE,
        }
    }

    /// Hive `InputFormat` class.
    pub fn input_format(self) -> &'static str {
        match self {
            Format::Parquet => "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            Format::Json | Format::Csv => "org.apache.hadoop.mapred.TextInputFormat",
        }
    }

    /// Hive `OutputFormat` class.
    pub fn output_format(self) -> &'static str {
        match self {
            Format::Parquet => "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            Format::Json | Format::Csv => {
                "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
            }
        }
    }

    /// SerDe parameters.
    pub fn serde_parameters(self) -> Vec<(&'static str, &'static str)> {
        match self {
            Format::Parquet => vec![("serialization.format", "1")],
            Format::Json => vec![],
            Format::Csv => vec![("field.delim", ","), ("serialization.format", ",")],
        }
    }

    /// Glue `classification` table parameter.
    pub fn classification(self) -> &'static str {
        match self {
            Format::Parquet => "parquet",
            Format::Json => "json",
            Format::Csv => "csv",
        }
    }

    /// Object key suffix.
    fn extension(self) -> &'static str {
        match self {
            Format::Parquet => "parquet",
            Format::Json => "json",
            Format::Csv => "csv",
        }
    }
}

/// One fixture table: its Glue-facing definition and its single data file.
#[derive(Debug, Clone)]
pub struct FixtureTable {
    /// Table name.
    pub name: &'static str,
    /// Storage format.
    pub format: Format,
    /// `(column name, Hive type)` in order.
    pub columns: Vec<(&'static str, &'static str)>,
    /// Key of the data object, relative to the bucket (`<name>/part-0.<ext>`).
    pub key: String,
    /// The serialized file.
    pub bytes: Bytes,
    /// The Arrow data the file was written from.
    pub batch: RecordBatch,
}

impl FixtureTable {
    /// The table's `Location` prefix inside `bucket`, with a trailing `/`.
    pub fn location(&self, bucket: &str) -> String {
        format!("s3://{bucket}/{}/", self.name)
    }

    /// Glue columns.
    pub fn glue_columns(&self) -> Vec<GlueColumn> {
        self.columns
            .iter()
            .map(|(name, ty)| GlueColumn {
                name: (*name).to_string(),
                column_type: Some((*ty).to_string()),
                comment: None,
            })
            .collect()
    }

    /// Table-level parameters (`classification`, `EXTERNAL`).
    pub fn parameters(&self) -> HashMap<String, String> {
        HashMap::from([
            (
                "classification".to_string(),
                self.format.classification().to_string(),
            ),
            ("EXTERNAL".to_string(), "TRUE".to_string()),
        ])
    }

    /// The Glue table definition as glaux's catalog sees it.
    pub fn glue_table(&self, database: &str, bucket: &str) -> GlueTable {
        GlueTable {
            name: self.name.to_string(),
            database_name: Some(database.to_string()),
            table_type: Some("EXTERNAL_TABLE".to_string()),
            storage_descriptor: Some(GlueStorageDescriptor {
                columns: self.glue_columns(),
                location: Some(self.location(bucket)),
                input_format: Some(self.format.input_format().to_string()),
                output_format: Some(self.format.output_format().to_string()),
                serde_info: Some(GlueSerDeInfo {
                    name: None,
                    serialization_library: Some(self.format.serde().to_string()),
                    parameters: self
                        .format
                        .serde_parameters()
                        .into_iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect(),
                }),
                compressed: false,
                parameters: HashMap::new(),
            }),
            partition_keys: Vec::new(),
            parameters: self.parameters(),
        }
    }
}

/// All fixture tables, serialized. Deterministic: the same bytes every call
/// (Parquet is written without timestamps or random metadata).
pub fn tables() -> Result<Vec<FixtureTable>> {
    let (customers_schema, customers_batch) = customers();
    let (orders_schema, orders_batch) = orders();
    let (countries_schema, countries_batch) = countries();
    let (events_schema, events_batch) = events();
    Ok(vec![
        build(
            "customers",
            Format::Parquet,
            vec![
                ("id", "bigint"),
                ("name", "string"),
                ("country", "string"),
                ("signup_date", "date"),
                ("tags", "array<string>"),
                ("profile", "string"),
            ],
            &customers_schema,
            customers_batch,
        )?,
        build(
            "orders",
            Format::Json,
            vec![
                ("id", "bigint"),
                ("customer_id", "bigint"),
                ("amount", "double"),
                ("status", "string"),
                ("created_at", "timestamp"),
                ("note", "string"),
                ("rush", "boolean"),
            ],
            &orders_schema,
            orders_batch,
        )?,
        build(
            "countries",
            Format::Csv,
            vec![
                ("code", "string"),
                ("name", "string"),
                ("continent", "string"),
                ("population", "bigint"),
                ("gdp_per_capita", "double"),
            ],
            &countries_schema,
            countries_batch,
        )?,
        build(
            "events",
            Format::Parquet,
            vec![("id", "bigint"), ("at", "timestamp")],
            &events_schema,
            events_batch,
        )?,
    ])
}

/// Look one fixture table up by name.
pub fn table(name: &str) -> Result<FixtureTable> {
    tables()?
        .into_iter()
        .find(|t| t.name == name)
        .ok_or_else(|| HarnessError::new(format!("no fixture table named {name}")))
}

/// Table names in the order [`tables`] returns them.
pub const TABLE_NAMES: &[&str] = &["customers", "orders", "countries", "events"];

fn build(
    name: &'static str,
    format: Format,
    columns: Vec<(&'static str, &'static str)>,
    schema: &Arc<Schema>,
    batch: RecordBatch,
) -> Result<FixtureTable> {
    let declared: Vec<&str> = columns.iter().map(|(n, _)| *n).collect();
    let actual: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    if declared != actual {
        return Err(HarnessError::new(format!(
            "{name}: Glue columns {declared:?} do not match the fixture schema {actual:?}"
        )));
    }
    let bytes = match format {
        Format::Parquet => to_parquet(schema, &batch)?,
        Format::Json => to_ndjson(&batch)?,
        Format::Csv => to_csv(&batch)?,
    };
    Ok(FixtureTable {
        name,
        format,
        columns,
        key: format!("{name}/part-0.{}", format.extension()),
        bytes,
        batch,
    })
}

fn to_parquet(schema: &Arc<Schema>, batch: &RecordBatch) -> Result<Bytes> {
    let props = parquet::file::properties::WriterProperties::builder()
        .set_created_by("glaux-fidelity".to_string())
        .build();
    let mut buf = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, Arc::clone(schema), Some(props))
        .map_err(|e| HarnessError::new(format!("parquet writer: {e}")))?;
    writer
        .write(batch)
        .map_err(|e| HarnessError::new(format!("parquet write: {e}")))?;
    writer
        .close()
        .map_err(|e| HarnessError::new(format!("parquet close: {e}")))?;
    Ok(Bytes::from(buf))
}

/// One JSON line per row. Temporal values use Athena's text forms
/// (`yyyy-MM-dd HH:mm:ss.SSS`, `yyyy-MM-dd`), which both the OpenX SerDe
/// and glaux's NDJSON reader accept for `timestamp` / `date` columns.
fn to_ndjson(batch: &RecordBatch) -> Result<Bytes> {
    let mut out = String::new();
    for row in 0..batch.num_rows() {
        let mut object = Map::new();
        for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
            object.insert(field.name().clone(), json_value(column, row)?);
        }
        out.push_str(&Value::Object(object).to_string());
        out.push('\n');
    }
    Ok(Bytes::from(out))
}

fn json_value(column: &ArrayRef, row: usize) -> Result<Value> {
    if column.is_null(row) {
        return Ok(Value::Null);
    }
    let unsupported = |what: &str| {
        HarnessError::new(format!(
            "fixture column type {what} is not serializable to NDJSON by this harness"
        ))
    };
    Ok(match column.data_type() {
        DataType::Int64 => Value::from(
            column
                .as_primitive::<arrow::datatypes::Int64Type>()
                .value(row),
        ),
        DataType::Float64 => {
            let v = column
                .as_primitive::<arrow::datatypes::Float64Type>()
                .value(row);
            serde_json::Number::from_f64(v)
                .map(Value::Number)
                .ok_or_else(|| unsupported("non-finite double"))?
        }
        DataType::Boolean => Value::Bool(column.as_boolean().value(row)),
        DataType::Utf8 => Value::String(column.as_string::<i32>().value(row).to_string()),
        DataType::Timestamp(_, None) | DataType::Date32 => Value::String(text(column, row)?),
        DataType::List(_) => {
            let list = column.as_list::<i32>();
            let items = list.value(row);
            let mut values = Vec::with_capacity(items.len());
            for i in 0..items.len() {
                values.push(json_value(&items, i)?);
            }
            Value::Array(values)
        }
        other => return Err(unsupported(&other.to_string())),
    })
}

/// Delimited text without header or quoting (LazySimpleSerDe semantics:
/// quotes would be data). Values must therefore not contain the delimiter;
/// the builder refuses fixtures that do rather than corrupting them.
fn to_csv(batch: &RecordBatch) -> Result<Bytes> {
    let mut out = String::new();
    for row in 0..batch.num_rows() {
        let mut fields = Vec::with_capacity(batch.num_columns());
        for column in batch.columns() {
            let value = if column.is_null(row) {
                // LazySimpleSerDe's default null representation.
                "\\N".to_string()
            } else {
                text(column, row)?
            };
            if value.contains(',') || value.contains('\n') {
                return Err(HarnessError::new(format!(
                    "CSV fixture value {value:?} contains the delimiter or a newline"
                )));
            }
            fields.push(value);
        }
        out.push_str(&fields.join(","));
        out.push('\n');
    }
    Ok(Bytes::from(out))
}

/// Athena-style text of one value.
fn text(column: &ArrayRef, row: usize) -> Result<String> {
    let formatter = ArrayFormatter::try_new(column.as_ref(), &format_options())
        .map_err(|e| HarnessError::new(format!("formatting fixture value: {e}")))?;
    Ok(formatter.value(row).to_string())
}
