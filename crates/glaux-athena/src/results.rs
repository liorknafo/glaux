//! Arrow → Athena result encoding.
//!
//! Athena's `GetQueryResults` wire format carries every value as a string
//! (`Datum.VarCharValue`), `NULL` as an empty `Datum`, and a first row of
//! column headers. This module converts DataFusion's `RecordBatch`es into
//! that shape once, at query completion, so paging is a slice.
//!
//! # Never silently wrong
//!
//! Arrow types with no Athena equivalent (`UInt64`, durations, intervals,
//! unions, ...) are an explicit [`ResultError::UnsupportedType`] naming the
//! column and the Arrow type, never a lossy best-effort string.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow::util::display::{ArrayFormatter, FormatOptions};

use crate::model::{ColumnInfo, Datum, Row};

/// Athena's timestamp text form: `2024-01-31 12:34:56.789`.
const TIMESTAMP_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.3f";
/// Athena's date text form.
const DATE_FORMAT: &str = "%Y-%m-%d";

/// Errors converting query output into Athena's result encoding.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ResultError {
    /// A result column has an Arrow type Athena cannot represent.
    #[error("column {column} has Arrow type {data_type} which has no Athena equivalent")]
    UnsupportedType {
        /// The column name.
        column: String,
        /// The Arrow type, as `Display`ed.
        data_type: String,
    },
    /// Arrow failed to format a value.
    #[error("failed to format value in column {column} at row {row}: {message}")]
    Format {
        /// The column name.
        column: String,
        /// The row index within the full result.
        row: usize,
        /// Arrow's diagnostic.
        message: String,
    },
}

/// A fully encoded result set: column metadata plus every row as strings.
/// Row `0` of [`Self::rows`] is the header row, exactly as Athena returns it
/// on the first page of `GetQueryResults`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedResultSet {
    /// Column metadata in result order.
    pub columns: Vec<ColumnInfo>,
    /// Header row followed by data rows; `None` is `NULL`.
    pub rows: Vec<Vec<Option<String>>>,
}

impl EncodedResultSet {
    /// Number of data rows (excluding the header).
    pub fn data_row_count(&self) -> usize {
        self.rows.len().saturating_sub(1)
    }

    /// Athena `Row`s for the half-open row range `start..end` (clamped).
    pub fn rows_for_page(&self, start: usize, end: usize) -> Vec<Row> {
        let end = end.min(self.rows.len());
        let start = start.min(end);
        self.rows[start..end]
            .iter()
            .map(|row| Row {
                data: row
                    .iter()
                    .map(|value| Datum {
                        var_char_value: value.clone(),
                    })
                    .collect(),
            })
            .collect()
    }

    /// The result set as Athena writes it to the `OutputLocation` CSV: every
    /// non-null field double-quoted (quotes doubled), `NULL` as an empty
    /// unquoted field, `\n` line endings, header row first.
    pub fn to_csv(&self) -> String {
        let mut out = String::new();
        for row in &self.rows {
            let mut first = true;
            for value in row {
                if !first {
                    out.push(',');
                }
                first = false;
                if let Some(value) = value {
                    out.push('"');
                    out.push_str(&value.replace('"', "\"\""));
                    out.push('"');
                }
            }
            out.push('\n');
        }
        out
    }
}

/// Athena type name plus precision/scale for an Arrow type.
fn athena_type(column: &str, data_type: &DataType) -> Result<(String, i32, i32), ResultError> {
    let unsupported = || ResultError::UnsupportedType {
        column: column.to_string(),
        data_type: data_type.to_string(),
    };
    Ok(match data_type {
        DataType::Null => ("unknown".into(), 0, 0),
        DataType::Boolean => ("boolean".into(), 0, 0),
        DataType::Int8 => ("tinyint".into(), 3, 0),
        DataType::Int16 | DataType::UInt8 => ("smallint".into(), 5, 0),
        DataType::Int32 | DataType::UInt16 => ("integer".into(), 10, 0),
        DataType::Int64 | DataType::UInt32 => ("bigint".into(), 19, 0),
        DataType::Float16 | DataType::Float32 => ("real".into(), 17, 0),
        DataType::Float64 => ("double".into(), 17, 0),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            ("varchar".into(), i32::MAX, 0)
        }
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => ("varbinary".into(), i32::MAX, 0),
        DataType::Date32 | DataType::Date64 => ("date".into(), 0, 0),
        DataType::Timestamp(_, None) => ("timestamp".into(), 3, 0),
        DataType::Timestamp(_, Some(_)) => ("timestamp with time zone".into(), 3, 0),
        DataType::Time32(_) | DataType::Time64(_) => ("time".into(), 3, 0),
        DataType::Decimal128(p, s) | DataType::Decimal256(p, s) => {
            ("decimal".into(), i32::from(*p), i32::from(*s))
        }
        DataType::List(inner)
        | DataType::LargeList(inner)
        | DataType::FixedSizeList(inner, _)
        | DataType::ListView(inner)
        | DataType::LargeListView(inner) => {
            athena_type(column, inner.data_type())?;
            ("array".into(), 0, 0)
        }
        DataType::Struct(fields) => {
            for field in fields {
                athena_type(column, field.data_type())?;
            }
            ("row".into(), 0, 0)
        }
        DataType::Map(entries, _) => {
            athena_type(column, entries.data_type())?;
            ("map".into(), 0, 0)
        }
        DataType::Dictionary(_, value) => return athena_type(column, value),
        DataType::RunEndEncoded(_, value) => return athena_type(column, value.data_type()),
        // No Athena equivalent: UInt64 overflows bigint; durations,
        // intervals and unions have no Athena type at all.
        _ => return Err(unsupported()),
    })
}

/// Build Athena `ColumnInfo` metadata for a result schema.
pub fn column_infos(schema: &SchemaRef) -> Result<Vec<ColumnInfo>, ResultError> {
    schema
        .fields()
        .iter()
        .map(|field| {
            let (type_name, precision, scale) = athena_type(field.name(), field.data_type())?;
            Ok(ColumnInfo {
                catalog_name: "hive".to_string(),
                schema_name: String::new(),
                table_name: String::new(),
                name: field.name().clone(),
                label: field.name().clone(),
                type_name,
                precision,
                scale,
                nullable: "UNKNOWN".to_string(),
                case_sensitive: matches!(
                    field.data_type(),
                    DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
                ),
            })
        })
        .collect()
}

fn format_options() -> FormatOptions<'static> {
    FormatOptions::new()
        .with_display_error(false)
        .with_null("null")
        .with_timestamp_format(Some(TIMESTAMP_FORMAT))
        .with_timestamp_tz_format(Some(TIMESTAMP_FORMAT))
        .with_date_format(Some(DATE_FORMAT))
        .with_datetime_format(Some(TIMESTAMP_FORMAT))
}

/// Format one value of `array` at `index` in Athena's text form. Returns
/// `None` for `NULL`.
fn format_value(
    column: &str,
    array: &ArrayRef,
    index: usize,
) -> Result<Option<String>, ResultError> {
    if array.is_null(index) {
        return Ok(None);
    }
    let format_error = |message: String| ResultError::Format {
        column: column.to_string(),
        row: index,
        message,
    };
    let nested = |inner: ArrayRef| -> Result<Vec<String>, ResultError> {
        (0..inner.len())
            .map(|i| Ok(format_value(column, &inner, i)?.unwrap_or_else(|| "null".to_string())))
            .collect()
    };
    let text = match array.data_type() {
        DataType::List(_) => {
            let values = nested(array.as_list::<i32>().value(index))?;
            format!("[{}]", values.join(", "))
        }
        DataType::LargeList(_) => {
            let values = nested(array.as_list::<i64>().value(index))?;
            format!("[{}]", values.join(", "))
        }
        DataType::FixedSizeList(_, _) => {
            let values = nested(array.as_fixed_size_list().value(index))?;
            format!("[{}]", values.join(", "))
        }
        DataType::Struct(fields) => {
            let strukt = array.as_struct();
            let mut parts = Vec::with_capacity(fields.len());
            for (field, child) in fields.iter().zip(strukt.columns()) {
                let value = format_value(column, child, index)?.unwrap_or_else(|| "null".into());
                parts.push(format!("{}={value}", field.name()));
            }
            format!("{{{}}}", parts.join(", "))
        }
        DataType::Map(_, _) => {
            let map = array.as_map();
            let start = map.value_offsets()[index] as usize;
            let end = map.value_offsets()[index + 1] as usize;
            let keys = nested(map.keys().slice(start, end - start))?;
            let values = nested(map.values().slice(start, end - start))?;
            let parts: Vec<String> = keys
                .into_iter()
                .zip(values)
                .map(|(k, v)| format!("{k}={v}"))
                .collect();
            format!("{{{}}}", parts.join(", "))
        }
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => {
            // Athena renders varbinary as space-separated hex byte pairs.
            let bytes: &[u8] = match array.data_type() {
                DataType::Binary => array.as_binary::<i32>().value(index),
                DataType::LargeBinary => array.as_binary::<i64>().value(index),
                DataType::BinaryView => array.as_binary_view().value(index),
                _ => array.as_fixed_size_binary().value(index),
            };
            bytes
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<Vec<_>>()
                .join(" ")
        }
        _ => {
            let options = format_options();
            let formatter = ArrayFormatter::try_new(array.as_ref(), &options)
                .map_err(|e| format_error(e.to_string()))?;
            formatter
                .value(index)
                .try_to_string()
                .map_err(|e| format_error(e.to_string()))?
        }
    };
    Ok(Some(text))
}

/// Encode `batches` (all sharing `schema`) into Athena's result form.
pub fn encode_result_set(
    schema: &SchemaRef,
    batches: &[RecordBatch],
) -> Result<EncodedResultSet, ResultError> {
    let columns = column_infos(schema)?;
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    let mut rows = Vec::with_capacity(total_rows + 1);
    rows.push(
        schema
            .fields()
            .iter()
            .map(|f| Some(f.name().clone()))
            .collect::<Vec<_>>(),
    );
    for batch in batches {
        let arrays: Vec<ArrayRef> = batch.columns().iter().map(Arc::clone).collect();
        for i in 0..batch.num_rows() {
            let mut row = Vec::with_capacity(arrays.len());
            for (field, array) in schema.fields().iter().zip(&arrays) {
                row.push(format_value(field.name(), array, i)?);
            }
            rows.push(row);
        }
    }
    Ok(EncodedResultSet { columns, rows })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int32Array,
        Int64Array, ListArray, StringArray, StructArray, TimestampMillisecondArray, UInt64Array,
    };
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{Field, Fields, Schema, TimeUnit};

    use super::*;

    #[test]
    fn scalars_are_encoded_the_way_athena_prints_them() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
            Field::new("ok", DataType::Boolean, true),
            Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), true),
            Field::new("d", DataType::Date32, true),
            Field::new("amount", DataType::Decimal128(10, 2), true),
            Field::new("blob", DataType::Binary, true),
            Field::new("small", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("a\"b"), None])),
                Arc::new(Float64Array::from(vec![Some(1.5), Some(2.0)])),
                Arc::new(BooleanArray::from(vec![Some(true), Some(false)])),
                Arc::new(TimestampMillisecondArray::from(vec![
                    Some(1_706_704_496_789),
                    None,
                ])),
                Arc::new(Date32Array::from(vec![Some(19_753), None])),
                Arc::new(
                    Decimal128Array::from(vec![Some(12_345), None])
                        .with_precision_and_scale(10, 2)
                        .unwrap(),
                ),
                Arc::new(BinaryArray::from(vec![Some(b"ab".as_slice()), None])),
                Arc::new(Int32Array::from(vec![Some(7), None])),
            ],
        )
        .unwrap();

        let encoded = encode_result_set(&schema, &[batch]).unwrap();
        let types: Vec<&str> = encoded
            .columns
            .iter()
            .map(|c| c.type_name.as_str())
            .collect();
        assert_eq!(
            types,
            [
                "bigint",
                "varchar",
                "double",
                "boolean",
                "timestamp",
                "date",
                "decimal",
                "varbinary",
                "integer"
            ]
        );
        assert_eq!(encoded.columns[6].precision, 10);
        assert_eq!(encoded.columns[6].scale, 2);
        assert_eq!(encoded.columns[0].catalog_name, "hive");

        assert_eq!(
            encoded.rows[0],
            [
                "id", "name", "score", "ok", "ts", "d", "amount", "blob", "small"
            ]
            .map(|s| Some(s.to_string()))
        );
        assert_eq!(
            encoded.rows[1],
            vec![
                Some("1".into()),
                Some("a\"b".into()),
                Some("1.5".into()),
                Some("true".into()),
                Some("2024-01-31 12:34:56.789".into()),
                Some("2024-01-31".into()),
                Some("123.45".into()),
                Some("61 62".into()),
                Some("7".into()),
            ]
        );
        assert_eq!(
            encoded.rows[2],
            vec![
                Some("2".into()),
                None,
                Some("2.0".into()),
                Some("false".into()),
                None,
                None,
                None,
                None,
                None,
            ]
        );

        let csv = encoded.to_csv();
        let lines: Vec<&str> = csv.lines().collect();
        assert_eq!(
            lines[0],
            "\"id\",\"name\",\"score\",\"ok\",\"ts\",\"d\",\"amount\",\"blob\",\"small\""
        );
        assert_eq!(
            lines[1],
            "\"1\",\"a\"\"b\",\"1.5\",\"true\",\"2024-01-31 12:34:56.789\",\"2024-01-31\",\"123.45\",\"61 62\",\"7\""
        );
        assert_eq!(lines[2], "\"2\",,\"2.0\",\"false\",,,,,");
    }

    #[test]
    fn nested_values_use_trino_text_form() {
        let item = Arc::new(Field::new("item", DataType::Int32, true));
        let list = ListArray::new(
            item,
            OffsetBuffer::new(vec![0, 2, 3].into()),
            Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])),
            None,
        );
        let fields = Fields::from(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, true),
        ]);
        let strukt = StructArray::new(
            fields.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["x", "y"])),
            ],
            None,
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("l", list.data_type().clone(), true),
            Field::new("s", DataType::Struct(fields), true),
        ]));
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(list), Arc::new(strukt)])
                .unwrap();
        let encoded = encode_result_set(&schema, &[batch]).unwrap();
        assert_eq!(encoded.columns[0].type_name, "array");
        assert_eq!(encoded.columns[1].type_name, "row");
        assert_eq!(encoded.rows[1][0].as_deref(), Some("[1, null]"));
        assert_eq!(encoded.rows[1][1].as_deref(), Some("{a=1, b=x}"));
        assert_eq!(encoded.rows[2][0].as_deref(), Some("[3]"));
    }

    #[test]
    fn unsupported_arrow_types_error_naming_the_column() {
        let schema = Arc::new(Schema::new(vec![Field::new("big", DataType::UInt64, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(UInt64Array::from(vec![1]))],
        )
        .unwrap();
        let err = encode_result_set(&schema, &[batch]).unwrap_err();
        assert_eq!(
            err,
            ResultError::UnsupportedType {
                column: "big".into(),
                data_type: "UInt64".into(),
            }
        );
    }

    #[test]
    fn paging_clamps_to_the_available_rows() {
        let encoded = EncodedResultSet {
            columns: vec![],
            rows: vec![vec![Some("h".into())], vec![Some("1".into())]],
        };
        assert_eq!(encoded.data_row_count(), 1);
        assert_eq!(encoded.rows_for_page(1, 10).len(), 1);
        assert!(encoded.rows_for_page(5, 10).is_empty());
    }
}
