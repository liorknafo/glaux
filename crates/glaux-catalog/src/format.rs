//! SerDe → DataFusion [`FileFormat`] mapping.
//!
//! Glue tables describe their physical format through the storage
//! descriptor's SerDe class. glaux maps the three v0.1 formats:
//!
//! | SerDe class | Reader |
//! |---|---|
//! | `...parquet.serde.ParquetHiveSerDe` | native Parquet |
//! | `org.openx.data.jsonserde.JsonSerDe` (and Hive `JsonSerDe`) | NDJSON |
//! | `...lazy.LazySimpleSerDe` | CSV with the configured delimiters |
//!
//! Anything else — OpenCSVSerde, Avro, ORC, a missing SerDe — errors
//! explicitly naming the class, never a guessed reader.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;

use crate::error::{CatalogError, Result};
use crate::glue::{GlueStorageDescriptor, GlueTable};

/// SerDe classes read natively as Parquet.
const PARQUET_SERDE: &str = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";
/// Input format accepted as Parquet when the SerDe class is absent.
const PARQUET_INPUT_FORMAT: &str = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
/// The OpenX JSON SerDe (what Athena documents for JSON tables).
const OPENX_JSON_SERDE: &str = "org.openx.data.jsonserde.JsonSerDe";
/// The Hive-bundled JSON SerDe; identical NDJSON semantics for reads.
const HIVE_JSON_SERDE: &str = "org.apache.hive.hcatalog.data.JsonSerDe";
/// LazySimpleSerDe: delimited text, read as CSV with configured delimiters.
const LAZY_SIMPLE_SERDE: &str = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";

/// Quote byte used for LazySimpleSerDe tables. LazySimpleSerDe performs no
/// quote handling at all (a `"` is data), but Arrow's CSV reader requires
/// *some* quote byte — NUL is used because it cannot appear in valid
/// delimited text.
const NO_QUOTING: u8 = 0x00;

/// Resolve the DataFusion [`FileFormat`] for a Glue table from its SerDe
/// configuration. Errors name the unsupported class or the missing piece.
pub(crate) fn file_format_for_table(
    database: &str,
    table: &GlueTable,
    sd: &GlueStorageDescriptor,
) -> Result<Arc<dyn FileFormat>> {
    let unsupported = |message: String| CatalogError::UnsupportedSerDe {
        database: database.to_string(),
        table: table.name.clone(),
        message,
    };

    let serde_library = sd
        .serde_info
        .as_ref()
        .and_then(|s| s.serialization_library.as_deref());

    match serde_library {
        Some(PARQUET_SERDE) => Ok(Arc::new(ParquetFormat::default())),
        Some(OPENX_JSON_SERDE) | Some(HIVE_JSON_SERDE) => {
            require_uncompressed(database, table, sd, "JSON")?;
            Ok(Arc::new(JsonFormat::default()))
        }
        Some(LAZY_SIMPLE_SERDE) => {
            require_uncompressed(database, table, sd, "CSV")?;
            let empty = HashMap::new();
            let serde_params = sd
                .serde_info
                .as_ref()
                .map(|s| &s.parameters)
                .unwrap_or(&empty);
            csv_format(database, table, sd, serde_params).map(|f| Arc::new(f) as _)
        }
        Some(other) => Err(unsupported(format!(
            "SerDe class {other:?} has no glaux reader mapping \
             (supported: ParquetHiveSerDe, OpenX JsonSerDe, LazySimpleSerDe)"
        ))),
        None => {
            // Some Parquet tables omit SerDe info but carry the Parquet
            // input format — accept that; anything else is ambiguous.
            if sd.input_format.as_deref() == Some(PARQUET_INPUT_FORMAT) {
                Ok(Arc::new(ParquetFormat::default()))
            } else {
                Err(unsupported(
                    "storage descriptor has no SerDe serialization library, \
                     so the file format cannot be determined"
                        .to_string(),
                ))
            }
        }
    }
}

/// v0.1 reads only uncompressed JSON/CSV objects; a compressed table errors
/// rather than producing garbage rows.
fn require_uncompressed(
    database: &str,
    table: &GlueTable,
    sd: &GlueStorageDescriptor,
    format: &str,
) -> Result<()> {
    if sd.compressed {
        return Err(CatalogError::UnsupportedSerDe {
            database: database.to_string(),
            table: table.name.clone(),
            message: format!(
                "compressed {format} tables (StorageDescriptor.Compressed = true) \
                 are not supported in v0.1"
            ),
        });
    }
    Ok(())
}

fn csv_format(
    database: &str,
    table: &GlueTable,
    sd: &GlueStorageDescriptor,
    serde_params: &HashMap<String, String>,
) -> Result<CsvFormat> {
    let invalid = |message: String| CatalogError::UnsupportedSerDe {
        database: database.to_string(),
        table: table.name.clone(),
        message,
    };

    // Field delimiter: `field.delim` wins; otherwise `serialization.format`
    // (Hive stores the delimiter there as a decimal byte value, e.g. "1"
    // for Ctrl-A, or as a literal character); Hive's default is Ctrl-A.
    let delimiter = match serde_params.get("field.delim") {
        Some(delim) => single_byte("field.delim", delim).map_err(&invalid)?,
        None => match serde_params.get("serialization.format") {
            Some(fmt) => match fmt.parse::<u8>() {
                Ok(byte) => byte,
                Err(_) => single_byte("serialization.format", fmt).map_err(&invalid)?,
            },
            None => 0x01, // Hive's LazySimpleSerDe default: Ctrl-A
        },
    };

    let escape = serde_params
        .get("escape.delim")
        .map(|e| single_byte("escape.delim", e))
        .transpose()
        .map_err(&invalid)?;

    // `skip.header.line.count` may live in table parameters (Athena
    // TBLPROPERTIES), the storage descriptor, or the SerDe parameters.
    let header_lines = table
        .parameters
        .get("skip.header.line.count")
        .or_else(|| sd.parameters.get("skip.header.line.count"))
        .or_else(|| serde_params.get("skip.header.line.count"));
    let has_header = match header_lines.map(String::as_str) {
        None | Some("0") => false,
        Some("1") => true,
        Some(other) => {
            return Err(invalid(format!(
                "skip.header.line.count = {other:?} is not supported (only 0 or 1)"
            )));
        }
    };

    Ok(CsvFormat::default()
        .with_has_header(has_header)
        .with_delimiter(delimiter)
        .with_quote(NO_QUOTING)
        .with_escape(escape))
}

/// Interpret a SerDe delimiter parameter as exactly one byte, understanding
/// the escape spellings Hive uses (`\t`, ``, `\001`).
fn single_byte(name: &str, value: &str) -> std::result::Result<u8, String> {
    let unescaped: String = match value {
        "\\t" => "\t".to_string(),
        "\\r" => "\r".to_string(),
        "\\n" => "\n".to_string(),
        v if v.starts_with("\\u") && v.len() == 6 => {
            let code = u32::from_str_radix(&v[2..], 16)
                .map_err(|_| format!("{name} = {value:?} is not a valid \\uXXXX escape"))?;
            char::from_u32(code)
                .map(String::from)
                .ok_or_else(|| format!("{name} = {value:?} is not a valid character"))?
        }
        v if v.starts_with('\\') && v.len() > 1 && v[1..].chars().all(|c| c.is_ascii_digit()) => {
            let code = u32::from_str_radix(&v[1..], 8)
                .map_err(|_| format!("{name} = {value:?} is not a valid octal escape"))?;
            char::from_u32(code)
                .map(String::from)
                .ok_or_else(|| format!("{name} = {value:?} is not a valid character"))?
        }
        v => v.to_string(),
    };
    let bytes = unescaped.as_bytes();
    if bytes.len() != 1 {
        return Err(format!(
            "{name} = {value:?} must be a single byte, got {} bytes",
            bytes.len()
        ));
    }
    Ok(bytes[0])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::glue::GlueSerDeInfo;
    use datafusion::datasource::file_format::FileFormat as _;

    fn table_with(sd: GlueStorageDescriptor) -> GlueTable {
        GlueTable {
            name: "t".to_string(),
            database_name: Some("db".to_string()),
            table_type: Some("EXTERNAL_TABLE".to_string()),
            storage_descriptor: Some(sd),
            partition_keys: Vec::new(),
            parameters: HashMap::new(),
        }
    }

    fn sd(serde_lib: Option<&str>, params: &[(&str, &str)]) -> GlueStorageDescriptor {
        GlueStorageDescriptor {
            serde_info: Some(GlueSerDeInfo {
                name: None,
                serialization_library: serde_lib.map(str::to_string),
                parameters: params
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            }),
            ..Default::default()
        }
    }

    #[test]
    fn parquet_serde_maps_to_parquet() {
        let descriptor = sd(Some(PARQUET_SERDE), &[]);
        let table = table_with(descriptor.clone());
        let format = file_format_for_table("db", &table, &descriptor).unwrap();
        assert_eq!(format.get_ext(), "parquet");
    }

    #[test]
    fn parquet_input_format_without_serde_maps_to_parquet() {
        let descriptor = GlueStorageDescriptor {
            input_format: Some(PARQUET_INPUT_FORMAT.to_string()),
            ..Default::default()
        };
        let table = table_with(descriptor.clone());
        let format = file_format_for_table("db", &table, &descriptor).unwrap();
        assert_eq!(format.get_ext(), "parquet");
    }

    #[test]
    fn openx_json_maps_to_json() {
        let descriptor = sd(Some(OPENX_JSON_SERDE), &[]);
        let table = table_with(descriptor.clone());
        let format = file_format_for_table("db", &table, &descriptor).unwrap();
        assert_eq!(format.get_ext(), "json");
    }

    #[test]
    fn lazy_simple_maps_to_csv() {
        let descriptor = sd(Some(LAZY_SIMPLE_SERDE), &[("field.delim", "|")]);
        let table = table_with(descriptor.clone());
        let format = file_format_for_table("db", &table, &descriptor).unwrap();
        assert_eq!(format.get_ext(), "csv");
    }

    #[test]
    fn unknown_serde_errors_naming_the_class() {
        let descriptor = sd(Some("org.apache.hadoop.hive.serde2.OpenCSVSerde"), &[]);
        let table = table_with(descriptor.clone());
        let err = file_format_for_table("db", &table, &descriptor).unwrap_err();
        assert!(
            err.to_string().contains("OpenCSVSerde"),
            "error must name the SerDe: {err}"
        );
    }

    #[test]
    fn missing_serde_errors() {
        let descriptor = GlueStorageDescriptor::default();
        let table = table_with(descriptor.clone());
        let err = file_format_for_table("db", &table, &descriptor).unwrap_err();
        assert!(err.to_string().contains("no SerDe"), "{err}");
    }

    #[test]
    fn compressed_csv_errors() {
        let mut descriptor = sd(Some(LAZY_SIMPLE_SERDE), &[]);
        descriptor.compressed = true;
        let table = table_with(descriptor.clone());
        let err = file_format_for_table("db", &table, &descriptor).unwrap_err();
        assert!(err.to_string().contains("compressed CSV"), "{err}");
    }

    #[test]
    fn header_count_beyond_one_errors() {
        let descriptor = sd(Some(LAZY_SIMPLE_SERDE), &[("skip.header.line.count", "2")]);
        let table = table_with(descriptor.clone());
        let err = file_format_for_table("db", &table, &descriptor).unwrap_err();
        assert!(err.to_string().contains("skip.header.line.count"), "{err}");
    }

    #[test]
    fn delimiter_escape_spellings() {
        assert_eq!(single_byte("field.delim", "\\t").unwrap(), b'\t');
        assert_eq!(single_byte("field.delim", "\\u0001").unwrap(), 0x01);
        assert_eq!(single_byte("field.delim", "\\001").unwrap(), 0x01);
        assert_eq!(single_byte("field.delim", ";").unwrap(), b';');
        assert!(single_byte("field.delim", "ab").is_err());
    }
}
