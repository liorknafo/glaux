//! The `<QueryExecutionId>.csv.metadata` companion file Athena writes next
//! to every result CSV.
//!
//! AWS does not document this file. It is a Protocol Buffers message
//! carrying the `ResultSetMetadata` of the query, and the layout used here
//! is the one reverse-engineered by the open-source Athena JDBC driver
//! (`io.burt.athena.result.AthenaMetaDataParser`) and its companion
//! write-up, which real tooling already parses:
//!
//! ```text
//! Metadata {
//!     1: string  catalog            ("hive")
//!     4: repeated ColumnInfo columns
//! }
//! ColumnInfo {
//!     1: string catalog_name        ("hive")
//!     2: string schema_name         ("" for computed results)
//!     3: string table_name          ("" for computed results)
//!     4: string name
//!     5: string label
//!     6: string type                ("varchar", "bigint", ...)
//!     7: int32  precision
//!     8: int32  scale
//!     9: int32  nullable            (1 NOT_NULL, 2 NULLABLE, 3 UNKNOWN)
//!    10: int32  case_sensitive      (0 false, 1 true)
//! }
//! ```
//!
//! # Fidelity caveat
//!
//! Field numbers and wire types above are exactly what the JDBC parser
//! reads, so any parser built against real Athena metadata files decodes
//! glaux's output. What the real file carries *beyond* those fields (the
//! exact contents of the top-level string and any fields the parser skips)
//! is unknown, so glaux writes only the documented fields. The file is
//! standard protobuf wire format either way — `protoc --decode_raw` shows
//! the structure above. Deviation recorded in LIO-23's PR.
//!
//! Encoding is done by hand: the format is nine fields, which does not
//! justify a protobuf code generator in the dependency tree.

use crate::model::ColumnInfo;

/// Top-level field carrying the catalog name.
const FIELD_CATALOG: u32 = 1;
/// Top-level repeated field carrying one `ColumnInfo` message each.
const FIELD_COLUMNS: u32 = 4;

const COL_CATALOG_NAME: u32 = 1;
const COL_SCHEMA_NAME: u32 = 2;
const COL_TABLE_NAME: u32 = 3;
const COL_NAME: u32 = 4;
const COL_LABEL: u32 = 5;
const COL_TYPE: u32 = 6;
const COL_PRECISION: u32 = 7;
const COL_SCALE: u32 = 8;
const COL_NULLABLE: u32 = 9;
const COL_CASE_SENSITIVE: u32 = 10;

const WIRE_VARINT: u32 = 0;
const WIRE_LEN: u32 = 2;

/// Athena's `ColumnNullable` enum in the metadata file.
fn nullable_code(nullable: &str) -> u64 {
    match nullable {
        "NOT_NULL" => 1,
        "NULLABLE" => 2,
        _ => 3,
    }
}

fn nullable_name(code: u64) -> &'static str {
    match code {
        1 => "NOT_NULL",
        2 => "NULLABLE",
        _ => "UNKNOWN",
    }
}

fn put_varint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

fn put_tag(out: &mut Vec<u8>, field: u32, wire: u32) {
    put_varint(out, u64::from(field << 3 | wire));
}

fn put_string(out: &mut Vec<u8>, field: u32, value: &str) {
    put_bytes(out, field, value.as_bytes());
}

fn put_bytes(out: &mut Vec<u8>, field: u32, value: &[u8]) {
    put_tag(out, field, WIRE_LEN);
    put_varint(out, value.len() as u64);
    out.extend_from_slice(value);
}

/// Encode an `int32` field. Protobuf sign-extends negative `int32`s to ten
/// bytes; Athena's precision/scale are never negative, but the encoding is
/// kept exact rather than truncating.
fn put_int32(out: &mut Vec<u8>, field: u32, value: i32) {
    put_tag(out, field, WIRE_VARINT);
    put_varint(out, i64::from(value) as u64);
}

fn encode_column(column: &ColumnInfo) -> Vec<u8> {
    let mut out = Vec::new();
    put_string(&mut out, COL_CATALOG_NAME, &column.catalog_name);
    put_string(&mut out, COL_SCHEMA_NAME, &column.schema_name);
    put_string(&mut out, COL_TABLE_NAME, &column.table_name);
    put_string(&mut out, COL_NAME, &column.name);
    put_string(&mut out, COL_LABEL, &column.label);
    put_string(&mut out, COL_TYPE, &column.type_name);
    put_int32(&mut out, COL_PRECISION, column.precision);
    put_int32(&mut out, COL_SCALE, column.scale);
    put_tag(&mut out, COL_NULLABLE, WIRE_VARINT);
    put_varint(&mut out, nullable_code(&column.nullable));
    put_tag(&mut out, COL_CASE_SENSITIVE, WIRE_VARINT);
    put_varint(&mut out, u64::from(column.case_sensitive));
    out
}

/// Encode the `.csv.metadata` file for a result set with `columns`.
pub fn encode_metadata(columns: &[ColumnInfo]) -> Vec<u8> {
    let mut out = Vec::new();
    let catalog = columns
        .first()
        .map(|c| c.catalog_name.as_str())
        .unwrap_or("hive");
    put_string(&mut out, FIELD_CATALOG, catalog);
    for column in columns {
        put_bytes(&mut out, FIELD_COLUMNS, &encode_column(column));
    }
    out
}

/// Why a metadata file could not be decoded.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum MetadataError {
    /// The bytes are not well-formed protobuf.
    #[error("malformed .csv.metadata at byte {offset}: {message}")]
    Malformed {
        /// Byte offset where decoding stopped.
        offset: usize,
        /// What went wrong.
        message: String,
    },
}

struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn malformed(&self, message: impl Into<String>) -> MetadataError {
        MetadataError::Malformed {
            offset: self.pos,
            message: message.into(),
        }
    }

    fn varint(&mut self) -> Result<u64, MetadataError> {
        let mut value = 0u64;
        for shift in (0..64).step_by(7) {
            let byte = *self
                .bytes
                .get(self.pos)
                .ok_or_else(|| self.malformed("varint runs past the end"))?;
            self.pos += 1;
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(self.malformed("varint longer than 10 bytes"))
    }

    fn bytes(&mut self) -> Result<&'a [u8], MetadataError> {
        let len = self.varint()? as usize;
        let end = self
            .pos
            .checked_add(len)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| self.malformed(format!("length {len} runs past the end")))?;
        let slice = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(slice)
    }

    fn string(&mut self) -> Result<String, MetadataError> {
        let bytes = self.bytes()?;
        String::from_utf8(bytes.to_vec()).map_err(|e| self.malformed(format!("invalid UTF-8: {e}")))
    }

    /// Skip a field of the given wire type.
    fn skip(&mut self, wire: u32) -> Result<(), MetadataError> {
        match wire {
            WIRE_VARINT => self.varint().map(drop),
            1 => self.advance(8),
            WIRE_LEN => self.bytes().map(drop),
            5 => self.advance(4),
            other => Err(self.malformed(format!("unsupported wire type {other}"))),
        }
    }

    fn advance(&mut self, n: usize) -> Result<(), MetadataError> {
        if self.pos + n > self.bytes.len() {
            return Err(self.malformed("fixed-width field runs past the end"));
        }
        self.pos += n;
        Ok(())
    }

    fn done(&self) -> bool {
        self.pos >= self.bytes.len()
    }

    fn tag(&mut self) -> Result<(u32, u32), MetadataError> {
        let tag = self.varint()?;
        let tag = u32::try_from(tag).map_err(|_| self.malformed("tag out of range"))?;
        Ok((tag >> 3, tag & 7))
    }
}

fn decode_column(bytes: &[u8]) -> Result<ColumnInfo, MetadataError> {
    let mut r = Reader { bytes, pos: 0 };
    let mut column = ColumnInfo {
        catalog_name: String::new(),
        schema_name: String::new(),
        table_name: String::new(),
        name: String::new(),
        label: String::new(),
        type_name: String::new(),
        precision: 0,
        scale: 0,
        nullable: "UNKNOWN".to_string(),
        case_sensitive: false,
    };
    while !r.done() {
        let (field, wire) = r.tag()?;
        match (field, wire) {
            (COL_CATALOG_NAME, WIRE_LEN) => column.catalog_name = r.string()?,
            (COL_SCHEMA_NAME, WIRE_LEN) => column.schema_name = r.string()?,
            (COL_TABLE_NAME, WIRE_LEN) => column.table_name = r.string()?,
            (COL_NAME, WIRE_LEN) => column.name = r.string()?,
            (COL_LABEL, WIRE_LEN) => column.label = r.string()?,
            (COL_TYPE, WIRE_LEN) => column.type_name = r.string()?,
            (COL_PRECISION, WIRE_VARINT) => column.precision = r.varint()? as i32,
            (COL_SCALE, WIRE_VARINT) => column.scale = r.varint()? as i32,
            (COL_NULLABLE, WIRE_VARINT) => column.nullable = nullable_name(r.varint()?).to_string(),
            (COL_CASE_SENSITIVE, WIRE_VARINT) => column.case_sensitive = r.varint()? == 1,
            (_, wire) => r.skip(wire)?,
        }
    }
    Ok(column)
}

/// Decode a `.csv.metadata` file back into Athena `ColumnInfo`s. Unknown
/// fields are skipped, as a protobuf parser would.
pub fn decode_metadata(bytes: &[u8]) -> Result<Vec<ColumnInfo>, MetadataError> {
    let mut r = Reader { bytes, pos: 0 };
    let mut columns = Vec::new();
    while !r.done() {
        let (field, wire) = r.tag()?;
        match (field, wire) {
            (FIELD_COLUMNS, WIRE_LEN) => columns.push(decode_column(r.bytes()?)?),
            (_, wire) => r.skip(wire)?,
        }
    }
    Ok(columns)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(name: &str, type_name: &str, precision: i32, scale: i32, cs: bool) -> ColumnInfo {
        ColumnInfo {
            catalog_name: "hive".into(),
            schema_name: String::new(),
            table_name: String::new(),
            name: name.into(),
            label: name.into(),
            type_name: type_name.into(),
            precision,
            scale,
            nullable: "UNKNOWN".into(),
            case_sensitive: cs,
        }
    }

    #[test]
    fn wire_layout_matches_the_jdbc_parser_expectations() {
        let bytes = encode_metadata(&[column("n", "bigint", 19, 0, false)]);
        // 0x0a = field 1, length-delimited: the catalog.
        assert_eq!(&bytes[..6], b"\x0a\x04hive");
        // 0x22 = field 4, length-delimited: one column message.
        assert_eq!(bytes[6], 0x22);
        let len = bytes[7] as usize;
        let column = &bytes[8..8 + len];
        assert_eq!(8 + len, bytes.len());
        assert_eq!(
            column,
            [
                b"\x0a\x04hive".as_slice(), // 1: catalog_name
                b"\x12\x00",                // 2: schema_name ""
                b"\x1a\x00",                // 3: table_name ""
                b"\x22\x01n",               // 4: name
                b"\x2a\x01n",               // 5: label
                b"\x32\x06bigint",          // 6: type
                b"\x38\x13",                // 7: precision 19
                b"\x40\x00",                // 8: scale 0
                b"\x48\x03",                // 9: nullable UNKNOWN
                b"\x50\x00",                // 10: case_sensitive false
            ]
            .concat()
        );
    }

    #[test]
    fn metadata_round_trips_including_large_precision() {
        let columns = vec![
            column("name", "varchar", i32::MAX, 0, true),
            column("amount", "decimal", 10, 2, false),
            ColumnInfo {
                nullable: "NOT_NULL".into(),
                ..column("id", "bigint", 19, 0, false)
            },
        ];
        let bytes = encode_metadata(&columns);
        // i32::MAX as a varint is five bytes: ff ff ff ff 07.
        assert!(bytes.windows(6).any(|w| w == b"\x38\xff\xff\xff\xff\x07"));
        assert_eq!(decode_metadata(&bytes).unwrap(), columns);
    }

    #[test]
    fn empty_result_sets_still_produce_a_catalog_header() {
        let bytes = encode_metadata(&[]);
        assert_eq!(bytes, b"\x0a\x04hive");
        assert_eq!(decode_metadata(&bytes).unwrap(), Vec::<ColumnInfo>::new());
    }

    #[test]
    fn truncated_files_are_rejected_with_an_offset() {
        let mut bytes = encode_metadata(&[column("n", "bigint", 19, 0, false)]);
        bytes.truncate(bytes.len() - 3);
        let err = decode_metadata(&bytes).unwrap_err();
        assert!(matches!(err, MetadataError::Malformed { .. }), "{err}");
    }
}
