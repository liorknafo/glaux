//! Hive/Glue type string → Arrow [`DataType`] mapping.
//!
//! Glue stores column types as Hive type strings (`bigint`,
//! `array<struct<x:int>>`, `decimal(38,9)`, ...). This module parses that
//! grammar with a small recursive-descent parser and maps every supported
//! type to the Arrow type DataFusion should read the data as.
//!
//! Unsupported or malformed type strings error explicitly, naming the exact
//! construct — never a guessed fallback type.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, TimeUnit};

use crate::error::{CatalogError, Result};

/// Map one Glue/Hive column type string to an Arrow [`DataType`].
///
/// `column` is used only for error context so failures name the exact
/// column and type string involved.
pub fn hive_type_to_arrow(column: &str, type_string: &str) -> Result<DataType> {
    let mut parser = Parser {
        input: type_string,
        pos: 0,
    };
    let err = |message: String| CatalogError::UnsupportedHiveType {
        column: column.to_string(),
        type_string: type_string.to_string(),
        message,
    };
    let data_type = parser.parse_type().map_err(&err)?;
    parser.skip_whitespace();
    if parser.pos != parser.input.len() {
        return Err(err(format!(
            "unexpected trailing characters at offset {}: {:?}",
            parser.pos,
            &parser.input[parser.pos..]
        )));
    }
    Ok(data_type)
}

/// Recursive-descent parser over a Hive type string.
struct Parser<'a> {
    input: &'a str,
    pos: usize,
}

/// Internal parse result: errors are plain messages; the public entry point
/// wraps them with column/type context.
type ParseResult<T> = std::result::Result<T, String>;

impl Parser<'_> {
    fn rest(&self) -> &str {
        &self.input[self.pos..]
    }

    fn skip_whitespace(&mut self) {
        let trimmed = self.rest().trim_start();
        self.pos = self.input.len() - trimmed.len();
    }

    /// Consume `c` if it is the next non-whitespace character.
    fn eat(&mut self, c: char) -> bool {
        self.skip_whitespace();
        if self.rest().starts_with(c) {
            self.pos += c.len_utf8();
            true
        } else {
            false
        }
    }

    fn expect(&mut self, c: char) -> ParseResult<()> {
        if self.eat(c) {
            Ok(())
        } else {
            Err(format!(
                "expected {c:?} at offset {}, found {:?}",
                self.pos,
                self.rest().chars().next().map(String::from).unwrap_or_default()
            ))
        }
    }

    /// Read an identifier: `[A-Za-z0-9_$]+`.
    fn identifier(&mut self) -> ParseResult<String> {
        self.skip_whitespace();
        let rest = self.rest();
        let end = rest
            .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == '$'))
            .unwrap_or(rest.len());
        if end == 0 {
            return Err(format!("expected a type name at offset {}", self.pos));
        }
        let ident = rest[..end].to_string();
        self.pos += end;
        Ok(ident)
    }

    /// Read a non-negative integer argument, e.g. inside `decimal(38,9)`.
    fn integer(&mut self) -> ParseResult<u64> {
        let ident = self.identifier()?;
        ident
            .parse::<u64>()
            .map_err(|_| format!("expected an integer, found {ident:?}"))
    }

    fn parse_type(&mut self) -> ParseResult<DataType> {
        let name = self.identifier()?;
        match name.to_ascii_lowercase().as_str() {
            "boolean" => Ok(DataType::Boolean),
            "tinyint" => Ok(DataType::Int8),
            "smallint" => Ok(DataType::Int16),
            "int" | "integer" => Ok(DataType::Int32),
            "bigint" => Ok(DataType::Int64),
            "float" | "real" => Ok(DataType::Float32),
            "double" => Ok(DataType::Float64),
            "string" => Ok(DataType::Utf8),
            "binary" => Ok(DataType::Binary),
            "date" => Ok(DataType::Date32),
            // Athena timestamps are millisecond-precision, but files commonly
            // carry micro/nanosecond values; nanoseconds preserves everything
            // and DataFusion casts file values to this table type.
            "timestamp" => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
            "decimal" | "numeric" => self.parse_decimal(),
            "varchar" | "char" => {
                // Length arguments only constrain writes; reads treat both
                // as unbounded UTF-8. The argument is validated and dropped.
                if self.eat('(') {
                    self.integer()?;
                    self.expect(')')?;
                }
                Ok(DataType::Utf8)
            }
            "array" => {
                self.expect('<')?;
                let item = self.parse_type()?;
                self.expect('>')?;
                Ok(DataType::List(Arc::new(Field::new("item", item, true))))
            }
            "map" => {
                self.expect('<')?;
                let key = self.parse_type()?;
                self.expect(',')?;
                let value = self.parse_type()?;
                self.expect('>')?;
                let entries = Field::new(
                    "key_value",
                    DataType::Struct(Fields::from(vec![
                        Field::new("key", key, false),
                        Field::new("value", value, true),
                    ])),
                    false,
                );
                Ok(DataType::Map(Arc::new(entries), false))
            }
            "struct" => {
                self.expect('<')?;
                let mut fields = Vec::new();
                loop {
                    let field_name = self.identifier()?;
                    self.expect(':')?;
                    let field_type = self.parse_type()?;
                    fields.push(Field::new(field_name, field_type, true));
                    if !self.eat(',') {
                        break;
                    }
                }
                self.expect('>')?;
                Ok(DataType::Struct(Fields::from(fields)))
            }
            other => Err(format!("unsupported Hive type {other:?}")),
        }
    }

    fn parse_decimal(&mut self) -> ParseResult<DataType> {
        // Hive's default when no arguments are given is decimal(10,0).
        let (precision, scale) = if self.eat('(') {
            let precision = self.integer()?;
            let scale = if self.eat(',') { self.integer()? } else { 0 };
            self.expect(')')?;
            (precision, scale)
        } else {
            (10, 0)
        };
        if precision == 0 || precision > 38 {
            return Err(format!(
                "decimal precision {precision} is outside the supported range 1..=38"
            ));
        }
        if scale > precision {
            return Err(format!(
                "decimal scale {scale} exceeds precision {precision}"
            ));
        }
        Ok(DataType::Decimal128(precision as u8, scale as i8))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(s: &str) -> DataType {
        hive_type_to_arrow("c", s).expect("type should parse")
    }

    fn parse_err(s: &str) -> String {
        hive_type_to_arrow("c", s)
            .expect_err("type should not parse")
            .to_string()
    }

    #[test]
    fn primitives() {
        assert_eq!(parse("boolean"), DataType::Boolean);
        assert_eq!(parse("tinyint"), DataType::Int8);
        assert_eq!(parse("smallint"), DataType::Int16);
        assert_eq!(parse("int"), DataType::Int32);
        assert_eq!(parse("integer"), DataType::Int32);
        assert_eq!(parse("bigint"), DataType::Int64);
        assert_eq!(parse("float"), DataType::Float32);
        assert_eq!(parse("double"), DataType::Float64);
        assert_eq!(parse("string"), DataType::Utf8);
        assert_eq!(parse("binary"), DataType::Binary);
        assert_eq!(parse("date"), DataType::Date32);
        assert_eq!(
            parse("timestamp"),
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
    }

    #[test]
    fn case_insensitive_and_whitespace() {
        assert_eq!(parse("BIGINT"), DataType::Int64);
        assert_eq!(
            parse("Array< String >"),
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
        );
    }

    #[test]
    fn char_types() {
        assert_eq!(parse("varchar(255)"), DataType::Utf8);
        assert_eq!(parse("char(10)"), DataType::Utf8);
        assert_eq!(parse("varchar"), DataType::Utf8);
    }

    #[test]
    fn decimals() {
        assert_eq!(parse("decimal(38,9)"), DataType::Decimal128(38, 9));
        assert_eq!(parse("decimal(5)"), DataType::Decimal128(5, 0));
        assert_eq!(parse("decimal"), DataType::Decimal128(10, 0));
        assert!(parse_err("decimal(39,0)").contains("precision 39"));
        assert!(parse_err("decimal(5,6)").contains("scale 6 exceeds precision 5"));
    }

    #[test]
    fn nested_types() {
        assert_eq!(
            parse("array<int>"),
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true)))
        );
        assert_eq!(
            parse("struct<a:int,b:string>"),
            DataType::Struct(Fields::from(vec![
                Field::new("a", DataType::Int32, true),
                Field::new("b", DataType::Utf8, true),
            ]))
        );
        assert_eq!(
            parse("map<string,bigint>"),
            DataType::Map(
                Arc::new(Field::new(
                    "key_value",
                    DataType::Struct(Fields::from(vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", DataType::Int64, true),
                    ])),
                    false,
                )),
                false,
            )
        );
        // Deep nesting: array<struct<x:int,ys:array<double>>>
        assert_eq!(
            parse("array<struct<x:int,ys:array<double>>>"),
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(Fields::from(vec![
                    Field::new("x", DataType::Int32, true),
                    Field::new(
                        "ys",
                        DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
                        true,
                    ),
                ])),
                true,
            )))
        );
    }

    #[test]
    fn errors_name_the_construct() {
        let err = parse_err("uniontype<int,string>");
        assert!(err.contains("uniontype"), "error should name the type: {err}");
        assert!(err.contains("column c"), "error should name the column: {err}");

        let err = parse_err("array<int");
        assert!(err.contains("expected '>'"), "unclosed array: {err}");

        let err = parse_err("int garbage");
        assert!(err.contains("trailing characters"), "{err}");

        let err = parse_err("struct<a int>");
        assert!(err.contains("expected ':'"), "{err}");
    }
}
