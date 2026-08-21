//! Table-driven Trino-dialect corpus: every `tests/corpus/*.sql` file runs
//! through [`TrinoEngine`] over the fixture tables below.
//!
//! File format — directives are `--` comments at the top:
//!
//! ```sql
//! -- error: <substring>      negative case: the query must fail and the
//!                            error message must contain <substring>
//! SELECT ...
//! ```
//!
//! Positive cases run the result through the Athena result encoder (so a
//! result type Athena cannot carry, such as `UInt64`, fails the case) and
//! compare the Athena column types plus the pretty-printed rows against the
//! sibling `<name>.expected` file. Set `GLAUX_REGEN_CORPUS=1` to (re)write
//! the expected files after checking the output by hand against Trino
//! semantics.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Float64Array, Int64Array, ListBuilder, RecordBatch,
    StringArray, StringBuilder, TimestampMillisecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::util::pretty::pretty_format_batches;
use chrono::NaiveDate;
use datafusion::catalog::MemTable;
use datafusion::prelude::SessionContext;
use glaux_athena::{EngineError, QueryEngine, QueryRequest, TrinoEngine, encode_result_set};

fn corpus_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/corpus")
}

fn days(y: i32, m: u32, d: u32) -> i32 {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    (NaiveDate::from_ymd_opt(y, m, d).unwrap() - epoch).num_days() as i32
}

fn millis(y: i32, m: u32, d: u32, h: u32, mi: u32, s: u32) -> i64 {
    NaiveDate::from_ymd_opt(y, m, d)
        .unwrap()
        .and_hms_opt(h, mi, s)
        .unwrap()
        .and_utc()
        .timestamp_millis()
}

fn string_list(values: Vec<Option<Vec<Option<&str>>>>) -> ArrayRef {
    let mut builder = ListBuilder::new(StringBuilder::new());
    for row in values {
        match row {
            Some(items) => {
                for item in items {
                    builder.values().append_option(item);
                }
                builder.append(true);
            }
            None => builder.append(false),
        }
    }
    Arc::new(builder.finish())
}

/// `customers`: id, name, country, signup_date, tags (array<varchar>),
/// profile (JSON text).
fn customers() -> (Arc<Schema>, RecordBatch) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("country", DataType::Utf8, true),
        Field::new("signup_date", DataType::Date32, true),
        Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
        Field::new("profile", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                Some("Alice"),
                Some("Bob"),
                Some("Carol"),
                Some("Dan"),
                None,
            ])),
            Arc::new(StringArray::from(vec![
                Some("US"),
                Some("DE"),
                Some("US"),
                Some("FR"),
                Some("DE"),
            ])),
            Arc::new(Date32Array::from(vec![
                Some(days(2023, 1, 15)),
                Some(days(2023, 6, 30)),
                Some(days(2024, 1, 31)),
                Some(days(2024, 2, 29)),
                None,
            ])),
            string_list(vec![
                Some(vec![Some("vip"), Some("early")]),
                Some(vec![Some("trial")]),
                Some(vec![]),
                Some(vec![Some("vip"), None, Some("beta")]),
                None,
            ]),
            Arc::new(StringArray::from(vec![
                Some(r#"{"age": 34, "plan": "pro", "address": {"city": "NYC", "zip": "10001"}, "scores": [10, 20, 30]}"#),
                Some(r#"{"age": 28, "plan": "free", "address": {"city": "Berlin"}, "scores": []}"#),
                Some(r#"{"age": null, "plan": "pro", "flags": {"beta": true}}"#),
                Some(r#"{"age": 45, "plan": "enterprise", "scores": [1]}"#),
                None,
            ])),
        ],
    )
    .unwrap();
    (schema, batch)
}

/// `orders`: id, customer_id, amount, status, created_at (timestamp ms),
/// note.
fn orders() -> (Arc<Schema>, RecordBatch) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, true),
        Field::new("amount", DataType::Float64, true),
        Field::new("status", DataType::Utf8, true),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new("note", DataType::Utf8, true),
        Field::new("rush", DataType::Boolean, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![
                101, 102, 103, 104, 105, 106, 107, 108,
            ])),
            Arc::new(Int64Array::from(vec![
                Some(1),
                Some(1),
                Some(2),
                Some(3),
                Some(3),
                Some(3),
                Some(9),
                None,
            ])),
            Arc::new(Float64Array::from(vec![
                Some(120.5),
                Some(35.0),
                Some(80.25),
                Some(15.0),
                Some(240.0),
                None,
                Some(60.0),
                Some(10.0),
            ])),
            Arc::new(StringArray::from(vec![
                Some("shipped"),
                Some("shipped"),
                Some("pending"),
                Some("cancelled"),
                Some("shipped"),
                Some("pending"),
                Some("shipped"),
                None,
            ])),
            Arc::new(TimestampMillisecondArray::from(vec![
                Some(millis(2024, 1, 5, 10, 30, 0)),
                Some(millis(2024, 1, 31, 23, 59, 59)),
                Some(millis(2024, 2, 14, 8, 0, 0)),
                Some(millis(2024, 3, 1, 0, 0, 0)),
                Some(millis(2024, 3, 15, 12, 0, 0)),
                Some(millis(2024, 12, 31, 18, 45, 10)),
                Some(millis(2025, 1, 1, 0, 0, 0)),
                None,
            ])),
            Arc::new(StringArray::from(vec![
                Some("Order #101: gift wrap, ref ABC-123"),
                Some("  padded  "),
                Some("Überweisung"),
                Some("a,b,,c"),
                Some("2024-03-15 12:00:00"),
                Some("no-digits"),
                Some("x=1;y=22;z=333"),
                None,
            ])),
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                Some(true),
                None,
                Some(true),
                Some(false),
                Some(false),
                None,
            ])),
        ],
    )
    .unwrap();
    (schema, batch)
}

fn engine() -> TrinoEngine {
    let ctx = SessionContext::new();
    for (name, (schema, batch)) in [("customers", customers()), ("orders", orders())] {
        ctx.register_table(
            name,
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();
    }
    TrinoEngine::new(ctx, "datafusion")
}

struct Case {
    name: String,
    sql: String,
    expect_error: Option<String>,
}

fn load_cases() -> Vec<Case> {
    let mut paths: Vec<PathBuf> = fs::read_dir(corpus_dir())
        .expect("tests/corpus exists")
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "sql"))
        .collect();
    paths.sort();
    assert!(
        paths.len() >= 20,
        "corpus looks truncated: {} files",
        paths.len()
    );
    paths
        .into_iter()
        .map(|path| {
            let text = fs::read_to_string(&path).unwrap();
            let expect_error = text
                .lines()
                .take_while(|l| l.starts_with("--"))
                .find_map(|l| l.strip_prefix("-- error:").map(|s| s.trim().to_string()));
            Case {
                name: path.file_stem().unwrap().to_string_lossy().into_owned(),
                sql: text,
                expect_error,
            }
        })
        .collect()
}

fn normalise(text: &str) -> String {
    text.lines()
        .map(str::trim_end)
        .collect::<Vec<_>>()
        .join("\n")
        .trim_end()
        .to_string()
}

async fn run(engine: &TrinoEngine, sql: &str) -> Result<String, EngineError> {
    let out = engine
        .execute(QueryRequest {
            sql: sql.to_string(),
            catalog: None,
            database: None,
        })
        .await?;
    // Every positive result must be encodable for the Athena API; the
    // encoder refuses Arrow types Athena has no name for.
    let encoded = encode_result_set(&out.schema, &out.batches)
        .map_err(|e| EngineError::Execution(format!("result encoding failed: {e}")))?;
    let types = encoded
        .columns
        .iter()
        .map(|c| match c.type_name.as_str() {
            "decimal" => format!("{}({},{})", c.type_name, c.precision, c.scale),
            other => other.to_string(),
        })
        .collect::<Vec<_>>()
        .join(", ");
    let batches = if out.batches.is_empty() {
        vec![RecordBatch::new_empty(out.schema)]
    } else {
        out.batches
    };
    Ok(format!(
        "types: {types}\n{}",
        pretty_format_batches(&batches).unwrap()
    ))
}

#[tokio::test]
async fn corpus_matches_expected_results_and_negative_cases_fail_by_name() {
    let engine = engine();
    let regen = std::env::var_os("GLAUX_REGEN_CORPUS").is_some();
    let mut failures = Vec::new();
    for case in load_cases() {
        let result = run(&engine, &case.sql).await;
        match (&case.expect_error, result) {
            (Some(needle), Err(err)) => {
                let message = err.to_string();
                if !message.contains(needle.as_str()) {
                    failures.push(format!(
                        "{}: error does not name {needle:?}:\n    {message}",
                        case.name
                    ));
                }
                // Negative cases must be user errors (category 2), never
                // internal failures, so clients get InvalidRequest-shaped
                // details.
                if err.category() != 2 {
                    failures.push(format!(
                        "{}: expected a user error (category 2), got {err:?}",
                        case.name
                    ));
                }
            }
            (Some(needle), Ok(table)) => failures.push(format!(
                "{}: expected an error naming {needle:?} but the query succeeded:\n{table}",
                case.name
            )),
            (None, Err(err)) => failures.push(format!("{}: failed: {err}", case.name)),
            (None, Ok(table)) => {
                let expected_path = corpus_dir().join(format!("{}.expected", case.name));
                let actual = normalise(&table);
                if regen {
                    fs::write(&expected_path, format!("{actual}\n")).unwrap();
                    continue;
                }
                match fs::read_to_string(&expected_path) {
                    Ok(expected) if normalise(&expected) == actual => {}
                    Ok(expected) => failures.push(format!(
                        "{}: result differs from {}:\n--- expected\n{}\n--- actual\n{actual}",
                        case.name,
                        expected_path.display(),
                        normalise(&expected)
                    )),
                    Err(_) => failures.push(format!(
                        "{}: missing {} (run with GLAUX_REGEN_CORPUS=1 and review the output)\n{actual}",
                        case.name,
                        expected_path.display()
                    )),
                }
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} corpus failure(s):\n\n{}",
        failures.len(),
        failures.join("\n\n")
    );
}
