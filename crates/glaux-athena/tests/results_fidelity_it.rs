//! Results fidelity: for every positive corpus query, the `<id>.csv` the
//! service writes to the `OutputLocation` must agree value-for-value with
//! the paged `GetQueryResults` answer, and `<id>.csv.metadata` must decode
//! to the same `ColumnInfo`s. The CSV is parsed with the `csv` crate (what
//! pandas-style tooling effectively does) and, independently, with a
//! quote-aware splitter that can tell `""` (empty string) from an empty
//! unquoted field (`NULL`) — the distinction Athena's CSV carries and the
//! `csv` crate cannot report.

mod common;

use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::MemoryStorage;
use common::fixtures::all_tables;
use datafusion::catalog::MemTable;
use datafusion::prelude::SessionContext;
use glaux_athena::model::ColumnInfo;
use glaux_athena::{AthenaService, AthenaServiceConfig, TrinoEngine, decode_metadata};
use glaux_catalog::StorageBackend;
use serde_json::{Value, json};

const OUTPUT: &str = "s3://results/q/";

struct Harness {
    service: Arc<AthenaService>,
    storage: Arc<MemoryStorage>,
}

fn harness() -> Harness {
    let ctx = SessionContext::new();
    for (name, schema, batch) in all_tables() {
        ctx.register_table(
            name,
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();
    }
    let storage = Arc::new(MemoryStorage::new());
    let service = Arc::new(AthenaService::new(
        AthenaServiceConfig {
            default_workgroup: "primary".into(),
            default_output_location: Some(OUTPUT.into()),
        },
        Arc::new(TrinoEngine::new(ctx, "datafusion")),
        Arc::clone(&storage) as Arc<dyn StorageBackend>,
    ));
    Harness { service, storage }
}

impl Harness {
    async fn call(&self, action: &str, body: Value) -> Value {
        self.service
            .handle(action, body.to_string().as_bytes())
            .await
            .unwrap_or_else(|e| panic!("{action} {body}: {e}"))
    }

    /// Start `sql` and wait for a terminal state; returns the
    /// `QueryExecution` JSON.
    async fn run(&self, sql: &str) -> Value {
        let started = self
            .call("StartQueryExecution", json!({ "QueryString": sql }))
            .await;
        let id = started["QueryExecutionId"].as_str().unwrap().to_string();
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let out = self
                .call("GetQueryExecution", json!({ "QueryExecutionId": id }))
                .await;
            let qe = out["QueryExecution"].clone();
            match qe["Status"]["State"].as_str().unwrap() {
                "SUCCEEDED" | "FAILED" | "CANCELLED" => return qe,
                _ if Instant::now() > deadline => panic!("{id} never finished: {qe}"),
                _ => tokio::time::sleep(Duration::from_millis(5)).await,
            }
        }
    }

    /// Every `GetQueryResults` page of `id` with `page` rows each:
    /// `(rows, columns)`, rows as `None` for NULL.
    async fn all_pages(
        &self,
        id: &str,
        page: usize,
    ) -> (Vec<Vec<Option<String>>>, Vec<ColumnInfo>) {
        let mut rows = Vec::new();
        let mut columns = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut body = json!({ "QueryExecutionId": id, "MaxResults": page });
            if let Some(t) = &token {
                body["NextToken"] = Value::String(t.clone());
            }
            let out = self.call("GetQueryResults", body).await;
            if columns.is_empty() {
                columns = serde_json::from_value(
                    out["ResultSet"]["ResultSetMetadata"]["ColumnInfo"].clone(),
                )
                .unwrap();
            }
            let page_rows = out["ResultSet"]["Rows"].as_array().unwrap();
            assert!(page_rows.len() <= page);
            for row in page_rows {
                rows.push(
                    row["Data"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|d| d["VarCharValue"].as_str().map(str::to_string))
                        .collect(),
                );
            }
            token = out["NextToken"].as_str().map(str::to_string);
            if token.is_none() {
                return (rows, columns);
            }
        }
    }
}

/// One CSV field as written: its text and whether it was quoted. Athena
/// quotes every non-null value, so an unquoted empty field is `NULL`.
#[derive(Debug, PartialEq, Eq)]
struct RawField {
    text: String,
    quoted: bool,
}

/// RFC 4180 splitter that keeps the quoted/unquoted distinction.
fn split_raw(csv: &str) -> Vec<Vec<RawField>> {
    let mut rows = Vec::new();
    let mut row = Vec::new();
    let mut field = String::new();
    let mut quoted = false;
    let mut in_quotes = false;
    let mut chars = csv.chars().peekable();
    while let Some(c) = chars.next() {
        if in_quotes {
            match c {
                '"' if chars.peek() == Some(&'"') => {
                    chars.next();
                    field.push('"');
                }
                '"' => in_quotes = false,
                other => field.push(other),
            }
            continue;
        }
        match c {
            '"' => {
                assert!(field.is_empty(), "quote inside an unquoted field");
                in_quotes = true;
                quoted = true;
            }
            ',' | '\n' => {
                row.push(RawField {
                    text: std::mem::take(&mut field),
                    quoted: std::mem::take(&mut quoted),
                });
                if c == '\n' {
                    rows.push(std::mem::take(&mut row));
                }
            }
            other => field.push(other),
        }
    }
    assert!(!in_quotes, "unterminated quote");
    assert!(
        row.is_empty() && field.is_empty(),
        "CSV must end with a newline"
    );
    rows
}

/// Parse with the `csv` crate, the way generic tooling would.
fn parse_with_csv_crate(bytes: &[u8]) -> Vec<Vec<String>> {
    csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(false)
        .from_reader(bytes)
        .records()
        .map(|r| r.unwrap().iter().map(str::to_string).collect())
        .collect()
}

fn positive_corpus_queries() -> Vec<(String, String)> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/corpus");
    let mut paths: Vec<_> = fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "sql"))
        .collect();
    paths.sort();
    paths
        .into_iter()
        .filter_map(|path| {
            let text = fs::read_to_string(&path).unwrap();
            let negative = text
                .lines()
                .take_while(|l| l.starts_with("--"))
                .any(|l| l.starts_with("-- error:"));
            (!negative).then(|| {
                (
                    path.file_stem().unwrap().to_string_lossy().into_owned(),
                    text,
                )
            })
        })
        .collect()
}

/// Fetch the CSV + metadata for a succeeded query and check every
/// agreement the story demands. Returns a description of the first
/// mismatch, if any.
async fn check_fidelity(h: &Harness, qe: &Value) -> Result<(), String> {
    let id = qe["QueryExecutionId"].as_str().unwrap();
    let location = qe["ResultConfiguration"]["OutputLocation"]
        .as_str()
        .ok_or("no OutputLocation")?;
    if location != format!("{OUTPUT}{id}.csv") {
        return Err(format!("OutputLocation {location} is not the CSV path"));
    }
    let key = format!("q/{id}.csv");
    let csv_bytes = h
        .storage
        .get_object("results", &key)
        .await
        .map_err(|e| format!("CSV missing: {e}"))?;
    let metadata = h
        .storage
        .get_object("results", &format!("{key}.metadata"))
        .await
        .map_err(|e| format!("metadata missing: {e}"))?;
    let csv_text = std::str::from_utf8(&csv_bytes).map_err(|e| e.to_string())?;

    let (api_rows, columns) = h.all_pages(id, 3).await;
    let decoded = decode_metadata(&metadata).map_err(|e| e.to_string())?;
    if decoded != columns {
        return Err(format!(
            "metadata columns differ:\n  file: {decoded:?}\n  api:  {columns:?}"
        ));
    }

    let raw = split_raw(csv_text);
    if raw.len() != api_rows.len() {
        return Err(format!(
            "CSV has {} rows, GetQueryResults {}",
            raw.len(),
            api_rows.len()
        ));
    }
    // Header row = column names.
    let header: Vec<&str> = raw[0].iter().map(|f| f.text.as_str()).collect();
    let names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
    if header != names {
        return Err(format!("header {header:?} != columns {names:?}"));
    }
    for (i, (csv_row, api_row)) in raw.iter().zip(&api_rows).enumerate() {
        if csv_row.len() != api_row.len() {
            return Err(format!("row {i}: field count differs"));
        }
        for (j, (field, datum)) in csv_row.iter().zip(api_row).enumerate() {
            let expected = match datum {
                Some(text) => RawField {
                    text: text.clone(),
                    quoted: true,
                },
                None => RawField {
                    text: String::new(),
                    quoted: false,
                },
            };
            if *field != expected {
                return Err(format!(
                    "row {i} column {j} ({}): CSV {field:?} vs API {datum:?}",
                    columns[j].name
                ));
            }
        }
    }
    // The csv crate must see the same text (modulo the NULL distinction).
    let generic = parse_with_csv_crate(&csv_bytes);
    let flattened: Vec<Vec<String>> = api_rows
        .iter()
        .map(|r| r.iter().map(|d| d.clone().unwrap_or_default()).collect())
        .collect();
    if generic != flattened {
        return Err("csv crate parse disagrees with GetQueryResults".to_string());
    }
    Ok(())
}

#[tokio::test]
async fn corpus_csvs_agree_with_get_query_results() {
    let h = harness();
    let queries = positive_corpus_queries();
    assert!(queries.len() >= 10, "corpus looks truncated");
    let mut failures = Vec::new();
    for (name, sql) in queries {
        let qe = h.run(&sql).await;
        if qe["Status"]["State"] != "SUCCEEDED" {
            failures.push(format!("{name}: {}", qe["Status"]));
            continue;
        }
        if let Err(e) = check_fidelity(&h, &qe).await {
            failures.push(format!("{name}: {e}"));
        }
    }
    assert!(
        failures.is_empty(),
        "{} fidelity failure(s):\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[tokio::test]
async fn empty_strings_nulls_quotes_and_commas_survive_the_csv() {
    let h = harness();
    let qe = h
        .run(
            "SELECT '' AS empty, CAST(NULL AS VARCHAR) AS missing, 'a,\"b' AS tricky, \
              note FROM orders WHERE id IN (104, 108) ORDER BY id",
        )
        .await;
    assert_eq!(qe["Status"]["State"], "SUCCEEDED", "{qe}");
    check_fidelity(&h, &qe).await.unwrap();

    let id = qe["QueryExecutionId"].as_str().unwrap();
    let csv = h
        .storage
        .get_object("results", &format!("q/{id}.csv"))
        .await
        .unwrap();
    let text = String::from_utf8(csv.to_vec()).unwrap();
    assert_eq!(
        text,
        "\"empty\",\"missing\",\"tricky\",\"note\"\n\
         \"\",,\"a,\"\"b\",\"a,b,,c\"\n\
         \"\",,\"a,\"\"b\",\n"
    );
}

#[tokio::test]
async fn empty_result_sets_write_a_header_only_csv() {
    let h = harness();
    let qe = h.run("SELECT id, amount FROM orders WHERE id < 0").await;
    assert_eq!(qe["Status"]["State"], "SUCCEEDED", "{qe}");
    check_fidelity(&h, &qe).await.unwrap();
    let id = qe["QueryExecutionId"].as_str().unwrap();
    let csv = h
        .storage
        .get_object("results", &format!("q/{id}.csv"))
        .await
        .unwrap();
    assert_eq!(csv.as_ref(), b"\"id\",\"amount\"\n");
}

#[tokio::test]
async fn failed_queries_write_nothing() {
    let h = harness();
    let qe = h.run("SELECT CAST('x' AS INTEGER)").await;
    assert_eq!(qe["Status"]["State"], "FAILED", "{qe}");
    let id = qe["QueryExecutionId"].as_str().unwrap();
    // The path is still reported (Athena does), but nothing is there.
    assert_eq!(
        qe["ResultConfiguration"]["OutputLocation"],
        format!("{OUTPUT}{id}.csv")
    );
    let objects = h.storage.list_objects("results", "q").await.unwrap();
    assert!(objects.is_empty(), "{objects:?}");
}
