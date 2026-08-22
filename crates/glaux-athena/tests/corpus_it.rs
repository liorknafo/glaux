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

mod common;

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use common::fixtures::{customers, events, orders};
use datafusion::catalog::MemTable;
use datafusion::prelude::SessionContext;
use glaux_athena::{EngineError, QueryEngine, QueryRequest, TrinoEngine, encode_result_set};

fn corpus_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/corpus")
}

fn engine() -> TrinoEngine {
    let ctx = SessionContext::new();
    for (name, (schema, batch)) in [
        ("customers", customers()),
        ("orders", orders()),
        ("events", events()),
    ] {
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
