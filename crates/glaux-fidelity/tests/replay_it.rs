//! The offline differential run, as CI executes it: every corpus case and
//! Firehose artifact scenario must match its snapshot, the snapshots must
//! be well-formed and complete, and `docs/sql-coverage.md` must be the
//! document a fresh replay renders.

use glaux_fidelity::coverage;
use glaux_fidelity::snapshot::{Outcome, Provenance};
use glaux_fidelity::suite::{self, CaseResult};
use glaux_fidelity::{corpus, corpus_dir, firehose, repo_root, snapshot_dir};

#[tokio::test]
async fn every_corpus_case_matches_its_snapshot() {
    let cases = corpus::load(&corpus_dir()).unwrap();
    let reports = suite::replay(&cases, &snapshot_dir()).await.unwrap();
    let mut failures = Vec::new();
    for report in &reports {
        match &report.result {
            CaseResult::Match => {}
            CaseResult::MissingSnapshot => failures.push(format!(
                "{}: no snapshot (run `cargo run -p glaux-fidelity -- replay --update`, or `record`)",
                report.case.name
            )),
            CaseResult::Mismatch(differences) => failures.push(format!(
                "{}:\n    {}",
                report.case.name,
                differences.join("\n    ")
            )),
            CaseResult::MatchUnverifiedError { recorded, actual } => failures.push(format!(
                "{}: both FAILED but the errors could not be compared (no needle, no error code)\n    recorded: {recorded}\n    glaux:    {actual}",
                report.case.name
            )),
        }
    }
    for orphan in suite::orphans(&cases, &snapshot_dir()).unwrap() {
        failures.push(format!("{orphan}.snap: no corpus case of that name"));
    }
    assert!(
        failures.is_empty(),
        "{} fidelity failure(s):\n\n{}",
        failures.len(),
        failures.join("\n\n")
    );
    // The corpus must cover the acceptance surface.
    let covers = |needle: &str| cases.iter().any(|c| c.name.contains(needle));
    for needle in ["join", "cte", "subquer", "window", "group_by", "neg_"] {
        assert!(covers(needle), "corpus has no case named *{needle}*");
    }
    for table in ["customers", "orders", "countries"] {
        assert!(
            cases.iter().any(|c| c.tables.contains(&table)),
            "corpus never reads {table}"
        );
    }
}

#[tokio::test]
async fn firehose_artifacts_match_their_snapshots() {
    let reports = firehose::replay(&snapshot_dir()).await.unwrap();
    assert_eq!(reports.len(), firehose::SCENARIOS.len());
    let failures: Vec<String> = reports
        .iter()
        .filter_map(|r| match &r.result {
            CaseResult::Match => None,
            CaseResult::MissingSnapshot => Some(format!("{}: no snapshot", r.name)),
            CaseResult::Mismatch(d) => Some(format!("{}:\n    {}", r.name, d.join("\n    "))),
            CaseResult::MatchUnverifiedError { recorded, actual } => Some(format!(
                "{}: unverified error pair\n    recorded: {recorded}\n    glaux: {actual}",
                r.name
            )),
        })
        .collect();
    assert!(failures.is_empty(), "{}", failures.join("\n\n"));
}

/// The Parquet object Firehose writes for the `orders` fixture must decode
/// to exactly what Athena (glaux) returns for the same rows: the two data
/// paths share one Glue schema and one text encoding.
#[tokio::test]
async fn firehose_parquet_round_trips_through_athena_text_form() {
    let artifact = firehose::run("firehose_parquet_conversion").await.unwrap();
    let Outcome::Succeeded { rows, .. } = artifact else {
        panic!("scenario failed");
    };
    let parquet_lines: Vec<String> = rows
        .iter()
        .filter(|r| r[1].as_deref() == Some("parquet"))
        .filter_map(|r| r[3].clone())
        .collect();
    assert!(parquet_lines.len() > 1, "no parquet rows: {rows:?}");
    assert_eq!(
        parquet_lines[0],
        "id bigint, customer_id bigint, amount double, status varchar, created_at timestamp, \
         note varchar, rush boolean"
    );
    let error_lines = rows
        .iter()
        .filter(|r| r[1].as_deref() == Some("error"))
        .count();
    assert_eq!(
        error_lines, 2,
        "two malformed records must land in the error object"
    );

    let athena = glaux_fidelity::glaux::GlauxAthena::new().await.unwrap();
    let Outcome::Succeeded { rows: sql_rows, .. } = athena
        .run("SELECT id, customer_id, amount, status, created_at, note, rush FROM orders ORDER BY id")
        .await
        .unwrap()
    else {
        panic!("query failed");
    };
    let sql_lines: Vec<String> = sql_rows
        .iter()
        .map(|r| {
            serde_json::Value::Array(
                r.iter()
                    .map(|c| {
                        c.clone()
                            .map(serde_json::Value::String)
                            .unwrap_or(serde_json::Value::Null)
                    })
                    .collect(),
            )
            .to_string()
        })
        .collect();
    assert_eq!(parquet_lines[1..], sql_lines[..]);
}

#[tokio::test]
async fn coverage_doc_carries_a_fresh_fidelity_section() {
    let cases = corpus::load(&corpus_dir()).unwrap();
    let reports = suite::replay(&cases, &snapshot_dir()).await.unwrap();
    let artifacts = firehose::replay(&snapshot_dir()).await.unwrap();
    let rendered = coverage::render_document(&reports, &artifacts);
    let path = repo_root().join("docs/sql-coverage.md");
    let on_disk = std::fs::read_to_string(&path).unwrap_or_default();
    assert_eq!(
        on_disk,
        rendered,
        "{} is stale: run `cargo run -p glaux-fidelity -- coverage`",
        path.display()
    );
    // Provenance is stated, never implied.
    let unverified = reports
        .iter()
        .filter(|r| matches!(r.provenance, Some(Provenance::Unverified { .. })))
        .count();
    if unverified > 0 {
        assert!(
            on_disk.contains("UNVERIFIED"),
            "unverified snapshots must be called out"
        );
    }
}
