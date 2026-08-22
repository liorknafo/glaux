//! Keeps the shim registry, DataFusion, the corpus, and `docs/sql-coverage.md`
//! in agreement:
//!
//! - every DataFusion function a shim targets exists in the session the
//!   engine builds (a DataFusion upgrade that renames one fails here);
//! - every supported function and construct is exercised by at least one
//!   corpus query, so the "supported" column is backed by a test;
//! - every function the registry refuses really is refused by name, with
//!   plausible arguments;
//! - the checked-in coverage doc equals what the registry renders
//!   (`GLAUX_REGEN_DOCS=1` rewrites it).

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use datafusion::prelude::SessionContext;
use glaux_athena::dialect::registry::{CONSTRUCTS, ConstructStatus, FUNCTIONS, ShimKind};
use glaux_athena::dialect::{GlauxSqlError, registry, translate};
use glaux_athena::{QueryEngine, QueryRequest, TrinoEngine};

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn corpus_text() -> (String, String) {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/corpus");
    let mut positive = String::new();
    let mut negative = String::new();
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_some_and(|e| e == "sql") {
            let text = fs::read_to_string(&path).unwrap();
            let body: String = text
                .lines()
                .filter(|l| !l.starts_with("--"))
                .collect::<Vec<_>>()
                .join("\n");
            if text.contains("-- error:") {
                negative.push_str(&body);
            } else {
                positive.push_str(&body);
            }
            positive.push('\n');
        }
    }
    (positive.to_uppercase(), negative.to_uppercase())
}

#[test]
fn every_translation_target_exists_in_datafusion() {
    let engine = TrinoEngine::new(SessionContext::new(), "datafusion");
    let state = engine.context().state();
    let mut known: HashSet<String> = HashSet::new();
    known.extend(state.scalar_functions().keys().cloned());
    known.extend(state.aggregate_functions().keys().cloned());
    known.extend(state.window_functions().keys().cloned());
    for udf in state.scalar_functions().values() {
        known.extend(udf.aliases().iter().cloned());
    }
    for udaf in state.aggregate_functions().values() {
        known.extend(udaf.aliases().iter().cloned());
    }
    let missing: Vec<String> = registry::datafusion_targets()
        .iter()
        .filter(|name| !known.contains(**name))
        .map(|n| n.to_string())
        .collect();
    assert!(
        missing.is_empty(),
        "registry targets DataFusion functions that do not exist: {missing:?}"
    );
}

#[test]
fn every_supported_function_and_construct_is_exercised_by_the_corpus() {
    let (positive, negative) = corpus_text();
    let mut missing = Vec::new();
    for shim in FUNCTIONS.iter().filter(|s| s.kind != ShimKind::Unsupported) {
        let upper = shim.name.to_uppercase();
        let used = positive.contains(&format!("{upper}("))
            || positive.contains(&format!(" {upper} "))
            || positive.contains(&format!(" {upper}\n"))
            || positive.contains(&format!(" {upper},"));
        if !used {
            missing.push(format!("function {}", shim.name));
        }
    }
    for construct in CONSTRUCTS
        .iter()
        .filter(|c| c.status == ConstructStatus::Supported)
    {
        assert!(
            !construct.corpus_marker.is_empty(),
            "{} needs a corpus marker",
            construct.name
        );
        // Constructs whose observable behaviour is an error (type checking,
        // overflow) are exercised by negative cases.
        let marker = construct.corpus_marker.to_uppercase();
        if !positive.contains(&marker) && !negative.contains(&marker) {
            missing.push(format!(
                "construct {} (marker {:?})",
                construct.name, construct.corpus_marker
            ));
        }
    }
    assert!(
        missing.is_empty(),
        "supported but not covered by tests/corpus: {missing:#?}"
    );
}

#[tokio::test]
async fn every_refused_function_is_refused_by_name() {
    let engine = TrinoEngine::new(SessionContext::new(), "datafusion");
    for shim in FUNCTIONS.iter().filter(|s| s.kind == ShimKind::Unsupported) {
        let sql = format!("SELECT {}(1, 2)", shim.name);
        // Translation alone must already refuse it...
        match translate(&sql) {
            Err(GlauxSqlError::Unsupported { construct, .. }) => {
                assert_eq!(construct, format!("function {}", shim.name))
            }
            other => panic!("{}: expected Unsupported, got {other:?}", shim.name),
        }
        // ...and so must the engine, as a user error naming the function.
        let err = engine
            .execute(QueryRequest {
                sql,
                catalog: None,
                database: None,
            })
            .await
            .unwrap_err();
        assert_eq!(err.category(), 2, "{}", shim.name);
        assert!(
            err.to_string().contains(&format!("function {}", shim.name)),
            "{}: {err}",
            shim.name
        );
    }
    for construct in CONSTRUCTS
        .iter()
        .filter(|c| c.status == ConstructStatus::Unsupported)
    {
        assert!(
            !construct.notes.is_empty(),
            "{} must explain what to do instead",
            construct.name
        );
    }
}

#[test]
fn coverage_doc_is_fresh() {
    // The document is the registry rendering followed by the differential
    // fidelity section that `glaux-fidelity` appends (and verifies in its
    // own tests). Regenerating here rewrites only the registry part and
    // keeps the section.
    const SECTION_MARKER: &str = "<!-- GENERATED by glaux-fidelity";
    let path = repo_root().join("docs/sql-coverage.md");
    let rendered = registry::render_coverage();
    let on_disk = fs::read_to_string(&path).unwrap_or_default();
    let section = on_disk
        .find(SECTION_MARKER)
        .map(|i| on_disk[i..].to_string())
        .unwrap_or_default();
    if std::env::var_os("GLAUX_REGEN_DOCS").is_some() {
        let mut doc = rendered;
        if !section.is_empty() {
            doc.push('\n');
            doc.push_str(&section);
        }
        fs::write(&path, doc).unwrap();
        return;
    }
    let registry_part = match on_disk.find(SECTION_MARKER) {
        Some(i) => on_disk[..i].trim_end_matches('\n').to_string() + "\n",
        None => on_disk.clone(),
    };
    assert_eq!(
        registry_part,
        rendered,
        "{} is stale: run `GLAUX_REGEN_DOCS=1 cargo test -p glaux-athena --test coverage_doc`",
        path.display()
    );
}
