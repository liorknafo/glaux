//! Running the corpus against glaux and reconciling it with the snapshots.

use std::path::Path;

use chrono::Utc;

use crate::corpus::{Case, Compare};
use crate::glaux::GlauxAthena;
use crate::snapshot::{Outcome, Provenance, Snapshot, diff};
use crate::{HarnessError, Result};

/// How one case fared in a replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CaseResult {
    /// glaux agrees with the snapshot.
    Match,
    /// glaux disagrees; the listed differences.
    Mismatch(Vec<String>),
    /// No snapshot exists for the case.
    MissingSnapshot,
}

/// One case's replay report.
#[derive(Debug, Clone)]
pub struct CaseReport {
    /// The case.
    pub case: Case,
    /// What glaux produced.
    pub actual: Outcome,
    /// The snapshot's provenance, when one exists.
    pub provenance: Option<Provenance>,
    /// The verdict.
    pub result: CaseResult,
}

impl CaseReport {
    /// `true` when the case matched its snapshot.
    pub fn passed(&self) -> bool {
        self.result == CaseResult::Match
    }
}

/// Today's date for provenance headers.
pub fn today() -> String {
    Utc::now().format("%Y-%m-%d").to_string()
}

/// Run every case against glaux and diff it with its snapshot.
pub async fn replay(cases: &[Case], snapshot_dir: &Path) -> Result<Vec<CaseReport>> {
    let glaux = GlauxAthena::new().await?;
    let mut reports = Vec::with_capacity(cases.len());
    for case in cases {
        let actual = glaux.run(&case.sql).await?;
        let snapshot = Snapshot::read(snapshot_dir, &case.name)?;
        let (provenance, result) = match snapshot {
            None => (None, CaseResult::MissingSnapshot),
            Some(snap) => {
                let differences = diff(
                    &snap.outcome,
                    &actual,
                    effective_compare(case, snap.compare),
                    case.expect_error.as_deref(),
                );
                let result = if differences.is_empty() {
                    CaseResult::Match
                } else {
                    CaseResult::Mismatch(differences)
                };
                (Some(snap.provenance), result)
            }
        };
        reports.push(CaseReport {
            case: case.clone(),
            actual,
            provenance,
            result,
        });
    }
    Ok(reports)
}

/// The corpus file decides how rows compare; a snapshot recorded under a
/// different mode is compared the stricter way so neither side can relax
/// the contract unilaterally.
fn effective_compare(case: &Case, recorded: Compare) -> Compare {
    if case.compare == Compare::Ordered || recorded == Compare::Ordered {
        Compare::Ordered
    } else {
        Compare::Unordered
    }
}

/// Write snapshots from glaux's own output, marked `UNVERIFIED`. Existing
/// snapshots recorded against real Athena are never overwritten by this
/// path: downgrading a verified expectation to a self-recorded one would
/// hide a regression.
pub async fn self_record(cases: &[Case], snapshot_dir: &Path) -> Result<Vec<String>> {
    let glaux = GlauxAthena::new().await?;
    let mut written = Vec::new();
    let recorded = today();
    for case in cases {
        if let Some(existing) = Snapshot::read(snapshot_dir, &case.name)?
            && existing.provenance.is_verified()
        {
            continue;
        }
        let outcome = glaux.run(&case.sql).await?;
        if let (Some(needle), Outcome::Failed { message }) = (&case.expect_error, &outcome)
            && !message.contains(needle.as_str())
        {
            return Err(HarnessError::new(format!(
                "{}: glaux's error does not name {needle:?}: {message}",
                case.name
            )));
        }
        if let (None, Outcome::Failed { message }) = (&case.expect_error, &outcome) {
            return Err(HarnessError::new(format!(
                "{}: positive case failed on glaux, refusing to snapshot a failure: {message}",
                case.name
            )));
        }
        Snapshot {
            case: case.name.clone(),
            provenance: Provenance::Unverified {
                recorded: recorded.clone(),
            },
            compare: case.compare,
            outcome,
        }
        .write(snapshot_dir)?;
        written.push(case.name.clone());
    }
    Ok(written)
}

/// Snapshots on disk that no corpus case refers to.
pub fn orphans(cases: &[Case], snapshot_dir: &Path) -> Result<Vec<String>> {
    let mut out = Vec::new();
    if !snapshot_dir.is_dir() {
        return Ok(out);
    }
    for entry in std::fs::read_dir(snapshot_dir)? {
        let path = entry?.path();
        if path.extension().is_some_and(|e| e == "snap") {
            let stem = path.file_stem().unwrap().to_string_lossy().into_owned();
            if !cases.iter().any(|c| c.name == stem) && !stem.starts_with("firehose_") {
                out.push(stem);
            }
        }
    }
    out.sort();
    Ok(out)
}
