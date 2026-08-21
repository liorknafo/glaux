//! Differential fidelity suite for glaux.
//!
//! The SQL corpus under `crates/glaux-athena/tests/corpus` is executed two
//! ways over the *same* fixture data, stored as real Parquet, NDJSON and
//! delimited-text objects behind Glue table definitions:
//!
//! - **record** ([`athena`]) — against real AWS Athena, through a scratch
//!   S3 bucket and Glue database that exist only for the run. Results are
//!   normalized and written to `tests/snapshots/` with an `athena`
//!   provenance header.
//! - **replay** ([`glaux`]) — offline, in CI, against glaux's own Athena
//!   service (`GlueCatalogProvider` over an in-memory object store, the
//!   `TrinoEngine`, and the `AthenaService` API surface), diffing every
//!   outcome against its snapshot ([`snapshot::diff`]).
//!
//! When no AWS profile is available the snapshots are recorded from glaux
//! itself and carry an explicit `UNVERIFIED` provenance — the harness and
//! CI wiring are complete, and a later `record` run upgrades them.
//!
//! [`firehose`] applies the same idea to Firehose delivery artifacts: S3
//! object-key patterns and Parquet round-trips are snapshotted with the
//! variable parts (timestamps, UUIDs) masked.
//!
//! [`coverage`] renders the corpus results as the "Differential fidelity
//! corpus" section appended to `docs/sql-coverage.md`.
//!
//! # Never silently wrong
//!
//! A snapshot that cannot be parsed, a case without a snapshot, a column
//! whose Athena type differs, a row that does not match within the
//! documented tolerances, or an error that does not name the construct the
//! corpus expects — every one is a reported difference, never a pass.

pub mod athena;
pub mod corpus;
pub mod coverage;
pub mod firehose;
pub mod fixtures;
pub mod glaux;
pub mod memory;
pub mod snapshot;
pub mod suite;

use std::path::{Path, PathBuf};

/// Error type for every fallible harness operation.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct HarnessError(pub String);

impl HarnessError {
    /// Build an error from any displayable message.
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl From<std::io::Error> for HarnessError {
    fn from(e: std::io::Error) -> Self {
        Self(e.to_string())
    }
}

/// Convenience result alias.
pub type Result<T> = std::result::Result<T, HarnessError>;

/// The repository root (this crate lives in `crates/glaux-fidelity`).
pub fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("crate lives in <repo>/crates/glaux-fidelity")
}

/// Where the SQL corpus lives.
pub fn corpus_dir() -> PathBuf {
    repo_root().join("crates/glaux-athena/tests/corpus")
}

/// Where the snapshots live.
pub fn snapshot_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/snapshots")
}

/// The Glue database the fixture tables are registered in (both in the
/// in-memory catalog and, suffixed, in the scratch AWS account).
pub const DATABASE: &str = "glaux_fidelity";
