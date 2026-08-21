//! Glue Data Catalog integration and S3 access for glaux.
//!
//! This crate provides the storage/metadata layer every glaux service
//! consumes:
//!
//! - [`StorageBackend`] — S3-shaped object access (get / ranged get / put /
//!   list) with a network implementation ([`NetworkStorageBackend`]) over
//!   `object_store`, supporting custom endpoints and path-style addressing
//!   for emulators (fakecloud, MinIO) as well as real AWS S3.
//! - [`GlueApi`] — the Glue Data Catalog read surface (`GetDatabases`,
//!   `GetTables`, `GetTable`, `GetPartitions`) with a lightweight network
//!   client ([`NetworkGlueApi`]) speaking Glue's JSON 1.1 wire protocol with
//!   SigV4 signing.
//! - [`GlauxConfig`] — the unified configuration story (TOML file +
//!   `GLAUX_*` environment + CLI overrides) shared by every binary.
//!
//! An **in-process** backend pair (direct fakecloud state calls) is supplied
//! by the AGPL all-in-one binary from its side of the dependency boundary;
//! this crate stays free of fakecloud dependencies.
//!
//! On top of that access layer sits the DataFusion catalog surface:
//!
//! - [`GlueCatalogProvider`] / [`GlueSchemaProvider`] / [`GlueTableProvider`]
//!   — Glue databases as DataFusion schemas, tables as listing-style table
//!   providers over [`StorageBackend`], with Hive/Glue type mapping, SerDe →
//!   reader mapping (Parquet, OpenX JSON → NDJSON, LazySimpleSerDe → CSV),
//!   partition pruning from Glue partition metadata, and Athena-style
//!   partition projection computed locally with zero `GetPartitions` calls.
//!
//! # Never silently wrong
//!
//! Every fallible path errors explicitly with the construct involved: an
//! unreadable object, an unknown config key, an unparsable Glue response.
//! Nothing is fabricated, defaulted-away, or silently skipped.

pub mod config;
pub mod error;
mod format;
pub mod glue;
mod projection;
pub mod provider;
mod schema_adapt;
pub mod storage;
pub mod types;

pub use config::{AthenaConfig, AwsCredentials, ConfigOverrides, FirehoseLimits, GlauxConfig};
pub use error::{CatalogError, Result};
pub use glue::{
    GlueApi, GlueColumn, GlueDatabase, GluePartition, GlueSerDeInfo, GlueStorageDescriptor,
    GlueTable, NetworkGlueApi,
};
pub use provider::{GlueCatalogProvider, GlueSchemaProvider, GlueTableProvider};
pub use storage::{NetworkStorageBackend, ObjectSummary, StorageBackend};
pub use types::hive_type_to_arrow;
