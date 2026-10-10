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
//! Still to come in later stories: the DataFusion `GlueCatalogProvider`
//! (databases/tables/partitions as DataFusion catalog traits, partition
//! pruning and projection) and SerDe → reader mapping.
//!
//! # Never silently wrong
//!
//! Every fallible path errors explicitly with the construct involved: an
//! unreadable object, an unknown config key, an unparsable Glue response.
//! Nothing is fabricated, defaulted-away, or silently skipped.

pub mod config;
pub mod error;
pub mod glue;
pub mod storage;

pub use config::{AthenaConfig, AwsCredentials, ConfigOverrides, FirehoseLimits, GlauxConfig};
pub use error::{CatalogError, Result};
pub use glue::{
    GlueApi, GlueColumn, GlueDatabase, GluePartition, GlueSerDeInfo, GlueStorageDescriptor,
    GlueTable, NetworkGlueApi,
};
pub use storage::{NetworkStorageBackend, ObjectSummary, StorageBackend};
