//! Glue Data Catalog integration and S3 access for glaux.
//!
//! This crate will provide:
//!
//! - A `GlueCatalogProvider` implementing DataFusion's catalog traits against
//!   the Glue API: databases, tables, columns, and partitions, with
//!   Hive-style partition pruning from Glue partition metadata and partition
//!   projection (computed partitions with no Glue entries).
//! - SerDe mapping: Parquet SerDe -> native, OpenX JSON SerDe -> NDJSON
//!   reader, LazySimpleSerDe -> CSV reader with configured delimiters.
//! - The internal S3/Glue access trait with two backends: **network** (any
//!   endpoint — fakecloud over HTTP, MinIO, real AWS) and **in-process**
//!   (direct fakecloud state calls; used only by the AGPL all-in-one binary,
//!   which supplies that backend from its side of the dependency boundary).
//!
//! # Never silently wrong
//!
//! Tables whose storage format or SerDe cannot be mapped produce an explicit
//! error naming the SerDe — never an empty or fabricated table.
//!
//! This crate is currently scaffolding (LIO-18): it deliberately exports no
//! API yet. The real modules land in subsequent stories — glaux never stubs
//! a data path with fake behavior in the meantime.
