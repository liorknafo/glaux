//! Athena-compatible API surface and SQL execution engine for glaux.
//!
//! This crate will provide:
//!
//! - The Athena API actions for v0.1: `StartQueryExecution`,
//!   `GetQueryExecution`, `GetQueryResults` (paginated),
//!   `StopQueryExecution`, `ListQueryExecutions`, `BatchGetQueryExecution`,
//!   and basic workgroup support, with the faithful async query lifecycle
//!   (`QUEUED -> RUNNING -> SUCCEEDED/FAILED`).
//! - Real SQL execution: Trino-dialect parsing via `sqlparser`, translation
//!   through a function-shim layer, and execution on Apache DataFusion over
//!   data in S3.
//! - Results written as CSV + `.metadata` to the configured S3
//!   `OutputLocation`, matching real Athena.
//!
//! # Never silently wrong
//!
//! Any SQL construct this engine cannot execute faithfully produces an
//! explicit error naming the construct. This crate never synthesizes query
//! results.
//!
//! This crate is currently scaffolding (LIO-18): it deliberately exports no
//! API yet. The real modules land in subsequent stories — glaux never stubs
//! a data path with fake behavior in the meantime.
