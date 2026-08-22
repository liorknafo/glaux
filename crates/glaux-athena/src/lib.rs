//! Athena-compatible API surface and SQL execution engine for glaux.
//!
//! - [`AthenaService`] — the query lifecycle state machine and action
//!   dispatcher: `StartQueryExecution`, `GetQueryExecution`,
//!   `GetQueryResults` (paginated, Athena `Row`/`Datum` encoding),
//!   `StopQueryExecution`, `ListQueryExecutions`, `BatchGetQueryExecution`,
//!   and basic workgroups. Queries move `QUEUED → RUNNING →
//!   SUCCEEDED | FAILED | CANCELLED` on tokio tasks; cancellation aborts the
//!   engine mid-flight. Results are also written as CSV to the
//!   `OutputLocation`, as real Athena does.
//! - [`QueryEngine`] — the execution seam. [`TrinoEngine`] is the engine
//!   Athena clients should get: Trino-dialect parse → shim rewrite →
//!   DataFusion plan (see [`dialect`]). [`DataFusionEngine`] underneath it
//!   speaks DataFusion's own dialect.
//! - [`http::router`] / [`http::dispatch`] — the AWS JSON 1.1 transport, as
//!   a complete axum router or a request → response function.
//!
//! # Never silently wrong
//!
//! Any SQL construct this engine cannot execute faithfully produces an
//! explicit error naming the construct, surfaced as a `FAILED` query with
//! `AthenaError` details. Unsupported actions, unmappable result types, and
//! unwritable result locations all fail explicitly. This crate never
//! synthesizes query results.

pub mod dialect;
pub mod engine;
pub mod error;
pub mod http;
pub mod model;
pub mod results;
pub mod service;

pub use dialect::{GlauxSqlError, TrinoEngine};
pub use engine::{DataFusionEngine, EngineError, QueryEngine, QueryOutput, QueryRequest};
pub use error::AthenaError;
pub use model::QueryState;
pub use results::{EncodedResultSet, ResultError, encode_result_set};
pub use service::{AthenaService, AthenaServiceConfig, ENGINE_VERSION};
