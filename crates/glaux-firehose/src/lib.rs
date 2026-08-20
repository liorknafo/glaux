//! Firehose-compatible API and delivery engine for glaux.
//!
//! This crate will provide:
//!
//! - The Firehose API actions for v0.1: `CreateDeliveryStream`,
//!   `DescribeDeliveryStream`, `ListDeliveryStreams`, `UpdateDestination`,
//!   `DeleteDeliveryStream`, `PutRecord`, `PutRecordBatch` (Direct-PUT
//!   sources only).
//! - A real delivery engine: buffering that honors `BufferingHints`
//!   (`SizeInMBs` / `IntervalInSeconds`, flush on first threshold, per-stream
//!   tokio timers), S3 writes with real prefix semantics (default
//!   `YYYY/MM/DD/HH/`, custom `!{timestamp:...}` expressions,
//!   `ErrorOutputPrefix`, GZIP), and JSON -> Parquet record format conversion
//!   driven by the Glue table schema configured on the stream.
//!
//! # Never silently wrong
//!
//! Records that cannot be converted are routed to the error prefix like real
//! Firehose — never dropped, never written with fabricated contents.
//!
//! This crate is currently scaffolding (LIO-18): it deliberately exports no
//! API yet. The real modules land in subsequent stories — glaux never stubs
//! a data path with fake behavior in the meantime.
