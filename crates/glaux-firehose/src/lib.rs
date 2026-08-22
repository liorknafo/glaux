//! Firehose-compatible API and delivery engine for glaux.
//!
//! - [`FirehoseService`] — the delivery stream registry and action
//!   dispatcher: `CreateDeliveryStream`, `DescribeDeliveryStream`,
//!   `ListDeliveryStreams`, `UpdateDestination`, `DeleteDeliveryStream`,
//!   `PutRecord`, `PutRecordBatch` (Direct-PUT sources, S3 destinations).
//!   Real AWS limits are enforced: 1 MiB per record, 4 MiB and 500 records
//!   per batch, stream name rules.
//! - [`StreamBuffer`] — the per-stream buffering engine honoring
//!   `BufferingHints`: flush on `SizeInMBs` **or** `IntervalInSeconds`,
//!   whichever comes first, on per-stream tokio timers; graceful flush on
//!   delete and [`FirehoseService::shutdown`].
//! - [`DeliverySink`] — where flushed batches go. The S3 sink (prefix
//!   semantics, compression, JSON → Parquet conversion) is its own module;
//!   [`RecordingSink`] keeps batches in memory for tests and embedders.
//! - [`http::router`] / [`http::dispatch`] — the AWS JSON 1.1 transport, as
//!   a complete axum router or a request → response function.
//!
//! # Never silently wrong
//!
//! Configuration glaux cannot honor (Kinesis sources, non-S3 destinations,
//! Lambda transforms, dynamic partitioning, KMS, ORC output, ...) is
//! rejected with an error naming the construct. Records are never dropped:
//! a failed flush is reported to the producer or retried, and shutdown
//! flushes every buffer.

pub mod buffer;
pub mod error;
pub mod http;
pub mod model;
pub mod service;
pub mod sink;

pub use buffer::StreamBuffer;
pub use error::FirehoseError;
pub use service::{FirehoseService, FirehoseServiceConfig, validate_stream_name};
pub use sink::{DeliverySink, FlushBatch, FlushReason, RecordingSink, SinkError};
