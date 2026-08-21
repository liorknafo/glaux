//! The hand-off between the buffering engine and the thing that writes
//! objects: [`DeliverySink`].
//!
//! The buffering engine knows nothing about S3, prefixes, compression, or
//! format conversion; when a buffer flushes it hands a [`FlushBatch`] — the
//! records plus a snapshot of the destination they were accepted under — to
//! the sink. The S3 sink lives in its own module; [`RecordingSink`] is the
//! in-memory implementation tests and embedders use to observe flushes.

use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::Notify;

use crate::model::ExtendedS3DestinationDescription;

/// Why a buffer flushed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushReason {
    /// The buffered bytes reached `SizeInMBs`.
    Size,
    /// The buffer's age reached `IntervalInSeconds`.
    Interval,
    /// The stream is being deleted.
    Delete,
    /// The service is shutting down.
    Shutdown,
}

/// One buffer's worth of records, ready for delivery.
#[derive(Debug, Clone)]
pub struct FlushBatch {
    /// The delivery stream name.
    pub stream_name: String,
    /// The delivery stream ARN.
    pub stream_arn: String,
    /// The delivery stream `VersionId` the destination was accepted under
    /// (the `<DeliveryStreamVersion>` component of S3 object names).
    pub stream_version: String,
    /// The destination as configured when the flush happened.
    pub destination: Arc<ExtendedS3DestinationDescription>,
    /// The records, in acceptance order, exactly as the producer sent them.
    pub records: Vec<Bytes>,
    /// Why the flush happened.
    pub reason: FlushReason,
    /// When the first record of this buffer was accepted.
    pub opened_at: SystemTime,
    /// When the flush was triggered.
    pub flushed_at: SystemTime,
}

impl FlushBatch {
    /// Total payload bytes.
    pub fn total_bytes(&self) -> usize {
        self.records.iter().map(Bytes::len).sum()
    }
}

/// A sink failed to deliver a batch. The buffer keeps the records and
/// retries on the next interval; producers blocked on a size flush see it
/// as `ServiceUnavailableException`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct SinkError {
    /// Diagnostic naming what failed.
    pub message: String,
}

impl SinkError {
    /// Build a sink error.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

/// Receives flushed buffers.
#[async_trait]
pub trait DeliverySink: Send + Sync + 'static {
    /// Deliver one batch. Must either persist every record or return an
    /// error; partial success is not a valid outcome.
    async fn deliver(&self, batch: FlushBatch) -> Result<(), SinkError>;
}

/// A sink that records every batch in memory and can be told to fail.
///
/// Handy for tests and for embedders that want to observe what the engine
/// would have written.
#[derive(Default)]
pub struct RecordingSink {
    batches: Mutex<Vec<FlushBatch>>,
    fail_with: Mutex<Option<String>>,
    notify: Notify,
}

impl std::fmt::Debug for RecordingSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecordingSink")
            .field("batches", &self.batches.lock().unwrap().len())
            .finish()
    }
}

impl RecordingSink {
    /// An empty, always-succeeding sink.
    pub fn new() -> Self {
        Self::default()
    }

    /// Every batch delivered so far, in order.
    pub fn batches(&self) -> Vec<FlushBatch> {
        self.batches.lock().unwrap().clone()
    }

    /// Number of batches delivered so far.
    pub fn len(&self) -> usize {
        self.batches.lock().unwrap().len()
    }

    /// Whether nothing has been delivered.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Make every subsequent `deliver` fail with `message` (`None` to
    /// succeed again).
    pub fn set_failure(&self, message: Option<&str>) {
        *self.fail_with.lock().unwrap() = message.map(str::to_string);
    }

    /// Wait until at least `n` batches have been delivered.
    pub async fn wait_for(&self, n: usize) {
        loop {
            let notified = self.notify.notified();
            if self.len() >= n {
                return;
            }
            notified.await;
        }
    }
}

#[async_trait]
impl DeliverySink for RecordingSink {
    async fn deliver(&self, batch: FlushBatch) -> Result<(), SinkError> {
        if let Some(message) = self.fail_with.lock().unwrap().clone() {
            return Err(SinkError::new(message));
        }
        self.batches.lock().unwrap().push(batch);
        self.notify.notify_waiters();
        Ok(())
    }
}
