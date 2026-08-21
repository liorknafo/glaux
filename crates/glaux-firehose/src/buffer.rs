//! The per-stream buffering engine.
//!
//! Each delivery stream owns a [`StreamBuffer`]: records accumulate until
//! either the buffered bytes reach `SizeInMBs` or the buffer's age reaches
//! `IntervalInSeconds` — whichever comes first — and the accumulated
//! records are handed to the [`DeliverySink`] as one [`FlushBatch`].
//!
//! # Semantics (matching the real service)
//!
//! - The interval clock starts when the first record enters an empty
//!   buffer, not when the stream was created or last flushed. An idle
//!   stream never flushes empty batches.
//! - The size threshold is checked after each record is appended; the
//!   record that crosses it is included in the flushed batch.
//! - `UpdateDestination` changes apply to the *next* flush decision: the
//!   deadline of an already-open buffer is recomputed against the new
//!   interval, and the destination snapshot handed to the sink is whatever
//!   is current at flush time.
//!
//! # Failure handling
//!
//! A sink failure never loses data silently. On a size-triggered flush the
//! previously buffered records are restored and the producer receives the
//! error for the record it just sent (so a retrying client behaves
//! correctly). On an interval-triggered flush the records are restored and
//! retried after another interval, with the failure logged through
//! `tracing`.

use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::model::ExtendedS3DestinationDescription;
use crate::sink::{DeliverySink, FlushBatch, FlushReason, SinkError};

/// Records waiting in a buffer, together with when the buffer was opened.
struct Pending {
    records: Vec<Bytes>,
    bytes: usize,
    /// Wall-clock opening time, reported to the sink.
    opened_at: SystemTime,
    /// Tokio-clock opening time, used for deadline arithmetic (so paused
    /// test clocks and the real clock behave identically).
    opened_instant: Instant,
}

struct Inner {
    destination: Arc<ExtendedS3DestinationDescription>,
    size_bytes: usize,
    interval: Duration,
    pending: Option<Pending>,
    /// When the open buffer must flush; `None` while the buffer is empty.
    deadline: Option<Instant>,
    closed: bool,
}

/// The buffer for one delivery stream. Create with [`StreamBuffer::spawn`];
/// the interval timer runs on its own tokio task until [`close`] is called.
///
/// [`close`]: StreamBuffer::close
pub struct StreamBuffer {
    stream_name: String,
    stream_arn: String,
    sink: Arc<dyn DeliverySink>,
    inner: Mutex<Inner>,
    /// Woken whenever `deadline` changes so the timer task re-reads it.
    wake: Notify,
    timer: Mutex<Option<JoinHandle<()>>>,
}

impl std::fmt::Debug for StreamBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.lock().unwrap();
        f.debug_struct("StreamBuffer")
            .field("stream_name", &self.stream_name)
            .field("size_bytes", &inner.size_bytes)
            .field("interval", &inner.interval)
            .field(
                "buffered_records",
                &inner.pending.as_ref().map_or(0, |p| p.records.len()),
            )
            .field("closed", &inner.closed)
            .finish()
    }
}

fn usize_from(bytes: u64) -> usize {
    usize::try_from(bytes).unwrap_or(usize::MAX)
}

impl StreamBuffer {
    /// Create the buffer for `stream_name` and start its interval timer.
    pub fn spawn(
        stream_name: impl Into<String>,
        stream_arn: impl Into<String>,
        destination: Arc<ExtendedS3DestinationDescription>,
        sink: Arc<dyn DeliverySink>,
    ) -> Arc<Self> {
        let hints = destination.buffering_hints.resolved();
        let buffer = Arc::new(Self {
            stream_name: stream_name.into(),
            stream_arn: stream_arn.into(),
            sink,
            inner: Mutex::new(Inner {
                destination,
                size_bytes: usize_from(hints.size_bytes()),
                interval: hints.interval(),
                pending: None,
                deadline: None,
                closed: false,
            }),
            wake: Notify::new(),
            timer: Mutex::new(None),
        });
        let task = tokio::spawn(Arc::clone(&buffer).run_timer());
        *buffer.timer.lock().unwrap() = Some(task);
        buffer
    }

    /// The stream name.
    pub fn stream_name(&self) -> &str {
        &self.stream_name
    }

    /// The destination the next flush will be delivered under.
    pub fn destination(&self) -> Arc<ExtendedS3DestinationDescription> {
        Arc::clone(&self.inner.lock().unwrap().destination)
    }

    /// Number of records currently buffered.
    pub fn buffered_records(&self) -> usize {
        self.inner
            .lock()
            .unwrap()
            .pending
            .as_ref()
            .map_or(0, |p| p.records.len())
    }

    /// Bytes currently buffered.
    pub fn buffered_bytes(&self) -> usize {
        self.inner
            .lock()
            .unwrap()
            .pending
            .as_ref()
            .map_or(0, |p| p.bytes)
    }

    /// Replace the destination (after `UpdateDestination`). New thresholds
    /// apply immediately: an open buffer's deadline is recomputed from its
    /// opening time, and a buffer already over the new size threshold is
    /// flushed by the timer task right away.
    pub fn update_destination(&self, destination: Arc<ExtendedS3DestinationDescription>) {
        let hints = destination.buffering_hints.resolved();
        {
            let mut inner = self.inner.lock().unwrap();
            inner.size_bytes = usize_from(hints.size_bytes());
            inner.interval = hints.interval();
            inner.destination = destination;
            if let Some(pending) = &inner.pending {
                let over_size = pending.bytes >= inner.size_bytes;
                inner.deadline = Some(if over_size {
                    Instant::now()
                } else {
                    pending.opened_instant + inner.interval
                });
            }
        }
        self.wake.notify_one();
    }

    /// Accept one record. Returns once the record is buffered, or — when it
    /// crosses the size threshold — once the resulting batch has been
    /// delivered. On delivery failure the record is *not* accepted (the
    /// caller must report the error), while the previously buffered records
    /// stay queued for the interval retry.
    pub async fn push(&self, data: Bytes) -> Result<(), SinkError> {
        let flush = {
            let mut inner = self.inner.lock().unwrap();
            if inner.closed {
                return Err(SinkError::new(format!(
                    "delivery stream {} is no longer accepting records",
                    self.stream_name
                )));
            }
            let interval = inner.interval;
            let size_bytes = inner.size_bytes;
            let pending = inner.pending.get_or_insert_with(|| Pending {
                records: Vec::new(),
                bytes: 0,
                opened_at: SystemTime::now(),
                opened_instant: Instant::now(),
            });
            let newly_opened = pending.records.is_empty();
            pending.bytes += data.len();
            pending.records.push(data);
            if pending.bytes >= size_bytes {
                inner.deadline = None;
                let pending = inner.pending.take().expect("pending was just populated");
                Some(self.batch(&inner, pending, FlushReason::Size))
            } else {
                if newly_opened {
                    inner.deadline = Some(Instant::now() + interval);
                }
                None
            }
        };
        self.wake.notify_one();

        let Some(batch) = flush else {
            return Ok(());
        };
        match self.sink.deliver(batch.clone()).await {
            Ok(()) => Ok(()),
            Err(err) => {
                let mut records = batch.records;
                records.pop(); // the record being pushed: the caller owns retrying it
                if !records.is_empty() {
                    self.restore(records, batch.opened_at);
                }
                Err(err)
            }
        }
    }

    /// Flush whatever is buffered right now with `reason`, regardless of
    /// thresholds. Records are restored on failure.
    pub async fn flush(&self, reason: FlushReason) -> Result<(), SinkError> {
        let batch = {
            let mut inner = self.inner.lock().unwrap();
            inner.deadline = None;
            let Some(pending) = inner.pending.take() else {
                return Ok(());
            };
            self.batch(&inner, pending, reason)
        };
        self.wake.notify_one();
        let opened_at = batch.opened_at;
        let records = batch.records.clone();
        match self.sink.deliver(batch).await {
            Ok(()) => Ok(()),
            Err(err) => {
                self.restore(records, opened_at);
                Err(err)
            }
        }
    }

    /// Stop the interval timer and refuse further records. Call
    /// [`flush`](Self::flush) afterwards to deliver what is left.
    pub fn close(&self) {
        self.inner.lock().unwrap().closed = true;
        if let Some(task) = self.timer.lock().unwrap().take() {
            task.abort();
        }
    }

    /// Stop the timer, then deliver any remaining records with `reason`.
    pub async fn close_and_flush(&self, reason: FlushReason) -> Result<(), SinkError> {
        self.close();
        self.flush(reason).await
    }

    fn batch(&self, inner: &Inner, pending: Pending, reason: FlushReason) -> FlushBatch {
        FlushBatch {
            stream_name: self.stream_name.clone(),
            stream_arn: self.stream_arn.clone(),
            destination: Arc::clone(&inner.destination),
            records: pending.records,
            reason,
            opened_at: pending.opened_at,
            flushed_at: SystemTime::now(),
        }
    }

    /// Put records back at the front of the buffer after a failed delivery
    /// and schedule a retry one interval out.
    fn restore(&self, mut records: Vec<Bytes>, opened_at: SystemTime) {
        {
            let mut inner = self.inner.lock().unwrap();
            let interval = inner.interval;
            match inner.pending.as_mut() {
                Some(pending) => {
                    records.append(&mut pending.records);
                    pending.records = records;
                    pending.bytes = pending.records.iter().map(Bytes::len).sum();
                    pending.opened_at = opened_at;
                }
                None => {
                    let bytes = records.iter().map(Bytes::len).sum();
                    inner.pending = Some(Pending {
                        records,
                        bytes,
                        opened_at,
                        opened_instant: Instant::now(),
                    });
                }
            }
            if !inner.closed {
                inner.deadline = Some(Instant::now() + interval);
            }
        }
        self.wake.notify_one();
    }

    async fn run_timer(self: Arc<Self>) {
        loop {
            let deadline = self.inner.lock().unwrap().deadline;
            match deadline {
                None => self.wake.notified().await,
                Some(deadline) => {
                    tokio::select! {
                        _ = tokio::time::sleep_until(deadline) => {
                            if let Err(err) = self.flush(FlushReason::Interval).await {
                                tracing::error!(
                                    stream = %self.stream_name,
                                    error = %err,
                                    "interval flush failed; records retained for retry"
                                );
                            }
                        }
                        _ = self.wake.notified() => {}
                    }
                }
            }
        }
    }
}

impl Drop for StreamBuffer {
    fn drop(&mut self) {
        if let Some(task) = self.timer.get_mut().unwrap().take() {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{BufferingHints, CompressionFormat, EncryptionConfiguration};
    use crate::sink::RecordingSink;

    fn destination(size_mb: u64, interval_s: u64) -> Arc<ExtendedS3DestinationDescription> {
        Arc::new(ExtendedS3DestinationDescription {
            role_arn: "arn:aws:iam::000000000000:role/firehose".into(),
            bucket_arn: "arn:aws:s3:::bucket".into(),
            prefix: None,
            error_output_prefix: None,
            buffering_hints: BufferingHints {
                size_in_m_bs: Some(size_mb),
                interval_in_seconds: Some(interval_s),
            },
            compression_format: CompressionFormat::Uncompressed,
            encryption_configuration: EncryptionConfiguration::default(),
            cloud_watch_logging_options: None,
            processing_configuration: None,
            s3_backup_mode: None,
            data_format_conversion_configuration: None,
            dynamic_partitioning_configuration: None,
            file_extension: None,
            custom_time_zone: None,
        })
    }

    fn buffer(sink: &Arc<RecordingSink>, size_mb: u64, interval_s: u64) -> Arc<StreamBuffer> {
        StreamBuffer::spawn(
            "s",
            "arn:aws:firehose:us-east-1:000000000000:deliverystream/s",
            destination(size_mb, interval_s),
            Arc::clone(sink) as Arc<dyn DeliverySink>,
        )
    }

    /// Let the timer task observe the paused clock.
    async fn settle() {
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn interval_flush_fires_exactly_at_the_deadline_from_first_record() {
        let sink = Arc::new(RecordingSink::new());
        let b = buffer(&sink, 128, 60);

        // Idle streams never flush.
        tokio::time::advance(Duration::from_secs(600)).await;
        settle().await;
        assert!(sink.is_empty());

        b.push(Bytes::from_static(b"one")).await.unwrap();
        tokio::time::advance(Duration::from_secs(30)).await;
        settle().await;
        b.push(Bytes::from_static(b"two")).await.unwrap();

        // 59.999s after the first record: nothing yet.
        tokio::time::advance(Duration::from_millis(29_999)).await;
        settle().await;
        assert!(sink.is_empty(), "flushed before the interval elapsed");

        tokio::time::advance(Duration::from_millis(1)).await;
        settle().await;
        let batches = sink.batches();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].reason, FlushReason::Interval);
        assert_eq!(
            batches[0].records,
            vec![Bytes::from("one"), Bytes::from("two")]
        );
        assert_eq!(b.buffered_records(), 0);

        // The clock restarts with the next first record, not from the flush.
        tokio::time::advance(Duration::from_secs(100)).await;
        settle().await;
        assert_eq!(sink.len(), 1);
        b.push(Bytes::from_static(b"three")).await.unwrap();
        tokio::time::advance(Duration::from_secs(60)).await;
        settle().await;
        assert_eq!(sink.len(), 2);
        assert_eq!(sink.batches()[1].records, vec![Bytes::from("three")]);
    }

    #[tokio::test(start_paused = true)]
    async fn size_flush_includes_the_crossing_record_and_resets_the_interval() {
        let sink = Arc::new(RecordingSink::new());
        let b = buffer(&sink, 1, 300);
        let chunk = Bytes::from(vec![b'x'; 512 * 1024]);

        b.push(chunk.clone()).await.unwrap();
        assert_eq!(b.buffered_bytes(), 512 * 1024);
        assert!(sink.is_empty());

        b.push(Bytes::from(vec![b'y'; 512 * 1024 - 1]))
            .await
            .unwrap();
        assert!(sink.is_empty(), "one byte short of the threshold");

        b.push(Bytes::from_static(b"z")).await.unwrap();
        let batches = sink.batches();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].reason, FlushReason::Size);
        assert_eq!(batches[0].records.len(), 3);
        assert_eq!(batches[0].total_bytes(), 1024 * 1024);

        // The interval timer was cancelled by the size flush.
        tokio::time::advance(Duration::from_secs(301)).await;
        settle().await;
        assert_eq!(sink.len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn whichever_threshold_comes_first_wins() {
        let sink = Arc::new(RecordingSink::new());
        let b = buffer(&sink, 1, 10);

        // Interval first.
        b.push(Bytes::from_static(b"small")).await.unwrap();
        tokio::time::advance(Duration::from_secs(10)).await;
        settle().await;
        assert_eq!(sink.len(), 1);
        assert_eq!(sink.batches()[0].reason, FlushReason::Interval);

        // Size first.
        b.push(Bytes::from(vec![0u8; 1024 * 1024])).await.unwrap();
        assert_eq!(sink.len(), 2);
        assert_eq!(sink.batches()[1].reason, FlushReason::Size);
    }

    #[tokio::test(start_paused = true)]
    async fn interval_zero_flushes_every_record_immediately() {
        let sink = Arc::new(RecordingSink::new());
        let b = buffer(&sink, 128, 0);
        b.push(Bytes::from_static(b"a")).await.unwrap();
        settle().await;
        b.push(Bytes::from_static(b"b")).await.unwrap();
        settle().await;
        assert_eq!(sink.len(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn updated_hints_apply_to_the_open_buffer() {
        let sink = Arc::new(RecordingSink::new());
        let b = buffer(&sink, 128, 300);
        b.push(Bytes::from_static(b"a")).await.unwrap();
        tokio::time::advance(Duration::from_secs(20)).await;
        settle().await;

        // Shorten the interval to 30s: the buffer opened 20s ago, so it
        // must flush 10s from now.
        b.update_destination(destination(128, 30));
        tokio::time::advance(Duration::from_millis(9_999)).await;
        settle().await;
        assert!(sink.is_empty());
        tokio::time::advance(Duration::from_millis(1)).await;
        settle().await;
        assert_eq!(sink.len(), 1);
        assert_eq!(
            sink.batches()[0]
                .destination
                .buffering_hints
                .interval_in_seconds,
            Some(30)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn size_flush_failure_restores_prior_records_and_rejects_the_new_one() {
        let sink = Arc::new(RecordingSink::new());
        let b = buffer(&sink, 1, 60);
        b.push(Bytes::from(vec![1u8; 600 * 1024])).await.unwrap();

        sink.set_failure(Some("bucket unreachable"));
        let err = b
            .push(Bytes::from(vec![2u8; 600 * 1024]))
            .await
            .unwrap_err();
        assert_eq!(err.message, "bucket unreachable");
        assert_eq!(b.buffered_records(), 1, "earlier record retained");
        assert_eq!(b.buffered_bytes(), 600 * 1024);

        // The interval retry delivers the retained record once the sink is
        // healthy again.
        sink.set_failure(None);
        tokio::time::advance(Duration::from_secs(60)).await;
        settle().await;
        assert_eq!(sink.len(), 1);
        assert_eq!(sink.batches()[0].records[0][0], 1u8);
    }

    #[tokio::test(start_paused = true)]
    async fn interval_flush_failure_retries_after_another_interval() {
        let sink = Arc::new(RecordingSink::new());
        let b = buffer(&sink, 128, 30);
        b.push(Bytes::from_static(b"a")).await.unwrap();
        sink.set_failure(Some("down"));
        tokio::time::advance(Duration::from_secs(30)).await;
        settle().await;
        assert!(sink.is_empty());
        assert_eq!(b.buffered_records(), 1);

        sink.set_failure(None);
        tokio::time::advance(Duration::from_secs(29)).await;
        settle().await;
        assert!(sink.is_empty());
        tokio::time::advance(Duration::from_secs(1)).await;
        settle().await;
        assert_eq!(sink.len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn close_and_flush_delivers_the_remainder_and_refuses_new_records() {
        let sink = Arc::new(RecordingSink::new());
        let b = buffer(&sink, 128, 300);
        b.push(Bytes::from_static(b"a")).await.unwrap();
        b.close_and_flush(FlushReason::Shutdown).await.unwrap();
        assert_eq!(sink.len(), 1);
        assert_eq!(sink.batches()[0].reason, FlushReason::Shutdown);
        assert!(b.push(Bytes::from_static(b"late")).await.is_err());

        // Empty close is a no-op.
        let c = buffer(&sink, 128, 300);
        c.close_and_flush(FlushReason::Shutdown).await.unwrap();
        assert_eq!(sink.len(), 1);
    }
}
