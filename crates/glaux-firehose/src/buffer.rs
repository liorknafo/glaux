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
//!
//! # Cancellation safety
//!
//! Delivery never runs inside a caller's future. Once a batch leaves the
//! buffer it is handed to a dedicated tokio task that owns the records
//! until the sink has accepted them; the caller merely awaits that task's
//! outcome. Dropping the caller — an HTTP handler whose client went away,
//! or the interval timer being stopped by [`StreamBuffer::close`] — cannot
//! drop the batch. If the delivery task ends without the sink's
//! acceptance (error or panic) the records are put back. `close_and_flush`
//! waits for every in-flight delivery before its final flush, so a
//! failed in-flight batch is retried there rather than stranded.

use std::pin::pin;
use std::sync::atomic::{AtomicUsize, Ordering};
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
    /// The stream `VersionId` matching `destination`.
    version_id: String,
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
    /// Delivery tasks currently running.
    in_flight: AtomicUsize,
    /// Signalled (with `notify_waiters`) whenever `in_flight` drops to zero.
    idle: Notify,
}

/// Owns a batch's records for the lifetime of a delivery task and puts
/// them back into the buffer unless delivery is acknowledged. This is what
/// makes a panicking sink (or any early exit) unable to lose data.
struct DeliveryGuard {
    buffer: Arc<StreamBuffer>,
    /// The records to restore if the delivery is not acknowledged.
    records: Option<Vec<Bytes>>,
    opened_at: SystemTime,
}

impl DeliveryGuard {
    /// The sink accepted the batch: nothing to restore.
    fn acknowledge(&mut self) {
        self.records = None;
    }
}

impl Drop for DeliveryGuard {
    fn drop(&mut self) {
        if let Some(records) = self.records.take()
            && !records.is_empty()
        {
            self.buffer.restore(records, self.opened_at);
        }
        if self.buffer.in_flight.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.buffer.idle.notify_waiters();
        }
    }
}

/// A delivery running on its own task, plus the caller-liveness token.
struct Delivery {
    task: JoinHandle<Result<(), SinkError>>,
    caller_alive: tokio::sync::oneshot::Sender<()>,
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
                version_id: "1".to_string(),
                size_bytes: usize_from(hints.size_bytes()),
                interval: hints.interval(),
                pending: None,
                deadline: None,
                closed: false,
            }),
            wake: Notify::new(),
            timer: Mutex::new(None),
            in_flight: AtomicUsize::new(0),
            idle: Notify::new(),
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
    ///
    /// `version_id` is the stream's new `VersionId`; it is reported on every
    /// subsequent [`FlushBatch`] and ends up in S3 object names.
    pub fn update_destination(
        &self,
        destination: Arc<ExtendedS3DestinationDescription>,
        version_id: impl Into<String>,
    ) {
        let hints = destination.buffering_hints.resolved();
        {
            let mut inner = self.inner.lock().unwrap();
            inner.size_bytes = usize_from(hints.size_bytes());
            inner.interval = hints.interval();
            inner.destination = destination;
            inner.version_id = version_id.into();
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
    ///
    /// Cancellation-safe: dropping the returned future (e.g. because the
    /// HTTP client disconnected) does not abandon the batch; delivery
    /// completes on its own task. The record being pushed is then treated
    /// like any other member of the batch — restored on failure — since the
    /// caller is no longer there to retry it.
    pub async fn push(self: &Arc<Self>, data: Bytes) -> Result<(), SinkError> {
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
                let batch = self.batch(&inner, pending, FlushReason::Size);
                Some(self.spawn_delivery(batch, true))
            } else {
                if newly_opened {
                    inner.deadline = Some(Instant::now() + interval);
                }
                None
            }
        };
        self.wake.notify_one();

        let Some(delivery) = flush else {
            return Ok(());
        };
        Self::await_delivery(delivery).await
    }

    /// Flush whatever is buffered right now with `reason`, regardless of
    /// thresholds. Records are restored on failure. Cancellation-safe in
    /// the same way as [`push`](Self::push).
    pub async fn flush(self: &Arc<Self>, reason: FlushReason) -> Result<(), SinkError> {
        let delivery = {
            let mut inner = self.inner.lock().unwrap();
            inner.deadline = None;
            let Some(pending) = inner.pending.take() else {
                return Ok(());
            };
            let batch = self.batch(&inner, pending, reason);
            self.spawn_delivery(batch, false)
        };
        self.wake.notify_one();
        Self::await_delivery(delivery).await
    }

    /// Hand `batch` to the sink on its own task. Must be called while
    /// `inner` is locked so that `in_flight` is incremented before anyone
    /// can observe the records as gone from `pending`.
    ///
    /// With `pop_last_on_failure`, a failed delivery restores every record
    /// but the last (the one a `push` caller is about to be told about) —
    /// unless the caller has gone away, in which case that record is
    /// restored too.
    fn spawn_delivery(self: &Arc<Self>, batch: FlushBatch, pop_last_on_failure: bool) -> Delivery {
        self.in_flight.fetch_add(1, Ordering::AcqRel);
        let buffer = Arc::clone(self);
        let mut guard = DeliveryGuard {
            buffer: Arc::clone(self),
            records: Some(batch.records.clone()),
            opened_at: batch.opened_at,
        };
        // The caller holds `caller_alive` while it awaits; dropping the
        // caller's future closes the channel, which the task can observe.
        let (caller_alive, mut alive_rx) = tokio::sync::oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            let result = buffer.sink.deliver(batch).await;
            match result {
                Ok(()) => guard.acknowledge(),
                Err(_) if pop_last_on_failure => {
                    // `try_recv` reports `Closed` once the caller's sender
                    // has been dropped, i.e. the caller was cancelled.
                    let caller_gone = matches!(
                        alive_rx.try_recv(),
                        Err(tokio::sync::oneshot::error::TryRecvError::Closed)
                    );
                    if !caller_gone && let Some(records) = guard.records.as_mut() {
                        records.pop();
                    }
                }
                Err(_) => {}
            }
            drop(guard);
            result
        });
        Delivery { task, caller_alive }
    }

    async fn await_delivery(delivery: Delivery) -> Result<(), SinkError> {
        // Held until this future completes or is dropped.
        let _caller_alive = delivery.caller_alive;
        match delivery.task.await {
            Ok(result) => result,
            Err(join) => Err(SinkError::new(format!(
                "delivery task ended abnormally (records restored to the buffer): {join}"
            ))),
        }
    }

    /// Stop the interval timer and refuse further records. An interval
    /// flush already in progress is allowed to finish. Call
    /// [`flush`](Self::flush) or [`close_and_flush`](Self::close_and_flush)
    /// afterwards to deliver what is left.
    pub fn close(&self) {
        self.inner.lock().unwrap().closed = true;
        self.wake.notify_one();
    }

    /// Wait until no delivery task is running.
    async fn wait_idle(&self) {
        loop {
            let mut idle = pin!(self.idle.notified());
            idle.as_mut().enable();
            if self.in_flight.load(Ordering::Acquire) == 0 {
                return;
            }
            idle.await;
        }
    }

    /// Stop the timer, wait for any in-flight delivery to settle, then
    /// deliver any remaining records (including ones an in-flight delivery
    /// failed on and restored) with `reason`.
    pub async fn close_and_flush(self: &Arc<Self>, reason: FlushReason) -> Result<(), SinkError> {
        self.close();
        self.wait_idle().await;
        self.flush(reason).await
    }

    fn batch(&self, inner: &Inner, pending: Pending, reason: FlushReason) -> FlushBatch {
        FlushBatch {
            stream_name: self.stream_name.clone(),
            stream_arn: self.stream_arn.clone(),
            stream_version: inner.version_id.clone(),
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
            let deadline = {
                let inner = self.inner.lock().unwrap();
                if inner.closed {
                    return;
                }
                inner.deadline
            };
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
    use async_trait::async_trait;
    use tokio::sync::Semaphore;

    /// A sink whose `deliver` parks at an await point until released, so
    /// tests can drop or abort whoever triggered the flush mid-delivery.
    struct GatedSink {
        inner: RecordingSink,
        gate: Semaphore,
        entered: AtomicUsize,
    }

    impl GatedSink {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                inner: RecordingSink::new(),
                gate: Semaphore::new(0),
                entered: AtomicUsize::new(0),
            })
        }

        fn release(&self) {
            self.gate.add_permits(1);
        }

        fn entered(&self) -> usize {
            self.entered.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl DeliverySink for GatedSink {
        async fn deliver(&self, batch: FlushBatch) -> Result<(), SinkError> {
            self.entered.fetch_add(1, Ordering::SeqCst);
            // Each release lets exactly one delivery through.
            self.gate
                .acquire()
                .await
                .expect("gate never closes")
                .forget();
            self.inner.deliver(batch).await
        }
    }

    fn gated_buffer(sink: &Arc<GatedSink>, size_mb: u64, interval_s: u64) -> Arc<StreamBuffer> {
        StreamBuffer::spawn(
            "s",
            "arn:aws:firehose:us-east-1:000000000000:deliverystream/s",
            destination(size_mb, interval_s),
            Arc::clone(sink) as Arc<dyn DeliverySink>,
        )
    }

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
        b.update_destination(destination(128, 30), "2");
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

    #[tokio::test(start_paused = true)]
    async fn close_during_an_in_flight_interval_flush_does_not_drop_the_batch() {
        let sink = GatedSink::new();
        let b = gated_buffer(&sink, 128, 10);
        b.push(Bytes::from_static(b"a")).await.unwrap();
        b.push(Bytes::from_static(b"b")).await.unwrap();
        tokio::time::advance(Duration::from_secs(10)).await;
        settle().await;
        assert_eq!(
            sink.entered(),
            1,
            "interval flush is parked inside the sink"
        );
        assert_eq!(b.buffered_records(), 0);

        // A delete/shutdown lands while the timer's flush is awaiting the
        // sink. It must wait for that delivery rather than abandon it.
        let closer = tokio::spawn({
            let b = Arc::clone(&b);
            async move { b.close_and_flush(FlushReason::Shutdown).await }
        });
        settle().await;
        assert!(
            !closer.is_finished(),
            "close_and_flush returned with a delivery in flight"
        );
        assert!(sink.inner.is_empty());

        sink.release();
        closer.await.unwrap().unwrap();
        let batches = sink.inner.batches();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].reason, FlushReason::Interval);
        assert_eq!(batches[0].records, vec![Bytes::from("a"), Bytes::from("b")]);
        assert_eq!(b.buffered_records(), 0);
        assert!(b.push(Bytes::from_static(b"late")).await.is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn close_retries_records_an_in_flight_flush_failed_on() {
        let sink = GatedSink::new();
        let b = gated_buffer(&sink, 128, 10);
        b.push(Bytes::from_static(b"a")).await.unwrap();
        tokio::time::advance(Duration::from_secs(10)).await;
        settle().await;
        assert_eq!(sink.entered(), 1);

        let closer = tokio::spawn({
            let b = Arc::clone(&b);
            async move { b.close_and_flush(FlushReason::Delete).await }
        });
        settle().await;

        // The in-flight interval delivery fails; the final flush must pick
        // the restored record up and deliver it once the sink is healthy.
        sink.inner.set_failure(Some("down"));
        sink.release();
        settle().await;
        sink.inner.set_failure(None);
        assert_eq!(sink.entered(), 2, "close flushed the restored record");
        sink.release();
        closer.await.unwrap().unwrap();
        let batches = sink.inner.batches();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].reason, FlushReason::Delete);
        assert_eq!(batches[0].records, vec![Bytes::from("a")]);
    }

    #[tokio::test(start_paused = true)]
    async fn dropping_a_push_mid_size_flush_still_delivers_every_record() {
        let sink = GatedSink::new();
        let b = gated_buffer(&sink, 1, 300);
        b.push(Bytes::from(vec![1u8; 600 * 1024])).await.unwrap();

        // The client that sent the crossing record times out and goes away
        // (axum drops the handler future) while the sink is mid-write.
        let pusher = tokio::spawn({
            let b = Arc::clone(&b);
            async move { b.push(Bytes::from(vec![2u8; 600 * 1024])).await }
        });
        settle().await;
        assert_eq!(sink.entered(), 1);
        pusher.abort();
        assert!(pusher.await.unwrap_err().is_cancelled());
        settle().await;

        sink.release();
        sink.inner.wait_for(1).await;
        let batches = sink.inner.batches();
        assert_eq!(batches[0].reason, FlushReason::Size);
        assert_eq!(
            batches[0].records.len(),
            2,
            "previously acknowledged record kept"
        );
        assert_eq!(b.buffered_records(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn dropping_a_push_whose_flush_then_fails_restores_the_whole_batch() {
        let sink = GatedSink::new();
        let b = gated_buffer(&sink, 1, 300);
        b.push(Bytes::from(vec![1u8; 600 * 1024])).await.unwrap();
        let pusher = tokio::spawn({
            let b = Arc::clone(&b);
            async move { b.push(Bytes::from(vec![2u8; 600 * 1024])).await }
        });
        settle().await;
        pusher.abort();
        let _ = pusher.await;

        // Nobody is left to retry the crossing record, so it is restored
        // along with the rest instead of being dropped.
        sink.inner.set_failure(Some("down"));
        sink.release();
        settle().await;
        assert!(sink.inner.is_empty());
        assert_eq!(b.buffered_records(), 2);
        assert_eq!(b.buffered_bytes(), 1200 * 1024);
    }

    #[tokio::test(start_paused = true)]
    async fn a_live_push_whose_flush_fails_still_owns_its_own_record() {
        let sink = GatedSink::new();
        let b = gated_buffer(&sink, 1, 300);
        b.push(Bytes::from(vec![1u8; 600 * 1024])).await.unwrap();
        let pusher = tokio::spawn({
            let b = Arc::clone(&b);
            async move { b.push(Bytes::from(vec![2u8; 600 * 1024])).await }
        });
        settle().await;
        sink.inner.set_failure(Some("down"));
        sink.release();
        let err = pusher.await.unwrap().unwrap_err();
        assert_eq!(err.message, "down");
        assert_eq!(b.buffered_records(), 1, "caller retries its own record");
    }

    #[tokio::test(start_paused = true)]
    async fn a_panicking_sink_restores_the_batch_and_reports_an_error() {
        struct PanickingSink;
        #[async_trait]
        impl DeliverySink for PanickingSink {
            async fn deliver(&self, _batch: FlushBatch) -> Result<(), SinkError> {
                panic!("sink bug");
            }
        }
        let b = StreamBuffer::spawn(
            "s",
            "arn",
            destination(128, 300),
            Arc::new(PanickingSink) as Arc<dyn DeliverySink>,
        );
        b.push(Bytes::from_static(b"a")).await.unwrap();
        let err = b.flush(FlushReason::Shutdown).await.unwrap_err();
        assert!(err.message.contains("ended abnormally"), "{err}");
        assert_eq!(b.buffered_records(), 1);
    }
}
