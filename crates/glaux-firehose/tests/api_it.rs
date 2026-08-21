//! The Firehose API through the real `aws-sdk-firehose` client against an
//! in-process glaux server: stream lifecycle, validation and limits,
//! `PutRecordBatch` partial failures, unsupported-configuration refusals,
//! and flush-on-delete / flush-on-shutdown into a recording sink.

use std::net::SocketAddr;
use std::sync::Arc;

use aws_credential_types::Credentials;
use aws_sdk_firehose::Client;
use aws_sdk_firehose::config::Region;
use aws_sdk_firehose::error::SdkError;
use aws_sdk_firehose::primitives::Blob;
use aws_sdk_firehose::types::{
    BufferingHints, CompressionFormat, DeliveryStreamStatus, ExtendedS3DestinationConfiguration,
    ExtendedS3DestinationUpdate, ProcessingConfiguration, Record,
};
use glaux_firehose::http::router;
use glaux_firehose::{FirehoseService, FirehoseServiceConfig, FlushReason, RecordingSink};

struct Harness {
    client: Client,
    service: Arc<FirehoseService>,
    sink: Arc<RecordingSink>,
}

async fn harness() -> Harness {
    let sink = Arc::new(RecordingSink::new());
    let service = Arc::new(FirehoseService::new(
        FirehoseServiceConfig::default(),
        Arc::clone(&sink) as Arc<dyn glaux_firehose::DeliverySink>,
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    tokio::spawn(axum::serve(listener, router(Arc::clone(&service))).into_future());

    let config = aws_sdk_firehose::Config::builder()
        .behavior_version_latest()
        .region(Region::new("us-east-1"))
        .credentials_provider(Credentials::new("test", "test", None, None, "glaux-tests"))
        .endpoint_url(format!("http://{addr}"))
        .build();
    Harness {
        client: Client::from_conf(config),
        service,
        sink,
    }
}

fn s3_destination(size_mb: i32, interval_s: i32) -> ExtendedS3DestinationConfiguration {
    ExtendedS3DestinationConfiguration::builder()
        .role_arn("arn:aws:iam::000000000000:role/firehose")
        .bucket_arn("arn:aws:s3:::events")
        .prefix("raw/")
        .error_output_prefix("errors/")
        .buffering_hints(
            BufferingHints::builder()
                .size_in_mbs(size_mb)
                .interval_in_seconds(interval_s)
                .build(),
        )
        .compression_format(CompressionFormat::Gzip)
        .build()
        .unwrap()
}

async fn create(h: &Harness, name: &str, size_mb: i32, interval_s: i32) -> String {
    h.client
        .create_delivery_stream()
        .delivery_stream_name(name)
        .extended_s3_destination_configuration(s3_destination(size_mb, interval_s))
        .send()
        .await
        .unwrap()
        .delivery_stream_arn
        .unwrap()
}

fn error_message<E: std::fmt::Debug + aws_sdk_firehose::error::ProvideErrorMetadata, R>(
    err: &SdkError<E, R>,
) -> (String, String) {
    match err {
        SdkError::ServiceError(e) => (
            e.err().code().unwrap_or("<no code>").to_string(),
            e.err().message().unwrap_or("<no message>").to_string(),
        ),
        SdkError::ConstructionFailure(_) => {
            panic!("expected a service error, got ConstructionFailure")
        }
        SdkError::TimeoutError(_) => panic!("expected a service error, got TimeoutError"),
        SdkError::DispatchFailure(e) => {
            panic!("expected a service error, got DispatchFailure {e:?}")
        }
        SdkError::ResponseError(_) => panic!("expected a service error, got ResponseError"),
        _ => panic!("expected a service error"),
    }
}

#[tokio::test]
async fn create_describe_list_update_delete_lifecycle() {
    let h = harness().await;

    let arn = create(&h, "orders", 5, 300).await;
    assert_eq!(
        arn,
        "arn:aws:firehose:us-east-1:000000000000:deliverystream/orders"
    );

    let desc = h
        .client
        .describe_delivery_stream()
        .delivery_stream_name("orders")
        .send()
        .await
        .unwrap()
        .delivery_stream_description
        .unwrap();
    assert_eq!(desc.delivery_stream_status, DeliveryStreamStatus::Active);
    assert_eq!(desc.delivery_stream_arn, arn);
    assert_eq!(desc.version_id, "1");
    assert_eq!(desc.destinations.len(), 1);
    let dest = desc.destinations[0]
        .extended_s3_destination_description
        .as_ref()
        .unwrap();
    assert_eq!(dest.bucket_arn, "arn:aws:s3:::events");
    assert_eq!(dest.prefix.as_deref(), Some("raw/"));
    assert_eq!(dest.error_output_prefix.as_deref(), Some("errors/"));
    assert_eq!(dest.compression_format, CompressionFormat::Gzip);
    assert_eq!(dest.buffering_hints.as_ref().unwrap().size_in_mbs, Some(5));
    assert_eq!(
        dest.buffering_hints.as_ref().unwrap().interval_in_seconds,
        Some(300)
    );
    assert!(
        dest.encryption_configuration
            .as_ref()
            .unwrap()
            .no_encryption_config
            .is_some()
    );

    // Duplicate create is ResourceInUse.
    let err = h
        .client
        .create_delivery_stream()
        .delivery_stream_name("orders")
        .extended_s3_destination_configuration(s3_destination(5, 300))
        .send()
        .await
        .unwrap_err();
    assert_eq!(error_message(&err).0, "ResourceInUseException");

    // Listing pages lexicographically.
    create(&h, "alpha", 5, 300).await;
    create(&h, "zeta", 5, 300).await;
    let page = h
        .client
        .list_delivery_streams()
        .limit(2)
        .send()
        .await
        .unwrap();
    assert_eq!(page.delivery_stream_names, vec!["alpha", "orders"]);
    assert!(page.has_more_delivery_streams);
    let page = h
        .client
        .list_delivery_streams()
        .limit(2)
        .exclusive_start_delivery_stream_name("orders")
        .send()
        .await
        .unwrap();
    assert_eq!(page.delivery_stream_names, vec!["zeta"]);
    assert!(!page.has_more_delivery_streams);

    // Update with a stale version is refused; with the right one it applies
    // only the given fields and bumps the version.
    let update = ExtendedS3DestinationUpdate::builder()
        .buffering_hints(
            BufferingHints::builder()
                .size_in_mbs(64)
                .interval_in_seconds(60)
                .build(),
        )
        .compression_format(CompressionFormat::Uncompressed)
        .build();
    let err = h
        .client
        .update_destination()
        .delivery_stream_name("orders")
        .current_delivery_stream_version_id("7")
        .destination_id("destinationId-000000000001")
        .extended_s3_destination_update(update.clone())
        .send()
        .await
        .unwrap_err();
    assert_eq!(error_message(&err).0, "ConcurrentModificationException");
    h.client
        .update_destination()
        .delivery_stream_name("orders")
        .current_delivery_stream_version_id("1")
        .destination_id("destinationId-000000000001")
        .extended_s3_destination_update(update)
        .send()
        .await
        .unwrap();
    let desc = h
        .client
        .describe_delivery_stream()
        .delivery_stream_name("orders")
        .send()
        .await
        .unwrap()
        .delivery_stream_description
        .unwrap();
    assert_eq!(desc.version_id, "2");
    let dest = desc.destinations[0]
        .extended_s3_destination_description
        .as_ref()
        .unwrap();
    assert_eq!(dest.prefix.as_deref(), Some("raw/"), "untouched field kept");
    assert_eq!(dest.compression_format, CompressionFormat::Uncompressed);
    assert_eq!(dest.buffering_hints.as_ref().unwrap().size_in_mbs, Some(64));

    // Delete, then everything about it is ResourceNotFound.
    h.client
        .delete_delivery_stream()
        .delivery_stream_name("orders")
        .send()
        .await
        .unwrap();
    let err = h
        .client
        .describe_delivery_stream()
        .delivery_stream_name("orders")
        .send()
        .await
        .unwrap_err();
    let (code, message) = error_message(&err);
    assert_eq!(code, "ResourceNotFoundException");
    assert_eq!(
        message,
        "Firehose orders under account 000000000000 not found."
    );
    let err = h
        .client
        .put_record()
        .delivery_stream_name("orders")
        .record(Record::builder().data(Blob::new("x")).build().unwrap())
        .send()
        .await
        .unwrap_err();
    assert_eq!(error_message(&err).0, "ResourceNotFoundException");
}

#[tokio::test]
async fn stream_name_and_configuration_validation() {
    let h = harness().await;

    for bad in ["", "has space", "a/b", &"x".repeat(65)] {
        let err = h
            .client
            .create_delivery_stream()
            .delivery_stream_name(bad)
            .extended_s3_destination_configuration(s3_destination(5, 300))
            .send()
            .await
            .unwrap_err();
        let (code, message) = error_message(&err);
        assert_eq!(code, "ValidationException", "name {bad:?}");
        assert!(message.contains("deliveryStreamName"), "{message}");
    }

    // Every unsupported construct is named in the error.
    let cases: Vec<(&str, ExtendedS3DestinationConfiguration)> = vec![
        (
            "BucketARN",
            ExtendedS3DestinationConfiguration::builder()
                .role_arn("arn:aws:iam::000000000000:role/firehose")
                .bucket_arn("events")
                .build()
                .unwrap(),
        ),
        (
            "SizeInMBs",
            ExtendedS3DestinationConfiguration::builder()
                .role_arn("arn:aws:iam::000000000000:role/firehose")
                .bucket_arn("arn:aws:s3:::events")
                .buffering_hints(BufferingHints::builder().size_in_mbs(129).build())
                .build()
                .unwrap(),
        ),
        (
            "IntervalInSeconds",
            ExtendedS3DestinationConfiguration::builder()
                .role_arn("arn:aws:iam::000000000000:role/firehose")
                .bucket_arn("arn:aws:s3:::events")
                .buffering_hints(BufferingHints::builder().interval_in_seconds(901).build())
                .build()
                .unwrap(),
        ),
        (
            "ProcessingConfiguration",
            ExtendedS3DestinationConfiguration::builder()
                .role_arn("arn:aws:iam::000000000000:role/firehose")
                .bucket_arn("arn:aws:s3:::events")
                .processing_configuration(ProcessingConfiguration::builder().enabled(true).build())
                .build()
                .unwrap(),
        ),
    ];
    for (construct, config) in cases {
        let err = h
            .client
            .create_delivery_stream()
            .delivery_stream_name("bad")
            .extended_s3_destination_configuration(config)
            .send()
            .await
            .unwrap_err();
        let (code, message) = error_message(&err);
        assert!(
            code == "ValidationException" || code == "InvalidArgumentException",
            "{construct}: {code}"
        );
        assert!(message.contains(construct), "{construct}: {message}");
    }

    // Non-S3 destinations and Kinesis sources are refused by name.
    let err = h
        .client
        .create_delivery_stream()
        .delivery_stream_name("redshift")
        .redshift_destination_configuration(
            aws_sdk_firehose::types::RedshiftDestinationConfiguration::builder()
                .role_arn("arn:aws:iam::000000000000:role/firehose")
                .cluster_jdbcurl("jdbc:redshift://x")
                .copy_command(
                    aws_sdk_firehose::types::CopyCommand::builder()
                        .data_table_name("t")
                        .build()
                        .unwrap(),
                )
                .username("u")
                .password("p")
                .s3_configuration(
                    aws_sdk_firehose::types::S3DestinationConfiguration::builder()
                        .role_arn("arn:aws:iam::000000000000:role/firehose")
                        .bucket_arn("arn:aws:s3:::events")
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap_err();
    let (code, message) = error_message(&err);
    assert_eq!(code, "InvalidArgumentException");
    assert!(
        message.contains("RedshiftDestinationConfiguration"),
        "{message}"
    );

    let err = h
        .client
        .create_delivery_stream()
        .delivery_stream_name("kinesis")
        .delivery_stream_type(aws_sdk_firehose::types::DeliveryStreamType::KinesisStreamAsSource)
        .extended_s3_destination_configuration(s3_destination(5, 300))
        .send()
        .await
        .unwrap_err();
    let (code, message) = error_message(&err);
    assert_eq!(code, "InvalidArgumentException");
    assert!(message.contains("KinesisStreamAsSource"), "{message}");

    // Nothing was created along the way.
    assert!(
        h.client
            .list_delivery_streams()
            .send()
            .await
            .unwrap()
            .delivery_stream_names
            .is_empty()
    );
}

#[tokio::test]
async fn put_record_enforces_the_record_limit_and_buffers() {
    let h = harness().await;
    create(&h, "s", 1, 300).await;

    let out = h
        .client
        .put_record()
        .delivery_stream_name("s")
        .record(
            Record::builder()
                .data(Blob::new(vec![b'a'; 1024 * 1024]))
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert!(!out.record_id.is_empty());
    assert!(!out.encrypted.unwrap_or(true));

    // Exactly 1 MiB filled the 1 MiB buffer: flushed as one batch.
    h.sink.wait_for(1).await;
    let batches = h.sink.batches();
    assert_eq!(batches[0].reason, FlushReason::Size);
    assert_eq!(batches[0].total_bytes(), 1024 * 1024);
    assert_eq!(batches[0].stream_name, "s");
    assert_eq!(batches[0].destination.bucket(), "events");

    let err = h
        .client
        .put_record()
        .delivery_stream_name("s")
        .record(
            Record::builder()
                .data(Blob::new(vec![b'a'; 1024 * 1024 + 1]))
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap_err();
    let (code, message) = error_message(&err);
    assert_eq!(code, "ValidationException");
    assert!(message.contains("1048576"), "{message}");
    assert_eq!(h.service.buffered_records("s"), Some(0));

    // Small records stay buffered until a threshold.
    h.client
        .put_record()
        .delivery_stream_name("s")
        .record(
            Record::builder()
                .data(Blob::new("{\"a\":1}"))
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(h.service.buffered_records("s"), Some(1));
    assert_eq!(h.sink.len(), 1);
}

#[tokio::test]
async fn put_record_batch_limits_and_per_record_results() {
    let h = harness().await;
    create(&h, "b", 1, 300).await;
    let small = || Record::builder().data(Blob::new("r")).build().unwrap();

    // 501 records: whole request rejected.
    let err = h
        .client
        .put_record_batch()
        .delivery_stream_name("b")
        .set_records(Some((0..501).map(|_| small()).collect()))
        .send()
        .await
        .unwrap_err();
    let (code, message) = error_message(&err);
    assert_eq!(code, "ValidationException");
    assert!(message.contains("500"), "{message}");

    // > 4 MiB total: whole request rejected.
    let err = h
        .client
        .put_record_batch()
        .delivery_stream_name("b")
        .set_records(Some(
            (0..5)
                .map(|_| {
                    Record::builder()
                        .data(Blob::new(vec![b'z'; 1000 * 1024]))
                        .build()
                        .unwrap()
                })
                .collect(),
        ))
        .send()
        .await
        .unwrap_err();
    let (code, message) = error_message(&err);
    assert_eq!(code, "InvalidArgumentException");
    assert!(message.contains("4 MB"), "{message}");
    assert_eq!(h.service.buffered_records("b"), Some(0), "nothing accepted");

    // Mixed batch: an oversized record fails alone; the rest are accepted.
    let out = h
        .client
        .put_record_batch()
        .delivery_stream_name("b")
        .records(small())
        .records(
            Record::builder()
                .data(Blob::new(vec![b'q'; 1024 * 1024 + 1]))
                .build()
                .unwrap(),
        )
        .records(small())
        .send()
        .await
        .unwrap();
    assert_eq!(out.failed_put_count, 1);
    assert_eq!(out.request_responses.len(), 3);
    assert!(out.request_responses[0].record_id.is_some());
    assert_eq!(
        out.request_responses[1].error_code.as_deref(),
        Some("ValidationException")
    );
    assert!(out.request_responses[1].record_id.is_none());
    assert!(out.request_responses[2].record_id.is_some());
    assert_eq!(h.service.buffered_records("b"), Some(2));

    // Sink failure mid-batch: the failing record and every later one are
    // reported as ServiceUnavailable; earlier ones stand.
    h.sink.set_failure(Some("bucket gone"));
    let big = || {
        Record::builder()
            .data(Blob::new(vec![b'w'; 600 * 1024]))
            .build()
            .unwrap()
    };
    let out = h
        .client
        .put_record_batch()
        .delivery_stream_name("b")
        .records(big()) // buffered (≈600 KiB)
        .records(big()) // crosses 1 MiB → flush → fails
        .records(small()) // not attempted
        .send()
        .await
        .unwrap();
    assert_eq!(out.failed_put_count, 2);
    assert!(out.request_responses[0].record_id.is_some());
    assert_eq!(
        out.request_responses[1].error_code.as_deref(),
        Some("ServiceUnavailableException")
    );
    assert!(
        out.request_responses[1]
            .error_message
            .as_deref()
            .unwrap()
            .contains("bucket gone")
    );
    assert_eq!(
        out.request_responses[2].error_code.as_deref(),
        Some("ServiceUnavailableException")
    );
    // Two small + the first big one are retained for the interval retry.
    assert_eq!(h.service.buffered_records("b"), Some(3));
    assert!(h.sink.is_empty());

    // Single PutRecord sees the same failure as an explicit error.
    h.sink.set_failure(Some("still gone"));
    let err = h
        .client
        .put_record()
        .delivery_stream_name("b")
        .record(big())
        .send()
        .await
        .unwrap_err();
    let (code, message) = error_message(&err);
    assert_eq!(code, "ServiceUnavailableException");
    assert!(message.contains("still gone"), "{message}");
}

#[tokio::test]
async fn delete_and_shutdown_flush_pending_buffers() {
    let h = harness().await;
    create(&h, "d1", 128, 900).await;
    create(&h, "d2", 128, 900).await;
    create(&h, "empty", 128, 900).await;
    for (stream, payload) in [("d1", "one"), ("d1", "two"), ("d2", "three")] {
        h.client
            .put_record()
            .delivery_stream_name(stream)
            .record(Record::builder().data(Blob::new(payload)).build().unwrap())
            .send()
            .await
            .unwrap();
    }

    // Delete flushes d1's two records as one batch.
    h.client
        .delete_delivery_stream()
        .delivery_stream_name("d1")
        .send()
        .await
        .unwrap();
    let batches = h.sink.batches();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].reason, FlushReason::Delete);
    assert_eq!(batches[0].stream_name, "d1");
    assert_eq!(batches[0].records.len(), 2);

    // A delete whose flush fails keeps the stream (DELETING) and its data.
    h.sink.set_failure(Some("offline"));
    let err = h
        .client
        .delete_delivery_stream()
        .delivery_stream_name("d2")
        .send()
        .await
        .unwrap_err();
    assert_eq!(error_message(&err).0, "ServiceUnavailableException");
    assert_eq!(h.service.buffered_records("d2"), Some(1));
    let status = h
        .client
        .describe_delivery_stream()
        .delivery_stream_name("d2")
        .send()
        .await
        .unwrap()
        .delivery_stream_description
        .unwrap()
        .delivery_stream_status;
    assert_eq!(status, DeliveryStreamStatus::Deleting);

    // Shutdown flushes d2 once the sink is back and skips empty streams.
    h.sink.set_failure(None);
    let failures = h.service.shutdown().await;
    assert!(failures.is_empty(), "{failures:?}");
    let batches = h.sink.batches();
    assert_eq!(batches.len(), 2);
    assert_eq!(batches[1].reason, FlushReason::Shutdown);
    assert_eq!(batches[1].stream_name, "d2");
    assert_eq!(batches[1].records, vec![bytes::Bytes::from("three")]);

    // After shutdown, producers are told explicitly.
    let err = h
        .client
        .put_record()
        .delivery_stream_name("empty")
        .record(Record::builder().data(Blob::new("late")).build().unwrap())
        .send()
        .await
        .unwrap_err();
    assert_eq!(error_message(&err).0, "ServiceUnavailableException");
}

#[tokio::test]
async fn unknown_actions_and_bad_targets_are_explicit() {
    let h = harness().await;
    let err = h
        .client
        .list_tags_for_delivery_stream()
        .delivery_stream_name("x")
        .send()
        .await
        .unwrap_err();
    let (code, message) = error_message(&err);
    assert_eq!(code, "UnknownOperationException");
    assert!(message.contains("ListTagsForDeliveryStream"), "{message}");
}
