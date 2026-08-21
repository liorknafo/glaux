//! Full Athena lifecycle through the real `aws-sdk-athena` client against an
//! in-process glaux server: start → poll → page results, failure shapes,
//! cancellation of a running DataFusion scan, listing/batch lookups,
//! workgroups, and the CSV written to the `OutputLocation`. Queries run
//! through the Trino dialect layer ([`TrinoEngine`]), as they do in the
//! binaries.

use std::fmt;
use std::net::SocketAddr;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use arrow::array::{
    BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray, TimestampMillisecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use async_trait::async_trait;
use aws_credential_types::Credentials;
use aws_sdk_athena::Client;
use aws_sdk_athena::config::Region;
use aws_sdk_athena::operation::get_query_results::GetQueryResultsError;
use aws_sdk_athena::operation::start_query_execution::StartQueryExecutionError;
use aws_sdk_athena::types::{
    QueryExecutionContext, QueryExecutionState, ResultConfiguration, WorkGroupConfiguration,
};
use bytes::Bytes;
use datafusion::catalog::MemTable;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use futures::TryStreamExt;
use glaux_athena::http::router;
use glaux_athena::{AthenaService, AthenaServiceConfig, TrinoEngine};
use glaux_catalog::{CatalogError, ObjectSummary, StorageBackend};
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{GetOptions, ObjectMeta, ObjectStore, ObjectStoreExt as _, PutPayload};
use parquet::arrow::ArrowWriter;

// ---------------------------------------------------------------------------
// In-memory StorageBackend (captures the result CSVs)
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct MemoryStorage {
    store: InMemory,
}

fn storage_error(op: &'static str, key: &str) -> impl FnOnce(object_store::Error) -> CatalogError {
    let key = key.to_string();
    move |source| CatalogError::Storage {
        operation: op,
        bucket: "results".to_string(),
        key,
        source: Box::new(source),
    }
}

#[async_trait]
impl StorageBackend for MemoryStorage {
    async fn get_object(&self, _bucket: &str, key: &str) -> glaux_catalog::Result<Bytes> {
        self.store
            .get(&ObjectPath::from(key))
            .await
            .map_err(storage_error("get", key))?
            .bytes()
            .await
            .map_err(storage_error("get", key))
    }

    async fn get_object_range(
        &self,
        _bucket: &str,
        key: &str,
        range: Range<u64>,
    ) -> glaux_catalog::Result<Bytes> {
        self.store
            .get_range(&ObjectPath::from(key), range)
            .await
            .map_err(storage_error("get_range", key))
    }

    async fn get_object_suffix(
        &self,
        _bucket: &str,
        key: &str,
        length: u64,
    ) -> glaux_catalog::Result<Bytes> {
        let options = GetOptions {
            range: Some(object_store::GetRange::Suffix(length)),
            ..Default::default()
        };
        self.store
            .get_opts(&ObjectPath::from(key), options)
            .await
            .map_err(storage_error("get_suffix", key))?
            .bytes()
            .await
            .map_err(storage_error("get_suffix", key))
    }

    async fn put_object(&self, bucket: &str, key: &str, data: Bytes) -> glaux_catalog::Result<()> {
        if bucket == "missing-bucket" {
            return Err(CatalogError::Storage {
                operation: "put",
                bucket: bucket.to_string(),
                key: key.to_string(),
                source: Box::new(object_store::Error::NotFound {
                    path: key.to_string(),
                    source: "bucket does not exist".into(),
                }),
            });
        }
        self.store
            .put(&ObjectPath::from(key), PutPayload::from(data))
            .await
            .map_err(storage_error("put", key))?;
        Ok(())
    }

    async fn list_objects(
        &self,
        _bucket: &str,
        prefix: &str,
    ) -> glaux_catalog::Result<Vec<ObjectSummary>> {
        let prefix_path = (!prefix.is_empty()).then(|| ObjectPath::from(prefix));
        let metas: Vec<ObjectMeta> = self
            .store
            .list(prefix_path.as_ref())
            .try_collect()
            .await
            .map_err(storage_error("list", prefix))?;
        Ok(metas
            .into_iter()
            .map(|m| ObjectSummary {
                key: m.location.to_string(),
                size: m.size,
            })
            .collect())
    }

    fn object_store(&self, _bucket: &str) -> glaux_catalog::Result<Arc<dyn ObjectStore>> {
        unreachable!("tests do not read table data through the storage backend")
    }
}

// ---------------------------------------------------------------------------
// A never-ending, slow table whose stream reports when it is dropped, so
// cancellation can be proven to reach the DataFusion task.
// ---------------------------------------------------------------------------

struct DropFlag(Arc<AtomicBool>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

struct SlowPartition {
    schema: SchemaRef,
    dropped: Arc<AtomicBool>,
}

impl fmt::Debug for SlowPartition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SlowPartition")
    }
}

impl PartitionStream for SlowPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let schema = Arc::clone(&self.schema);
        let batch_schema = Arc::clone(&schema);
        let guard = DropFlag(Arc::clone(&self.dropped));
        let stream = futures::stream::unfold((0i64, guard), move |(i, guard)| {
            let schema = Arc::clone(&batch_schema);
            async move {
                tokio::time::sleep(Duration::from_millis(20)).await;
                let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![i]))])
                    .unwrap();
                Some((Ok(batch), (i + 1, guard)))
            }
        });
        Box::pin(RecordBatchStreamAdapter::new(schema, stream))
    }
}

// ---------------------------------------------------------------------------
// Fixtures + server
// ---------------------------------------------------------------------------

struct Harness {
    client: Client,
    addr: SocketAddr,
    storage: Arc<MemoryStorage>,
    slow_dropped: Arc<AtomicBool>,
    parquet_size: u64,
    _tmp: tempfile::TempDir,
}

fn people_batch() -> (SchemaRef, RecordBatch) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float64, true),
        Field::new("active", DataType::Boolean, true),
        Field::new(
            "seen_at",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                Some("ann"),
                Some("bob"),
                None,
                Some("dee"),
                Some("eve"),
            ])),
            Arc::new(Float64Array::from(vec![1.5, 2.0, 3.25, 4.0, 5.5])),
            Arc::new(BooleanArray::from(vec![true, false, true, false, true])),
            Arc::new(TimestampMillisecondArray::from(vec![
                Some(1_706_704_496_789),
                None,
                Some(0),
                Some(1_700_000_000_000),
                Some(1_700_000_000_001),
            ])),
        ],
    )
    .unwrap();
    (schema, batch)
}

async fn start() -> Harness {
    let ctx = SessionContext::new();
    let (schema, batch) = people_batch();
    ctx.register_table(
        "people",
        Arc::new(MemTable::try_new(Arc::clone(&schema), vec![vec![batch.clone()]]).unwrap()),
    )
    .unwrap();

    let slow_dropped = Arc::new(AtomicBool::new(false));
    let slow_schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
    let slow = StreamingTable::try_new(
        Arc::clone(&slow_schema),
        vec![Arc::new(SlowPartition {
            schema: slow_schema,
            dropped: Arc::clone(&slow_dropped),
        })],
    )
    .unwrap();
    ctx.register_table("slow_scan", Arc::new(slow)).unwrap();

    let tmp = tempfile::tempdir().unwrap();
    let parquet_path = tmp.path().join("events.parquet");
    {
        let file = std::fs::File::create(&parquet_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }
    let parquet_size = std::fs::metadata(&parquet_path).unwrap().len();
    ctx.register_parquet(
        "events",
        parquet_path.to_str().unwrap(),
        ParquetReadOptions::default(),
    )
    .await
    .unwrap();

    let storage = Arc::new(MemoryStorage {
        store: InMemory::new(),
    });
    let service = Arc::new(AthenaService::new(
        AthenaServiceConfig::default(),
        Arc::new(TrinoEngine::new(ctx, "datafusion")),
        Arc::clone(&storage) as Arc<dyn StorageBackend>,
    ));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router(service)).await.unwrap();
    });

    let config = aws_sdk_athena::Config::builder()
        .behavior_version_latest()
        .region(Region::new("us-east-1"))
        .endpoint_url(format!("http://{addr}"))
        .credentials_provider(Credentials::new("test", "test", None, None, "test"))
        .build();
    Harness {
        client: Client::from_conf(config),
        addr,
        storage,
        slow_dropped,
        parquet_size,
        _tmp: tmp,
    }
}

const OUTPUT: &str = "s3://results/athena/";

async fn start_query(h: &Harness, sql: &str) -> String {
    h.client
        .start_query_execution()
        .query_string(sql)
        .result_configuration(
            ResultConfiguration::builder()
                .output_location(OUTPUT)
                .build(),
        )
        .send()
        .await
        .expect("StartQueryExecution")
        .query_execution_id
        .expect("id")
}

async fn wait_terminal(h: &Harness, id: &str) -> aws_sdk_athena::types::QueryExecution {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let qe = h
            .client
            .get_query_execution()
            .query_execution_id(id)
            .send()
            .await
            .expect("GetQueryExecution")
            .query_execution
            .expect("QueryExecution");
        let state = qe.status().and_then(|s| s.state()).cloned();
        match state {
            Some(
                QueryExecutionState::Succeeded
                | QueryExecutionState::Failed
                | QueryExecutionState::Cancelled,
            ) => return qe,
            _ if Instant::now() > deadline => panic!("query {id} never finished: {qe:?}"),
            _ => tokio::time::sleep(Duration::from_millis(10)).await,
        }
    }
}

async fn wait_state(h: &Harness, id: &str, wanted: QueryExecutionState) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let qe = h
            .client
            .get_query_execution()
            .query_execution_id(id)
            .send()
            .await
            .unwrap()
            .query_execution
            .unwrap();
        let state = qe.status().and_then(|s| s.state()).cloned();
        if state.as_ref() == Some(&wanted) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "query {id} never reached {wanted:?}: {qe:?}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Page through `GetQueryResults` with `page_size`, returning every row as
/// strings (`None` = NULL) and the column types.
async fn all_rows(
    h: &Harness,
    id: &str,
    page_size: i32,
) -> (Vec<Vec<Option<String>>>, Vec<(String, String)>) {
    let mut rows = Vec::new();
    let mut columns: Vec<(String, String)> = Vec::new();
    let mut token: Option<String> = None;
    let mut pages = 0;
    loop {
        let out = h
            .client
            .get_query_results()
            .query_execution_id(id)
            .max_results(page_size)
            .set_next_token(token.clone())
            .send()
            .await
            .expect("GetQueryResults");
        pages += 1;
        let rs = out.result_set().expect("ResultSet");
        if pages == 1 {
            columns = rs
                .result_set_metadata()
                .map(|m| {
                    m.column_info()
                        .iter()
                        .map(|c| (c.name().to_string(), c.r#type().to_string()))
                        .collect()
                })
                .unwrap_or_default();
        }
        assert!(
            rs.rows().len() <= page_size as usize,
            "page {pages} exceeded MaxResults"
        );
        for row in rs.rows() {
            rows.push(
                row.data()
                    .iter()
                    .map(|d| d.var_char_value().map(str::to_string))
                    .collect(),
            );
        }
        token = out.next_token().map(str::to_string);
        if token.is_none() {
            break;
        }
        assert!(pages < 100, "runaway pagination");
    }
    (rows, columns)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn start_poll_and_page_results_through_the_aws_sdk() {
    let h = start().await;
    let id = start_query(
        &h,
        "SELECT id, name, score, active, seen_at FROM people ORDER BY id",
    )
    .await;

    let qe = wait_terminal(&h, &id).await;
    let status = qe.status().unwrap();
    assert_eq!(
        status.state(),
        Some(&QueryExecutionState::Succeeded),
        "{qe:?}"
    );
    assert!(status.submission_date_time().is_some());
    assert!(status.completion_date_time().is_some());
    assert_eq!(qe.statement_type().map(|s| s.as_str()), Some("DML"));
    assert_eq!(qe.work_group(), Some("primary"));
    assert_eq!(
        qe.result_configuration().and_then(|r| r.output_location()),
        Some(OUTPUT)
    );
    let stats = qe.statistics().unwrap();
    assert!(stats.engine_execution_time_in_millis().is_some());
    assert!(stats.total_execution_time_in_millis().is_some());
    assert!(stats.query_queue_time_in_millis().is_some());
    assert_eq!(
        stats.data_scanned_in_bytes(),
        Some(0),
        "memory table scans read no bytes"
    );

    // Page size 2 → header + 5 rows = 3 pages; the header row only appears
    // once and every page respects MaxResults.
    let (rows, columns) = all_rows(&h, &id, 2).await;
    assert_eq!(
        columns,
        [
            ("id", "bigint"),
            ("name", "varchar"),
            ("score", "double"),
            ("active", "boolean"),
            ("seen_at", "timestamp"),
        ]
        .map(|(n, t)| (n.to_string(), t.to_string()))
    );
    let s = |v: &str| Some(v.to_string());
    assert_eq!(rows.len(), 6);
    assert_eq!(
        rows[0],
        vec![s("id"), s("name"), s("score"), s("active"), s("seen_at")]
    );
    assert_eq!(
        rows[1],
        vec![
            s("1"),
            s("ann"),
            s("1.5"),
            s("true"),
            s("2024-01-31 12:34:56.789")
        ]
    );
    assert_eq!(rows[2], vec![s("2"), s("bob"), s("2.0"), s("false"), None]);
    assert_eq!(
        rows[3],
        vec![
            s("3"),
            None,
            s("3.25"),
            s("true"),
            s("1970-01-01 00:00:00.000")
        ]
    );

    // The same rows come back with the default page size in one page.
    let (single_page, _) = all_rows(&h, &id, 1000).await;
    assert_eq!(single_page, rows);

    // The CSV landed at OutputLocation/<id>.csv in Athena's quoting style.
    let csv = h
        .storage
        .get_object("results", &format!("athena/{id}.csv"))
        .await
        .expect("result csv written");
    let csv = String::from_utf8(csv.to_vec()).unwrap();
    let lines: Vec<&str> = csv.lines().collect();
    assert_eq!(lines[0], "\"id\",\"name\",\"score\",\"active\",\"seen_at\"");
    assert_eq!(
        lines[3],
        "\"3\",,\"3.25\",\"true\",\"1970-01-01 00:00:00.000\""
    );
    assert_eq!(lines.len(), 6);
}

#[tokio::test]
async fn parquet_scans_report_data_scanned_bytes() {
    let h = start().await;
    let id = start_query(&h, "SELECT count(*) FROM events WHERE active").await;
    let qe = wait_terminal(&h, &id).await;
    assert_eq!(
        qe.status().unwrap().state(),
        Some(&QueryExecutionState::Succeeded),
        "{qe:?}"
    );
    let scanned = qe.statistics().unwrap().data_scanned_in_bytes().unwrap();
    assert!(scanned > 0, "parquet scan must account bytes");
    assert!(
        scanned <= h.parquet_size as i64,
        "scanned {scanned} > file size {}",
        h.parquet_size
    );
    let (rows, _) = all_rows(&h, &id, 1000).await;
    assert_eq!(rows[1], vec![Some("3".to_string())]);
}

#[tokio::test]
async fn bad_sql_fails_with_athena_error_details_not_fake_rows() {
    let h = start().await;

    // Parse failure.
    let id = start_query(&h, "SELEC id FROM people").await;
    let qe = wait_terminal(&h, &id).await;
    let status = qe.status().unwrap();
    assert_eq!(status.state(), Some(&QueryExecutionState::Failed));
    let err = status.athena_error().expect("AthenaError details");
    assert_eq!(err.error_category(), Some(2), "user error");
    assert!(
        err.error_message().unwrap().contains("SYNTAX_ERROR"),
        "{err:?}"
    );
    assert!(
        status.state_change_reason().unwrap().contains("SELEC"),
        "message names the bad token: {status:?}"
    );
    assert_eq!(qe.statistics().unwrap().data_scanned_in_bytes(), Some(0));

    // Unknown column: the planner's diagnostic is passed through verbatim.
    let id = start_query(&h, "SELECT nope FROM people").await;
    let qe = wait_terminal(&h, &id).await;
    assert!(
        qe.status()
            .unwrap()
            .state_change_reason()
            .unwrap()
            .contains("No field named nope"),
        "{qe:?}"
    );

    // Writes/DDL are refused by name instead of being executed locally.
    let id = start_query(&h, "CREATE TABLE t AS SELECT * FROM people").await;
    let qe = wait_terminal(&h, &id).await;
    assert_eq!(qe.statement_type().map(|s| s.as_str()), Some("DDL"));
    let reason = qe.status().unwrap().state_change_reason().unwrap();
    assert!(reason.contains("NOT_SUPPORTED"), "{reason}");
    assert!(reason.contains("CreateMemoryTable"), "{reason}");

    // Results are not available for a failed query.
    let err = h
        .client
        .get_query_results()
        .query_execution_id(&id)
        .send()
        .await
        .unwrap_err();
    match err.into_service_error() {
        GetQueryResultsError::InvalidRequestException(e) => {
            assert!(
                e.message().unwrap().contains("Final query state: FAILED"),
                "{e:?}"
            );
        }
        other => panic!("expected InvalidRequestException, got {other:?}"),
    }
}

#[tokio::test]
async fn trino_dialect_runs_and_unsupported_constructs_fail_by_name() {
    let h = start().await;

    // Trino-only functions and argument orders work through the SDK.
    let id = start_query(
        &h,
        "SELECT id, if(active, 'yes', 'no') AS flag, \
                date_diff('day', DATE '2024-01-01', CAST(seen_at AS DATE)) AS days, \
                element_at(split(name, 'n'), 1) AS head \
         FROM people WHERE id = 1",
    )
    .await;
    let qe = wait_terminal(&h, &id).await;
    assert_eq!(
        qe.status().unwrap().state(),
        Some(&QueryExecutionState::Succeeded),
        "{qe:?}"
    );
    let (rows, columns) = all_rows(&h, &id, 10).await;
    assert_eq!(
        columns.iter().map(|(_, t)| t.as_str()).collect::<Vec<_>>(),
        ["bigint", "varchar", "bigint", "varchar"]
    );
    let s = |v: &str| Some(v.to_string());
    assert_eq!(rows[0], vec![s("id"), s("flag"), s("days"), s("head")]);
    // ann, active, seen 2024-01-31T12:34:56.789Z → 30 days after New Year.
    assert_eq!(rows[1], vec![s("1"), s("yes"), s("30"), s("a")]);

    // A construct glaux cannot translate fails the query with a user-category
    // AthenaError naming the construct — no rows are ever synthesised.
    let id = start_query(&h, "SELECT transform(ARRAY[1, 2], x -> x + 1)").await;
    let qe = wait_terminal(&h, &id).await;
    let status = qe.status().unwrap();
    assert_eq!(status.state(), Some(&QueryExecutionState::Failed));
    let err = status.athena_error().expect("AthenaError details");
    assert_eq!(err.error_category(), Some(2), "user error");
    assert_eq!(err.error_type(), Some(1003), "NOT_SUPPORTED");
    let message = err.error_message().unwrap();
    assert!(message.contains("NOT_SUPPORTED"), "{message}");
    assert!(message.contains("lambda expression"), "{message}");

    // Unknown functions are refused even when DataFusion has a same-named
    // function with different semantics.
    let id = start_query(&h, "SELECT repeat('a', 3)").await;
    let qe = wait_terminal(&h, &id).await;
    let reason = qe.status().unwrap().state_change_reason().unwrap();
    assert!(reason.contains("function repeat"), "{reason}");
}

#[tokio::test]
async fn result_write_failure_fails_the_query() {
    let h = start().await;
    let id = h
        .client
        .start_query_execution()
        .query_string("SELECT 1")
        .result_configuration(
            ResultConfiguration::builder()
                .output_location("s3://missing-bucket/out/")
                .build(),
        )
        .send()
        .await
        .unwrap()
        .query_execution_id
        .unwrap();
    let qe = wait_terminal(&h, &id).await;
    let status = qe.status().unwrap();
    assert_eq!(status.state(), Some(&QueryExecutionState::Failed));
    assert!(
        status
            .state_change_reason()
            .unwrap()
            .contains("s3://missing-bucket/out/"),
        "{status:?}"
    );
    assert_eq!(status.athena_error().unwrap().error_category(), Some(1));
}

#[tokio::test]
async fn stop_query_execution_aborts_the_running_scan() {
    let h = start().await;
    let id = start_query(&h, "SELECT sum(n) FROM slow_scan").await;
    wait_state(&h, &id, QueryExecutionState::Running).await;
    assert!(!h.slow_dropped.load(Ordering::SeqCst));

    h.client
        .stop_query_execution()
        .query_execution_id(&id)
        .send()
        .await
        .expect("StopQueryExecution");

    let qe = wait_terminal(&h, &id).await;
    let status = qe.status().unwrap();
    assert_eq!(status.state(), Some(&QueryExecutionState::Cancelled));
    assert!(status.completion_date_time().is_some());
    assert!(
        qe.statistics()
            .unwrap()
            .total_execution_time_in_millis()
            .is_some()
    );

    // The DataFusion stream was dropped, i.e. the engine task really was
    // aborted rather than left running in the background.
    let deadline = Instant::now() + Duration::from_secs(5);
    while !h.slow_dropped.load(Ordering::SeqCst) {
        assert!(Instant::now() < deadline, "DataFusion scan was not aborted");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Stopping again is a no-op success; results are refused.
    h.client
        .stop_query_execution()
        .query_execution_id(&id)
        .send()
        .await
        .expect("second stop is idempotent");
    let err = h
        .client
        .get_query_results()
        .query_execution_id(&id)
        .send()
        .await
        .unwrap_err();
    match err.into_service_error() {
        GetQueryResultsError::InvalidRequestException(e) => {
            assert!(e.message().unwrap().contains("CANCELLED"), "{e:?}");
        }
        other => panic!("expected InvalidRequestException, got {other:?}"),
    }
}

#[tokio::test]
async fn results_of_a_running_query_are_refused_and_unknown_ids_are_errors() {
    let h = start().await;
    let id = start_query(&h, "SELECT count(*) FROM slow_scan").await;
    wait_state(&h, &id, QueryExecutionState::Running).await;
    let err = h
        .client
        .get_query_results()
        .query_execution_id(&id)
        .send()
        .await
        .unwrap_err();
    match err.into_service_error() {
        GetQueryResultsError::InvalidRequestException(e) => {
            assert!(
                e.message().unwrap().contains("Query has not yet finished"),
                "{e:?}"
            );
        }
        other => panic!("expected InvalidRequestException, got {other:?}"),
    }
    h.client
        .stop_query_execution()
        .query_execution_id(&id)
        .send()
        .await
        .unwrap();

    let err = h
        .client
        .get_query_execution()
        .query_execution_id("does-not-exist")
        .send()
        .await
        .unwrap_err();
    let service_err = err.into_service_error();
    assert!(
        service_err.is_invalid_request_exception(),
        "{service_err:?}"
    );

    // A bogus NextToken is an explicit error, not an empty page.
    let ok_id = start_query(&h, "SELECT 1").await;
    wait_terminal(&h, &ok_id).await;
    let err = h
        .client
        .get_query_results()
        .query_execution_id(&ok_id)
        .next_token("garbage")
        .send()
        .await
        .unwrap_err();
    match err.into_service_error() {
        GetQueryResultsError::InvalidRequestException(e) => {
            assert_eq!(e.athena_error_code(), Some("INVALID_NEXT_TOKEN"));
        }
        other => panic!("expected InvalidRequestException, got {other:?}"),
    }
}

#[tokio::test]
async fn list_and_batch_get_follow_athena_ordering_and_paging() {
    let h = start().await;
    let mut ids = Vec::new();
    for i in 0..3 {
        ids.push(start_query(&h, &format!("SELECT {i}")).await);
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    for id in &ids {
        wait_terminal(&h, id).await;
    }

    // Newest first, paged by MaxResults.
    let page1 = h
        .client
        .list_query_executions()
        .max_results(2)
        .send()
        .await
        .unwrap();
    assert_eq!(
        page1.query_execution_ids(),
        &[ids[2].clone(), ids[1].clone()]
    );
    let page2 = h
        .client
        .list_query_executions()
        .max_results(2)
        .next_token(page1.next_token().unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(page2.query_execution_ids(), &[ids[0].clone()]);
    assert!(page2.next_token().is_none());

    let batch = h
        .client
        .batch_get_query_execution()
        .query_execution_ids(&ids[0])
        .query_execution_ids("nope")
        .query_execution_ids(&ids[2])
        .send()
        .await
        .unwrap();
    let got: Vec<&str> = batch
        .query_executions()
        .iter()
        .map(|q| q.query_execution_id().unwrap())
        .collect();
    assert_eq!(got, [ids[0].as_str(), ids[2].as_str()]);
    assert_eq!(batch.query_executions()[1].query(), Some("SELECT 2"));
    let unprocessed = batch.unprocessed_query_execution_ids();
    assert_eq!(unprocessed.len(), 1);
    assert_eq!(unprocessed[0].query_execution_id(), Some("nope"));
    assert!(unprocessed[0].error_message().unwrap().contains("nope"));
}

#[tokio::test]
async fn client_request_token_is_idempotent() {
    let h = start().await;
    let start = |sql: &'static str| {
        h.client
            .start_query_execution()
            .query_string(sql)
            .client_request_token("token-0123456789abcdef0123456789abcdef")
            .result_configuration(
                ResultConfiguration::builder()
                    .output_location(OUTPUT)
                    .build(),
            )
            .send()
    };
    let first = start("SELECT 1").await.unwrap().query_execution_id.unwrap();
    let second = start("SELECT 1").await.unwrap().query_execution_id.unwrap();
    assert_eq!(first, second);
    let err = start("SELECT 2").await.unwrap_err().into_service_error();
    assert!(err.is_invalid_request_exception(), "{err:?}");
}

#[tokio::test]
async fn workgroups_supply_output_locations_and_missing_ones_are_rejected() {
    let h = start().await;

    // The default workgroup has no OutputLocation, so a query without one
    // is refused exactly as Athena refuses it.
    let err = h
        .client
        .start_query_execution()
        .query_string("SELECT 1")
        .send()
        .await
        .unwrap_err();
    match err.into_service_error() {
        StartQueryExecutionError::InvalidRequestException(e) => {
            assert!(e.message().unwrap().contains("No output location"), "{e:?}");
        }
        other => panic!("expected InvalidRequestException, got {other:?}"),
    }

    h.client
        .create_work_group()
        .name("analytics")
        .description("team wg")
        .configuration(
            WorkGroupConfiguration::builder()
                .result_configuration(
                    ResultConfiguration::builder()
                        .output_location("s3://results/wg/")
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .expect("CreateWorkGroup");

    // Duplicate names are rejected.
    let dup = h
        .client
        .create_work_group()
        .name("analytics")
        .send()
        .await
        .unwrap_err()
        .into_service_error();
    assert!(dup.is_invalid_request_exception(), "{dup:?}");

    let wg = h
        .client
        .get_work_group()
        .work_group("analytics")
        .send()
        .await
        .unwrap()
        .work_group
        .unwrap();
    assert_eq!(wg.name(), "analytics");
    assert_eq!(wg.description(), Some("team wg"));
    assert_eq!(
        wg.configuration()
            .and_then(|c| c.result_configuration())
            .and_then(|r| r.output_location()),
        Some("s3://results/wg/")
    );

    let missing = h
        .client
        .get_work_group()
        .work_group("ghost")
        .send()
        .await
        .unwrap_err()
        .into_service_error();
    // Athena's GetWorkGroup models only InvalidRequestException for this.
    assert!(missing.is_invalid_request_exception(), "{missing:?}");

    let listed = h.client.list_work_groups().send().await.unwrap();
    let names: Vec<&str> = listed
        .work_groups()
        .iter()
        .filter_map(|w| w.name())
        .collect();
    assert_eq!(names, ["analytics", "primary"]);

    // A query in that workgroup inherits its OutputLocation, writes its CSV
    // there, and reports the workgroup + database context back.
    let id = h
        .client
        .start_query_execution()
        .query_string("SELECT name FROM people WHERE id = 1")
        .work_group("analytics")
        .query_execution_context(
            QueryExecutionContext::builder()
                .database("public")
                .catalog("AwsDataCatalog")
                .build(),
        )
        .send()
        .await
        .unwrap()
        .query_execution_id
        .unwrap();
    let qe = wait_terminal(&h, &id).await;
    assert_eq!(
        qe.status().unwrap().state(),
        Some(&QueryExecutionState::Succeeded),
        "{qe:?}"
    );
    assert_eq!(qe.work_group(), Some("analytics"));
    assert_eq!(
        qe.result_configuration().and_then(|r| r.output_location()),
        Some("s3://results/wg/")
    );
    assert_eq!(
        qe.query_execution_context().and_then(|c| c.database()),
        Some("public")
    );
    h.storage
        .get_object("results", &format!("wg/{id}.csv"))
        .await
        .expect("csv under the workgroup prefix");
    let (rows, _) = all_rows(&h, &id, 10).await;
    assert_eq!(rows[1], vec![Some("ann".to_string())]);

    // Only the analytics query shows up when filtering the listing.
    let listed = h
        .client
        .list_query_executions()
        .work_group("analytics")
        .send()
        .await
        .unwrap();
    assert_eq!(listed.query_execution_ids(), &[id]);
}

#[tokio::test]
async fn unknown_actions_and_malformed_bodies_are_explicit_errors() {
    let h = start().await;
    // Drive the wire protocol directly for shapes the SDK cannot produce.
    let http = reqwest_lite::Client::new(&h).await;
    let (status, body) = http.post("AmazonAthena.CreateNamedQuery", "{}").await;
    assert_eq!(status, 400);
    assert!(body.contains("UnknownOperationException"), "{body}");
    assert!(body.contains("CreateNamedQuery"), "{body}");

    let (status, body) = http
        .post("AmazonAthena.GetQueryExecution", "{not json")
        .await;
    assert_eq!(status, 400);
    assert!(body.contains("SerializationException"), "{body}");

    let (status, body) = http.post("SomethingElse.Op", "{}").await;
    assert_eq!(status, 400);
    assert!(body.contains("UnknownOperationException"), "{body}");
}

/// Minimal raw HTTP client over the SDK's endpoint (tokio TCP), so the tests
/// can send requests the typed SDK will not build.
mod reqwest_lite {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    pub struct Client {
        addr: String,
    }

    impl Client {
        pub async fn new(h: &super::Harness) -> Self {
            Self {
                addr: h.addr.to_string(),
            }
        }

        pub async fn post(&self, target: &str, body: &str) -> (u16, String) {
            let mut stream = tokio::net::TcpStream::connect(&self.addr).await.unwrap();
            let request = format!(
                "POST / HTTP/1.1\r\nHost: {}\r\nX-Amz-Target: {target}\r\n\
                 Content-Type: application/x-amz-json-1.1\r\nContent-Length: {}\r\n\
                 Connection: close\r\n\r\n{body}",
                self.addr,
                body.len()
            );
            stream.write_all(request.as_bytes()).await.unwrap();
            let mut response = Vec::new();
            stream.read_to_end(&mut response).await.unwrap();
            let response = String::from_utf8_lossy(&response).to_string();
            let status: u16 = response
                .split_whitespace()
                .nth(1)
                .and_then(|s| s.parse().ok())
                .expect("status line");
            let body = response
                .split_once("\r\n\r\n")
                .map(|(_, b)| b.to_string())
                .unwrap_or_default();
            (status, body)
        }
    }
}
