//! End-to-end tests for `GlueCatalogProvider` through DataFusion SQL, using
//! an in-memory fake `GlueApi` (which counts `GetPartitions` calls) and an
//! in-memory object store whose every access is logged, so the tests can
//! prove which objects a query actually touched.
//!
//! Fixtures are real files written by arrow/parquet writers: Parquet, NDJSON,
//! and delimited text.

use std::collections::HashMap;
use std::fmt;
use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::{
    Array, ArrayRef, Float32Array, Float64Array, Int32Array, Int64Array, StringArray,
    TimestampMillisecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use futures::stream::BoxStream;
use glaux_catalog::{
    CatalogError, GlueApi, GlueCatalogProvider, GlueColumn, GlueDatabase, GluePartition,
    GlueSerDeInfo, GlueStorageDescriptor, GlueTable, ObjectSummary, StorageBackend,
};
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt as _, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use parquet::arrow::ArrowWriter;

const PARQUET_SERDE: &str = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";
const OPENX_JSON_SERDE: &str = "org.openx.data.jsonserde.JsonSerDe";
const LAZY_SIMPLE_SERDE: &str = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
const BUCKET: &str = "data-lake";

// ---------------------------------------------------------------------------
// Access-logged in-memory object store + StorageBackend
// ---------------------------------------------------------------------------

/// Object store that records every object key read (`get`) and every prefix
/// listed (`list`), delegating to [`InMemory`].
#[derive(Debug)]
struct LoggingStore {
    inner: InMemory,
    reads: Mutex<Vec<String>>,
    lists: Mutex<Vec<String>>,
}

impl fmt::Display for LoggingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LoggingStore")
    }
}

#[async_trait]
impl ObjectStore for LoggingStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.reads.lock().unwrap().push(location.to_string());
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<ObjectPath>>,
    ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.lists
            .lock()
            .unwrap()
            .push(prefix.map(ToString::to_string).unwrap_or_default());
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// [`StorageBackend`] over one [`LoggingStore`] serving every bucket.
struct MemoryStorage {
    store: Arc<LoggingStore>,
}

impl MemoryStorage {
    fn new() -> Self {
        Self {
            store: Arc::new(LoggingStore {
                inner: InMemory::new(),
                reads: Mutex::new(Vec::new()),
                lists: Mutex::new(Vec::new()),
            }),
        }
    }

    async fn put(&self, key: &str, data: impl Into<Bytes>) {
        self.store
            .put(&ObjectPath::from(key), PutPayload::from(data.into()))
            .await
            .expect("put fixture object");
    }

    fn reads(&self) -> Vec<String> {
        self.store.reads.lock().unwrap().clone()
    }

    fn lists(&self) -> Vec<String> {
        self.store.lists.lock().unwrap().clone()
    }

    fn clear_log(&self) {
        self.store.reads.lock().unwrap().clear();
        self.store.lists.lock().unwrap().clear();
    }
}

fn storage_error(
    operation: &'static str,
    key: &str,
) -> impl FnOnce(object_store::Error) -> CatalogError {
    let key = key.to_string();
    move |source| CatalogError::Storage {
        operation,
        bucket: BUCKET.to_string(),
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

    async fn put_object(&self, _bucket: &str, key: &str, data: Bytes) -> glaux_catalog::Result<()> {
        self.store
            .put(&ObjectPath::from(key), PutPayload::from(data))
            .await
            .map_err(storage_error("put", key))?;
        Ok(())
    }

    async fn delete_object(&self, _bucket: &str, key: &str) -> glaux_catalog::Result<()> {
        match self.store.delete(&ObjectPath::from(key)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(storage_error("delete", key)(e)),
        }
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
        Ok(Arc::clone(&self.store) as Arc<dyn ObjectStore>)
    }
}

// ---------------------------------------------------------------------------
// In-memory fake Glue
// ---------------------------------------------------------------------------

#[derive(Default)]
struct FakeGlue {
    databases: Vec<GlueDatabase>,
    tables: HashMap<(String, String), GlueTable>,
    partitions: HashMap<(String, String), Vec<GluePartition>>,
    get_partitions_calls: AtomicUsize,
}

impl FakeGlue {
    fn with_database(mut self, name: &str) -> Self {
        self.databases.push(GlueDatabase {
            name: name.to_string(),
            description: None,
            location_uri: None,
            parameters: HashMap::new(),
        });
        self
    }

    fn with_table(mut self, database: &str, table: GlueTable) -> Self {
        self.tables
            .insert((database.to_string(), table.name.clone()), table);
        self
    }

    fn with_partitions(
        mut self,
        database: &str,
        table: &str,
        partitions: Vec<GluePartition>,
    ) -> Self {
        self.partitions
            .insert((database.to_string(), table.to_string()), partitions);
        self
    }

    fn get_partitions_calls(&self) -> usize {
        self.get_partitions_calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl GlueApi for FakeGlue {
    async fn get_databases(&self) -> glaux_catalog::Result<Vec<GlueDatabase>> {
        Ok(self.databases.clone())
    }

    async fn get_tables(&self, database: &str) -> glaux_catalog::Result<Vec<GlueTable>> {
        Ok(self
            .tables
            .iter()
            .filter(|((db, _), _)| db == database)
            .map(|(_, t)| t.clone())
            .collect())
    }

    async fn get_table(&self, database: &str, table: &str) -> glaux_catalog::Result<GlueTable> {
        self.tables
            .get(&(database.to_string(), table.to_string()))
            .cloned()
            .ok_or_else(|| CatalogError::GlueEntityNotFound {
                message: format!("Table {table} not found in database {database}"),
            })
    }

    async fn get_partitions(
        &self,
        database: &str,
        table: &str,
        _expression: Option<&str>,
    ) -> glaux_catalog::Result<Vec<GluePartition>> {
        self.get_partitions_calls.fetch_add(1, Ordering::SeqCst);
        Ok(self
            .partitions
            .get(&(database.to_string(), table.to_string()))
            .cloned()
            .unwrap_or_default())
    }
}

// ---------------------------------------------------------------------------
// Fixture builders
// ---------------------------------------------------------------------------

fn column(name: &str, hive_type: &str) -> GlueColumn {
    GlueColumn {
        name: name.to_string(),
        column_type: Some(hive_type.to_string()),
        comment: None,
    }
}

fn params(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn storage_descriptor(
    columns: Vec<GlueColumn>,
    location: &str,
    serde: &str,
    serde_params: &[(&str, &str)],
) -> GlueStorageDescriptor {
    GlueStorageDescriptor {
        columns,
        location: Some(location.to_string()),
        serde_info: Some(GlueSerDeInfo {
            name: None,
            serialization_library: Some(serde.to_string()),
            parameters: params(serde_params),
        }),
        ..Default::default()
    }
}

fn table(
    name: &str,
    sd: GlueStorageDescriptor,
    partition_keys: Vec<GlueColumn>,
    parameters: &[(&str, &str)],
) -> GlueTable {
    GlueTable {
        name: name.to_string(),
        database_name: Some("lake".to_string()),
        table_type: Some("EXTERNAL_TABLE".to_string()),
        storage_descriptor: Some(sd),
        partition_keys,
        parameters: params(parameters),
    }
}

fn partition(values: &[&str], location: &str, serde: &str) -> GluePartition {
    GluePartition {
        values: values.iter().map(ToString::to_string).collect(),
        storage_descriptor: Some(storage_descriptor(Vec::new(), location, serde, &[])),
        parameters: HashMap::new(),
    }
}

/// Parquet bytes for rows `(id: bigint, amount: double, ts: timestamp(ms))`.
fn sales_parquet(ids: &[i64], amounts: &[f64]) -> Bytes {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ]));
    let ts: Vec<i64> = ids.iter().map(|id| 1_700_000_000_000 + id * 1000).collect();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef,
            Arc::new(Float64Array::from(amounts.to_vec())),
            Arc::new(TimestampMillisecondArray::from(ts)),
        ],
    )
    .unwrap();
    let mut buf = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    Bytes::from(buf)
}

async fn context(glue: Arc<FakeGlue>, storage: Arc<MemoryStorage>) -> SessionContext {
    let catalog = GlueCatalogProvider::try_new(glue, storage)
        .await
        .expect("catalog provider");
    let ctx = SessionContext::new();
    ctx.register_catalog("glue", Arc::new(catalog));
    ctx
}

async fn query(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
    ctx.sql(sql)
        .await
        .unwrap_or_else(|e| panic!("planning {sql}: {e}"))
        .collect()
        .await
        .unwrap_or_else(|e| panic!("executing {sql}: {e}"))
}

async fn query_err(ctx: &SessionContext, sql: &str) -> String {
    match ctx.sql(sql).await {
        Err(e) => e.to_string(),
        Ok(df) => match df.collect().await {
            Err(e) => e.to_string(),
            Ok(batches) => panic!(
                "{sql} should have failed, got:\n{}",
                pretty_format_batches(&batches).unwrap()
            ),
        },
    }
}

fn int64_column(batches: &[RecordBatch], index: usize) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|b| {
            b.column(index)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64 column")
                .values()
                .to_vec()
        })
        .collect()
}

fn string_column(batches: &[RecordBatch], index: usize) -> Vec<String> {
    batches
        .iter()
        .flat_map(|b| {
            let array = b
                .column(index)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("string column");
            (0..array.len())
                .map(|i| array.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Partitioned Parquet table backed by Glue partitions: a partition-column
/// filter must read only the matching partition's objects.
#[tokio::test]
async fn partitioned_parquet_scans_only_matching_partitions() {
    let storage = Arc::new(MemoryStorage::new());
    for (day, ids, amounts) in [
        ("2024-01-01", &[1i64, 2][..], &[10.0f64, 20.0][..]),
        ("2024-01-02", &[3, 4], &[30.0, 40.0]),
        ("2024-01-03", &[5, 6], &[50.0, 60.0]),
    ] {
        storage
            .put(
                &format!("sales/day={day}/part-0.parquet"),
                sales_parquet(ids, amounts),
            )
            .await;
    }
    // Hidden files and directory markers must be ignored.
    storage
        .put("sales/day=2024-01-02/_SUCCESS", Bytes::new())
        .await;
    storage
        .put(
            "sales/day=2024-01-02/.hidden.parquet",
            Bytes::from_static(b"junk"),
        )
        .await;

    let glue = Arc::new(
        FakeGlue::default()
            .with_database("lake")
            .with_table(
                "lake",
                table(
                    "sales",
                    storage_descriptor(
                        vec![
                            column("id", "bigint"),
                            column("amount", "double"),
                            column("ts", "timestamp"),
                        ],
                        &format!("s3://{BUCKET}/sales/"),
                        PARQUET_SERDE,
                        &[],
                    ),
                    vec![column("day", "string")],
                    &[],
                ),
            )
            .with_partitions(
                "lake",
                "sales",
                ["2024-01-01", "2024-01-02", "2024-01-03"]
                    .iter()
                    .map(|day| {
                        partition(
                            &[day],
                            &format!("s3://{BUCKET}/sales/day={day}/"),
                            PARQUET_SERDE,
                        )
                    })
                    .collect(),
            ),
    );
    let ctx = context(Arc::clone(&glue), Arc::clone(&storage)).await;

    // Catalog surface.
    let catalog = ctx.catalog("glue").unwrap();
    assert_eq!(catalog.schema_names(), vec!["lake".to_string()]);
    assert_eq!(
        catalog.schema("lake").unwrap().table_names(),
        vec!["sales".to_string()]
    );

    // Full scan: every partition, typed schema, partition column appended.
    let batches = query(
        &ctx,
        "SELECT id, amount, day, ts FROM glue.lake.sales ORDER BY id",
    )
    .await;
    assert_eq!(int64_column(&batches, 0), vec![1, 2, 3, 4, 5, 6]);
    assert_eq!(
        string_column(&batches, 2),
        vec![
            "2024-01-01",
            "2024-01-01",
            "2024-01-02",
            "2024-01-02",
            "2024-01-03",
            "2024-01-03"
        ]
    );
    assert_eq!(
        batches[0].schema().field(3).data_type(),
        &DataType::Timestamp(TimeUnit::Nanosecond, None)
    );
    let full_scan_reads = storage.reads();
    assert!(
        !full_scan_reads
            .iter()
            .any(|k| k.contains("_SUCCESS") || k.contains(".hidden")),
        "hidden files must never be read: {full_scan_reads:?}"
    );

    // Pruned scan: only day=2024-01-02 objects may be listed or read.
    storage.clear_log();
    let batches = query(
        &ctx,
        "SELECT id FROM glue.lake.sales WHERE day = '2024-01-02' ORDER BY id",
    )
    .await;
    assert_eq!(int64_column(&batches, 0), vec![3, 4]);
    let reads = storage.reads();
    assert!(!reads.is_empty(), "the matching partition must be read");
    assert!(
        reads.iter().all(|k| k.starts_with("sales/day=2024-01-02/")),
        "only the matching partition may be read, got {reads:?}"
    );
    let lists = storage.lists();
    assert_eq!(
        lists,
        vec!["sales/day=2024-01-02".to_string()],
        "only the matching partition may be listed"
    );

    // Range predicate on the partition column prunes too.
    storage.clear_log();
    let batches = query(
        &ctx,
        "SELECT sum(amount) FROM glue.lake.sales WHERE day > '2024-01-01'",
    )
    .await;
    let total = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(0);
    assert_eq!(total, 180.0);
    assert!(
        storage
            .reads()
            .iter()
            .all(|k| !k.contains("day=2024-01-01")),
        "day=2024-01-01 must be pruned: {:?}",
        storage.reads()
    );

    // A non-partition predicate cannot prune but must still be correct.
    let batches = query(
        &ctx,
        "SELECT id FROM glue.lake.sales WHERE amount >= 50 ORDER BY id",
    )
    .await;
    assert_eq!(int64_column(&batches, 0), vec![5, 6]);

    // Integer-typed partition keys prune through typed comparison.
    assert!(glue.get_partitions_calls() >= 1);
}

/// Typed (non-string) partition keys are parsed into the declared type, so
/// numeric predicates prune correctly (no lexical string comparison).
#[tokio::test]
async fn integer_partition_keys_prune_numerically() {
    let storage = Arc::new(MemoryStorage::new());
    for year in [9, 10, 11] {
        storage
            .put(
                &format!("events/year={year}/data.parquet"),
                sales_parquet(&[year], &[1.0]),
            )
            .await;
    }
    let glue = Arc::new(
        FakeGlue::default()
            .with_database("lake")
            .with_table(
                "lake",
                table(
                    "events",
                    storage_descriptor(
                        vec![
                            column("id", "bigint"),
                            column("amount", "double"),
                            column("ts", "timestamp"),
                        ],
                        &format!("s3://{BUCKET}/events"),
                        PARQUET_SERDE,
                        &[],
                    ),
                    vec![column("year", "int")],
                    &[],
                ),
            )
            .with_partitions(
                "lake",
                "events",
                [9, 10, 11]
                    .iter()
                    .map(|y| {
                        partition(
                            &[&y.to_string()],
                            &format!("s3://{BUCKET}/events/year={y}"),
                            PARQUET_SERDE,
                        )
                    })
                    .collect(),
            ),
    );
    let ctx = context(glue, Arc::clone(&storage)).await;

    // Lexically "9" > "10", numerically it is not.
    let batches = query(
        &ctx,
        "SELECT id, year FROM glue.lake.events WHERE year >= 10 ORDER BY id",
    )
    .await;
    assert_eq!(int64_column(&batches, 0), vec![10, 11]);
    assert_eq!(batches[0].schema().field(1).data_type(), &DataType::Int32);
    let years: Vec<i32> = batches
        .iter()
        .flat_map(|b| {
            b.column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(years, vec![10, 11]);
    assert!(
        storage.reads().iter().all(|k| !k.contains("year=9/")),
        "year=9 must be pruned: {:?}",
        storage.reads()
    );
}

/// OpenX JSON SerDe → NDJSON reader, with nested struct/array/map columns
/// typed from the Glue schema.
#[tokio::test]
async fn json_table_reads_ndjson_with_nested_types() {
    let storage = Arc::new(MemoryStorage::new());
    let ndjson = concat!(
        r#"{"id":1,"user":{"name":"ada","age":36},"tags":["x","y"],"score":1.5,"attrs":{"k":"v"}}"#,
        "\n",
        r#"{"id":2,"user":{"name":"linus","age":54},"tags":[],"score":2.5,"attrs":{}}"#,
        "\n",
        r#"{"id":3,"user":null,"tags":["z"],"score":null,"attrs":null}"#,
        "\n",
    );
    storage.put("events/part-0.json", ndjson).await;

    let glue = Arc::new(FakeGlue::default().with_database("lake").with_table(
        "lake",
        table(
            "events",
            storage_descriptor(
                vec![
                    column("id", "bigint"),
                    column("user", "struct<name:string,age:int>"),
                    column("tags", "array<string>"),
                    column("score", "double"),
                    column("attrs", "map<string,string>"),
                ],
                &format!("s3://{BUCKET}/events/"),
                OPENX_JSON_SERDE,
                &[],
            ),
            Vec::new(),
            &[("classification", "json")],
        ),
    ));
    let ctx = context(glue, storage).await;

    let batches = query(
        &ctx,
        "SELECT id, user['name'] AS name, user['age'] AS age, cardinality(tags) AS n_tags, \
         score, attrs['k'] AS k \
         FROM glue.lake.events ORDER BY id",
    )
    .await;
    let rendered = pretty_format_batches(&batches).unwrap().to_string();
    assert_eq!(int64_column(&batches, 0), vec![1, 2, 3]);
    let schema = batches[0].schema();
    assert_eq!(schema.field(1).data_type(), &DataType::Utf8, "{rendered}");
    assert_eq!(schema.field(2).data_type(), &DataType::Int32, "{rendered}");
    assert_eq!(
        schema.field(4).data_type(),
        &DataType::Float64,
        "{rendered}"
    );

    let names = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "ada");
    assert_eq!(names.value(1), "linus");
    assert!(names.is_null(2));
    let ages = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ages.value(0), 36);
    assert!(ages.is_null(2));
    let n_tags = batches[0]
        .column(3)
        .as_any()
        .downcast_ref::<arrow::array::UInt64Array>()
        .unwrap();
    assert_eq!(n_tags.values(), &[2, 0, 1]);
    let k = batches[0]
        .column(5)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(k.value(0), "v");
    assert!(k.is_null(1));
}

/// LazySimpleSerDe → CSV reader honoring `field.delim` and
/// `skip.header.line.count`, with typed columns.
#[tokio::test]
async fn csv_table_reads_delimited_text_with_configured_delimiter() {
    let storage = Arc::new(MemoryStorage::new());
    storage
        .put(
            "people/people.csv",
            "id|name|height|city\n1|ada|1.62|london\n2|grace|1.70|new york\n3|\"quoted\"|1.80|\n",
        )
        .await;
    // A second partition-less file proves multi-file listing works.
    storage
        .put(
            "people/more.csv",
            "id|name|height|city\n4|linus|1.77|helsinki\n",
        )
        .await;

    let glue = Arc::new(FakeGlue::default().with_database("lake").with_table(
        "lake",
        table(
            "people",
            storage_descriptor(
                vec![
                    column("id", "int"),
                    column("name", "string"),
                    column("height", "double"),
                    column("city", "string"),
                ],
                &format!("s3://{BUCKET}/people/"),
                LAZY_SIMPLE_SERDE,
                &[("field.delim", "|"), ("serialization.format", "|")],
            ),
            Vec::new(),
            &[("skip.header.line.count", "1")],
        ),
    ));
    let ctx = context(glue, storage).await;

    let batches = query(
        &ctx,
        "SELECT id, name, height, city FROM glue.lake.people ORDER BY id",
    )
    .await;
    let schema = batches[0].schema();
    assert_eq!(schema.field(0).data_type(), &DataType::Int32);
    assert_eq!(schema.field(2).data_type(), &DataType::Float64);
    let ids: Vec<i32> = batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(ids, vec![1, 2, 3, 4]);
    // LazySimpleSerDe does no quote processing: quotes are data.
    assert_eq!(
        string_column(&batches, 1),
        vec!["ada", "grace", "\"quoted\"", "linus"]
    );
    let heights: Vec<f64> = batches
        .iter()
        .flat_map(|b| {
            b.column(2)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(heights, vec![1.62, 1.70, 1.80, 1.77]);

    let batches = query(
        &ctx,
        "SELECT count(*) FROM glue.lake.people WHERE city = 'new york'",
    )
    .await;
    assert_eq!(int64_column(&batches, 0), vec![1]);
}

/// Partition projection over a date range: partitions are computed locally,
/// Glue `GetPartitions` is never called, and date predicates prune.
#[tokio::test]
async fn projected_date_partitions_resolve_without_get_partitions() {
    let storage = Arc::new(MemoryStorage::new());
    for (i, day) in ["2024-03-01", "2024-03-02", "2024-03-03", "2024-03-04"]
        .iter()
        .enumerate()
    {
        let id = i as i64 + 1;
        storage
            .put(
                &format!("logs/dt={day}/part.parquet"),
                sales_parquet(&[id], &[id as f64]),
            )
            .await;
    }
    // An object outside the projected range must be invisible.
    storage
        .put(
            "logs/dt=2024-03-05/part.parquet",
            sales_parquet(&[99], &[99.0]),
        )
        .await;

    let glue = Arc::new(
        FakeGlue::default()
            .with_database("lake")
            .with_table(
                "lake",
                table(
                    "logs",
                    storage_descriptor(
                        vec![
                            column("id", "bigint"),
                            column("amount", "double"),
                            column("ts", "timestamp"),
                        ],
                        &format!("s3://{BUCKET}/logs/"),
                        PARQUET_SERDE,
                        &[],
                    ),
                    vec![column("dt", "date")],
                    &[
                        ("projection.enabled", "true"),
                        ("projection.dt.type", "date"),
                        ("projection.dt.range", "2024-03-01,2024-03-04"),
                        ("projection.dt.format", "yyyy-MM-dd"),
                        ("projection.dt.interval", "1"),
                        ("projection.dt.interval.unit", "DAYS"),
                    ],
                ),
            )
            // Even if Glue had stale partition entries they must be ignored.
            .with_partitions(
                "lake",
                "logs",
                vec![partition(
                    &["1999-01-01"],
                    &format!("s3://{BUCKET}/nowhere/"),
                    PARQUET_SERDE,
                )],
            ),
    );
    let ctx = context(Arc::clone(&glue), Arc::clone(&storage)).await;

    let batches = query(&ctx, "SELECT id, dt FROM glue.lake.logs ORDER BY id").await;
    assert_eq!(int64_column(&batches, 0), vec![1, 2, 3, 4]);
    assert_eq!(batches[0].schema().field(1).data_type(), &DataType::Date32);
    assert_eq!(
        glue.get_partitions_calls(),
        0,
        "projection must never call GetPartitions"
    );
    assert!(
        storage.reads().iter().all(|k| !k.contains("dt=2024-03-05")),
        "objects outside the projected range must not be read"
    );

    storage.clear_log();
    let batches = query(
        &ctx,
        "SELECT id FROM glue.lake.logs WHERE dt >= DATE '2024-03-03' ORDER BY id",
    )
    .await;
    assert_eq!(int64_column(&batches, 0), vec![3, 4]);
    let lists = storage.lists();
    assert_eq!(
        lists,
        vec![
            "logs/dt=2024-03-03".to_string(),
            "logs/dt=2024-03-04".to_string()
        ],
        "only in-range partitions may be listed"
    );
    assert!(
        storage
            .reads()
            .iter()
            .all(|k| k.contains("dt=2024-03-03") || k.contains("dt=2024-03-04")),
        "pruned partitions must not be read: {:?}",
        storage.reads()
    );
    assert_eq!(glue.get_partitions_calls(), 0);
}

/// Projection with a `storage.location.template` and enum/integer columns.
#[tokio::test]
async fn projected_enum_and_integer_partitions_follow_location_template() {
    let storage = Arc::new(MemoryStorage::new());
    let mut id = 0;
    for region in ["eu", "us"] {
        for shard in ["00", "01"] {
            id += 1;
            storage
                .put(
                    &format!("raw/{region}/shard-{shard}/f.parquet"),
                    sales_parquet(&[id], &[1.0]),
                )
                .await;
        }
    }
    let glue = Arc::new(FakeGlue::default().with_database("lake").with_table(
        "lake",
        table(
            "raw",
            storage_descriptor(
                vec![
                    column("id", "bigint"),
                    column("amount", "double"),
                    column("ts", "timestamp"),
                ],
                &format!("s3://{BUCKET}/raw/"),
                PARQUET_SERDE,
                &[],
            ),
            vec![column("region", "string"), column("shard", "int")],
            &[
                ("projection.enabled", "true"),
                ("projection.region.type", "enum"),
                ("projection.region.values", "eu,us"),
                ("projection.shard.type", "integer"),
                ("projection.shard.range", "0,1"),
                ("projection.shard.digits", "2"),
                (
                    "storage.location.template",
                    &format!("s3://{BUCKET}/raw/${{region}}/shard-${{shard}}/"),
                ),
            ],
        ),
    ));
    let ctx = context(Arc::clone(&glue), Arc::clone(&storage)).await;

    let batches = query(
        &ctx,
        "SELECT id, region, shard FROM glue.lake.raw WHERE region = 'us' AND shard = 1",
    )
    .await;
    assert_eq!(int64_column(&batches, 0), vec![4]);
    assert_eq!(string_column(&batches, 1), vec!["us"]);
    assert_eq!(storage.lists(), vec!["raw/us/shard-01".to_string()]);
    assert_eq!(glue.get_partitions_calls(), 0);
}

/// Unsupported metadata fails loudly at table resolution, naming the
/// construct — never an empty or guessed result.
#[tokio::test]
async fn unsupported_constructs_error_explicitly() {
    let storage = Arc::new(MemoryStorage::new());
    let sd = |serde: &str| {
        storage_descriptor(
            vec![column("id", "bigint")],
            &format!("s3://{BUCKET}/t/"),
            serde,
            &[],
        )
    };
    let mut bad_type = table("bad_type", sd(PARQUET_SERDE), Vec::new(), &[]);
    bad_type.storage_descriptor.as_mut().unwrap().columns =
        vec![column("u", "uniontype<int,string>")];
    let mut no_location = table("no_location", sd(PARQUET_SERDE), Vec::new(), &[]);
    no_location.storage_descriptor.as_mut().unwrap().location = None;
    let mut view = table("a_view", sd(PARQUET_SERDE), Vec::new(), &[]);
    view.table_type = Some("VIRTUAL_VIEW".to_string());

    let glue = Arc::new(
        FakeGlue::default()
            .with_database("lake")
            .with_table(
                "lake",
                table(
                    "orc_table",
                    sd("org.apache.hadoop.hive.ql.io.orc.OrcSerde"),
                    Vec::new(),
                    &[],
                ),
            )
            .with_table("lake", bad_type)
            .with_table("lake", no_location)
            .with_table("lake", view)
            .with_table(
                "lake",
                table(
                    "bad_projection",
                    sd(PARQUET_SERDE),
                    vec![column("user", "string")],
                    &[
                        ("projection.enabled", "true"),
                        ("projection.user.type", "injected"),
                    ],
                ),
            )
            .with_table(
                "lake",
                table(
                    "null_partition",
                    sd(PARQUET_SERDE),
                    vec![column("day", "string")],
                    &[],
                ),
            )
            .with_partitions(
                "lake",
                "null_partition",
                vec![partition(
                    &["__HIVE_DEFAULT_PARTITION__"],
                    &format!("s3://{BUCKET}/t/day=__HIVE_DEFAULT_PARTITION__/"),
                    PARQUET_SERDE,
                )],
            ),
    );
    let ctx = context(glue, storage).await;

    let err = query_err(&ctx, "SELECT * FROM glue.lake.orc_table").await;
    assert!(err.contains("OrcSerde"), "must name the SerDe: {err}");

    let err = query_err(&ctx, "SELECT * FROM glue.lake.bad_type").await;
    assert!(err.contains("uniontype"), "must name the type: {err}");
    assert!(err.contains("column u"), "must name the column: {err}");

    let err = query_err(&ctx, "SELECT * FROM glue.lake.no_location").await;
    assert!(err.contains("no location"), "{err}");

    let err = query_err(&ctx, "SELECT * FROM glue.lake.a_view").await;
    assert!(err.contains("VIRTUAL_VIEW"), "{err}");

    let err = query_err(&ctx, "SELECT * FROM glue.lake.bad_projection").await;
    assert!(err.contains("injected"), "{err}");

    let err = query_err(&ctx, "SELECT * FROM glue.lake.null_partition").await;
    assert!(err.contains("__HIVE_DEFAULT_PARTITION__"), "{err}");

    // A table that does not exist is the planner's own not-found error.
    let err = query_err(&ctx, "SELECT * FROM glue.lake.nope").await;
    assert!(err.contains("nope"), "{err}");
}

// ---------------------------------------------------------------------------
// File schema vs Glue schema
// ---------------------------------------------------------------------------

/// Parquet bytes for a one-row-group file with the given schema and columns.
fn parquet_with(fields: Vec<Field>, columns: Vec<ArrayRef>) -> Bytes {
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
    let mut buf = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    Bytes::from(buf)
}

fn parquet_table(name: &str, columns: Vec<GlueColumn>) -> GlueTable {
    table(
        name,
        storage_descriptor(
            columns,
            &format!("s3://{BUCKET}/{name}/"),
            PARQUET_SERDE,
            &[],
        ),
        Vec::new(),
        &[],
    )
}

/// Parquet columns are matched to Glue columns by name, case-insensitively
/// (Athena's `ParquetHiveSerDe` default), in projections and predicates.
#[tokio::test]
async fn parquet_columns_match_glue_columns_case_insensitively() {
    let storage = Arc::new(MemoryStorage::new());
    storage
        .put(
            "cm/part-0.parquet",
            parquet_with(
                vec![
                    Field::new("ID", DataType::Int64, false),
                    Field::new("UserName", DataType::Utf8, true),
                ],
                vec![
                    Arc::new(Int64Array::from(vec![7, 8])),
                    Arc::new(StringArray::from(vec!["ada", "linus"])),
                ],
            ),
        )
        .await;
    let glue = Arc::new(FakeGlue::default().with_database("lake").with_table(
        "lake",
        parquet_table(
            "cm",
            vec![column("id", "bigint"), column("username", "string")],
        ),
    ));
    let ctx = context(glue, storage).await;

    let batches = query(&ctx, "SELECT id, username FROM glue.lake.cm ORDER BY id").await;
    assert_eq!(batches[0].schema().field(0).name(), "id");
    assert_eq!(batches[0].schema().field(1).name(), "username");
    assert_eq!(int64_column(&batches, 0), vec![7, 8]);
    assert_eq!(string_column(&batches, 1), vec!["ada", "linus"]);

    let batches = query(&ctx, "SELECT * FROM glue.lake.cm WHERE id = 8").await;
    assert_eq!(int64_column(&batches, 0), vec![8]);
    assert_eq!(string_column(&batches, 1), vec!["linus"]);

    let batches = query(
        &ctx,
        "SELECT count(*) FROM glue.lake.cm WHERE username = 'ada'",
    )
    .await;
    assert_eq!(int64_column(&batches, 0), vec![1]);
}

/// A Glue column absent from a Parquet file reads as NULL (Athena's
/// schema-evolution semantics for Parquet); a Parquet type that widens into
/// the Glue type is cast; a narrower or unrelated type, or two file columns
/// that differ only by case, error naming the column and both types.
#[tokio::test]
async fn parquet_file_schema_differences_are_null_widened_or_rejected() {
    let storage = Arc::new(MemoryStorage::new());
    // Glue: id bigint, amount double, note string. File: id as Int32 (widens),
    // amount as Float32 (widens), no `note` column (NULL).
    storage
        .put(
            "evolved/part-0.parquet",
            parquet_with(
                vec![
                    Field::new("id", DataType::Int32, false),
                    Field::new("amount", DataType::Float32, false),
                ],
                vec![
                    Arc::new(Int32Array::from(vec![1, 2])),
                    Arc::new(Float32Array::from(vec![1.5, 2.5])),
                ],
            ),
        )
        .await;
    // Glue: id int. File: id Int64 — narrowing, rejected.
    storage
        .put(
            "narrow/part-0.parquet",
            parquet_with(
                vec![Field::new("id", DataType::Int64, false)],
                vec![Arc::new(Int64Array::from(vec![1]))],
            ),
        )
        .await;
    // Glue: id bigint. File: id string — unrelated, rejected.
    storage
        .put(
            "mistyped/part-0.parquet",
            parquet_with(
                vec![Field::new("id", DataType::Utf8, false)],
                vec![Arc::new(StringArray::from(vec!["1"]))],
            ),
        )
        .await;
    // Glue: id bigint. File: both `id` and `ID` — ambiguous under
    // case-insensitive matching when neither spelling is exact... here `id`
    // is exact so it wins; `Id`/`ID` without an exact match is ambiguous.
    storage
        .put(
            "ambiguous/part-0.parquet",
            parquet_with(
                vec![
                    Field::new("Id", DataType::Int64, false),
                    Field::new("ID", DataType::Int64, false),
                ],
                vec![
                    Arc::new(Int64Array::from(vec![1])),
                    Arc::new(Int64Array::from(vec![2])),
                ],
            ),
        )
        .await;

    let glue = Arc::new(
        FakeGlue::default()
            .with_database("lake")
            .with_table(
                "lake",
                parquet_table(
                    "evolved",
                    vec![
                        column("id", "bigint"),
                        column("amount", "double"),
                        column("note", "string"),
                    ],
                ),
            )
            .with_table("lake", parquet_table("narrow", vec![column("id", "int")]))
            .with_table(
                "lake",
                parquet_table("mistyped", vec![column("id", "bigint")]),
            )
            .with_table(
                "lake",
                parquet_table("ambiguous", vec![column("id", "bigint")]),
            ),
    );
    let ctx = context(glue, storage).await;

    let batches = query(
        &ctx,
        "SELECT id, amount, note FROM glue.lake.evolved ORDER BY id",
    )
    .await;
    assert_eq!(batches[0].schema().field(0).data_type(), &DataType::Int64);
    assert_eq!(batches[0].schema().field(1).data_type(), &DataType::Float64);
    assert_eq!(int64_column(&batches, 0), vec![1, 2]);
    let amounts = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(amounts.values(), &[1.5, 2.5]);
    assert_eq!(
        batches[0].column(2).null_count(),
        2,
        "missing column is NULL"
    );
    let batches = query(
        &ctx,
        "SELECT count(*) FROM glue.lake.evolved WHERE note IS NULL AND id > 1",
    )
    .await;
    assert_eq!(int64_column(&batches, 0), vec![1]);

    let err = query_err(&ctx, "SELECT id FROM glue.lake.narrow").await;
    assert!(
        err.contains("\"id\"")
            && err.contains("Int64")
            && err.contains("Int32")
            && err.contains("HIVE_BAD_DATA"),
        "must name the column and both types: {err}"
    );

    let err = query_err(&ctx, "SELECT id FROM glue.lake.mistyped").await;
    assert!(
        err.contains("\"id\"") && err.contains("Utf8") && err.contains("Int64"),
        "must name the column and both types: {err}"
    );

    let err = query_err(&ctx, "SELECT id FROM glue.lake.ambiguous").await;
    assert!(
        err.contains("\"id\"") && err.contains("differ only by case") && err.contains("\"Id\""),
        "must name the column and the clashing file columns: {err}"
    );
}

/// OpenX JSON keys are matched to Glue columns case-insensitively at every
/// struct level (the SerDe's `case.insensitive = true` default); missing
/// keys are NULL, unknown keys are ignored, and keys that differ only by
/// case within one record are rejected.
#[tokio::test]
async fn json_keys_match_glue_columns_case_insensitively() {
    let storage = Arc::new(MemoryStorage::new());
    storage
        .put(
            "users/part-0.json",
            concat!(
                r#"{"userId":1,"Profile":{"FirstName":"ada","Tags":["x","y"]},"extra":true}"#,
                "\n",
                r#"{"USERID":2,"profile":{"firstname":"linus"}}"#,
                "\n",
                r#"{"userid":3}"#,
                "\n",
            ),
        )
        .await;
    storage
        .put(
            "clash/part-0.json",
            concat!(r#"{"userId":1,"USERID":2}"#, "\n"),
        )
        .await;
    storage
        .put(
            "exact/part-0.json",
            concat!(r#"{"userId":1}"#, "\n", r#"{"userid":2}"#, "\n"),
        )
        .await;

    let users_columns = || {
        vec![
            column("userid", "bigint"),
            column("profile", "struct<firstname:string,tags:array<string>>"),
        ]
    };
    let json_table = |name: &str, serde_params: &[(&str, &str)]| {
        table(
            name,
            storage_descriptor(
                users_columns(),
                &format!("s3://{BUCKET}/{name}/"),
                OPENX_JSON_SERDE,
                serde_params,
            ),
            Vec::new(),
            &[],
        )
    };
    let glue = Arc::new(
        FakeGlue::default()
            .with_database("lake")
            .with_table("lake", json_table("users", &[]))
            .with_table("lake", json_table("clash", &[]))
            .with_table(
                "lake",
                json_table("exact", &[("case.insensitive", "false")]),
            ),
    );
    let ctx = context(glue, storage).await;

    let batches = query(
        &ctx,
        "SELECT userid, profile['firstname'] AS firstname, cardinality(profile['tags']) AS n \
         FROM glue.lake.users ORDER BY userid",
    )
    .await;
    assert_eq!(int64_column(&batches, 0), vec![1, 2, 3]);
    let names = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "ada");
    assert_eq!(names.value(1), "linus");
    assert!(names.is_null(2), "missing key reads as NULL");
    let n = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<arrow::array::UInt64Array>()
        .unwrap();
    assert_eq!(n.value(0), 2);
    assert!(n.is_null(1));

    let batches = query(
        &ctx,
        "SELECT count(*) FROM glue.lake.users WHERE profile['firstname'] = 'ada'",
    )
    .await;
    assert_eq!(int64_column(&batches, 0), vec![1]);

    let err = query_err(&ctx, "SELECT userid FROM glue.lake.clash").await;
    assert!(
        err.contains("userid") && err.contains("line 1") && err.contains("clash/part-0.json"),
        "must name the column, line, and object: {err}"
    );

    // `case.insensitive = false`: exact matching, as the SerDe would do.
    let batches = query(&ctx, "SELECT userid FROM glue.lake.exact").await;
    let ids = batches
        .iter()
        .flat_map(|b| {
            let a = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            (0..a.len())
                .map(|i| a.is_valid(i).then(|| a.value(i)))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(ids, vec![None, Some(2)]);
}
