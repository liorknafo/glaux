//! The record side: the corpus against real AWS Athena.
//!
//! A run creates scratch resources that exist only for its duration —
//! `s3://glaux-fidelity-<id>` holding the fixture files and query results,
//! and Glue database `glaux_fidelity_<id>` with the three fixture tables
//! defined exactly as the offline catalog defines them — executes every
//! case through `StartQueryExecution` / `GetQueryExecution` / paginated
//! `GetQueryResults`, and tears everything down again (pass `--keep` to
//! inspect). Spend is minimal: the fixtures are a few kilobytes, so every
//! query scans well under Athena's 10 MB billing minimum.
//!
//! Credentials come from the named AWS profile (`glaux-sandbox` by
//! default) through the standard provider chain; nothing is read from the
//! environment that the profile does not supply.

use std::time::Duration;

use aws_config::BehaviorVersion;
use aws_sdk_athena::error::DisplayErrorContext;
use aws_sdk_athena::types::{QueryExecutionContext, QueryExecutionState, ResultConfiguration};
use aws_sdk_glue::types::{
    Column as GlueColumn, DatabaseInput, SerDeInfo, StorageDescriptor, TableInput,
};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketLocationConstraint, CreateBucketConfiguration, Delete, ObjectIdentifier,
};
use futures::StreamExt as _;

use crate::corpus::Case;
use crate::fixtures;
use crate::glaux::type_text;
use crate::snapshot::{Column, Outcome, Provenance, Snapshot};
use crate::suite::today;
use crate::{DATABASE, HarnessError, Result};

/// Recording options.
#[derive(Debug, Clone)]
pub struct RecordOptions {
    /// AWS profile name.
    pub profile: String,
    /// Region override (otherwise the profile's).
    pub region: Option<String>,
    /// Existing bucket to use instead of creating a scratch one. It is
    /// never deleted; only the objects this run wrote are.
    pub bucket: Option<String>,
    /// Leave the scratch resources in place.
    pub keep: bool,
    /// Queries in flight at once.
    pub concurrency: usize,
}

impl Default for RecordOptions {
    fn default() -> Self {
        Self {
            profile: "glaux-sandbox".to_string(),
            region: None,
            bucket: None,
            keep: false,
            concurrency: 6,
        }
    }
}

/// What a recording produced.
#[derive(Debug)]
pub struct Recording {
    /// One snapshot per case, with `athena` provenance.
    pub snapshots: Vec<Snapshot>,
    /// Region the queries ran in.
    pub region: String,
    /// `EffectiveEngineVersion` Athena reported.
    pub engine: String,
}

/// Whether an AWS profile of that name is configured locally
/// (`~/.aws/credentials` or `~/.aws/config`).
pub fn profile_exists(profile: &str) -> bool {
    let Some(home) = std::env::var_os("HOME") else {
        return false;
    };
    let home = std::path::PathBuf::from(home);
    let headers = [format!("[{profile}]"), format!("[profile {profile}]")];
    [".aws/credentials", ".aws/config"]
        .iter()
        .filter_map(|f| std::fs::read_to_string(home.join(f)).ok())
        .any(|text| {
            text.lines()
                .any(|l| headers.contains(&l.trim().to_string()))
        })
}

struct Scratch {
    s3: aws_sdk_s3::Client,
    glue: aws_sdk_glue::Client,
    athena: aws_sdk_athena::Client,
    bucket: String,
    own_bucket: bool,
    database: String,
    region: String,
}

fn sdk<E: std::error::Error + 'static, R: std::fmt::Debug>(
    what: &str,
) -> impl FnOnce(aws_sdk_s3::error::SdkError<E, R>) -> HarnessError + '_ {
    move |e| HarnessError::new(format!("{what}: {}", DisplayErrorContext(&e)))
}

impl Scratch {
    async fn create(options: &RecordOptions) -> Result<Self> {
        let mut loader =
            aws_config::defaults(BehaviorVersion::latest()).profile_name(&options.profile);
        if let Some(region) = &options.region {
            loader = loader.region(aws_config::Region::new(region.clone()));
        }
        let config = loader.load().await;
        let region = config.region().map(ToString::to_string).ok_or_else(|| {
            HarnessError::new(format!(
                "profile {} has no region; pass --region",
                options.profile
            ))
        })?;
        let s3 = aws_sdk_s3::Client::new(&config);
        let glue = aws_sdk_glue::Client::new(&config);
        let athena = aws_sdk_athena::Client::new(&config);
        let id = uuid::Uuid::new_v4().simple().to_string()[..10].to_string();
        let (bucket, own_bucket) = match &options.bucket {
            Some(b) => (b.clone(), false),
            None => {
                let name = format!("glaux-fidelity-{id}");
                let mut req = s3.create_bucket().bucket(&name);
                if region != "us-east-1" {
                    req = req.create_bucket_configuration(
                        CreateBucketConfiguration::builder()
                            .location_constraint(BucketLocationConstraint::from(region.as_str()))
                            .build(),
                    );
                }
                req.send().await.map_err(sdk("CreateBucket"))?;
                (name, true)
            }
        };
        let database = format!("{DATABASE}_{id}");
        let scratch = Self {
            s3,
            glue,
            athena,
            bucket,
            own_bucket,
            database,
            region,
        };
        scratch.seed().await?;
        Ok(scratch)
    }

    async fn seed(&self) -> Result<()> {
        let tables = fixtures::tables()?;
        for table in &tables {
            self.s3
                .put_object()
                .bucket(&self.bucket)
                .key(&table.key)
                .body(ByteStream::from(table.bytes.to_vec()))
                .send()
                .await
                .map_err(sdk("PutObject"))?;
        }
        self.glue
            .create_database()
            .database_input(
                DatabaseInput::builder()
                    .name(&self.database)
                    .description("glaux differential fidelity scratch database")
                    .build()
                    .map_err(|e| HarnessError::new(e.to_string()))?,
            )
            .send()
            .await
            .map_err(sdk("CreateDatabase"))?;
        for table in &tables {
            let columns: Vec<GlueColumn> = table
                .columns
                .iter()
                .map(|(name, ty)| GlueColumn::builder().name(*name).r#type(*ty).build())
                .collect::<std::result::Result<_, _>>()
                .map_err(|e| HarnessError::new(e.to_string()))?;
            let serde = SerDeInfo::builder()
                .serialization_library(table.format.serde())
                .set_parameters(Some(
                    table
                        .format
                        .serde_parameters()
                        .into_iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect(),
                ))
                .build();
            let sd = StorageDescriptor::builder()
                .set_columns(Some(columns))
                .location(table.location(&self.bucket))
                .input_format(table.format.input_format())
                .output_format(table.format.output_format())
                .serde_info(serde)
                .build();
            self.glue
                .create_table()
                .database_name(&self.database)
                .table_input(
                    TableInput::builder()
                        .name(table.name)
                        .table_type("EXTERNAL_TABLE")
                        .storage_descriptor(sd)
                        .set_parameters(Some(table.parameters()))
                        .build()
                        .map_err(|e| HarnessError::new(e.to_string()))?,
                )
                .send()
                .await
                .map_err(sdk("CreateTable"))?;
        }
        Ok(())
    }

    /// Run one case to completion. Returns the outcome and, on success,
    /// the effective engine version.
    async fn run(&self, case: &Case) -> Result<(Outcome, Option<String>)> {
        let started = self
            .athena
            .start_query_execution()
            .query_string(&case.sql)
            .query_execution_context(
                QueryExecutionContext::builder()
                    .database(&self.database)
                    .build(),
            )
            .result_configuration(
                ResultConfiguration::builder()
                    .output_location(format!("s3://{}/results/", self.bucket))
                    .build(),
            )
            .send()
            .await;
        let id = match started {
            Ok(out) => out
                .query_execution_id
                .ok_or_else(|| HarnessError::new("StartQueryExecution returned no id"))?,
            // Athena rejects some statements synchronously (InvalidRequest);
            // that is still an explicit failure of the query.
            Err(e) => {
                return Ok((
                    Outcome::Failed {
                        message: DisplayErrorContext(&e).to_string(),
                    },
                    None,
                ));
            }
        };
        let deadline = tokio::time::Instant::now() + Duration::from_secs(300);
        let execution = loop {
            let out = self
                .athena
                .get_query_execution()
                .query_execution_id(&id)
                .send()
                .await
                .map_err(sdk("GetQueryExecution"))?;
            let qe = out
                .query_execution
                .ok_or_else(|| HarnessError::new("GetQueryExecution without QueryExecution"))?;
            let state = qe.status().and_then(|s| s.state()).cloned();
            match state {
                Some(
                    QueryExecutionState::Succeeded
                    | QueryExecutionState::Failed
                    | QueryExecutionState::Cancelled,
                ) => break qe,
                _ if tokio::time::Instant::now() > deadline => {
                    return Err(HarnessError::new(format!(
                        "{}: {id} never finished",
                        case.name
                    )));
                }
                _ => tokio::time::sleep(Duration::from_millis(500)).await,
            }
        };
        let engine = execution
            .engine_version()
            .and_then(|v| v.effective_engine_version())
            .map(ToString::to_string);
        let status = execution.status().expect("terminal state has a status");
        if status.state() != Some(&QueryExecutionState::Succeeded) {
            let message = status
                .athena_error()
                .and_then(|e| e.error_message())
                .or(status.state_change_reason())
                .unwrap_or("query failed without a message")
                .to_string();
            return Ok((Outcome::Failed { message }, engine));
        }
        let mut pages = self
            .athena
            .get_query_results()
            .query_execution_id(&id)
            .max_results(1000)
            .into_paginator()
            .send();
        let mut columns: Option<Vec<Column>> = None;
        let mut rows = Vec::new();
        let mut first_page = true;
        while let Some(page) = pages.next().await {
            let page = page.map_err(sdk("GetQueryResults"))?;
            let result_set = page
                .result_set()
                .ok_or_else(|| HarnessError::new("GetQueryResults without ResultSet"))?;
            if columns.is_none() {
                columns = Some(
                    result_set
                        .result_set_metadata()
                        .map(|m| m.column_info())
                        .unwrap_or_default()
                        .iter()
                        .map(|c| Column {
                            name: c.name().to_string(),
                            type_name: type_text(
                                c.r#type(),
                                Some(c.precision().into()),
                                Some(c.scale().into()),
                            ),
                        })
                        .collect(),
                );
            }
            for (i, row) in result_set.rows().iter().enumerate() {
                if first_page && i == 0 {
                    continue; // header row
                }
                rows.push(
                    row.data()
                        .iter()
                        .map(|d| d.var_char_value().map(ToString::to_string))
                        .collect(),
                );
            }
            first_page = false;
        }
        Ok((
            Outcome::Succeeded {
                columns: columns.unwrap_or_default(),
                rows,
            },
            engine,
        ))
    }

    async fn teardown(&self) -> Result<()> {
        let mut problems = Vec::new();
        for table in fixtures::TABLE_NAMES {
            if let Err(e) = self
                .glue
                .delete_table()
                .database_name(&self.database)
                .name(*table)
                .send()
                .await
            {
                problems.push(format!("DeleteTable {table}: {}", DisplayErrorContext(&e)));
            }
        }
        if let Err(e) = self
            .glue
            .delete_database()
            .name(&self.database)
            .send()
            .await
        {
            problems.push(format!("DeleteDatabase: {}", DisplayErrorContext(&e)));
        }
        let mut pages = self
            .s3
            .list_objects_v2()
            .bucket(&self.bucket)
            .into_paginator()
            .send();
        while let Some(page) = pages.next().await {
            match page {
                Ok(page) => {
                    let ids: Vec<ObjectIdentifier> = page
                        .contents()
                        .iter()
                        .filter_map(|o| o.key())
                        .filter(|k| {
                            self.own_bucket
                                || k.starts_with("results/")
                                || fixtures::TABLE_NAMES
                                    .iter()
                                    .any(|t| k.starts_with(&format!("{t}/")))
                        })
                        .filter_map(|k| ObjectIdentifier::builder().key(k).build().ok())
                        .collect();
                    if ids.is_empty() {
                        continue;
                    }
                    let delete = Delete::builder().set_objects(Some(ids)).build();
                    match delete {
                        Ok(delete) => {
                            if let Err(e) = self
                                .s3
                                .delete_objects()
                                .bucket(&self.bucket)
                                .delete(delete)
                                .send()
                                .await
                            {
                                problems
                                    .push(format!("DeleteObjects: {}", DisplayErrorContext(&e)));
                            }
                        }
                        Err(e) => problems.push(format!("DeleteObjects: {e}")),
                    }
                }
                Err(e) => problems.push(format!("ListObjectsV2: {}", DisplayErrorContext(&e))),
            }
        }
        if self.own_bucket
            && let Err(e) = self.s3.delete_bucket().bucket(&self.bucket).send().await
        {
            problems.push(format!("DeleteBucket: {}", DisplayErrorContext(&e)));
        }
        if problems.is_empty() {
            Ok(())
        } else {
            Err(HarnessError::new(format!(
                "teardown left scratch resources behind (bucket {}, database {}):\n  {}",
                self.bucket,
                self.database,
                problems.join("\n  ")
            )))
        }
    }
}

/// Record `cases` against real Athena.
pub async fn record(cases: &[Case], options: &RecordOptions) -> Result<Recording> {
    let scratch = Scratch::create(options).await?;
    eprintln!(
        "scratch resources: s3://{} glue database {} ({})",
        scratch.bucket, scratch.database, scratch.region
    );
    let result = record_cases(&scratch, cases, options.concurrency).await;
    if options.keep {
        eprintln!("--keep: leaving scratch resources in place");
    } else {
        scratch.teardown().await?;
    }
    result
}

async fn record_cases(scratch: &Scratch, cases: &[Case], concurrency: usize) -> Result<Recording> {
    let recorded = today();
    let results: Vec<Result<(String, Outcome, Option<String>)>> = futures::stream::iter(cases)
        .map(|case| async move {
            let (outcome, engine) = scratch.run(case).await?;
            eprintln!("{:<50} {}", case.name, outcome.status());
            Ok((case.name.clone(), outcome, engine))
        })
        .buffer_unordered(concurrency.max(1))
        .collect()
        .await;
    let mut engine: Option<String> = None;
    let mut by_name = std::collections::HashMap::new();
    for r in results {
        let (name, outcome, version) = r?;
        if let Some(v) = version {
            engine.get_or_insert(v);
        }
        by_name.insert(name, outcome);
    }
    let engine = engine.unwrap_or_else(|| "unknown".to_string());
    let mut snapshots = Vec::with_capacity(cases.len());
    for case in cases {
        let outcome = by_name
            .remove(&case.name)
            .ok_or_else(|| HarnessError::new(format!("{}: no outcome recorded", case.name)))?;
        if let (Some(_), Outcome::Succeeded { rows, .. }) = (&case.expect_error, &outcome) {
            eprintln!(
                "WARNING {}: expected an error but Athena succeeded with {} row(s); \
                 the snapshot records the success — fix the corpus case",
                case.name,
                rows.len()
            );
        }
        snapshots.push(Snapshot {
            case: case.name.clone(),
            provenance: Provenance::Athena {
                region: scratch.region.clone(),
                engine: engine.clone(),
                recorded: recorded.clone(),
            },
            compare: case.compare,
            outcome,
        });
    }
    Ok(Recording {
        snapshots,
        region: scratch.region.clone(),
        engine,
    })
}
