//! The DataFusion catalog surface over Glue: [`GlueCatalogProvider`]
//! (Glue databases → DataFusion schemas), [`GlueSchemaProvider`] (Glue
//! tables → table providers), and [`GlueTableProvider`] (listing-style
//! scans over [`StorageBackend`] with Glue-driven partition handling).
//!
//! Partition handling comes in three flavors, chosen per table:
//!
//! - **Unpartitioned** — one scan over the table location.
//! - **Glue partitions** — `GetPartitions` metadata is the source of truth;
//!   partition-column filters prune partitions locally *before* any object
//!   listing, so pruned partitions are never touched.
//! - **Partition projection** — `projection.*` table parameters compute the
//!   partition list locally with **zero** `GetPartitions` calls.
//!
//! Filters are always reported as [`TableProviderFilterPushDown::Inexact`],
//! so DataFusion re-applies every filter after the scan — partition pruning
//! is purely an optimization and can never change results.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use arrow::array::{Array as _, BooleanArray};
use arrow::datatypes::{Field, FieldRef, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider, Session, TableProvider};
use datafusion::common::{DFSchema, DataFusionError, ScalarValue, project_schema};
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::listing::helpers::expr_applicable_for_cols;
use datafusion::datasource::physical_plan::FileGroup;
use datafusion::datasource::physical_plan::FileScanConfigBuilder;
use datafusion::datasource::table_schema::TableSchemaBuilder;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::empty::EmptyExec;

use crate::error::{CatalogError, Result};
use crate::format::file_format_for_table;
use crate::glue::{GlueApi, GlueTable};
use crate::projection::ProjectionConfig;
use crate::storage::StorageBackend;
use crate::types::hive_type_to_arrow;

/// Hive's marker for a null partition value.
const HIVE_NULL_PARTITION: &str = "__HIVE_DEFAULT_PARTITION__";

/// Wrap a [`CatalogError`] for return through DataFusion APIs; the full
/// error chain (and its construct-naming messages) is preserved.
fn external(error: CatalogError) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

/// DataFusion [`CatalogProvider`] over a Glue Data Catalog: every Glue
/// database appears as a schema.
///
/// The database and table listings are snapshotted at construction (the
/// DataFusion catalog traits are synchronous); table *resolution* is always
/// live against Glue, so table metadata is never stale.
pub struct GlueCatalogProvider {
    schemas: HashMap<String, Arc<GlueSchemaProvider>>,
}

impl fmt::Debug for GlueCatalogProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GlueCatalogProvider")
            .field("databases", &self.schemas.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl GlueCatalogProvider {
    /// Build a catalog provider, snapshotting the Glue database and table
    /// listings. Call again (or [`Self::try_new`] a fresh instance) to pick
    /// up newly created databases or tables.
    pub async fn try_new(
        glue: Arc<dyn GlueApi>,
        storage: Arc<dyn StorageBackend>,
    ) -> Result<Self> {
        let mut schemas = HashMap::new();
        for database in glue.get_databases().await? {
            let tables = glue.get_tables(&database.name).await?;
            let table_names = tables.into_iter().map(|t| t.name).collect();
            schemas.insert(
                database.name.clone(),
                Arc::new(GlueSchemaProvider {
                    glue: Arc::clone(&glue),
                    storage: Arc::clone(&storage),
                    database: database.name,
                    table_names,
                }),
            );
        }
        Ok(Self { schemas })
    }
}

impl CatalogProvider for GlueCatalogProvider {
    fn schema_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.schemas.keys().cloned().collect();
        names.sort();
        names
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas
            .get(name)
            .map(|schema| Arc::clone(schema) as Arc<dyn SchemaProvider>)
    }
}

/// DataFusion [`SchemaProvider`] for one Glue database.
pub struct GlueSchemaProvider {
    glue: Arc<dyn GlueApi>,
    storage: Arc<dyn StorageBackend>,
    database: String,
    /// Table names snapshotted when the catalog provider was built.
    table_names: Vec<String>,
}

impl fmt::Debug for GlueSchemaProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GlueSchemaProvider")
            .field("database", &self.database)
            .field("table_names", &self.table_names)
            .finish()
    }
}

#[async_trait]
impl SchemaProvider for GlueSchemaProvider {
    fn table_names(&self) -> Vec<String> {
        self.table_names.clone()
    }

    async fn table(
        &self,
        name: &str,
    ) -> std::result::Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        let glue_table = match self.glue.get_table(&self.database, name).await {
            Ok(table) => table,
            // A missing table is "no such table" to the planner, which then
            // produces its own explicit table-not-found error.
            Err(CatalogError::GlueEntityNotFound { .. }) => return Ok(None),
            Err(other) => return Err(external(other)),
        };
        let provider = GlueTableProvider::try_new(
            &self.database,
            glue_table,
            Arc::clone(&self.glue),
            Arc::clone(&self.storage),
        )
        .map_err(external)?;
        Ok(Some(Arc::new(provider)))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.table_names.iter().any(|t| t == name)
    }
}

/// How the partition list of a table is obtained.
enum PartitionSource {
    /// No partition keys: the table location is scanned directly.
    Unpartitioned,
    /// Partition list and locations come from Glue `GetPartitions`.
    Glue,
    /// Partition list is computed locally from `projection.*` parameters —
    /// zero `GetPartitions` calls.
    Projected(ProjectionConfig),
}

/// One concrete partition to scan: typed partition-column values plus the
/// object-store location of its data.
struct PartitionCandidate {
    values: Vec<ScalarValue>,
    prefix: String,
}

/// DataFusion [`TableProvider`] for one Glue table, scanning data through a
/// [`StorageBackend`].
pub struct GlueTableProvider {
    database: String,
    table_name: String,
    glue: Arc<dyn GlueApi>,
    storage: Arc<dyn StorageBackend>,
    /// Schema of the data files (no partition columns).
    file_schema: SchemaRef,
    /// Full table schema: file columns then partition columns.
    table_schema: SchemaRef,
    /// Partition-key fields, in Glue partition order.
    partition_fields: Vec<FieldRef>,
    format: Arc<dyn FileFormat>,
    /// SerDe class of the table, for rejecting partitions that override it.
    serde_library: Option<String>,
    /// Bucket every data location must live in.
    bucket: String,
    /// Key prefix of the table location within `bucket`.
    prefix: String,
    /// Raw table location URI (base for projected partition locations).
    location: String,
    partition_source: PartitionSource,
}

impl fmt::Debug for GlueTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GlueTableProvider")
            .field("database", &self.database)
            .field("table", &self.table_name)
            .field("location", &self.location)
            .finish()
    }
}

impl GlueTableProvider {
    /// Build a table provider from a Glue table definition. Every piece of
    /// metadata glaux cannot honor — a missing storage descriptor, an
    /// untyped column, an unmappable type, an unknown SerDe, a broken
    /// projection config — errors here, naming the construct.
    pub fn try_new(
        database: &str,
        table: GlueTable,
        glue: Arc<dyn GlueApi>,
        storage: Arc<dyn StorageBackend>,
    ) -> Result<Self> {
        let metadata_err = |message: String| CatalogError::TableMetadata {
            database: database.to_string(),
            table: table.name.clone(),
            message,
        };

        if table.table_type.as_deref() == Some("VIRTUAL_VIEW") {
            return Err(metadata_err(
                "Glue views (TableType VIRTUAL_VIEW) are not supported".to_string(),
            ));
        }
        let sd = table
            .storage_descriptor
            .as_ref()
            .ok_or_else(|| metadata_err("table has no storage descriptor".to_string()))?;
        let location = sd
            .location
            .clone()
            .ok_or_else(|| metadata_err("storage descriptor has no location".to_string()))?;
        let (bucket, prefix) = parse_s3_location(&location).map_err(&metadata_err)?;

        if sd.columns.is_empty() {
            return Err(metadata_err("table has no columns".to_string()));
        }
        let mut file_fields = Vec::with_capacity(sd.columns.len());
        for column in &sd.columns {
            let type_string = column.column_type.as_deref().ok_or_else(|| {
                metadata_err(format!("column {} has no type", column.name))
            })?;
            let data_type = hive_type_to_arrow(&column.name, type_string)?;
            file_fields.push(Field::new(&column.name, data_type, true));
        }

        let mut partition_fields: Vec<FieldRef> = Vec::with_capacity(table.partition_keys.len());
        for key in &table.partition_keys {
            let type_string = key.column_type.as_deref().ok_or_else(|| {
                metadata_err(format!("partition key {} has no type", key.name))
            })?;
            let data_type = hive_type_to_arrow(&key.name, type_string)?;
            // Partition columns are non-nullable, matching DataFusion's
            // ListingTable convention (null partitions are rejected below).
            partition_fields.push(Arc::new(Field::new(&key.name, data_type, false)));
        }

        let format = file_format_for_table(database, &table, sd)?;
        let serde_library = sd
            .serde_info
            .as_ref()
            .and_then(|s| s.serialization_library.clone());

        let file_schema: SchemaRef = Arc::new(Schema::new(file_fields));
        let table_schema: SchemaRef = Arc::new(Schema::new(
            file_schema
                .fields()
                .iter()
                .cloned()
                .chain(partition_fields.iter().cloned())
                .collect::<Vec<_>>(),
        ));

        let partition_source = match ProjectionConfig::from_table(database, &table) {
            Some(config) => PartitionSource::Projected(config?),
            None if partition_fields.is_empty() => PartitionSource::Unpartitioned,
            None => PartitionSource::Glue,
        };

        Ok(Self {
            database: database.to_string(),
            table_name: table.name,
            glue,
            storage,
            file_schema,
            table_schema,
            partition_fields,
            format,
            serde_library,
            bucket,
            prefix,
            location,
            partition_source,
        })
    }

    fn metadata_err(&self, message: String) -> CatalogError {
        CatalogError::TableMetadata {
            database: self.database.clone(),
            table: self.table_name.clone(),
            message,
        }
    }

    /// Parse the string partition values of one partition into typed
    /// [`ScalarValue`]s matching the partition-key column types.
    fn parse_partition_values(&self, values: &[String]) -> Result<Vec<ScalarValue>> {
        if values.len() != self.partition_fields.len() {
            return Err(self.metadata_err(format!(
                "partition has {} values but the table has {} partition keys",
                values.len(),
                self.partition_fields.len()
            )));
        }
        values
            .iter()
            .zip(&self.partition_fields)
            .map(|(value, field)| {
                if value == HIVE_NULL_PARTITION {
                    return Err(self.metadata_err(format!(
                        "partition column {} has a null partition value \
                         ({HIVE_NULL_PARTITION}), which is not supported",
                        field.name()
                    )));
                }
                ScalarValue::try_from_string(value.clone(), field.data_type()).map_err(|e| {
                    self.metadata_err(format!(
                        "partition value {value:?} for column {} is not a valid {}: {e}",
                        field.name(),
                        field.data_type()
                    ))
                })
            })
            .collect()
    }

    /// Turn a partition data location into a key prefix, requiring it to
    /// live in the table's bucket (multi-bucket tables are unsupported).
    fn partition_prefix(&self, location: &str) -> Result<String> {
        let (bucket, prefix) =
            parse_s3_location(location).map_err(|m| self.metadata_err(m))?;
        if bucket != self.bucket {
            return Err(self.metadata_err(format!(
                "partition location {location:?} is in bucket {bucket:?}, but the table \
                 lives in bucket {:?}; partitions outside the table bucket are not supported",
                self.bucket
            )));
        }
        Ok(prefix)
    }

    /// Resolve the full partition candidate list, before pruning.
    async fn resolve_candidates(&self) -> Result<Vec<PartitionCandidate>> {
        match &self.partition_source {
            PartitionSource::Unpartitioned => Ok(vec![PartitionCandidate {
                values: Vec::new(),
                prefix: self.prefix.clone(),
            }]),
            PartitionSource::Glue => {
                let partitions = self
                    .glue
                    .get_partitions(&self.database, &self.table_name, None)
                    .await?;
                partitions
                    .iter()
                    .map(|partition| {
                        let values = self.parse_partition_values(&partition.values)?;
                        let sd = partition.storage_descriptor.as_ref().ok_or_else(|| {
                            self.metadata_err(format!(
                                "partition {:?} has no storage descriptor",
                                partition.values
                            ))
                        })?;
                        if let Some(serde) = sd
                            .serde_info
                            .as_ref()
                            .and_then(|s| s.serialization_library.as_ref())
                            && Some(serde) != self.serde_library.as_ref()
                        {
                            return Err(self.metadata_err(format!(
                                "partition {:?} declares SerDe {serde:?}, which differs \
                                 from the table SerDe {:?}; per-partition formats are \
                                 not supported",
                                partition.values, self.serde_library
                            )));
                        }
                        let location = sd.location.as_deref().ok_or_else(|| {
                            self.metadata_err(format!(
                                "partition {:?} has no location",
                                partition.values
                            ))
                        })?;
                        Ok(PartitionCandidate {
                            values,
                            prefix: self.partition_prefix(location)?,
                        })
                    })
                    .collect()
            }
            PartitionSource::Projected(config) => config
                .enumerate(&self.location)?
                .into_iter()
                .map(|partition| {
                    Ok(PartitionCandidate {
                        values: self.parse_partition_values(&partition.values)?,
                        prefix: self.partition_prefix(&partition.location)?,
                    })
                })
                .collect(),
        }
    }

    /// Drop candidates that definitively fail a filter referencing only
    /// partition columns. Purely an optimization: every filter is also
    /// re-applied by DataFusion (all pushdown is [`Inexact`]), so skipping
    /// a filter here can only mean reading more data, never wrong results.
    ///
    /// [`Inexact`]: TableProviderFilterPushDown::Inexact
    fn prune_candidates(
        &self,
        state: &dyn Session,
        filters: &[Expr],
        candidates: Vec<PartitionCandidate>,
    ) -> std::result::Result<Vec<PartitionCandidate>, DataFusionError> {
        if self.partition_fields.is_empty() || candidates.is_empty() {
            return Ok(candidates);
        }
        let partition_col_names: Vec<&str> = self
            .partition_fields
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        let applicable: Vec<&Expr> = filters
            .iter()
            .filter(|filter| expr_applicable_for_cols(&partition_col_names, filter))
            .collect();
        if applicable.is_empty() {
            return Ok(candidates);
        }

        // One row per candidate partition, one column per partition key.
        let arrays = (0..self.partition_fields.len())
            .map(|i| {
                ScalarValue::iter_to_array(
                    candidates.iter().map(|c| c.values[i].clone()),
                )
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let schema = Arc::new(Schema::new(
            self.partition_fields
                .iter()
                .map(|f| Field::new(f.name(), f.data_type().clone(), true))
                .collect::<Vec<_>>(),
        ));
        let batch = RecordBatch::try_new(Arc::clone(&schema), arrays)?;
        let df_schema = DFSchema::try_from(Arc::clone(&schema))?;

        let mut keep = vec![true; candidates.len()];
        for filter in applicable {
            let physical = match state.create_physical_expr((*filter).clone(), &df_schema) {
                Ok(physical) => physical,
                Err(e) => {
                    // Cannot evaluate this filter against partition values
                    // (e.g. a subquery): skip it for pruning; DataFusion
                    // still applies it to the scanned rows.
                    tracing::debug!(
                        filter = %filter,
                        error = %e,
                        "skipping partition-pruning filter that cannot be \
                         evaluated on partition values"
                    );
                    continue;
                }
            };
            let values = physical.evaluate(&batch)?.into_array(batch.num_rows())?;
            let booleans = values.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "partition-pruning filter {filter} evaluated to non-boolean type {}",
                    values.data_type()
                ))
            })?;
            for (i, keep_flag) in keep.iter_mut().enumerate() {
                // A NULL filter result excludes the row, so the partition
                // can be pruned unless the result is definitively true.
                *keep_flag &= booleans.is_valid(i) && booleans.value(i);
            }
        }
        Ok(candidates
            .into_iter()
            .zip(keep)
            .filter_map(|(candidate, keep)| keep.then_some(candidate))
            .collect())
    }

    /// List the data files of one partition, applying Hive's hidden-file
    /// rules (path segments starting with `_` or `.` are ignored, as are
    /// zero-byte objects such as directory markers).
    async fn list_partition_files(
        &self,
        candidate: &PartitionCandidate,
    ) -> Result<Vec<PartitionedFile>> {
        let objects = self
            .storage
            .list_objects(&self.bucket, &candidate.prefix)
            .await?;
        Ok(objects
            .into_iter()
            .filter(|object| {
                object.size > 0
                    && !object
                        .key
                        .strip_prefix(candidate.prefix.trim_start_matches('/'))
                        .unwrap_or(&object.key)
                        .split('/')
                        .any(|segment| segment.starts_with('_') || segment.starts_with('.'))
            })
            .map(|object| {
                let mut file = PartitionedFile::new(object.key, object.size);
                file.partition_values = candidate.values.clone();
                file
            })
            .collect())
    }
}

#[async_trait]
impl TableProvider for GlueTableProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.table_schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> std::result::Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        // Inexact everywhere: filters reach `scan` for partition pruning,
        // and DataFusion re-applies them all — pruning can never be the
        // only thing standing between the user and a wrong result.
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> std::result::Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let candidates = self.resolve_candidates().await.map_err(external)?;
        let candidates = self.prune_candidates(state, filters, candidates)?;

        let mut files = Vec::new();
        for candidate in &candidates {
            files.extend(self.list_partition_files(candidate).await.map_err(external)?);
        }

        if files.is_empty() {
            let projected_schema = project_schema(&self.schema(), projection)?;
            return Ok(Arc::new(EmptyExec::new(projected_schema)));
        }

        // Make the table's bucket resolvable by DataFusion's runtime.
        let object_store_url = ObjectStoreUrl::parse(format!("s3://{}/", self.bucket))?;
        state.runtime_env().register_object_store(
            object_store_url.as_ref(),
            self.storage.object_store(&self.bucket).map_err(external)?,
        );

        // Round-robin files into up to `target_partitions` groups. Partition
        // values travel per-file, so mixing partitions in a group is fine.
        let group_count = state.config().target_partitions().max(1).min(files.len());
        let mut groups: Vec<Vec<PartitionedFile>> = vec![Vec::new(); group_count];
        for (i, file) in files.into_iter().enumerate() {
            groups[i % group_count].push(file);
        }
        let file_groups: Vec<FileGroup> = groups.into_iter().map(FileGroup::new).collect();

        let table_schema = TableSchemaBuilder::new(Arc::clone(&self.file_schema))
            .with_table_partition_cols(self.partition_fields.clone())
            .build();
        let file_source = self.format.file_source(table_schema);
        let scan_config = FileScanConfigBuilder::new(object_store_url, file_source)
            .with_file_groups(file_groups)
            .with_projection_indices(projection.cloned())?
            .with_limit(limit)
            .build();

        self.format.create_physical_plan(state, scan_config).await
    }
}

/// Split an `s3://bucket/prefix` URI into `(bucket, prefix)`. Accepts the
/// `s3`, `s3a`, and `s3n` schemes; anything else is an explicit error.
fn parse_s3_location(location: &str) -> std::result::Result<(String, String), String> {
    let rest = ["s3://", "s3a://", "s3n://"]
        .iter()
        .find_map(|scheme| location.strip_prefix(scheme))
        .ok_or_else(|| {
            format!("location {location:?} is not an s3://, s3a://, or s3n:// URI")
        })?;
    let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
    if bucket.is_empty() {
        return Err(format!("location {location:?} has an empty bucket"));
    }
    Ok((bucket.to_string(), prefix.trim_matches('/').to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn s3_location_parsing() {
        assert_eq!(
            parse_s3_location("s3://bucket/a/b/").unwrap(),
            ("bucket".to_string(), "a/b".to_string())
        );
        assert_eq!(
            parse_s3_location("s3a://bucket").unwrap(),
            ("bucket".to_string(), String::new())
        );
        assert!(parse_s3_location("file:///tmp/x").is_err());
        assert!(parse_s3_location("s3:///no-bucket").is_err());
    }
}
