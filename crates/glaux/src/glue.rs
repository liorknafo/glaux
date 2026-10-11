//! In-process [`GlueApi`] over fakecloud's Glue Data Catalog state.
//!
//! Every read goes straight to fakecloud's [`GlueAccounts`] behind its
//! `RwLock` and is mapped onto glaux's catalog types — no JSON round trip,
//! no HTTP. Partition `Expression` filtering reuses fakecloud's own
//! evaluator so server-side filtering behaves identically to the wire API.
//!
//! # Never silently wrong
//!
//! A missing database or table is [`CatalogError::GlueEntityNotFound`], as
//! Glue itself reports (`EntityNotFoundException`), never an empty result.

use std::collections::HashMap;
use std::fmt;

use async_trait::async_trait;
use fakecloud_glue::{
    Column, Database, Partition, SharedGlueState, StorageDescriptor, Table, partition_filter,
};
use glaux_catalog::{
    CatalogError, GlueApi, GlueColumn, GlueDatabase, GluePartition, GlueSerDeInfo,
    GlueStorageDescriptor, GlueTable,
};

/// [`GlueApi`] over in-process fakecloud Glue state.
pub struct InProcessGlue {
    state: SharedGlueState,
    account_id: String,
    region: String,
}

impl fmt::Debug for InProcessGlue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InProcessGlue")
            .field("account_id", &self.account_id)
            .field("region", &self.region)
            .finish()
    }
}

fn to_map(params: &std::collections::BTreeMap<String, String>) -> HashMap<String, String> {
    params.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
}

fn column(c: &Column) -> GlueColumn {
    GlueColumn {
        name: c.name.clone(),
        column_type: Some(c.column_type.clone()),
        comment: c.comment.clone(),
    }
}

fn storage_descriptor(sd: &StorageDescriptor) -> GlueStorageDescriptor {
    GlueStorageDescriptor {
        columns: sd.columns.iter().map(column).collect(),
        location: sd.location.clone(),
        input_format: sd.input_format.clone(),
        output_format: sd.output_format.clone(),
        serde_info: sd.serde_info.as_ref().map(|s| GlueSerDeInfo {
            name: s.name.clone(),
            serialization_library: s.serialization_library.clone(),
            parameters: to_map(&s.parameters),
        }),
        compressed: sd.compressed.unwrap_or(false),
        parameters: to_map(&sd.parameters),
    }
}

fn database(db: &Database) -> GlueDatabase {
    GlueDatabase {
        name: db.name.clone(),
        description: db.description.clone(),
        location_uri: db.location_uri.clone(),
        parameters: to_map(&db.parameters),
    }
}

fn table(t: &Table) -> GlueTable {
    GlueTable {
        name: t.name.clone(),
        database_name: Some(t.database_name.clone()),
        table_type: t.table_type.clone(),
        storage_descriptor: t.storage_descriptor.as_ref().map(storage_descriptor),
        partition_keys: t.partition_keys.iter().map(column).collect(),
        parameters: to_map(&t.parameters),
    }
}

fn partition(p: &Partition) -> GluePartition {
    GluePartition {
        values: p.values.clone(),
        storage_descriptor: p.storage_descriptor.as_ref().map(storage_descriptor),
        parameters: to_map(&p.parameters),
    }
}

impl InProcessGlue {
    /// Build a client over `state` for one fakecloud account and region
    /// (fakecloud partitions catalog state per account and per region).
    pub fn new(
        state: SharedGlueState,
        account_id: impl Into<String>,
        region: impl Into<String>,
    ) -> Self {
        Self {
            state,
            account_id: account_id.into(),
            region: region.into(),
        }
    }

    /// Run `f` over this account/region's database map under the read
    /// lock. An account with no catalog state yet simply has no databases.
    fn with_databases<T>(
        &self,
        f: impl FnOnce(Option<&std::collections::BTreeMap<String, Database>>) -> T,
    ) -> T {
        let accounts = self.state.read();
        let databases = accounts
            .get(&self.account_id)
            .and_then(|state| state.dbs_in(&self.region));
        f(databases)
    }

    fn with_table<T>(
        &self,
        database: &str,
        table_name: &str,
        f: impl FnOnce(&Table) -> T,
    ) -> glaux_catalog::Result<T> {
        self.with_databases(|databases| {
            let db = databases.and_then(|dbs| dbs.get(database)).ok_or_else(|| {
                CatalogError::GlueEntityNotFound {
                    message: format!("Database {database} not found."),
                }
            })?;
            let t = db
                .tables
                .get(table_name)
                .ok_or_else(|| CatalogError::GlueEntityNotFound {
                    message: format!("Table {table_name} not found."),
                })?;
            Ok(f(t))
        })
    }
}

#[async_trait]
impl GlueApi for InProcessGlue {
    async fn get_databases(&self) -> glaux_catalog::Result<Vec<GlueDatabase>> {
        Ok(self.with_databases(|databases| {
            databases
                .map(|dbs| dbs.values().map(database).collect())
                .unwrap_or_default()
        }))
    }

    async fn get_tables(&self, database: &str) -> glaux_catalog::Result<Vec<GlueTable>> {
        self.with_databases(|databases| {
            let db = databases.and_then(|dbs| dbs.get(database)).ok_or_else(|| {
                CatalogError::GlueEntityNotFound {
                    message: format!("Database {database} not found."),
                }
            })?;
            Ok(db.tables.values().map(table).collect())
        })
    }

    async fn get_table(
        &self,
        database: &str,
        table_name: &str,
    ) -> glaux_catalog::Result<GlueTable> {
        self.with_table(database, table_name, table)
    }

    async fn get_partitions(
        &self,
        database: &str,
        table_name: &str,
        expression: Option<&str>,
    ) -> glaux_catalog::Result<Vec<GluePartition>> {
        self.with_table(database, table_name, |t| {
            t.partitions
                .values()
                .filter(|p| match expression {
                    Some(expr) if !expr.trim().is_empty() => {
                        partition_filter::matches(expr, &t.partition_keys, &p.values)
                    }
                    _ => true,
                })
                .map(partition)
                .collect()
        })
    }
}
