//! `glaux` — the all-in-one local data plane.
//!
//! Links fakecloud's control-plane services together with glaux's real
//! Athena (DataFusion) and Firehose (delivery) engines in one process on one
//! port. fakecloud's own Athena and Firehose stubs are replaced on its
//! [`ServiceRegistry`](fakecloud_core::registry::ServiceRegistry) by name;
//! S3 and Glue are consumed **in-process** — glaux's engines read fakecloud's
//! state directly, with no HTTP hop (see [`storage`] and [`glue`]).
//!
//! This crate is AGPL-3.0 because it links fakecloud. The engine crates it
//! embeds (`glaux-athena`, `glaux-firehose`, `glaux-catalog`) are Apache-2.0
//! and never depend on fakecloud.
//!
//! The library surface exists so integration tests (and embedders) can build
//! the exact server the binary runs — see [`server::Glaux::build`].

pub mod bridge;
pub mod engine;
pub mod glue;
pub mod server;
pub mod storage;

pub use bridge::{AthenaAwsService, FirehoseAwsService};
pub use engine::LiveCatalogEngine;
pub use glue::InProcessGlue;
pub use server::{EMBEDDED_FAKECLOUD_SERVICES, FAKECLOUD_VERSION, Glaux, ServeOptions};
pub use storage::{InProcessObjectStore, InProcessStorage};
