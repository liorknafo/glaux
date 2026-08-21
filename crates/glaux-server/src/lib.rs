//! `glaux-server` — the standalone glaux binary as a library.
//!
//! The binary in `main.rs` is a thin shell around [`run`]: parse the CLI
//! ([`cli::Cli`]), resolve the unified configuration
//! ([`glaux_catalog::GlauxConfig`]: file → `GLAUX_*` env → flags), validate
//! the configured S3 and Glue endpoints *before* accepting traffic
//! ([`startup`]), then serve both services on one port ([`app`]) until a
//! shutdown signal arrives, at which point every Firehose buffer is flushed
//! before the process exits.
//!
//! # Wire protocol
//!
//! Both Athena and Firehose speak AWS JSON 1.1: `POST /` with an
//! `X-Amz-Target` header. The server routes on that header's service
//! prefix — `AmazonAthena.*` to the Athena service, `Firehose_20150804.*`
//! to the Firehose service — so a single `--endpoint-url` works for the
//! AWS CLI and every SDK. `GET /health` reports liveness and the configured
//! endpoints.
//!
//! # Never silently wrong
//!
//! Endpoints are required configuration, never assumptions: the server
//! refuses to start without them (or an explicit `--aws`), and it refuses
//! to start when the S3 or Glue endpoint it was given does not answer. An
//! `X-Amz-Target` outside the two served services is a 400 naming the
//! target, not a fallthrough. Shutdown never drops buffered records
//! silently: a failed final flush is logged per stream and makes the process
//! exit nonzero.

pub mod app;
pub mod cli;
pub mod engine;
pub mod startup;

use std::net::SocketAddr;
use std::sync::Arc;

use datafusion::prelude::SessionContext;
use glaux_athena::{AthenaService, AthenaServiceConfig, TrinoEngine};
use glaux_catalog::{GlauxConfig, GlueApi, NetworkGlueApi, NetworkStorageBackend, StorageBackend};
use glaux_firehose::{DeliverySink, FirehoseService, FirehoseServiceConfig, S3DeliverySink};

use crate::cli::Cli;
use crate::engine::GlueBackedEngine;

/// Everything that can stop the server from starting or make it exit
/// nonzero. Every variant carries an actionable message.
#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    /// Configuration could not be resolved or is incomplete.
    #[error("configuration error: {0}")]
    Config(String),
    /// A configured endpoint failed its startup probe.
    #[error("{0}")]
    Startup(#[from] startup::StartupError),
    /// The listen address could not be bound.
    #[error("cannot listen on {addr}: {source}. Pick another address with --listen / GLAUX_LISTEN")]
    Bind {
        /// The requested address.
        addr: String,
        /// The OS error.
        source: std::io::Error,
    },
    /// The HTTP server itself failed.
    #[error("HTTP server failed: {0}")]
    Serve(std::io::Error),
    /// Shutdown flushed every buffer it could, but some streams failed.
    #[error("shutdown: {0} Firehose stream(s) could not flush their buffers; see the log above")]
    ShutdownFlush(usize),
}

/// The name of the DataFusion catalog Athena's `AwsDataCatalog` maps to.
pub const CATALOG_NAME: &str = "awsdatacatalog";

/// Resolve configuration from the parsed CLI (file + env + flags) and
/// enforce the standalone-binary rule that endpoints are explicit.
pub fn resolve_config(cli: &Cli) -> Result<GlauxConfig, ServerError> {
    let config = GlauxConfig::load(cli.config.as_deref(), &cli.overrides())
        .map_err(|e| ServerError::Config(e.to_string()))?;
    if !cli.aws {
        let mut missing = Vec::new();
        if config.s3_endpoint.is_none() {
            missing.push("S3 (--s3-endpoint / GLAUX_S3_ENDPOINT / s3_endpoint in the config file)");
        }
        if config.glue_endpoint.is_none() {
            missing.push(
                "Glue (--glue-endpoint / GLAUX_GLUE_ENDPOINT / glue_endpoint in the config file)",
            );
        }
        if !missing.is_empty() {
            return Err(ServerError::Config(format!(
                "glaux-server needs explicit endpoints; missing: {}. Point it at an emulator \
                 (e.g. fakecloud: --s3-endpoint http://127.0.0.1:4566 --glue-endpoint \
                 http://127.0.0.1:4566) or pass --aws to use real AWS for endpoints left unset",
                missing.join(", ")
            )));
        }
    }
    Ok(config)
}

/// The wired services, ready to be served.
pub struct Services {
    /// The Athena service.
    pub athena: Arc<AthenaService>,
    /// The Firehose service.
    pub firehose: Arc<FirehoseService>,
}

/// Build both services over network S3/Glue clients for `config`.
pub fn build_services(config: &GlauxConfig) -> Services {
    let storage: Arc<dyn StorageBackend> = Arc::new(NetworkStorageBackend::new(config));
    let glue: Arc<dyn GlueApi> = Arc::new(NetworkGlueApi::new(config));

    let engine = GlueBackedEngine::new(
        TrinoEngine::new(SessionContext::new(), CATALOG_NAME),
        Arc::clone(&glue),
        Arc::clone(&storage),
        CATALOG_NAME,
    );
    let athena = Arc::new(AthenaService::new(
        AthenaServiceConfig::from(&config.athena),
        Arc::new(engine),
        Arc::clone(&storage),
    ));

    let sink: Arc<dyn DeliverySink> = Arc::new(S3DeliverySink::new(storage, glue));
    let firehose = Arc::new(FirehoseService::new(
        FirehoseServiceConfig::from(config),
        sink,
    ));

    Services { athena, firehose }
}

/// Resolve a shutdown future: `SIGINT` (Ctrl-C) or, on Unix, `SIGTERM`.
async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(e) = tokio::signal::ctrl_c().await {
            tracing::error!(error = %e, "failed to install Ctrl-C handler");
            std::future::pending::<()>().await;
        }
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(e) => {
                tracing::error!(error = %e, "failed to install SIGTERM handler");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => tracing::info!("received Ctrl-C, shutting down"),
        _ = terminate => tracing::info!("received SIGTERM, shutting down"),
    }
}

/// Run the server to completion: resolve config, probe endpoints, serve,
/// flush on shutdown. Returns once the process should exit.
pub async fn run(cli: Cli) -> Result<(), ServerError> {
    let config = resolve_config(&cli)?;
    tracing::info!(
        s3_endpoint = config.s3_endpoint.as_deref().unwrap_or("<real AWS>"),
        glue_endpoint = config.glue_endpoint.as_deref().unwrap_or("<real AWS>"),
        region = %config.region,
        account_id = %config.account_id,
        workgroup = %config.athena.workgroup,
        output_location = config.athena.output_location.as_deref().unwrap_or("<none>"),
        "resolved configuration"
    );

    let report = startup::validate(&config).await?;
    tracing::info!(
        glue_databases = report.glue_databases,
        s3_probe = %report.s3_probe,
        "startup validation passed"
    );

    let services = build_services(&config);
    let state = app::AppState::new(
        Arc::clone(&services.athena),
        Arc::clone(&services.firehose),
        &config,
    );
    let router = app::router(state);

    let listener = tokio::net::TcpListener::bind(&cli.listen)
        .await
        .map_err(|source| ServerError::Bind {
            addr: cli.listen.clone(),
            source,
        })?;
    let local: SocketAddr = listener.local_addr().map_err(ServerError::Serve)?;
    tracing::info!(
        addr = %local,
        "glaux-server listening; use --endpoint-url http://{local} for both athena and firehose"
    );

    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(ServerError::Serve)?;

    tracing::info!("HTTP server stopped; flushing Firehose buffers");
    let failures = services.firehose.shutdown().await;
    if failures.is_empty() {
        tracing::info!("all Firehose buffers flushed; bye");
        Ok(())
    } else {
        for (stream, err) in &failures {
            tracing::error!(stream, error = %err, "buffer not flushed on shutdown");
        }
        Err(ServerError::ShutdownFlush(failures.len()))
    }
}
