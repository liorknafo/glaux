//! `glaux-server` — the standalone glaux binary.
//!
//! Serves the Athena and Firehose APIs over HTTP against externally
//! configured S3 and Glue endpoints (fakecloud over HTTP, MinIO, or real
//! AWS). Endpoints are configuration, never assumptions. See the crate
//! docs in `lib.rs` for the full story.

use clap::Parser;
use glaux_server::cli::Cli;
use tracing_subscriber::EnvFilter;

/// Initialize `tracing` for the binary: `RUST_LOG`-controlled filtering
/// (default `info`), human-readable output on stderr.
fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    init_tracing();
    if let Err(err) = glaux_server::run(cli).await {
        // Both to the log (structured) and to plain stderr (so the message
        // is readable even with RUST_LOG=off).
        tracing::error!(error = %err, "glaux-server exiting");
        eprintln!("glaux-server: {err}");
        std::process::exit(1);
    }
}
