//! `glaux-server` — the standalone glaux binary.
//!
//! Serves the Athena and Firehose APIs over HTTP against externally
//! configured S3 and Glue endpoints (fakecloud over HTTP, MinIO, or real
//! AWS). Endpoints are configuration, never assumptions.

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

fn main() {
    init_tracing();

    // Scaffolding stage (LIO-18): the HTTP server and service wiring land in
    // subsequent stories. Per the "never silently wrong" rule, refuse to
    // start rather than accept requests we cannot serve faithfully.
    tracing::error!(
        "glaux-server is not yet implemented: this build contains only workspace scaffolding \
         (LIO-18). The Athena and Firehose services land in subsequent stories. Refusing to \
         start rather than serve an endpoint that could return fake data."
    );
    std::process::exit(1);
}
