//! `glaux` — the all-in-one binary.
//!
//! Links the fakecloud service crates together with the glaux Athena and
//! Firehose engines on a single port, replacing fakecloud's Athena stub with
//! real DataFusion-backed execution and defaulting S3/Glue access to
//! in-process fakecloud state.
//!
//! This binary is AGPL-3.0 licensed; the engine crates it links
//! (`glaux-athena`, `glaux-firehose`, `glaux-catalog`) are Apache-2.0 and
//! must never depend on fakecloud crates (enforced in CI).

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

    // Scaffolding stage (LIO-18): fakecloud embedding and service wiring land
    // in subsequent stories. Per the "never silently wrong" rule, refuse to
    // start rather than accept requests we cannot serve faithfully.
    tracing::error!(
        "glaux is not yet implemented: this build contains only workspace scaffolding (LIO-18). \
         The fakecloud embedding and the Athena and Firehose services land in subsequent \
         stories. Refusing to start rather than serve an endpoint that could return fake data."
    );
    std::process::exit(1);
}
