//! `glaux` — the all-in-one binary.
//!
//! Embedded fakecloud control plane plus glaux's real Athena and Firehose,
//! one process, one port (4566 by default). S3 and Glue are served by the
//! embedded fakecloud and consumed in-process by the engines.
//!
//! This binary is AGPL-3.0 licensed; the engine crates it links
//! (`glaux-athena`, `glaux-firehose`, `glaux-catalog`) are Apache-2.0 and
//! must never depend on fakecloud crates (enforced in CI).

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;

use glaux::{EMBEDDED_FAKECLOUD_SERVICES, FAKECLOUD_VERSION, Glaux, ServeOptions};
use glaux_catalog::{ConfigOverrides, GlauxConfig};
use tracing_subscriber::EnvFilter;

const USAGE: &str = "\
glaux — local data plane: embedded fakecloud + real Athena & Firehose on one port

USAGE:
    glaux [OPTIONS]

OPTIONS:
    --addr <HOST:PORT>                Bind address (default 0.0.0.0:4566; env GLAUX_ADDR)
    --config <PATH>                   TOML config file (see glaux-catalog docs)
    --region <REGION>                 AWS region (default us-east-1; env GLAUX_REGION)
    --account-id <ID>                 AWS account id (default 123456789012; env GLAUX_ACCOUNT_ID)
    --athena-output-location <S3URI>  Default Athena OutputLocation, e.g. s3://results/
    --athena-workgroup <NAME>         Default Athena workgroup (default primary)
    -h, --help                        Print this help
    -V, --version                     Print the version

S3 and Glue are always served in-process; GLAUX_S3_ENDPOINT / GLAUX_GLUE_ENDPOINT
are rejected here (use glaux-server to target an external endpoint).";

/// Parsed command line.
#[derive(Debug, Default)]
struct Cli {
    addr: Option<String>,
    config: Option<PathBuf>,
    overrides: ConfigOverrides,
}

fn parse_args(args: impl IntoIterator<Item = String>) -> Result<Option<Cli>, String> {
    let mut cli = Cli::default();
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        let mut value = |flag: &str| {
            args.next()
                .ok_or_else(|| format!("{flag} requires a value\n\n{USAGE}"))
        };
        match arg.as_str() {
            "-h" | "--help" => {
                println!("{USAGE}");
                return Ok(None);
            }
            "-V" | "--version" => {
                println!(
                    "glaux {} (fakecloud {FAKECLOUD_VERSION})",
                    env!("CARGO_PKG_VERSION")
                );
                return Ok(None);
            }
            "--addr" => cli.addr = Some(value("--addr")?),
            "--config" => cli.config = Some(PathBuf::from(value("--config")?)),
            "--region" => cli.overrides.region = Some(value("--region")?),
            "--account-id" => cli.overrides.account_id = Some(value("--account-id")?),
            "--athena-output-location" => {
                cli.overrides.athena_output_location = Some(value("--athena-output-location")?);
            }
            "--athena-workgroup" => {
                cli.overrides.athena_workgroup = Some(value("--athena-workgroup")?);
            }
            other => return Err(format!("unknown argument {other:?}\n\n{USAGE}")),
        }
    }
    Ok(Some(cli))
}

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

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    tracing::info!("shutting down: flushing Firehose buffers");
}

async fn run(cli: Cli) -> Result<(), String> {
    let config = GlauxConfig::load(cli.config.as_deref(), &cli.overrides)
        .map_err(|e| format!("configuration error: {e}"))?;
    let addr = cli
        .addr
        .or_else(|| std::env::var("GLAUX_ADDR").ok())
        .unwrap_or_else(|| "0.0.0.0:4566".to_string());
    let addr: SocketAddr = addr
        .parse()
        .map_err(|e| format!("invalid --addr {addr:?}: {e}"))?;
    let options = ServeOptions { addr };

    let glaux = Glaux::build(&config, &options)?;
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| format!("cannot bind {addr}: {e}"))?;
    let bound = listener
        .local_addr()
        .map_err(|e| format!("cannot read bound address: {e}"))?;

    tracing::info!(
        fakecloud = FAKECLOUD_VERSION,
        region = %config.region,
        account_id = %config.account_id,
        athena_output_location = ?config.athena.output_location,
        "glaux: embedded fakecloud services {:?}; glaux athena + firehose replace fakecloud's stubs",
        EMBEDDED_FAKECLOUD_SERVICES
    );
    tracing::info!(services = ?glaux.service_names(), "registered services");
    tracing::info!("listening on http://{bound}");

    let failed = glaux
        .serve(listener, shutdown_signal())
        .await
        .map_err(|e| format!("server error: {e}"))?;
    for (stream, err) in &failed {
        tracing::error!(stream, error = %err, "final Firehose flush failed; records lost");
    }
    if failed.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "{} delivery stream(s) failed their final flush",
            failed.len()
        ))
    }
}

fn main() -> ExitCode {
    init_tracing();
    let cli = match parse_args(std::env::args().skip(1)) {
        Ok(Some(cli)) => cli,
        Ok(None) => return ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("error: {message}");
            return ExitCode::from(2);
        }
    };
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(e) => {
            tracing::error!("failed to start the tokio runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    match runtime.block_on(run(cli)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            tracing::error!("{message}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn parses_every_flag() {
        let cli = parse_args(args(&[
            "--addr",
            "127.0.0.1:1",
            "--config",
            "/tmp/g.toml",
            "--region",
            "eu-west-1",
            "--account-id",
            "000000000001",
            "--athena-output-location",
            "s3://r/",
            "--athena-workgroup",
            "wg",
        ]))
        .unwrap()
        .unwrap();
        assert_eq!(cli.addr.as_deref(), Some("127.0.0.1:1"));
        assert_eq!(cli.config, Some(PathBuf::from("/tmp/g.toml")));
        assert_eq!(cli.overrides.region.as_deref(), Some("eu-west-1"));
        assert_eq!(cli.overrides.account_id.as_deref(), Some("000000000001"));
        assert_eq!(
            cli.overrides.athena_output_location.as_deref(),
            Some("s3://r/")
        );
        assert_eq!(cli.overrides.athena_workgroup.as_deref(), Some("wg"));
    }

    #[test]
    fn unknown_flags_and_missing_values_are_explicit_errors() {
        let err = parse_args(args(&["--bogus"])).unwrap_err();
        assert!(err.contains("unknown argument \"--bogus\""), "{err}");
        let err = parse_args(args(&["--addr"])).unwrap_err();
        assert!(err.contains("--addr requires a value"), "{err}");
    }

    #[test]
    fn help_and_version_exit_without_a_cli() {
        assert!(parse_args(args(&["--help"])).unwrap().is_none());
        assert!(parse_args(args(&["-V"])).unwrap().is_none());
    }
}
