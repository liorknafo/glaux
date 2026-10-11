//! Misconfiguration must fail loudly at startup with actionable messages.
//! These run the real binary and need no network.

use std::net::TcpListener;
use std::process::Command;

fn run(args: &[&str], env: &[(&str, &str)]) -> (i32, String) {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_glaux-server"));
    cmd.args(args).env("RUST_LOG", "off");
    for (k, v) in env {
        cmd.env(k, v);
    }
    let output = cmd.output().expect("run glaux-server");
    (
        output.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

/// A port nothing listens on (bound then released).
fn closed_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

#[test]
fn missing_endpoints_name_every_way_to_set_them() {
    let (code, stderr) = run(&[], &[]);
    assert_eq!(code, 1, "stderr: {stderr}");
    for needle in [
        "--s3-endpoint",
        "GLAUX_S3_ENDPOINT",
        "--glue-endpoint",
        "GLAUX_GLUE_ENDPOINT",
        "--aws",
    ] {
        assert!(stderr.contains(needle), "missing {needle:?} in: {stderr}");
    }
}

#[test]
fn only_one_endpoint_missing_is_reported_precisely() {
    let (code, stderr) = run(&["--s3-endpoint", "http://127.0.0.1:1"], &[]);
    assert_eq!(code, 1, "stderr: {stderr}");
    assert!(stderr.contains("missing: Glue"), "{stderr}");
    assert!(!stderr.contains("missing: S3"), "{stderr}");
}

#[test]
fn unreachable_endpoints_fail_startup_and_name_both() {
    let port = closed_port();
    let url = format!("http://127.0.0.1:{port}");
    let (code, stderr) = run(&["--s3-endpoint", &url, "--glue-endpoint", &url], &[]);
    assert_eq!(code, 1, "stderr: {stderr}");
    assert!(stderr.contains(&format!("Glue endpoint {url}")), "{stderr}");
    assert!(stderr.contains(&format!("S3 endpoint {url}")), "{stderr}");
    assert!(stderr.contains("--glue-endpoint"), "{stderr}");
    assert!(stderr.contains("--s3-endpoint"), "{stderr}");
}

#[test]
fn env_endpoints_are_honored_and_invalid_urls_are_rejected() {
    // A bare host:port is not an endpoint URL; the shared config layer
    // rejects it naming the field.
    let (code, stderr) = run(
        &[],
        &[
            ("GLAUX_S3_ENDPOINT", "127.0.0.1:4566"),
            ("GLAUX_GLUE_ENDPOINT", "http://127.0.0.1:4566"),
        ],
    );
    assert_eq!(code, 1, "stderr: {stderr}");
    assert!(stderr.contains("s3_endpoint"), "{stderr}");
    assert!(stderr.contains("http://"), "{stderr}");
}

#[test]
fn explicit_missing_config_file_is_an_error() {
    let (code, stderr) = run(&["--config", "/nonexistent/glaux.toml"], &[]);
    assert_eq!(code, 1, "stderr: {stderr}");
    assert!(stderr.contains("/nonexistent/glaux.toml"), "{stderr}");
}

#[test]
fn malformed_output_location_is_rejected_before_probing() {
    let port = closed_port();
    let url = format!("http://127.0.0.1:{port}");
    let (code, stderr) = run(
        &[
            "--s3-endpoint",
            &url,
            "--glue-endpoint",
            &url,
            "--athena-output-location",
            "results/",
        ],
        &[],
    );
    assert_eq!(code, 1, "stderr: {stderr}");
    assert!(stderr.contains("s3://bucket/prefix"), "{stderr}");
    assert!(stderr.contains("--athena-output-location"), "{stderr}");
}

#[test]
fn help_lists_the_configuration_surface() {
    let output = Command::new(env!("CARGO_BIN_EXE_glaux-server"))
        .arg("--help")
        .output()
        .unwrap();
    assert!(output.status.success());
    let help = String::from_utf8_lossy(&output.stdout);
    for flag in [
        "--listen",
        "--config",
        "--s3-endpoint",
        "--glue-endpoint",
        "--aws",
        "--athena-output-location",
        "--firehose-max-batch-records",
    ] {
        assert!(help.contains(flag), "missing {flag} in help: {help}");
    }
}
