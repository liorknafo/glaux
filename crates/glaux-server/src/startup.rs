//! Startup validation: prove the configured S3 and Glue endpoints answer
//! *before* the server accepts a single request.
//!
//! A server that starts happily against a typo'd endpoint and then fails
//! every query with a transport error is technically "not silently wrong",
//! but it is needlessly unhelpful. Every failure here names the endpoint,
//! the flag/env var that set it, and what to try.
//!
//! - **Glue** is probed with a real `GetDatabases` call through the same
//!   client the services use, so signing, protocol, and reachability are
//!   all exercised.
//! - **S3** is probed by listing the Athena `OutputLocation` bucket when
//!   one is configured (the bucket must exist for queries to succeed, so
//!   its absence is reported up front); otherwise by an HTTP request to the
//!   endpoint root, where any HTTP response — including `403` from real AWS
//!   — proves the endpoint is an S3-speaking host, and a connection failure
//!   does not.

use std::time::Duration;

use glaux_catalog::{GlauxConfig, GlueApi, NetworkGlueApi, NetworkStorageBackend, StorageBackend};

/// How long each probe may take before it is reported as unreachable.
pub const PROBE_TIMEOUT: Duration = Duration::from_secs(10);

/// A probe failed. The message is the whole story.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct StartupError {
    /// Which endpoint failed: `"s3"` or `"glue"`.
    pub endpoint: &'static str,
    /// Actionable diagnostic.
    pub message: String,
}

/// What the probes observed, for the startup log line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartupReport {
    /// Number of Glue databases visible at startup.
    pub glue_databases: usize,
    /// How S3 was probed (`listed s3://bucket/prefix` or `reached <url>`).
    pub s3_probe: String,
}

fn glue_endpoint_label(config: &GlauxConfig) -> String {
    config
        .glue_endpoint
        .clone()
        .unwrap_or_else(|| format!("https://glue.{}.amazonaws.com (real AWS)", config.region))
}

fn s3_endpoint_label(config: &GlauxConfig) -> String {
    config
        .s3_endpoint
        .clone()
        .unwrap_or_else(|| format!("https://s3.{}.amazonaws.com (real AWS)", config.region))
}

/// Split `s3://bucket/prefix` into `(bucket, prefix)`.
fn parse_s3_uri(uri: &str) -> Option<(&str, &str)> {
    let rest = uri.strip_prefix("s3://")?;
    let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
    if bucket.is_empty() {
        return None;
    }
    Some((bucket, prefix))
}

/// Probe Glue with `GetDatabases`.
pub async fn probe_glue(config: &GlauxConfig) -> Result<usize, StartupError> {
    let label = glue_endpoint_label(config);
    let glue = NetworkGlueApi::new(config).map_err(|e| StartupError {
        endpoint: "glue",
        message: format!(
            "cannot build a Glue client for endpoint {label}: {e}. Check --glue-endpoint / \
             GLAUX_GLUE_ENDPOINT, the region (--region, currently {:?}), and the credentials",
            config.region
        ),
    })?;
    let result = tokio::time::timeout(PROBE_TIMEOUT, glue.get_databases()).await;
    match result {
        Ok(Ok(databases)) => Ok(databases.len()),
        Ok(Err(e)) => Err(StartupError {
            endpoint: "glue",
            message: format!(
                "Glue endpoint {label} did not answer GetDatabases: {e}. Check --glue-endpoint / \
                 GLAUX_GLUE_ENDPOINT (is the emulator running and listening there?), the region \
                 (--region, currently {:?}), and the credentials if the endpoint verifies them",
                config.region
            ),
        }),
        Err(_) => Err(StartupError {
            endpoint: "glue",
            message: format!(
                "Glue endpoint {label} did not answer GetDatabases within {}s. Check \
                 --glue-endpoint / GLAUX_GLUE_ENDPOINT and that the host is reachable",
                PROBE_TIMEOUT.as_secs()
            ),
        }),
    }
}

/// Probe S3: list the Athena `OutputLocation` bucket when configured,
/// otherwise confirm the endpoint answers HTTP at all.
pub async fn probe_s3(config: &GlauxConfig) -> Result<String, StartupError> {
    let label = s3_endpoint_label(config);
    if let Some(output_location) = &config.athena.output_location {
        let Some((bucket, prefix)) = parse_s3_uri(output_location) else {
            return Err(StartupError {
                endpoint: "s3",
                message: format!(
                    "Athena output location {output_location:?} is not an s3://bucket/prefix URI; \
                     fix --athena-output-location / GLAUX_ATHENA_OUTPUT_LOCATION"
                ),
            });
        };
        let storage = NetworkStorageBackend::new(config);
        let result =
            tokio::time::timeout(PROBE_TIMEOUT, storage.list_objects(bucket, prefix)).await;
        return match result {
            Ok(Ok(_)) => Ok(format!("listed s3://{bucket}/{prefix} at {label}")),
            Ok(Err(e)) => Err(StartupError {
                endpoint: "s3",
                message: format!(
                    "S3 endpoint {label} could not list the Athena output location \
                     s3://{bucket}/{prefix}: {e}. Check --s3-endpoint / GLAUX_S3_ENDPOINT, and \
                     create the bucket {bucket:?} there if it does not exist (e.g. `aws \
                     --endpoint-url <s3-endpoint> s3 mb s3://{bucket}`)"
                ),
            }),
            Err(_) => Err(StartupError {
                endpoint: "s3",
                message: format!(
                    "S3 endpoint {label} did not answer a ListObjects on s3://{bucket}/ within {}s. \
                     Check --s3-endpoint / GLAUX_S3_ENDPOINT and that the host is reachable",
                    PROBE_TIMEOUT.as_secs()
                ),
            }),
        };
    }

    let url = config
        .s3_endpoint
        .clone()
        .unwrap_or_else(|| format!("https://s3.{}.amazonaws.com/", config.region));
    let client = reqwest::Client::builder()
        .timeout(PROBE_TIMEOUT)
        .build()
        .map_err(|e| StartupError {
            endpoint: "s3",
            message: format!("could not build the S3 probe HTTP client: {e}"),
        })?;
    match client.get(&url).send().await {
        Ok(response) => Ok(format!(
            "reached {label} (HTTP {})",
            response.status().as_u16()
        )),
        Err(e) => Err(StartupError {
            endpoint: "s3",
            message: format!(
                "S3 endpoint {label} is not reachable: {e}. Check --s3-endpoint / \
                 GLAUX_S3_ENDPOINT (is the emulator running and listening there?)"
            ),
        }),
    }
}

/// Run every startup probe. Both endpoints are probed even if the first
/// fails, so one restart fixes every misconfiguration; errors are reported
/// together.
pub async fn validate(config: &GlauxConfig) -> Result<StartupReport, StartupError> {
    let (glue, s3) = tokio::join!(probe_glue(config), probe_s3(config));
    match (glue, s3) {
        (Ok(glue_databases), Ok(s3_probe)) => Ok(StartupReport {
            glue_databases,
            s3_probe,
        }),
        (Err(g), Err(s)) => Err(StartupError {
            endpoint: "glue",
            message: format!("{}\n{}", g.message, s.message),
        }),
        (Err(e), Ok(_)) | (Ok(_), Err(e)) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn s3_uri_parsing() {
        assert_eq!(
            parse_s3_uri("s3://results/athena/"),
            Some(("results", "athena/"))
        );
        assert_eq!(parse_s3_uri("s3://results"), Some(("results", "")));
        assert_eq!(parse_s3_uri("s3:///x"), None);
        assert_eq!(parse_s3_uri("http://results/"), None);
    }
}
