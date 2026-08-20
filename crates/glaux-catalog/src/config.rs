//! Unified glaux configuration: file (TOML) + environment (`GLAUX_*`) + CLI
//! overrides, with one precedence story for every binary.
//!
//! Precedence, lowest to highest: built-in defaults → config file → `GLAUX_*`
//! environment variables → [`ConfigOverrides`] (populated from CLI flags by
//! the binaries; this crate deliberately has no CLI-parser dependency).
//!
//! Credentials merge as a *group*: the highest-precedence source that
//! provides an access key wins wholesale, so a CLI access key is never
//! silently paired with an env secret key from a different identity.

use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::error::{CatalogError, Result};

/// Static AWS credentials passed through to S3 and Glue clients.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AwsCredentials {
    /// AWS access key ID.
    pub access_key_id: String,
    /// AWS secret access key.
    pub secret_access_key: String,
    /// Optional session token for temporary credentials.
    pub session_token: Option<String>,
}

/// Athena service defaults.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AthenaConfig {
    /// Default S3 `OutputLocation` (e.g. `s3://results/`) used when a query
    /// does not specify one.
    pub output_location: Option<String>,
    /// Default workgroup name.
    pub workgroup: String,
}

impl Default for AthenaConfig {
    fn default() -> Self {
        Self {
            output_location: None,
            workgroup: "primary".to_string(),
        }
    }
}

/// Firehose service limits, mirroring the real service's documented caps.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FirehoseLimits {
    /// Maximum size of a single record in KiB (`PutRecord`); AWS caps this
    /// at 1024 KiB.
    pub max_record_kib: u64,
    /// Maximum number of records in a `PutRecordBatch` call; AWS caps this
    /// at 500.
    pub max_batch_records: u64,
    /// Maximum total payload of a `PutRecordBatch` call in MiB; AWS caps
    /// this at 4 MiB.
    pub max_batch_mib: u64,
}

impl Default for FirehoseLimits {
    fn default() -> Self {
        Self {
            max_record_kib: 1024,
            max_batch_records: 500,
            max_batch_mib: 4,
        }
    }
}

/// The unified glaux configuration consumed by every service and binary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlauxConfig {
    /// S3 endpoint URL (e.g. `http://127.0.0.1:4566`). `None` means real AWS.
    pub s3_endpoint: Option<String>,
    /// Glue endpoint URL. `None` means real AWS
    /// (`https://glue.<region>.amazonaws.com`).
    pub glue_endpoint: Option<String>,
    /// AWS region advertised and used for SigV4 signing.
    pub region: String,
    /// AWS account ID used in generated ARNs.
    pub account_id: String,
    /// Static credentials passed through to S3/Glue clients. `None` falls
    /// back to the ambient AWS environment (`AWS_ACCESS_KEY_ID`, profiles),
    /// or to a local placeholder identity when a custom endpoint is set.
    pub credentials: Option<AwsCredentials>,
    /// Athena defaults.
    pub athena: AthenaConfig,
    /// Firehose limits.
    pub firehose: FirehoseLimits,
}

impl Default for GlauxConfig {
    fn default() -> Self {
        Self {
            s3_endpoint: None,
            glue_endpoint: None,
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            credentials: None,
            athena: AthenaConfig::default(),
            firehose: FirehoseLimits::default(),
        }
    }
}

/// Highest-precedence configuration values, populated by the binaries from
/// their CLI flags. Every field is optional; `None` leaves the lower-
/// precedence value in place.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConfigOverrides {
    /// Override for [`GlauxConfig::s3_endpoint`].
    pub s3_endpoint: Option<String>,
    /// Override for [`GlauxConfig::glue_endpoint`].
    pub glue_endpoint: Option<String>,
    /// Override for [`GlauxConfig::region`].
    pub region: Option<String>,
    /// Override for [`GlauxConfig::account_id`].
    pub account_id: Option<String>,
    /// Override for [`GlauxConfig::credentials`] (wins as a group).
    pub credentials: Option<AwsCredentials>,
    /// Override for [`AthenaConfig::output_location`].
    pub athena_output_location: Option<String>,
    /// Override for [`AthenaConfig::workgroup`].
    pub athena_workgroup: Option<String>,
    /// Override for [`FirehoseLimits::max_record_kib`].
    pub firehose_max_record_kib: Option<u64>,
    /// Override for [`FirehoseLimits::max_batch_records`].
    pub firehose_max_batch_records: Option<u64>,
    /// Override for [`FirehoseLimits::max_batch_mib`].
    pub firehose_max_batch_mib: Option<u64>,
}

// ---------------------------------------------------------------------------
// TOML file shape. `deny_unknown_fields` everywhere: a typo in a config file
// must be an explicit error, never a silently ignored setting.
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    s3_endpoint: Option<String>,
    glue_endpoint: Option<String>,
    region: Option<String>,
    account_id: Option<String>,
    credentials: Option<FileCredentials>,
    athena: Option<FileAthena>,
    firehose: Option<FileFirehose>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileCredentials {
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileAthena {
    output_location: Option<String>,
    workgroup: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileFirehose {
    max_record_kib: Option<u64>,
    max_batch_records: Option<u64>,
    max_batch_mib: Option<u64>,
}

/// Names of all recognized `GLAUX_*` environment variables.
pub const ENV_VARS: &[&str] = &[
    "GLAUX_S3_ENDPOINT",
    "GLAUX_GLUE_ENDPOINT",
    "GLAUX_REGION",
    "GLAUX_ACCOUNT_ID",
    "GLAUX_ACCESS_KEY_ID",
    "GLAUX_SECRET_ACCESS_KEY",
    "GLAUX_SESSION_TOKEN",
    "GLAUX_ATHENA_OUTPUT_LOCATION",
    "GLAUX_ATHENA_WORKGROUP",
    "GLAUX_FIREHOSE_MAX_RECORD_KIB",
    "GLAUX_FIREHOSE_MAX_BATCH_RECORDS",
    "GLAUX_FIREHOSE_MAX_BATCH_MIB",
];

impl GlauxConfig {
    /// Load configuration from an optional TOML file, the process
    /// environment (`GLAUX_*`), and CLI overrides, in ascending precedence.
    ///
    /// A `file` that is passed explicitly but missing or malformed is an
    /// error — a requested config file is never silently skipped.
    pub fn load(file: Option<&Path>, overrides: &ConfigOverrides) -> Result<Self> {
        let file_source = match file {
            Some(path) => {
                let contents =
                    std::fs::read_to_string(path).map_err(|source| CatalogError::ConfigIo {
                        path: path.to_path_buf(),
                        source,
                    })?;
                Some((path.to_path_buf(), contents))
            }
            None => None,
        };
        Self::resolve(file_source, &|name| std::env::var(name).ok(), overrides)
    }

    /// Pure resolution over explicit sources; the seam used by unit tests.
    fn resolve(
        file_source: Option<(PathBuf, String)>,
        env: &dyn Fn(&str) -> Option<String>,
        overrides: &ConfigOverrides,
    ) -> Result<Self> {
        let mut config = Self::default();

        // 1. Config file.
        if let Some((path, contents)) = file_source {
            let file: FileConfig =
                toml::from_str(&contents).map_err(|e| CatalogError::ConfigParse {
                    path,
                    message: e.to_string(),
                })?;
            merge_opt(&mut config.s3_endpoint, file.s3_endpoint.map(Some));
            merge_opt(&mut config.glue_endpoint, file.glue_endpoint.map(Some));
            merge(&mut config.region, file.region);
            merge(&mut config.account_id, file.account_id);
            if let Some(c) = file.credentials {
                config.credentials = Some(AwsCredentials {
                    access_key_id: c.access_key_id,
                    secret_access_key: c.secret_access_key,
                    session_token: c.session_token,
                });
            }
            if let Some(a) = file.athena {
                merge_opt(&mut config.athena.output_location, a.output_location.map(Some));
                merge(&mut config.athena.workgroup, a.workgroup);
            }
            if let Some(f) = file.firehose {
                merge(&mut config.firehose.max_record_kib, f.max_record_kib);
                merge(&mut config.firehose.max_batch_records, f.max_batch_records);
                merge(&mut config.firehose.max_batch_mib, f.max_batch_mib);
            }
        }

        // 2. Environment.
        merge_opt(&mut config.s3_endpoint, env("GLAUX_S3_ENDPOINT").map(Some));
        merge_opt(&mut config.glue_endpoint, env("GLAUX_GLUE_ENDPOINT").map(Some));
        merge(&mut config.region, env("GLAUX_REGION"));
        merge(&mut config.account_id, env("GLAUX_ACCOUNT_ID"));
        match (env("GLAUX_ACCESS_KEY_ID"), env("GLAUX_SECRET_ACCESS_KEY")) {
            (Some(ak), Some(sk)) => {
                config.credentials = Some(AwsCredentials {
                    access_key_id: ak,
                    secret_access_key: sk,
                    session_token: env("GLAUX_SESSION_TOKEN"),
                });
            }
            (None, None) => {}
            (Some(_), None) => {
                return Err(CatalogError::ConfigInvalid(
                    "GLAUX_ACCESS_KEY_ID is set but GLAUX_SECRET_ACCESS_KEY is not; \
                     credentials must be provided as a pair"
                        .to_string(),
                ));
            }
            (None, Some(_)) => {
                return Err(CatalogError::ConfigInvalid(
                    "GLAUX_SECRET_ACCESS_KEY is set but GLAUX_ACCESS_KEY_ID is not; \
                     credentials must be provided as a pair"
                        .to_string(),
                ));
            }
        }
        merge_opt(
            &mut config.athena.output_location,
            env("GLAUX_ATHENA_OUTPUT_LOCATION").map(Some),
        );
        merge(&mut config.athena.workgroup, env("GLAUX_ATHENA_WORKGROUP"));
        merge(
            &mut config.firehose.max_record_kib,
            parse_env_u64(env, "GLAUX_FIREHOSE_MAX_RECORD_KIB")?,
        );
        merge(
            &mut config.firehose.max_batch_records,
            parse_env_u64(env, "GLAUX_FIREHOSE_MAX_BATCH_RECORDS")?,
        );
        merge(
            &mut config.firehose.max_batch_mib,
            parse_env_u64(env, "GLAUX_FIREHOSE_MAX_BATCH_MIB")?,
        );

        // 3. CLI overrides.
        merge_opt(&mut config.s3_endpoint, overrides.s3_endpoint.clone().map(Some));
        merge_opt(
            &mut config.glue_endpoint,
            overrides.glue_endpoint.clone().map(Some),
        );
        merge(&mut config.region, overrides.region.clone());
        merge(&mut config.account_id, overrides.account_id.clone());
        if let Some(c) = &overrides.credentials {
            config.credentials = Some(c.clone());
        }
        merge_opt(
            &mut config.athena.output_location,
            overrides.athena_output_location.clone().map(Some),
        );
        merge(
            &mut config.athena.workgroup,
            overrides.athena_workgroup.clone(),
        );
        merge(
            &mut config.firehose.max_record_kib,
            overrides.firehose_max_record_kib,
        );
        merge(
            &mut config.firehose.max_batch_records,
            overrides.firehose_max_batch_records,
        );
        merge(
            &mut config.firehose.max_batch_mib,
            overrides.firehose_max_batch_mib,
        );

        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        for (name, value) in [
            ("s3_endpoint", &self.s3_endpoint),
            ("glue_endpoint", &self.glue_endpoint),
        ] {
            if let Some(url) = value
                && !(url.starts_with("http://") || url.starts_with("https://"))
            {
                return Err(CatalogError::ConfigInvalid(format!(
                    "{name} must be an http:// or https:// URL, got {url:?}"
                )));
            }
        }
        if self.region.is_empty() {
            return Err(CatalogError::ConfigInvalid(
                "region must not be empty".to_string(),
            ));
        }
        if self.account_id.is_empty() {
            return Err(CatalogError::ConfigInvalid(
                "account_id must not be empty".to_string(),
            ));
        }
        for (name, value) in [
            ("firehose.max_record_kib", self.firehose.max_record_kib),
            ("firehose.max_batch_records", self.firehose.max_batch_records),
            ("firehose.max_batch_mib", self.firehose.max_batch_mib),
        ] {
            if value == 0 {
                return Err(CatalogError::ConfigInvalid(format!(
                    "{name} must be greater than zero"
                )));
            }
        }
        Ok(())
    }
}

/// Replace `dst` when the source provides a value.
fn merge<T>(dst: &mut T, src: Option<T>) {
    if let Some(v) = src {
        *dst = v;
    }
}

/// Replace an optional `dst` when the source provides a value.
fn merge_opt<T>(dst: &mut Option<T>, src: Option<Option<T>>) {
    if let Some(v) = src {
        *dst = v;
    }
}

fn parse_env_u64(env: &dyn Fn(&str) -> Option<String>, name: &str) -> Result<Option<u64>> {
    match env(name) {
        None => Ok(None),
        Some(raw) => raw
            .parse::<u64>()
            .map(Some)
            .map_err(|e| CatalogError::ConfigEnv {
                name: name.to_string(),
                message: format!("expected an unsigned integer, got {raw:?}: {e}"),
            }),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn no_env(_: &str) -> Option<String> {
        None
    }

    fn env_map(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |name: &str| map.get(name).cloned()
    }

    #[test]
    fn defaults_when_no_sources() {
        let config =
            GlauxConfig::resolve(None, &no_env, &ConfigOverrides::default()).expect("defaults");
        assert_eq!(config, GlauxConfig::default());
        assert_eq!(config.region, "us-east-1");
        assert_eq!(config.athena.workgroup, "primary");
        assert_eq!(config.firehose.max_record_kib, 1024);
        assert_eq!(config.firehose.max_batch_records, 500);
        assert_eq!(config.firehose.max_batch_mib, 4);
    }

    #[test]
    fn file_values_override_defaults() {
        let toml = r#"
            s3_endpoint = "http://127.0.0.1:4566"
            glue_endpoint = "http://127.0.0.1:4566"
            region = "eu-west-1"
            account_id = "000000000000"

            [credentials]
            access_key_id = "AKIDFILE"
            secret_access_key = "file-secret"

            [athena]
            output_location = "s3://results/"
            workgroup = "analytics"

            [firehose]
            max_record_kib = 512
        "#;
        let config = GlauxConfig::resolve(
            Some((PathBuf::from("glaux.toml"), toml.to_string())),
            &no_env,
            &ConfigOverrides::default(),
        )
        .expect("file config");
        assert_eq!(config.s3_endpoint.as_deref(), Some("http://127.0.0.1:4566"));
        assert_eq!(config.region, "eu-west-1");
        assert_eq!(config.account_id, "000000000000");
        let creds = config.credentials.expect("credentials");
        assert_eq!(creds.access_key_id, "AKIDFILE");
        assert_eq!(creds.session_token, None);
        assert_eq!(config.athena.output_location.as_deref(), Some("s3://results/"));
        assert_eq!(config.athena.workgroup, "analytics");
        assert_eq!(config.firehose.max_record_kib, 512);
        // Untouched limits keep defaults.
        assert_eq!(config.firehose.max_batch_records, 500);
    }

    #[test]
    fn env_overrides_file_and_cli_overrides_env() {
        let toml = r#"
            region = "eu-west-1"
            s3_endpoint = "http://file:1"
        "#;
        let env = env_map(&[
            ("GLAUX_REGION", "ap-southeast-2"),
            ("GLAUX_S3_ENDPOINT", "http://env:2"),
            ("GLAUX_GLUE_ENDPOINT", "http://env:2"),
        ]);
        let overrides = ConfigOverrides {
            s3_endpoint: Some("http://cli:3".to_string()),
            ..Default::default()
        };
        let config = GlauxConfig::resolve(
            Some((PathBuf::from("glaux.toml"), toml.to_string())),
            &env,
            &overrides,
        )
        .expect("layered config");
        // CLI beats env beats file.
        assert_eq!(config.s3_endpoint.as_deref(), Some("http://cli:3"));
        // Env beats file.
        assert_eq!(config.region, "ap-southeast-2");
        // Env beats default.
        assert_eq!(config.glue_endpoint.as_deref(), Some("http://env:2"));
    }

    #[test]
    fn credentials_merge_as_a_group() {
        let toml = r#"
            [credentials]
            access_key_id = "AKIDFILE"
            secret_access_key = "file-secret"
            session_token = "file-token"
        "#;
        let env = env_map(&[
            ("GLAUX_ACCESS_KEY_ID", "AKIDENV"),
            ("GLAUX_SECRET_ACCESS_KEY", "env-secret"),
        ]);
        let config = GlauxConfig::resolve(
            Some((PathBuf::from("glaux.toml"), toml.to_string())),
            &env,
            &ConfigOverrides::default(),
        )
        .expect("config");
        let creds = config.credentials.expect("credentials");
        // The env group wins wholesale: no file session token leaks through.
        assert_eq!(creds.access_key_id, "AKIDENV");
        assert_eq!(creds.secret_access_key, "env-secret");
        assert_eq!(creds.session_token, None);
    }

    #[test]
    fn partial_env_credentials_error_explicitly() {
        let env = env_map(&[("GLAUX_ACCESS_KEY_ID", "AKIDENV")]);
        let err = GlauxConfig::resolve(None, &env, &ConfigOverrides::default())
            .expect_err("partial credentials must fail");
        let msg = err.to_string();
        assert!(msg.contains("GLAUX_SECRET_ACCESS_KEY"), "got: {msg}");
    }

    #[test]
    fn unknown_file_key_errors_with_path() {
        let toml = "s3_endpint = \"http://oops:1\"\n";
        let err = GlauxConfig::resolve(
            Some((PathBuf::from("/etc/glaux.toml"), toml.to_string())),
            &no_env,
            &ConfigOverrides::default(),
        )
        .expect_err("typo key must fail");
        let msg = err.to_string();
        assert!(msg.contains("/etc/glaux.toml"), "got: {msg}");
        assert!(msg.contains("s3_endpint"), "got: {msg}");
    }

    #[test]
    fn invalid_env_number_names_the_variable() {
        let env = env_map(&[("GLAUX_FIREHOSE_MAX_RECORD_KIB", "lots")]);
        let err = GlauxConfig::resolve(None, &env, &ConfigOverrides::default())
            .expect_err("bad number must fail");
        let msg = err.to_string();
        assert!(msg.contains("GLAUX_FIREHOSE_MAX_RECORD_KIB"), "got: {msg}");
        assert!(msg.contains("lots"), "got: {msg}");
    }

    #[test]
    fn non_http_endpoint_rejected() {
        let overrides = ConfigOverrides {
            glue_endpoint: Some("127.0.0.1:4566".to_string()),
            ..Default::default()
        };
        let err = GlauxConfig::resolve(None, &no_env, &overrides)
            .expect_err("bare host:port must fail");
        assert!(err.to_string().contains("glue_endpoint"), "got: {err}");
    }

    #[test]
    fn zero_firehose_limit_rejected() {
        let overrides = ConfigOverrides {
            firehose_max_batch_records: Some(0),
            ..Default::default()
        };
        let err = GlauxConfig::resolve(None, &no_env, &overrides).expect_err("zero must fail");
        assert!(
            err.to_string().contains("firehose.max_batch_records"),
            "got: {err}"
        );
    }

    #[test]
    fn load_with_missing_explicit_file_errors() {
        let err = GlauxConfig::load(
            Some(Path::new("/nonexistent/glaux.toml")),
            &ConfigOverrides::default(),
        )
        .expect_err("missing file must fail");
        assert!(matches!(err, CatalogError::ConfigIo { .. }), "got: {err}");
    }
}
