//! Command-line surface. Flags are the highest-precedence configuration
//! source; they map one-to-one onto [`ConfigOverrides`] so the precedence
//! story (defaults → file → `GLAUX_*` env → flags) lives in one place,
//! [`glaux_catalog::GlauxConfig`].

use std::path::PathBuf;

use clap::Parser;
use glaux_catalog::{AwsCredentials, ConfigOverrides};

/// Default listen address. Distinct from fakecloud's 4566 so both can run
/// side by side on one host.
pub const DEFAULT_LISTEN: &str = "127.0.0.1:4570";

/// glaux-server: real Athena queries and real Firehose delivery over HTTP,
/// against the S3 and Glue endpoints you point it at.
#[derive(Debug, Parser, Clone)]
#[command(name = "glaux-server", version, about, long_about = None)]
pub struct Cli {
    /// Address to listen on.
    #[arg(long, env = "GLAUX_LISTEN", default_value = DEFAULT_LISTEN)]
    pub listen: String,

    /// TOML configuration file. Values in it are overridden by GLAUX_*
    /// environment variables and then by flags.
    #[arg(long, value_name = "PATH")]
    pub config: Option<PathBuf>,

    /// S3 endpoint URL (e.g. http://127.0.0.1:4566). Required unless --aws.
    #[arg(long, value_name = "URL")]
    pub s3_endpoint: Option<String>,

    /// Glue endpoint URL (e.g. http://127.0.0.1:4566). Required unless --aws.
    #[arg(long, value_name = "URL")]
    pub glue_endpoint: Option<String>,

    /// Use real AWS for any endpoint left unset (no emulator involved).
    #[arg(long)]
    pub aws: bool,

    /// AWS region used for SigV4 signing and in generated ARNs.
    #[arg(long)]
    pub region: Option<String>,

    /// AWS account ID used in generated ARNs.
    #[arg(long)]
    pub account_id: Option<String>,

    /// Static access key ID for S3/Glue (pair with --secret-access-key).
    #[arg(long, requires = "secret_access_key")]
    pub access_key_id: Option<String>,

    /// Static secret access key for S3/Glue (pair with --access-key-id).
    #[arg(long, requires = "access_key_id")]
    pub secret_access_key: Option<String>,

    /// Session token for temporary credentials.
    #[arg(long, requires = "access_key_id")]
    pub session_token: Option<String>,

    /// Default Athena OutputLocation (e.g. s3://results/) for the default
    /// workgroup.
    #[arg(long, value_name = "S3_URI")]
    pub athena_output_location: Option<String>,

    /// Default Athena workgroup name.
    #[arg(long)]
    pub athena_workgroup: Option<String>,

    /// Firehose: maximum size of one record, in KiB (AWS: 1024).
    #[arg(long, value_name = "KIB")]
    pub firehose_max_record_kib: Option<u64>,

    /// Firehose: maximum records per PutRecordBatch (AWS: 500).
    #[arg(long, value_name = "N")]
    pub firehose_max_batch_records: Option<u64>,

    /// Firehose: maximum PutRecordBatch payload, in MiB (AWS: 4).
    #[arg(long, value_name = "MIB")]
    pub firehose_max_batch_mib: Option<u64>,
}

impl Cli {
    /// The flag values as configuration overrides.
    pub fn overrides(&self) -> ConfigOverrides {
        let credentials = match (&self.access_key_id, &self.secret_access_key) {
            (Some(access_key_id), Some(secret_access_key)) => Some(AwsCredentials {
                access_key_id: access_key_id.clone(),
                secret_access_key: secret_access_key.clone(),
                session_token: self.session_token.clone(),
            }),
            _ => None,
        };
        ConfigOverrides {
            s3_endpoint: self.s3_endpoint.clone(),
            glue_endpoint: self.glue_endpoint.clone(),
            region: self.region.clone(),
            account_id: self.account_id.clone(),
            credentials,
            athena_output_location: self.athena_output_location.clone(),
            athena_workgroup: self.athena_workgroup.clone(),
            firehose_max_record_kib: self.firehose_max_record_kib,
            firehose_max_batch_records: self.firehose_max_batch_records,
            firehose_max_batch_mib: self.firehose_max_batch_mib,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flags_map_onto_overrides() {
        let cli = Cli::try_parse_from([
            "glaux-server",
            "--s3-endpoint",
            "http://s3:1",
            "--glue-endpoint",
            "http://glue:2",
            "--region",
            "eu-west-1",
            "--access-key-id",
            "AK",
            "--secret-access-key",
            "SK",
            "--athena-output-location",
            "s3://results/",
            "--firehose-max-batch-records",
            "7",
        ])
        .unwrap();
        let o = cli.overrides();
        assert_eq!(o.s3_endpoint.as_deref(), Some("http://s3:1"));
        assert_eq!(o.glue_endpoint.as_deref(), Some("http://glue:2"));
        assert_eq!(o.region.as_deref(), Some("eu-west-1"));
        let creds = o.credentials.unwrap();
        assert_eq!(creds.access_key_id, "AK");
        assert_eq!(creds.secret_access_key, "SK");
        assert_eq!(creds.session_token, None);
        assert_eq!(o.athena_output_location.as_deref(), Some("s3://results/"));
        assert_eq!(o.firehose_max_batch_records, Some(7));
        assert_eq!(o.firehose_max_record_kib, None);
        assert_eq!(cli.listen, DEFAULT_LISTEN);
        assert!(!cli.aws);
    }

    #[test]
    fn half_a_credential_pair_is_rejected_by_the_parser() {
        let err = Cli::try_parse_from(["glaux-server", "--access-key-id", "AK"]).unwrap_err();
        assert!(
            err.to_string().contains("--secret-access-key"),
            "got: {err}"
        );
    }

    #[test]
    fn verify_cli_definition() {
        use clap::CommandFactory;
        Cli::command().debug_assert();
    }
}
