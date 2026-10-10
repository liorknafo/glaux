//! Error types for the S3/Glue access layer.
//!
//! Every failure mode carries enough context to name the exact operation,
//! object, or configuration key involved — the "never silently wrong" rule
//! means errors here are part of the product surface, not an afterthought.

use std::path::PathBuf;

/// Errors produced by the glaux catalog access layer (config, S3, Glue).
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CatalogError {
    /// The config file could not be read from disk.
    #[error("failed to read config file {path}: {source}")]
    ConfigIo {
        /// Path of the config file that could not be read.
        path: PathBuf,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },

    /// The config file is not valid TOML (or contains unknown keys).
    #[error("invalid config file {path}: {message}")]
    ConfigParse {
        /// Path of the config file that failed to parse.
        path: PathBuf,
        /// TOML parser diagnostic, including the offending key/line.
        message: String,
    },

    /// An environment variable carried an unusable value.
    #[error("invalid value for environment variable {name}: {message}")]
    ConfigEnv {
        /// Name of the offending `GLAUX_*` variable.
        name: String,
        /// Why the value was rejected.
        message: String,
    },

    /// The merged configuration is internally inconsistent.
    #[error("invalid configuration: {0}")]
    ConfigInvalid(String),

    /// An S3 storage operation failed.
    #[error("S3 {operation} failed for s3://{bucket}/{key}: {source}")]
    Storage {
        /// The operation that failed (`get`, `get_range`, `put`, ...).
        operation: &'static str,
        /// Target bucket.
        bucket: String,
        /// Target key (empty for bucket-level operations such as list).
        key: String,
        /// Underlying object_store error (boxed: it is a large type).
        #[source]
        source: Box<object_store::Error>,
    },

    /// Constructing the object store client for a bucket failed.
    #[error("failed to build S3 client for bucket {bucket}: {source}")]
    StorageClient {
        /// Bucket the client was being built for.
        bucket: String,
        /// Underlying object_store error (boxed: it is a large type).
        #[source]
        source: Box<object_store::Error>,
    },

    /// The Glue endpoint returned an entity-not-found error
    /// (`EntityNotFoundException`).
    #[error("Glue entity not found: {message}")]
    GlueEntityNotFound {
        /// Message returned by the Glue endpoint.
        message: String,
    },

    /// The Glue endpoint returned a modeled API error.
    #[error("Glue API error {code}: {message}")]
    GlueApi {
        /// The Glue exception type, e.g. `InvalidInputException`.
        code: String,
        /// Message returned by the Glue endpoint.
        message: String,
    },

    /// The Glue HTTP client could not be constructed.
    #[error("failed to build Glue HTTP client: {source}")]
    GlueClient {
        /// Underlying HTTP client builder error.
        #[source]
        source: reqwest::Error,
    },

    /// The Glue request could not be sent or the response body not read.
    #[error("Glue request {action} to {endpoint} failed: {source}")]
    GlueTransport {
        /// The `X-Amz-Target` action, e.g. `GetTables`.
        action: &'static str,
        /// The endpoint the request was sent to.
        endpoint: String,
        /// Underlying HTTP client error.
        #[source]
        source: reqwest::Error,
    },

    /// The Glue response was not the JSON shape the action defines.
    #[error("Glue response for {action} could not be parsed: {message}")]
    GlueResponseParse {
        /// The `X-Amz-Target` action whose response failed to parse.
        action: &'static str,
        /// Parser diagnostic.
        message: String,
    },

    /// SigV4 signing of a request failed.
    #[error("failed to sign {service} request: {message}")]
    Signing {
        /// The AWS service the request was for (`glue`, `s3`).
        service: &'static str,
        /// Signer diagnostic.
        message: String,
    },
}

/// Convenience alias used throughout the crate.
pub type Result<T, E = CatalogError> = std::result::Result<T, E>;
