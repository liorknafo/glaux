//! Bridges from glaux's services to fakecloud's [`AwsService`] trait, so
//! they can be registered on fakecloud's [`ServiceRegistry`] under the
//! names its dispatcher routes `AmazonAthena.*` and `Firehose_20150804.*`
//! targets to — replacing fakecloud's own stubs of the same name.
//!
//! Both bridges hand the request to the engine crates' own
//! [`glaux_athena::http::dispatch`] / [`glaux_firehose::http::dispatch`]
//! functions and convert the resulting axum `Response` into an
//! [`AwsResponse`], so the wire behavior (status codes, `x-amzn-errortype`,
//! error bodies) is byte-for-byte what `glaux-server` produces.
//!
//! [`ServiceRegistry`]: fakecloud_core::registry::ServiceRegistry

use std::sync::Arc;

use async_trait::async_trait;
use axum::response::Response;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use glaux_athena::AthenaService;
use glaux_firehose::FirehoseService;
use http::{HeaderMap, StatusCode};

/// Registry name fakecloud routes `AmazonAthena.*` targets to.
pub const ATHENA_SERVICE_NAME: &str = "athena";
/// Registry name fakecloud routes `Firehose_*.*` targets to.
pub const FIREHOSE_SERVICE_NAME: &str = "firehose";

/// Convert an axum response into fakecloud's response type, preserving
/// status, content type, and every header the engine set.
async fn to_aws_response(response: Response) -> Result<AwsResponse, AwsServiceError> {
    let (parts, body) = response.into_parts();
    let bytes = axum::body::to_bytes(body, usize::MAX).await.map_err(|e| {
        AwsServiceError::aws_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalFailure",
            format!("failed to materialize the service response: {e}"),
        )
    })?;
    let mut headers: HeaderMap = parts.headers;
    let content_type = headers
        .remove(http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok().map(str::to_string))
        .unwrap_or_else(|| "application/x-amz-json-1.1".to_string());
    Ok(AwsResponse {
        status: parts.status,
        content_type,
        body: bytes.into(),
        headers,
    })
}

/// glaux Athena as a fakecloud service.
pub struct AthenaAwsService {
    inner: Arc<AthenaService>,
}

impl AthenaAwsService {
    /// Wrap a shared [`AthenaService`].
    pub fn new(inner: Arc<AthenaService>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl AwsService for AthenaAwsService {
    fn service_name(&self) -> &str {
        ATHENA_SERVICE_NAME
    }

    async fn handle(&self, request: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let response =
            glaux_athena::http::dispatch(&self.inner, &request.headers, &request.body).await;
        to_aws_response(response).await
    }

    fn supported_actions(&self) -> &[&str] {
        AthenaService::SUPPORTED_ACTIONS
    }
}

/// glaux Firehose as a fakecloud service.
pub struct FirehoseAwsService {
    inner: Arc<FirehoseService>,
}

impl FirehoseAwsService {
    /// Wrap a shared [`FirehoseService`].
    pub fn new(inner: Arc<FirehoseService>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl AwsService for FirehoseAwsService {
    fn service_name(&self) -> &str {
        FIREHOSE_SERVICE_NAME
    }

    async fn handle(&self, request: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let response =
            glaux_firehose::http::dispatch(&self.inner, &request.headers, &request.body).await;
        to_aws_response(response).await
    }

    fn supported_actions(&self) -> &[&str] {
        FirehoseService::SUPPORTED_ACTIONS
    }
}
