//! axum transport for the Firehose service: the AWS JSON 1.1 protocol
//! (`POST /` with `X-Amz-Target: Firehose_20150804.<Action>`).
//!
//! Two mounting styles are supported so the standalone server and the
//! all-in-one binary can each pick theirs:
//!
//! - [`router`] — a complete `Router` that answers `POST /`.
//! - [`dispatch`] — the request → response function behind it, for hosts
//!   that already route on `X-Amz-Target` themselves.

use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::extract::rejection::BytesRejection;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use serde_json::Value;

use crate::error::FirehoseError;
use crate::service::FirehoseService;

/// The `X-Amz-Target` service prefix.
pub const TARGET_PREFIX: &str = "Firehose_20150804.";

const CONTENT_TYPE_JSON_1_1: &str = "application/x-amz-json-1.1";

/// Parse `X-Amz-Target` into the bare action name.
pub fn action_from_target(headers: &HeaderMap) -> Result<String, FirehoseError> {
    let target = headers
        .get("x-amz-target")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| FirehoseError::UnknownOperation {
            action: "<missing X-Amz-Target header>".to_string(),
        })?;
    target
        .strip_prefix(TARGET_PREFIX)
        .map(str::to_string)
        .ok_or_else(|| FirehoseError::UnknownOperation {
            action: target.to_string(),
        })
}

fn json_response(status: StatusCode, body: &Value) -> Response {
    let request_id = uuid::Uuid::new_v4().to_string();
    let mut response = (status, body.to_string()).into_response();
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(CONTENT_TYPE_JSON_1_1),
    );
    if let Ok(value) = HeaderValue::from_str(&request_id) {
        headers.insert("x-amzn-requestid", value);
    }
    response
}

/// Render a [`FirehoseError`] as the AWS JSON 1.1 error response.
pub fn error_response(err: &FirehoseError) -> Response {
    let status = StatusCode::from_u16(err.http_status()).unwrap_or(StatusCode::BAD_REQUEST);
    let mut response = json_response(status, &err.to_json());
    response
        .headers_mut()
        .insert("x-amzn-errortype", HeaderValue::from_static(err.code()));
    response
}

/// Handle one AWS JSON 1.1 request: `headers` carry `X-Amz-Target`, `body`
/// is the JSON payload. Always produces a well-formed AWS response.
pub async fn dispatch(service: &FirehoseService, headers: &HeaderMap, body: &[u8]) -> Response {
    let action = match action_from_target(headers) {
        Ok(action) => action,
        Err(err) => return error_response(&err),
    };
    match service.handle(&action, body).await {
        Ok(value) => json_response(StatusCode::OK, &value),
        Err(err) => {
            // 5xx means glaux itself failed or the sink is unavailable —
            // that must be visible at default log levels, unlike the 4xx
            // refusals that are a normal part of the API contract.
            if err.http_status() >= 500 {
                tracing::warn!(action, error = %err, "firehose request failed");
            } else {
                tracing::debug!(action, error = %err, "firehose request failed");
            }
            error_response(&err)
        }
    }
}

async fn root(
    State(service): State<Arc<FirehoseService>>,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    match body {
        Ok(body) => dispatch(&service, &headers, &body).await,
        Err(rejection) => error_response(&body_rejection_error(&rejection)),
    }
}

/// Translate axum's body rejection into an AWS-shaped error.
///
/// `DefaultBodyLimit` rejects the `Bytes` extractor before [`dispatch`] is
/// reached, so without this the client would get axum's plain-text `413`,
/// which no AWS JSON 1.1 SDK can parse into an error type.
fn body_rejection_error(rejection: &BytesRejection) -> FirehoseError {
    if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE {
        FirehoseError::validation(format!(
            "request body exceeds the {MAX_BODY_BYTES} byte transport limit: a PutRecordBatch \
             request carrying the maximum 4 MB of records is about 5.4 MB once base64-encoded"
        ))
    } else {
        FirehoseError::validation(format!(
            "request body could not be read: {}",
            rejection.body_text()
        ))
    }
}

/// Request body cap for the router. A full 4 MiB `PutRecordBatch` is about
/// 5.4 MiB once base64-encoded into JSON; 8 MiB leaves room for framing
/// while still refusing runaway bodies. Oversized batches are then rejected
/// by the service with the AWS-shaped limit error, not by the transport.
pub const MAX_BODY_BYTES: usize = 8 * 1024 * 1024;

/// An axum router serving the Firehose API at `POST /`.
pub fn router(service: Arc<FirehoseService>) -> Router {
    Router::new()
        .route("/", post(root))
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .with_state(service)
}
