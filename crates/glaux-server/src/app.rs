//! The axum application: one `POST /` routed on `X-Amz-Target`, plus
//! `GET /health`.

use std::sync::Arc;
use std::time::Instant;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use glaux_athena::AthenaService;
use glaux_catalog::GlauxConfig;
use glaux_firehose::FirehoseService;
use serde_json::json;

/// Shared handler state.
#[derive(Clone)]
pub struct AppState {
    /// The Athena service.
    pub athena: Arc<AthenaService>,
    /// The Firehose service.
    pub firehose: Arc<FirehoseService>,
    /// Process start, for `uptime_seconds` in `/health`.
    pub started: Instant,
    /// Configured S3 endpoint, echoed by `/health` (`None` = real AWS).
    pub s3_endpoint: Option<String>,
    /// Configured Glue endpoint, echoed by `/health` (`None` = real AWS).
    pub glue_endpoint: Option<String>,
    /// Region, echoed by `/health`.
    pub region: String,
}

impl AppState {
    /// Assemble state from the wired services and the resolved config.
    pub fn new(
        athena: Arc<AthenaService>,
        firehose: Arc<FirehoseService>,
        config: &GlauxConfig,
    ) -> Self {
        Self {
            athena,
            firehose,
            started: Instant::now(),
            s3_endpoint: config.s3_endpoint.clone(),
            glue_endpoint: config.glue_endpoint.clone(),
            region: config.region.clone(),
        }
    }
}

/// `GET /health`: liveness plus what the server is configured against.
async fn health(State(state): State<AppState>) -> Response {
    let body = json!({
        "status": "ok",
        "service": "glaux-server",
        "version": env!("CARGO_PKG_VERSION"),
        "uptime_seconds": state.started.elapsed().as_secs(),
        "region": state.region,
        "s3_endpoint": state.s3_endpoint,
        "glue_endpoint": state.glue_endpoint,
        "services": {
            "athena": {
                "target_prefix": glaux_athena::http::TARGET_PREFIX,
                "actions": AthenaService::SUPPORTED_ACTIONS,
            },
            "firehose": {
                "target_prefix": glaux_firehose::http::TARGET_PREFIX,
                "actions": FirehoseService::SUPPORTED_ACTIONS,
            },
        },
    });
    (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        )],
        body.to_string(),
    )
        .into_response()
}

/// The AWS-shaped 400 for a target neither service owns.
fn unknown_target(target: Option<&str>) -> Response {
    let message = match target {
        Some(t) => format!(
            "X-Amz-Target {t:?} is not served by glaux-server: it serves {}* (Athena) and {}* \
             (Firehose) only",
            glaux_athena::http::TARGET_PREFIX,
            glaux_firehose::http::TARGET_PREFIX
        ),
        None => format!(
            "missing X-Amz-Target header: glaux-server expects AWS JSON 1.1 requests targeting \
             {}* (Athena) or {}* (Firehose)",
            glaux_athena::http::TARGET_PREFIX,
            glaux_firehose::http::TARGET_PREFIX
        ),
    };
    let body = json!({ "__type": "UnknownOperationException", "message": message });
    (
        StatusCode::BAD_REQUEST,
        [
            (
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/x-amz-json-1.1"),
            ),
            (
                header::HeaderName::from_static("x-amzn-errortype"),
                HeaderValue::from_static("UnknownOperationException"),
            ),
        ],
        body.to_string(),
    )
        .into_response()
}

/// `POST /`: route on the `X-Amz-Target` service prefix.
async fn dispatch(State(state): State<AppState>, headers: HeaderMap, body: Bytes) -> Response {
    let target = headers.get("x-amz-target").and_then(|v| v.to_str().ok());
    match target {
        Some(t) if t.starts_with(glaux_athena::http::TARGET_PREFIX) => {
            glaux_athena::http::dispatch(&state.athena, &headers, &body).await
        }
        Some(t) if t.starts_with(glaux_firehose::http::TARGET_PREFIX) => {
            glaux_firehose::http::dispatch(&state.firehose, &headers, &body).await
        }
        other => unknown_target(other),
    }
}

/// Build the router: `POST /` (both services) and `GET /health`. The body
/// limit is Firehose's, the larger of the two services' needs.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/", post(dispatch))
        .layer(DefaultBodyLimit::max(glaux_firehose::http::MAX_BODY_BYTES))
        .with_state(state)
}
