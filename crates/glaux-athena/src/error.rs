//! AWS-shaped errors for the Athena API surface.
//!
//! Every error maps onto the exception types real Athena clients already
//! handle (`InvalidRequestException`, `ResourceNotFoundException`,
//! `InternalServerException`, ...), carrying the same JSON body shape the
//! AWS JSON 1.1 protocol uses: `{"__type": "...", "Message": "..."}` plus the
//! `x-amzn-ErrorType` header.

use serde_json::{Value, json};

/// An error returned by an Athena API action, already shaped like the AWS
/// exception clients expect.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum AthenaError {
    /// `InvalidRequestException`: the request is malformed or refers to a
    /// query/workgroup in a state that does not allow the action.
    #[error("InvalidRequestException: {message}")]
    InvalidRequest {
        /// Human-readable message naming the offending field or state.
        message: String,
        /// Athena's finer-grained `AthenaErrorCode`, when one applies
        /// (e.g. `INVALID_NEXT_TOKEN`).
        athena_error_code: Option<String>,
    },

    /// `ResourceNotFoundException`: a named workgroup does not exist.
    #[error("ResourceNotFoundException: {message}")]
    ResourceNotFound {
        /// Human-readable message.
        message: String,
        /// The resource that was not found.
        resource_name: String,
    },

    /// `UnknownOperationException`: the `X-Amz-Target` names an action this
    /// service does not implement. Never silently wrong: unimplemented
    /// actions error explicitly instead of returning an empty success.
    #[error("UnknownOperationException: unsupported Athena action {action}")]
    UnknownOperation {
        /// The requested action name.
        action: String,
    },

    /// `SerializationException`: the request body is not valid JSON for the
    /// action's input shape.
    #[error("SerializationException: {message}")]
    Serialization {
        /// Parser diagnostic naming the field or syntax problem.
        message: String,
    },

    /// `InternalServerException`: the service itself failed.
    #[error("InternalServerException: {message}")]
    InternalServer {
        /// Diagnostic.
        message: String,
    },
}

impl AthenaError {
    /// Build an `InvalidRequestException` with no `AthenaErrorCode`.
    pub fn invalid_request(message: impl Into<String>) -> Self {
        Self::InvalidRequest {
            message: message.into(),
            athena_error_code: None,
        }
    }

    /// Build an `InvalidRequestException` carrying an `AthenaErrorCode`.
    pub fn invalid_request_with_code(message: impl Into<String>, code: impl Into<String>) -> Self {
        Self::InvalidRequest {
            message: message.into(),
            athena_error_code: Some(code.into()),
        }
    }

    /// Build an `InternalServerException`.
    pub fn internal(message: impl Into<String>) -> Self {
        Self::InternalServer {
            message: message.into(),
        }
    }

    /// The AWS exception type name, as sent in `__type` and
    /// `x-amzn-ErrorType`.
    pub fn code(&self) -> &'static str {
        match self {
            Self::InvalidRequest { .. } => "InvalidRequestException",
            Self::ResourceNotFound { .. } => "ResourceNotFoundException",
            Self::UnknownOperation { .. } => "UnknownOperationException",
            Self::Serialization { .. } => "SerializationException",
            Self::InternalServer { .. } => "InternalServerException",
        }
    }

    /// The HTTP status the AWS JSON protocol uses for this exception.
    pub fn http_status(&self) -> u16 {
        match self {
            Self::InvalidRequest { .. }
            | Self::ResourceNotFound { .. }
            | Self::UnknownOperation { .. }
            | Self::Serialization { .. } => 400,
            Self::InternalServer { .. } => 500,
        }
    }

    /// The human-readable message (the `Message` field).
    pub fn message(&self) -> String {
        match self {
            Self::InvalidRequest { message, .. }
            | Self::ResourceNotFound { message, .. }
            | Self::Serialization { message }
            | Self::InternalServer { message } => message.clone(),
            Self::UnknownOperation { action } => {
                format!("unsupported Athena action {action}: glaux does not implement it")
            }
        }
    }

    /// The AWS JSON 1.1 error body.
    pub fn to_json(&self) -> Value {
        let mut body = json!({
            "__type": self.code(),
            "Message": self.message(),
        });
        match self {
            Self::InvalidRequest {
                athena_error_code: Some(code),
                ..
            } => {
                body["AthenaErrorCode"] = Value::String(code.clone());
            }
            Self::ResourceNotFound { resource_name, .. } => {
                body["ResourceName"] = Value::String(resource_name.clone());
            }
            _ => {}
        }
        body
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_bodies_follow_the_aws_json_shape() {
        let err = AthenaError::invalid_request_with_code("bad token", "INVALID_NEXT_TOKEN");
        assert_eq!(err.http_status(), 400);
        assert_eq!(
            err.to_json(),
            json!({
                "__type": "InvalidRequestException",
                "Message": "bad token",
                "AthenaErrorCode": "INVALID_NEXT_TOKEN",
            })
        );

        let err = AthenaError::internal("boom");
        assert_eq!(err.http_status(), 500);
        assert_eq!(err.to_json()["__type"], "InternalServerException");
    }
}
