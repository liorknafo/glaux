//! AWS-shaped errors for the Firehose API surface.
//!
//! Every error maps onto the exception types real Firehose clients already
//! handle (`InvalidArgumentException`, `ResourceNotFoundException`,
//! `ResourceInUseException`, `LimitExceededException`,
//! `ServiceUnavailableException`, ...), carrying the AWS JSON 1.1 body
//! shape `{"__type": "...", "message": "..."}` plus the `x-amzn-ErrorType`
//! header.

use serde_json::{Value, json};

/// An error returned by a Firehose API action, already shaped like the AWS
/// exception clients expect.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum FirehoseError {
    /// `InvalidArgumentException`: a request parameter is invalid or names
    /// a configuration glaux does not support. The message always names the
    /// offending field or construct.
    #[error("InvalidArgumentException: {message}")]
    InvalidArgument {
        /// Human-readable message.
        message: String,
    },

    /// `ValidationException`: a parameter violates the API model's shape
    /// constraints (length, pattern, range). Real AWS raises this from the
    /// front door before the service sees the request.
    #[error("ValidationException: {message}")]
    Validation {
        /// Human-readable message.
        message: String,
    },

    /// `ResourceNotFoundException`: the named delivery stream does not exist.
    #[error("ResourceNotFoundException: {message}")]
    ResourceNotFound {
        /// Human-readable message.
        message: String,
    },

    /// `ResourceInUseException`: the stream already exists or is in a state
    /// that does not allow the action (e.g. `DELETING`).
    #[error("ResourceInUseException: {message}")]
    ResourceInUse {
        /// Human-readable message.
        message: String,
    },

    /// `LimitExceededException`: an account-level quota was hit.
    #[error("LimitExceededException: {message}")]
    LimitExceeded {
        /// Human-readable message.
        message: String,
    },

    /// `ConcurrentModificationException`: the `CurrentDeliveryStreamVersionId`
    /// passed to `UpdateDestination` is stale.
    #[error("ConcurrentModificationException: {message}")]
    ConcurrentModification {
        /// Human-readable message.
        message: String,
    },

    /// `ServiceUnavailableException`: the delivery sink refused the data, so
    /// the records could not be accepted. Never silently wrong: a flush
    /// failure is reported to the producer rather than swallowed.
    #[error("ServiceUnavailableException: {message}")]
    ServiceUnavailable {
        /// Human-readable message.
        message: String,
    },

    /// `UnknownOperationException`: the `X-Amz-Target` names an action this
    /// service does not implement.
    #[error("UnknownOperationException: unsupported Firehose action {action}")]
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

impl FirehoseError {
    /// Build an `InvalidArgumentException`.
    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self::InvalidArgument {
            message: message.into(),
        }
    }

    /// Build a `ValidationException`.
    pub fn validation(message: impl Into<String>) -> Self {
        Self::Validation {
            message: message.into(),
        }
    }

    /// Build the `ResourceNotFoundException` for a missing stream, worded
    /// like the real service.
    pub fn stream_not_found(name: &str, account_id: &str) -> Self {
        Self::ResourceNotFound {
            message: format!("Firehose {name} under account {account_id} not found."),
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
            Self::InvalidArgument { .. } => "InvalidArgumentException",
            Self::Validation { .. } => "ValidationException",
            Self::ResourceNotFound { .. } => "ResourceNotFoundException",
            Self::ResourceInUse { .. } => "ResourceInUseException",
            Self::LimitExceeded { .. } => "LimitExceededException",
            Self::ConcurrentModification { .. } => "ConcurrentModificationException",
            Self::ServiceUnavailable { .. } => "ServiceUnavailableException",
            Self::UnknownOperation { .. } => "UnknownOperationException",
            Self::Serialization { .. } => "SerializationException",
            Self::InternalServer { .. } => "InternalServerException",
        }
    }

    /// The HTTP status the AWS JSON protocol uses for this exception.
    pub fn http_status(&self) -> u16 {
        match self {
            Self::InvalidArgument { .. }
            | Self::Validation { .. }
            | Self::ResourceNotFound { .. }
            | Self::ResourceInUse { .. }
            | Self::LimitExceeded { .. }
            | Self::ConcurrentModification { .. }
            | Self::UnknownOperation { .. }
            | Self::Serialization { .. } => 400,
            Self::ServiceUnavailable { .. } => 503,
            Self::InternalServer { .. } => 500,
        }
    }

    /// The human-readable message.
    pub fn message(&self) -> String {
        match self {
            Self::InvalidArgument { message }
            | Self::Validation { message }
            | Self::ResourceNotFound { message }
            | Self::ResourceInUse { message }
            | Self::LimitExceeded { message }
            | Self::ConcurrentModification { message }
            | Self::ServiceUnavailable { message }
            | Self::Serialization { message }
            | Self::InternalServer { message } => message.clone(),
            Self::UnknownOperation { action } => {
                format!("unsupported Firehose action {action}: glaux does not implement it")
            }
        }
    }

    /// The AWS JSON 1.1 error body. Firehose spells the field `message`
    /// (lower-case), unlike Athena; SDKs accept either.
    pub fn to_json(&self) -> Value {
        json!({
            "__type": self.code(),
            "message": self.message(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_bodies_follow_the_aws_json_shape() {
        let err = FirehoseError::stream_not_found("orders", "000000000000");
        assert_eq!(err.http_status(), 400);
        assert_eq!(
            err.to_json(),
            json!({
                "__type": "ResourceNotFoundException",
                "message": "Firehose orders under account 000000000000 not found.",
            })
        );

        let err = FirehoseError::ServiceUnavailable {
            message: "sink down".into(),
        };
        assert_eq!(err.http_status(), 503);
        assert_eq!(err.code(), "ServiceUnavailableException");
    }
}
