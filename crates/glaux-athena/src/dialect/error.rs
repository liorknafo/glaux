//! Errors raised by the Trino dialect layer.
//!
//! Every variant names the construct that could not be translated so the
//! client learns *what* glaux refused rather than getting a wrong answer
//! (the product's "never silently wrong" rule). The errors convert into the
//! engine-level [`EngineError`] (which the query lifecycle surfaces as a
//! `FAILED` query with Athena error details) and into
//! [`AthenaError::InvalidRequest`] for synchronous API surfaces.

use crate::engine::EngineError;
use crate::error::AthenaError;

/// Why a Trino-dialect statement could not be translated for DataFusion.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum GlauxSqlError {
    /// The text is not a valid Trino/Athena statement.
    #[error("SYNTAX_ERROR: {message}")]
    Parse {
        /// The parser's diagnostic.
        message: String,
    },

    /// A construct glaux recognises but deliberately does not translate.
    #[error("NOT_SUPPORTED: {construct} is not supported: {message}")]
    Unsupported {
        /// The construct, e.g. `lambda expression` or `function try`.
        construct: String,
        /// Why, and what to do instead when there is an alternative.
        message: String,
    },

    /// A function name that is neither in glaux's Trino coverage table nor
    /// a translation target. Unknown names are refused up front rather than
    /// handed to DataFusion, whose same-named function might have
    /// different semantics.
    #[error(
        "FUNCTION_NOT_FOUND: function {name} is not in glaux's Trino coverage table \
         (see docs/sql-coverage.md)"
    )]
    UnknownFunction {
        /// The function name as written (lower-cased).
        name: String,
    },

    /// A function was called with an argument shape glaux cannot translate
    /// (wrong arity, a non-literal where a literal is required, ...).
    #[error("INVALID_FUNCTION_ARGUMENT: {function}: {message}")]
    InvalidArguments {
        /// The function name.
        function: String,
        /// What was wrong with the call.
        message: String,
    },
}

impl GlauxSqlError {
    /// Build an [`GlauxSqlError::Unsupported`].
    pub fn unsupported(construct: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Unsupported {
            construct: construct.into(),
            message: message.into(),
        }
    }

    /// Build an [`GlauxSqlError::InvalidArguments`].
    pub fn invalid_arguments(function: impl Into<String>, message: impl Into<String>) -> Self {
        Self::InvalidArguments {
            function: function.into(),
            message: message.into(),
        }
    }

    /// The construct this error names: the function or syntax element the
    /// client should look for in the coverage table.
    pub fn construct(&self) -> Option<&str> {
        match self {
            Self::Parse { .. } => None,
            Self::Unsupported { construct, .. } => Some(construct),
            Self::UnknownFunction { name } => Some(name),
            Self::InvalidArguments { function, .. } => Some(function),
        }
    }
}

impl From<GlauxSqlError> for EngineError {
    fn from(err: GlauxSqlError) -> Self {
        match err {
            GlauxSqlError::Parse { message } => EngineError::Plan(message),
            GlauxSqlError::Unsupported { construct, message } => {
                EngineError::Unsupported { construct, message }
            }
            GlauxSqlError::UnknownFunction { name } => EngineError::Unsupported {
                construct: format!("function {name}"),
                message: "it is not in glaux's Trino coverage table (see docs/sql-coverage.md)"
                    .to_string(),
            },
            GlauxSqlError::InvalidArguments { function, message } => {
                EngineError::InvalidArgument { function, message }
            }
        }
    }
}

impl From<GlauxSqlError> for AthenaError {
    fn from(err: GlauxSqlError) -> Self {
        AthenaError::invalid_request(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn errors_name_their_construct_in_every_surface() {
        let err = GlauxSqlError::unsupported("lambda expression", "use explicit SQL");
        assert_eq!(err.construct(), Some("lambda expression"));
        assert!(err.to_string().contains("lambda expression"));

        let engine: EngineError = err.clone().into();
        assert!(matches!(
            &engine,
            EngineError::Unsupported { construct, .. } if construct == "lambda expression"
        ));
        assert_eq!(engine.category(), 2);

        let athena: AthenaError = err.into();
        assert_eq!(athena.code(), "InvalidRequestException");
        assert!(athena.message().contains("lambda expression"));

        let unknown: EngineError = GlauxSqlError::UnknownFunction {
            name: "frobnicate".into(),
        }
        .into();
        assert!(matches!(
            unknown,
            EngineError::Unsupported { construct, .. } if construct == "function frobnicate"
        ));
    }
}
