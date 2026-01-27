//! Response types for grain method calls.
//!
//! This module provides the response object model for representing
//! the results of grain method invocations, including both successful
//! results and exceptions.

use bytes::Bytes;
use std::any::Any;
use std::fmt;
use std::sync::Arc;

/// The result of a grain method invocation.
///
/// A response can be:
/// - A completed response (for void methods)
/// - A typed result
/// - An exception
#[derive(Clone)]
pub enum Response {
    /// The method completed successfully with no return value (void).
    Completed,

    /// The method completed successfully with a result.
    Result(ResponseResult),

    /// The method failed with an exception.
    Exception(ResponseException),
}

impl Response {
    /// Create a completed response (for void methods).
    pub fn completed() -> Self {
        Response::Completed
    }

    /// Create a response from a successful result.
    pub fn from_result<T: Send + Sync + 'static>(value: T) -> Self {
        Response::Result(ResponseResult {
            value: Arc::new(value),
            serialized: None,
        })
    }

    /// Create a response from serialized bytes.
    pub fn from_bytes(bytes: Bytes) -> Self {
        Response::Result(ResponseResult {
            value: Arc::new(()),
            serialized: Some(bytes),
        })
    }

    /// Create a response from an exception.
    pub fn from_exception(exception: impl Into<String>) -> Self {
        Response::Exception(ResponseException {
            message: exception.into(),
            exception_type: None,
            stack_trace: None,
        })
    }

    /// Create a response from a detailed exception.
    pub fn from_exception_details(
        message: impl Into<String>,
        exception_type: impl Into<String>,
        stack_trace: Option<String>,
    ) -> Self {
        Response::Exception(ResponseException {
            message: message.into(),
            exception_type: Some(exception_type.into()),
            stack_trace,
        })
    }

    /// Check if this is a successful response.
    pub fn is_success(&self) -> bool {
        matches!(self, Response::Completed | Response::Result(_))
    }

    /// Check if this is an exception response.
    pub fn is_exception(&self) -> bool {
        matches!(self, Response::Exception(_))
    }

    /// Get the result value if this is a successful result response.
    pub fn get_result<T: Clone + 'static>(&self) -> Option<T> {
        match self {
            Response::Result(r) => r.value.downcast_ref::<T>().cloned(),
            _ => None,
        }
    }

    /// Get the serialized bytes if available.
    pub fn get_bytes(&self) -> Option<&Bytes> {
        match self {
            Response::Result(r) => r.serialized.as_ref(),
            _ => None,
        }
    }

    /// Get the exception if this is an exception response.
    pub fn get_exception(&self) -> Option<&ResponseException> {
        match self {
            Response::Exception(e) => Some(e),
            _ => None,
        }
    }

    /// Convert to a result, returning the exception message as an error.
    pub fn into_result<T: Clone + 'static>(self) -> Result<Option<T>, String> {
        match self {
            Response::Completed => Ok(None),
            Response::Result(r) => {
                let value = r.value.downcast_ref::<T>().cloned();
                Ok(value)
            }
            Response::Exception(e) => Err(e.message),
        }
    }

    /// Set the result value.
    pub fn set_result<T: Send + Sync + 'static>(&mut self, value: T) {
        *self = Response::from_result(value);
    }

    /// Set the exception.
    pub fn set_exception(&mut self, message: impl Into<String>) {
        *self = Response::from_exception(message);
    }
}

impl Default for Response {
    fn default() -> Self {
        Response::Completed
    }
}

impl fmt::Debug for Response {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Response::Completed => write!(f, "Response::Completed"),
            Response::Result(_) => write!(f, "Response::Result(...)"),
            Response::Exception(e) => write!(f, "Response::Exception({:?})", e.message),
        }
    }
}

/// A successful result from a grain method.
#[derive(Clone)]
pub struct ResponseResult {
    /// The result value (type-erased).
    value: Arc<dyn Any + Send + Sync>,

    /// The serialized form of the result (if available).
    serialized: Option<Bytes>,
}

impl ResponseResult {
    /// Get the result value as a specific type.
    pub fn get<T: Clone + 'static>(&self) -> Option<T> {
        self.value.downcast_ref::<T>().cloned()
    }

    /// Get the serialized bytes.
    pub fn bytes(&self) -> Option<&Bytes> {
        self.serialized.as_ref()
    }

    /// Set the serialized bytes.
    pub fn set_bytes(&mut self, bytes: Bytes) {
        self.serialized = Some(bytes);
    }
}

/// An exception from a grain method.
#[derive(Debug, Clone)]
pub struct ResponseException {
    /// The exception message.
    pub message: String,

    /// The type name of the exception.
    pub exception_type: Option<String>,

    /// The stack trace (if available).
    pub stack_trace: Option<String>,
}

impl ResponseException {
    /// Create a new exception.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            exception_type: None,
            stack_trace: None,
        }
    }

    /// Create an exception with type information.
    pub fn with_type(mut self, exception_type: impl Into<String>) -> Self {
        self.exception_type = Some(exception_type.into());
        self
    }

    /// Create an exception with a stack trace.
    pub fn with_stack_trace(mut self, stack_trace: impl Into<String>) -> Self {
        self.stack_trace = Some(stack_trace.into());
        self
    }
}

impl fmt::Display for ResponseException {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ref exc_type) = self.exception_type {
            write!(f, "{}: {}", exc_type, self.message)
        } else {
            write!(f, "{}", self.message)
        }
    }
}

impl std::error::Error for ResponseException {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_completed_response() {
        let response = Response::completed();
        assert!(response.is_success());
        assert!(!response.is_exception());
    }

    #[test]
    fn test_result_response() {
        let response = Response::from_result(42i32);
        assert!(response.is_success());
        assert!(!response.is_exception());
        assert_eq!(response.get_result::<i32>(), Some(42));
    }

    #[test]
    fn test_exception_response() {
        let response = Response::from_exception("Something went wrong");
        assert!(!response.is_success());
        assert!(response.is_exception());
        let exc = response.get_exception().unwrap();
        assert_eq!(exc.message, "Something went wrong");
    }

    #[test]
    fn test_exception_with_details() {
        let response = Response::from_exception_details(
            "Not found",
            "NotFoundException",
            Some("at MyMethod()".to_string()),
        );
        let exc = response.get_exception().unwrap();
        assert_eq!(exc.message, "Not found");
        assert_eq!(exc.exception_type.as_deref(), Some("NotFoundException"));
        assert_eq!(exc.stack_trace.as_deref(), Some("at MyMethod()"));
    }

    #[test]
    fn test_bytes_response() {
        let bytes = Bytes::from_static(b"hello");
        let response = Response::from_bytes(bytes.clone());
        assert!(response.is_success());
        assert_eq!(response.get_bytes(), Some(&bytes));
    }

    #[test]
    fn test_into_result_success() {
        let response = Response::from_result(42i32);
        let result = response.into_result::<i32>();
        assert_eq!(result, Ok(Some(42)));
    }

    #[test]
    fn test_into_result_completed() {
        let response = Response::completed();
        let result = response.into_result::<i32>();
        assert_eq!(result, Ok(None));
    }

    #[test]
    fn test_into_result_exception() {
        let response = Response::from_exception("error");
        let result = response.into_result::<i32>();
        assert_eq!(result, Err("error".to_string()));
    }

    #[test]
    fn test_set_result() {
        let mut response = Response::completed();
        response.set_result(42i32);
        assert_eq!(response.get_result::<i32>(), Some(42));
    }

    #[test]
    fn test_set_exception() {
        let mut response = Response::completed();
        response.set_exception("error");
        assert!(response.is_exception());
    }

    #[test]
    fn test_response_exception_display() {
        let exc = ResponseException::new("Not found")
            .with_type("NotFoundException");
        assert_eq!(format!("{}", exc), "NotFoundException: Not found");

        let exc_no_type = ResponseException::new("Error");
        assert_eq!(format!("{}", exc_no_type), "Error");
    }

    #[test]
    fn test_response_debug() {
        let completed = Response::completed();
        assert!(format!("{:?}", completed).contains("Completed"));

        let result = Response::from_result(42);
        assert!(format!("{:?}", result).contains("Result"));

        let exception = Response::from_exception("error");
        assert!(format!("{:?}", exception).contains("Exception"));
    }

    #[test]
    fn test_response_default() {
        let response = Response::default();
        assert!(matches!(response, Response::Completed));
    }
}
