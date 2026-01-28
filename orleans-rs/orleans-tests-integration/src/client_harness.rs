//! Test client harness for integration testing.
//!
//! This module provides utilities for invoking grains and verifying
//! results during integration tests.

use crate::error::{TestError, TestResult};
use crate::silo_process::ProcessEvent;
use crate::TestCluster;
use std::time::Duration;
use tracing::{debug, info};

/// Result of a grain invocation.
#[derive(Debug, Clone)]
pub struct GrainInvocationResult {
    /// Whether the invocation succeeded
    pub success: bool,
    /// Method that was invoked
    pub method: String,
    /// Result value (if applicable)
    pub result: Option<serde_json::Value>,
    /// Error message (if failed)
    pub error: Option<String>,
    /// Time taken for the invocation
    pub duration: Duration,
}

impl GrainInvocationResult {
    /// Check if the invocation succeeded.
    pub fn is_success(&self) -> bool {
        self.success
    }

    /// Get the result as a specific type.
    pub fn result_as<T: serde::de::DeserializeOwned>(&self) -> Option<T> {
        self.result.as_ref().and_then(|v| serde_json::from_value(v.clone()).ok())
    }

    /// Get the error message.
    pub fn error_message(&self) -> Option<&str> {
        self.error.as_deref()
    }
}

/// Test client harness for invoking grains.
pub struct TestClientHarness<'a> {
    cluster: &'a mut TestCluster,
    default_timeout: Duration,
}

impl<'a> TestClientHarness<'a> {
    /// Create a new test client harness.
    pub fn new(cluster: &'a mut TestCluster) -> Self {
        Self {
            cluster,
            default_timeout: Duration::from_secs(30),
        }
    }

    /// Set the default timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.default_timeout = timeout;
        self
    }

    /// Create a grain on a specific silo.
    pub async fn create_grain_on_silo(
        &mut self,
        silo_index: usize,
        grain_key: &str,
    ) -> TestResult<String> {
        let silo = self.cluster.silo_mut(silo_index).ok_or_else(|| {
            TestError::Configuration(format!("Silo index {} out of range", silo_index))
        })?;

        info!(
            silo_index = silo_index,
            grain_key = grain_key,
            "Creating grain"
        );

        // Wait for grain_created event
        let event = silo
            .wait_for_event(
                |e| matches!(e, ProcessEvent::GrainCreated { grain_id, .. } if grain_id.contains(grain_key)),
                self.default_timeout,
            )
            .await?;

        match event {
            ProcessEvent::GrainCreated {
                grain_id,
                silo,
                activation_id,
            } => {
                info!(
                    grain_id = %grain_id,
                    silo = %silo,
                    activation_id = %activation_id,
                    "Grain created"
                );
                Ok(grain_id)
            }
            _ => Err(TestError::GrainInvocation("Unexpected event".into())),
        }
    }

    /// Invoke a grain method and get the result.
    pub async fn invoke_grain_method(
        &mut self,
        silo_index: usize,
        timeout: Option<Duration>,
    ) -> TestResult<GrainInvocationResult> {
        let timeout = timeout.unwrap_or(self.default_timeout);
        let start = std::time::Instant::now();

        let silo = self.cluster.silo_mut(silo_index).ok_or_else(|| {
            TestError::Configuration(format!("Silo index {} out of range", silo_index))
        })?;

        debug!(silo_index = silo_index, "Waiting for grain invocation result");

        // Wait for grain_invoked event
        let event = silo
            .wait_for_event(
                |e| matches!(e, ProcessEvent::GrainInvoked { .. }),
                timeout,
            )
            .await?;

        let duration = start.elapsed();

        match event {
            ProcessEvent::GrainInvoked {
                success,
                method,
                result,
                error,
            } => {
                let invocation_result = GrainInvocationResult {
                    success,
                    method,
                    result,
                    error,
                    duration,
                };

                if invocation_result.success {
                    info!(
                        method = %invocation_result.method,
                        duration_ms = duration.as_millis(),
                        "Grain invocation succeeded"
                    );
                } else {
                    info!(
                        method = %invocation_result.method,
                        error = ?invocation_result.error,
                        "Grain invocation failed"
                    );
                }

                Ok(invocation_result)
            }
            _ => Err(TestError::GrainInvocation("Unexpected event".into())),
        }
    }

    /// Invoke a test grain (create + invoke).
    pub async fn invoke_test_grain(
        &mut self,
        silo_index: usize,
        grain_key: &str,
    ) -> TestResult<GrainInvocationResult> {
        // First wait for grain creation
        let _ = self.create_grain_on_silo(silo_index, grain_key).await?;

        // Then wait for invocation result
        self.invoke_grain_method(silo_index, None).await
    }

    /// Get the cluster.
    pub fn cluster(&self) -> &TestCluster {
        self.cluster
    }

    /// Get a mutable reference to the cluster.
    pub fn cluster_mut(&mut self) -> &mut TestCluster {
        self.cluster
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_grain_invocation_result() {
        let result = GrainInvocationResult {
            success: true,
            method: "increment".into(),
            result: Some(serde_json::json!(42)),
            error: None,
            duration: Duration::from_millis(10),
        };

        assert!(result.is_success());
        assert_eq!(result.result_as::<i32>(), Some(42));
        assert!(result.error_message().is_none());
    }

    #[test]
    fn test_grain_invocation_result_error() {
        let result = GrainInvocationResult {
            success: false,
            method: "increment".into(),
            result: None,
            error: Some("grain not found".into()),
            duration: Duration::from_millis(5),
        };

        assert!(!result.is_success());
        assert_eq!(result.error_message(), Some("grain not found"));
    }
}
