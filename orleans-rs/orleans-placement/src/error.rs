//! Error types for placement operations.

use orleans_core::SiloAddress;
use thiserror::Error;

/// Result type for placement operations.
pub type PlacementResult<T> = Result<T, PlacementError>;

/// Errors that can occur during placement operations.
#[derive(Debug, Error)]
pub enum PlacementError {
    /// No compatible silos are available for placement.
    #[error("no compatible silos available for placement")]
    NoCompatibleSilos,

    /// No silos with the specified role are available.
    #[error("no silos with role '{0}' available")]
    NoSilosWithRole(String),

    /// All compatible silos are overloaded.
    #[error("all compatible silos are overloaded")]
    AllSilosOverloaded,

    /// The specified silo is unavailable.
    #[error("silo {0} is unavailable")]
    SiloUnavailable(SiloAddress),

    /// The local silo is terminating and cannot accept new activations.
    #[error("local silo is terminating")]
    LocalSiloTerminating,

    /// Statistics are unavailable for placement decision.
    #[error("silo statistics unavailable")]
    StatisticsUnavailable,

    /// Invalid placement configuration.
    #[error("invalid placement configuration: {0}")]
    InvalidConfiguration(String),

    /// Placement strategy not found.
    #[error("placement strategy '{0}' not found")]
    StrategyNotFound(String),

    /// Placement director not found.
    #[error("placement director for strategy '{0}' not found")]
    DirectorNotFound(String),

    /// Internal placement error.
    #[error("internal placement error: {0}")]
    Internal(String),
}

impl PlacementError {
    /// Creates a new NoSilosWithRole error.
    pub fn no_silos_with_role(role: impl Into<String>) -> Self {
        PlacementError::NoSilosWithRole(role.into())
    }

    /// Creates a new SiloUnavailable error.
    pub fn silo_unavailable(silo: SiloAddress) -> Self {
        PlacementError::SiloUnavailable(silo)
    }

    /// Creates a new InvalidConfiguration error.
    pub fn invalid_configuration(msg: impl Into<String>) -> Self {
        PlacementError::InvalidConfiguration(msg.into())
    }

    /// Creates a new StrategyNotFound error.
    pub fn strategy_not_found(name: impl Into<String>) -> Self {
        PlacementError::StrategyNotFound(name.into())
    }

    /// Creates a new DirectorNotFound error.
    pub fn director_not_found(strategy: impl Into<String>) -> Self {
        PlacementError::DirectorNotFound(strategy.into())
    }

    /// Creates a new Internal error.
    pub fn internal(msg: impl Into<String>) -> Self {
        PlacementError::Internal(msg.into())
    }

    /// Returns true if this is a recoverable error that should trigger retry.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            PlacementError::SiloUnavailable(_)
                | PlacementError::StatisticsUnavailable
                | PlacementError::AllSilosOverloaded
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn make_silo() -> SiloAddress {
        SiloAddress::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 11111),
            1,
        )
    }

    #[test]
    fn test_no_compatible_silos_error() {
        let err = PlacementError::NoCompatibleSilos;
        assert!(err.to_string().contains("no compatible silos"));
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_no_silos_with_role_error() {
        let err = PlacementError::no_silos_with_role("worker");
        assert!(err.to_string().contains("worker"));
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_all_silos_overloaded_error() {
        let err = PlacementError::AllSilosOverloaded;
        assert!(err.to_string().contains("overloaded"));
        assert!(err.is_retryable());
    }

    #[test]
    fn test_silo_unavailable_error() {
        let silo = make_silo();
        let err = PlacementError::silo_unavailable(silo);
        assert!(err.to_string().contains("unavailable"));
        assert!(err.is_retryable());
    }

    #[test]
    fn test_local_silo_terminating_error() {
        let err = PlacementError::LocalSiloTerminating;
        assert!(err.to_string().contains("terminating"));
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_statistics_unavailable_error() {
        let err = PlacementError::StatisticsUnavailable;
        assert!(err.to_string().contains("statistics"));
        assert!(err.is_retryable());
    }

    #[test]
    fn test_invalid_configuration_error() {
        let err = PlacementError::invalid_configuration("bad value");
        assert!(err.to_string().contains("bad value"));
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_strategy_not_found_error() {
        let err = PlacementError::strategy_not_found("Custom");
        assert!(err.to_string().contains("Custom"));
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_director_not_found_error() {
        let err = PlacementError::director_not_found("RandomPlacement");
        assert!(err.to_string().contains("RandomPlacement"));
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_internal_error() {
        let err = PlacementError::internal("unexpected state");
        assert!(err.to_string().contains("unexpected state"));
        assert!(!err.is_retryable());
    }
}
