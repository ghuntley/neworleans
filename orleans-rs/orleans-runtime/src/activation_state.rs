//! Activation state machine for grain lifecycles.
//!
//! The activation state machine defines the valid states and transitions
//! for grain activations. A grain activation goes through the following
//! lifecycle:
//!
//! ```text
//! Creating -> Activating -> Valid -> Deactivating -> Invalid
//!                               |
//!                               +-> Migrating -> Invalid (if migration)
//! ```

use std::fmt;

/// The state of a grain activation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ActivationState {
    /// Initial state when the activation is being created.
    /// The grain instance is being constructed.
    Creating,

    /// The grain's OnActivateAsync is being called.
    /// The grain is initializing its state.
    Activating,

    /// The grain is ready to process requests.
    /// This is the normal operational state.
    Valid,

    /// The grain is being deactivated.
    /// OnDeactivateAsync is being called, no new requests accepted.
    Deactivating,

    /// The grain is being migrated to another silo.
    /// Similar to Deactivating but the grain will be reactivated elsewhere.
    Migrating,

    /// The grain activation is no longer valid.
    /// The grain has been deactivated and should be removed.
    Invalid,
}

impl ActivationState {
    /// Returns true if the activation can accept new requests.
    pub fn can_receive_messages(&self) -> bool {
        matches!(self, ActivationState::Valid)
    }

    /// Returns true if the activation is in a terminal state.
    pub fn is_terminal(&self) -> bool {
        matches!(self, ActivationState::Invalid)
    }

    /// Returns true if the activation is being deactivated.
    pub fn is_deactivating(&self) -> bool {
        matches!(
            self,
            ActivationState::Deactivating | ActivationState::Migrating
        )
    }

    /// Returns true if the activation is being created or activated.
    pub fn is_activating(&self) -> bool {
        matches!(
            self,
            ActivationState::Creating | ActivationState::Activating
        )
    }

    /// Attempts to transition to the next state.
    /// Returns the new state if the transition is valid, or None if invalid.
    pub fn transition_to(&self, target: ActivationState) -> Option<ActivationState> {
        match (self, target) {
            // Creating can transition to Activating or Invalid
            (ActivationState::Creating, ActivationState::Activating) => {
                Some(ActivationState::Activating)
            }
            (ActivationState::Creating, ActivationState::Invalid) => Some(ActivationState::Invalid),

            // Activating can transition to Valid or Invalid
            (ActivationState::Activating, ActivationState::Valid) => Some(ActivationState::Valid),
            (ActivationState::Activating, ActivationState::Invalid) => {
                Some(ActivationState::Invalid)
            }

            // Valid can transition to Deactivating or Migrating
            (ActivationState::Valid, ActivationState::Deactivating) => {
                Some(ActivationState::Deactivating)
            }
            (ActivationState::Valid, ActivationState::Migrating) => Some(ActivationState::Migrating),

            // Deactivating can only transition to Invalid
            (ActivationState::Deactivating, ActivationState::Invalid) => {
                Some(ActivationState::Invalid)
            }

            // Migrating can only transition to Invalid
            (ActivationState::Migrating, ActivationState::Invalid) => Some(ActivationState::Invalid),

            // Invalid is a terminal state
            (ActivationState::Invalid, _) => None,

            // All other transitions are invalid
            _ => None,
        }
    }
}

impl fmt::Display for ActivationState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ActivationState::Creating => write!(f, "Creating"),
            ActivationState::Activating => write!(f, "Activating"),
            ActivationState::Valid => write!(f, "Valid"),
            ActivationState::Deactivating => write!(f, "Deactivating"),
            ActivationState::Migrating => write!(f, "Migrating"),
            ActivationState::Invalid => write!(f, "Invalid"),
        }
    }
}

/// Reason why a grain is being deactivated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeactivationReason {
    /// Application explicitly requested deactivation.
    ApplicationRequested,

    /// The grain has been idle for too long.
    IdleTimeout,

    /// The silo is shutting down.
    SiloShutdown,

    /// The grain is being migrated to another silo.
    Migration,

    /// An unrecoverable error occurred.
    UnrecoverableError(String),

    /// The activation's lifecycle has expired.
    LifecycleExpired,
}

impl fmt::Display for DeactivationReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DeactivationReason::ApplicationRequested => write!(f, "application requested"),
            DeactivationReason::IdleTimeout => write!(f, "idle timeout"),
            DeactivationReason::SiloShutdown => write!(f, "silo shutdown"),
            DeactivationReason::Migration => write!(f, "migration"),
            DeactivationReason::UnrecoverableError(msg) => write!(f, "error: {}", msg),
            DeactivationReason::LifecycleExpired => write!(f, "lifecycle expired"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_creating_can_transition_to_activating() {
        assert_eq!(
            ActivationState::Creating.transition_to(ActivationState::Activating),
            Some(ActivationState::Activating)
        );
    }

    #[test]
    fn test_creating_can_transition_to_invalid() {
        assert_eq!(
            ActivationState::Creating.transition_to(ActivationState::Invalid),
            Some(ActivationState::Invalid)
        );
    }

    #[test]
    fn test_creating_cannot_transition_to_valid() {
        assert_eq!(
            ActivationState::Creating.transition_to(ActivationState::Valid),
            None
        );
    }

    #[test]
    fn test_activating_can_transition_to_valid() {
        assert_eq!(
            ActivationState::Activating.transition_to(ActivationState::Valid),
            Some(ActivationState::Valid)
        );
    }

    #[test]
    fn test_activating_can_transition_to_invalid() {
        assert_eq!(
            ActivationState::Activating.transition_to(ActivationState::Invalid),
            Some(ActivationState::Invalid)
        );
    }

    #[test]
    fn test_valid_can_transition_to_deactivating() {
        assert_eq!(
            ActivationState::Valid.transition_to(ActivationState::Deactivating),
            Some(ActivationState::Deactivating)
        );
    }

    #[test]
    fn test_valid_can_transition_to_migrating() {
        assert_eq!(
            ActivationState::Valid.transition_to(ActivationState::Migrating),
            Some(ActivationState::Migrating)
        );
    }

    #[test]
    fn test_deactivating_can_transition_to_invalid() {
        assert_eq!(
            ActivationState::Deactivating.transition_to(ActivationState::Invalid),
            Some(ActivationState::Invalid)
        );
    }

    #[test]
    fn test_migrating_can_transition_to_invalid() {
        assert_eq!(
            ActivationState::Migrating.transition_to(ActivationState::Invalid),
            Some(ActivationState::Invalid)
        );
    }

    #[test]
    fn test_invalid_is_terminal() {
        assert!(ActivationState::Invalid.is_terminal());
        assert!(!ActivationState::Valid.is_terminal());
    }

    #[test]
    fn test_invalid_cannot_transition() {
        assert_eq!(
            ActivationState::Invalid.transition_to(ActivationState::Creating),
            None
        );
        assert_eq!(
            ActivationState::Invalid.transition_to(ActivationState::Valid),
            None
        );
    }

    #[test]
    fn test_can_receive_messages() {
        assert!(!ActivationState::Creating.can_receive_messages());
        assert!(!ActivationState::Activating.can_receive_messages());
        assert!(ActivationState::Valid.can_receive_messages());
        assert!(!ActivationState::Deactivating.can_receive_messages());
        assert!(!ActivationState::Invalid.can_receive_messages());
    }

    #[test]
    fn test_is_deactivating() {
        assert!(!ActivationState::Valid.is_deactivating());
        assert!(ActivationState::Deactivating.is_deactivating());
        assert!(ActivationState::Migrating.is_deactivating());
    }

    #[test]
    fn test_is_activating() {
        assert!(ActivationState::Creating.is_activating());
        assert!(ActivationState::Activating.is_activating());
        assert!(!ActivationState::Valid.is_activating());
    }

    #[test]
    fn test_display() {
        assert_eq!(format!("{}", ActivationState::Creating), "Creating");
        assert_eq!(format!("{}", ActivationState::Valid), "Valid");
        assert_eq!(format!("{}", ActivationState::Deactivating), "Deactivating");
    }

    #[test]
    fn test_deactivation_reason_display() {
        assert_eq!(
            format!("{}", DeactivationReason::IdleTimeout),
            "idle timeout"
        );
        assert_eq!(
            format!("{}", DeactivationReason::SiloShutdown),
            "silo shutdown"
        );
    }
}
