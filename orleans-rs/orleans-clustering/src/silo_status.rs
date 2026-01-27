//! Silo status enumeration representing the lifecycle states of a silo.

use serde::{Deserialize, Serialize};

/// Represents the current status of a silo in the cluster.
///
/// The status follows a lifecycle:
/// Created -> Joining -> Active -> ShuttingDown -> Stopping -> Dead
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum SiloStatus {
    /// Silo has been created but not yet started joining.
    Created = 0,
    /// Silo is in the process of joining the cluster.
    Joining = 2,
    /// Silo is fully active and accepting grain activations.
    Active = 3,
    /// Silo has begun graceful shutdown.
    ShuttingDown = 4,
    /// Silo is stopping and will no longer accept requests.
    Stopping = 5,
    /// Silo is dead and should be removed from the cluster.
    Dead = 6,
}

impl SiloStatus {
    /// Returns true if the silo is in a terminating state.
    pub fn is_terminating(&self) -> bool {
        matches!(self, Self::ShuttingDown | Self::Stopping | Self::Dead)
    }

    /// Returns true if the silo can accept grain activations.
    pub fn is_active(&self) -> bool {
        matches!(self, Self::Active)
    }

    /// Returns true if the silo is participating in the cluster.
    pub fn is_alive(&self) -> bool {
        matches!(self, Self::Joining | Self::Active | Self::ShuttingDown)
    }

    /// Returns the next valid status in the lifecycle.
    pub fn next_status(&self) -> Option<SiloStatus> {
        match self {
            Self::Created => Some(Self::Joining),
            Self::Joining => Some(Self::Active),
            Self::Active => Some(Self::ShuttingDown),
            Self::ShuttingDown => Some(Self::Stopping),
            Self::Stopping => Some(Self::Dead),
            Self::Dead => None,
        }
    }

    /// Returns true if transitioning to the target status is valid.
    pub fn can_transition_to(&self, target: SiloStatus) -> bool {
        match (self, target) {
            // Normal lifecycle transitions
            (Self::Created, Self::Joining) => true,
            (Self::Joining, Self::Active) => true,
            (Self::Active, Self::ShuttingDown) => true,
            (Self::ShuttingDown, Self::Stopping) => true,
            (Self::Stopping, Self::Dead) => true,
            // Fast-track to Dead (failure detection)
            (_, Self::Dead) => true,
            // Same status is always valid (no-op)
            (a, b) if *a == b => true,
            _ => false,
        }
    }
}

impl Default for SiloStatus {
    fn default() -> Self {
        Self::Created
    }
}

impl std::fmt::Display for SiloStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Created => "Created",
            Self::Joining => "Joining",
            Self::Active => "Active",
            Self::ShuttingDown => "ShuttingDown",
            Self::Stopping => "Stopping",
            Self::Dead => "Dead",
        };
        write!(f, "{}", s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_terminating() {
        assert!(!SiloStatus::Created.is_terminating());
        assert!(!SiloStatus::Joining.is_terminating());
        assert!(!SiloStatus::Active.is_terminating());
        assert!(SiloStatus::ShuttingDown.is_terminating());
        assert!(SiloStatus::Stopping.is_terminating());
        assert!(SiloStatus::Dead.is_terminating());
    }

    #[test]
    fn test_is_active() {
        assert!(!SiloStatus::Created.is_active());
        assert!(!SiloStatus::Joining.is_active());
        assert!(SiloStatus::Active.is_active());
        assert!(!SiloStatus::ShuttingDown.is_active());
        assert!(!SiloStatus::Stopping.is_active());
        assert!(!SiloStatus::Dead.is_active());
    }

    #[test]
    fn test_is_alive() {
        assert!(!SiloStatus::Created.is_alive());
        assert!(SiloStatus::Joining.is_alive());
        assert!(SiloStatus::Active.is_alive());
        assert!(SiloStatus::ShuttingDown.is_alive());
        assert!(!SiloStatus::Stopping.is_alive());
        assert!(!SiloStatus::Dead.is_alive());
    }

    #[test]
    fn test_next_status() {
        assert_eq!(SiloStatus::Created.next_status(), Some(SiloStatus::Joining));
        assert_eq!(SiloStatus::Joining.next_status(), Some(SiloStatus::Active));
        assert_eq!(
            SiloStatus::Active.next_status(),
            Some(SiloStatus::ShuttingDown)
        );
        assert_eq!(
            SiloStatus::ShuttingDown.next_status(),
            Some(SiloStatus::Stopping)
        );
        assert_eq!(SiloStatus::Stopping.next_status(), Some(SiloStatus::Dead));
        assert_eq!(SiloStatus::Dead.next_status(), None);
    }

    #[test]
    fn test_can_transition_to() {
        // Valid forward transitions
        assert!(SiloStatus::Created.can_transition_to(SiloStatus::Joining));
        assert!(SiloStatus::Joining.can_transition_to(SiloStatus::Active));
        assert!(SiloStatus::Active.can_transition_to(SiloStatus::ShuttingDown));

        // Can always transition to Dead
        assert!(SiloStatus::Created.can_transition_to(SiloStatus::Dead));
        assert!(SiloStatus::Joining.can_transition_to(SiloStatus::Dead));
        assert!(SiloStatus::Active.can_transition_to(SiloStatus::Dead));

        // Same status is valid
        assert!(SiloStatus::Active.can_transition_to(SiloStatus::Active));

        // Invalid backward transitions (except to Dead)
        assert!(!SiloStatus::Active.can_transition_to(SiloStatus::Joining));
        assert!(!SiloStatus::Dead.can_transition_to(SiloStatus::Active));
    }

    #[test]
    fn test_display() {
        assert_eq!(format!("{}", SiloStatus::Created), "Created");
        assert_eq!(format!("{}", SiloStatus::Active), "Active");
        assert_eq!(format!("{}", SiloStatus::Dead), "Dead");
    }

    #[test]
    fn test_default() {
        assert_eq!(SiloStatus::default(), SiloStatus::Created);
    }

    #[test]
    fn test_serialization() {
        let status = SiloStatus::Active;
        let json = serde_json::to_string(&status).unwrap();
        let deserialized: SiloStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(status, deserialized);
    }
}
