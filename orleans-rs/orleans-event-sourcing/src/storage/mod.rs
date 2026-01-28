//! Storage implementations for event sourcing.

mod memory;

pub use memory::{InMemoryEventStorage, InMemorySnapshotStorage, InMemoryLogStorage};
