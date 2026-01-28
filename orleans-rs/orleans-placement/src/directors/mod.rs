//! Built-in placement director implementations.

mod random;
mod hash_based;
mod prefer_local;
mod activation_count;
mod resource_optimized;

pub use random::RandomPlacementDirector;
pub use hash_based::HashBasedPlacementDirector;
pub use prefer_local::PreferLocalPlacementDirector;
pub use activation_count::ActivationCountPlacementDirector;
pub use resource_optimized::ResourceOptimizedPlacementDirector;
