//! Configuration options for streaming.

use std::time::Duration;

/// Options for stream pulling agents.
#[derive(Clone, Debug)]
pub struct StreamPullingAgentOptions {
    /// Timer period for fetching messages from the queue.
    pub get_queue_messages_timer_period: Duration,
    /// Maximum number of events per container batch.
    pub batch_container_batch_size: usize,
    /// Maximum number of messages to cache.
    pub cache_size: usize,
    /// Maximum number of parallel queue reads.
    pub max_parallel_queue_reads: usize,
    /// Whether to initialize queues eagerly on startup.
    pub eager_init: bool,
}

impl Default for StreamPullingAgentOptions {
    fn default() -> Self {
        Self {
            get_queue_messages_timer_period: Duration::from_millis(100),
            batch_container_batch_size: 10,
            cache_size: 4096,
            max_parallel_queue_reads: 16,
            eager_init: false,
        }
    }
}

impl StreamPullingAgentOptions {
    /// Create options for testing (faster polling).
    pub fn for_testing() -> Self {
        Self {
            get_queue_messages_timer_period: Duration::from_millis(10),
            batch_container_batch_size: 5,
            cache_size: 100,
            max_parallel_queue_reads: 4,
            eager_init: true,
        }
    }

    /// Set the timer period for fetching messages.
    pub fn with_timer_period(mut self, period: Duration) -> Self {
        self.get_queue_messages_timer_period = period;
        self
    }

    /// Set the batch size.
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_container_batch_size = size;
        self
    }

    /// Set the cache size.
    pub fn with_cache_size(mut self, size: usize) -> Self {
        self.cache_size = size;
        self
    }
}

/// Options for stream lifecycle management.
#[derive(Clone, Debug)]
pub struct StreamLifecycleOptions {
    /// Timeout for stream initialization.
    pub init_timeout: Duration,
    /// Timeout for stream provider startup.
    pub startup_timeout: Duration,
    /// Timeout for stream provider shutdown.
    pub shutdown_timeout: Duration,
}

impl Default for StreamLifecycleOptions {
    fn default() -> Self {
        Self {
            init_timeout: Duration::from_secs(30),
            startup_timeout: Duration::from_secs(30),
            shutdown_timeout: Duration::from_secs(30),
        }
    }
}

impl StreamLifecycleOptions {
    /// Create options for testing (shorter timeouts).
    pub fn for_testing() -> Self {
        Self {
            init_timeout: Duration::from_secs(5),
            startup_timeout: Duration::from_secs(5),
            shutdown_timeout: Duration::from_secs(5),
        }
    }
}

/// Options for stream pub/sub.
#[derive(Clone, Debug)]
pub struct StreamPubSubOptions {
    /// Type of pub/sub to use.
    pub pub_sub_type: StreamPubSubType,
    /// Grace period for subscription cleanup.
    pub subscription_cleanup_grace_period: Duration,
}

impl Default for StreamPubSubOptions {
    fn default() -> Self {
        Self {
            pub_sub_type: StreamPubSubType::ExplicitGrainBasedAndImplicit,
            subscription_cleanup_grace_period: Duration::from_secs(60),
        }
    }
}

/// Type of pub/sub subscription management.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum StreamPubSubType {
    /// Both explicit and implicit subscriptions (default).
    #[default]
    ExplicitGrainBasedAndImplicit,
    /// Only explicit subscriptions.
    ExplicitGrainBasedOnly,
    /// Only implicit subscriptions.
    ImplicitOnly,
}

/// Options for stream cache eviction.
#[derive(Clone, Debug)]
pub struct StreamCacheEvictionOptions {
    /// Maximum age of cached messages before eviction.
    pub data_max_age: Duration,
    /// Minimum size to trigger eviction.
    pub data_min_size_before_eviction: usize,
    /// High watermark for cache size.
    pub high_watermark: usize,
    /// Low watermark for cache size (target after eviction).
    pub low_watermark: usize,
    /// Eviction strategy.
    pub eviction_strategy: CacheEvictionStrategy,
}

impl Default for StreamCacheEvictionOptions {
    fn default() -> Self {
        Self {
            data_max_age: Duration::from_secs(300), // 5 minutes
            data_min_size_before_eviction: 1000,
            high_watermark: 10000,
            low_watermark: 5000,
            eviction_strategy: CacheEvictionStrategy::Chronological,
        }
    }
}

/// Cache eviction strategy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum CacheEvictionStrategy {
    /// Evict oldest messages first.
    #[default]
    Chronological,
    /// Evict based on access patterns.
    LruBased,
    /// Custom eviction strategy.
    Custom,
}

/// Options for hash ring stream queue mapping.
#[derive(Clone, Debug)]
pub struct HashRingStreamQueueMapperOptions {
    /// Number of queues.
    pub total_queue_count: usize,
}

impl Default for HashRingStreamQueueMapperOptions {
    fn default() -> Self {
        Self {
            total_queue_count: 8,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pulling_agent_defaults() {
        let opts = StreamPullingAgentOptions::default();
        assert_eq!(opts.get_queue_messages_timer_period, Duration::from_millis(100));
        assert_eq!(opts.batch_container_batch_size, 10);
        assert_eq!(opts.cache_size, 4096);
    }

    #[test]
    fn test_pulling_agent_for_testing() {
        let opts = StreamPullingAgentOptions::for_testing();
        assert!(opts.get_queue_messages_timer_period < Duration::from_millis(50));
        assert!(opts.eager_init);
    }

    #[test]
    fn test_pulling_agent_builder() {
        let opts = StreamPullingAgentOptions::default()
            .with_timer_period(Duration::from_millis(50))
            .with_batch_size(20)
            .with_cache_size(100);

        assert_eq!(opts.get_queue_messages_timer_period, Duration::from_millis(50));
        assert_eq!(opts.batch_container_batch_size, 20);
        assert_eq!(opts.cache_size, 100);
    }

    #[test]
    fn test_lifecycle_defaults() {
        let opts = StreamLifecycleOptions::default();
        assert_eq!(opts.init_timeout, Duration::from_secs(30));
        assert_eq!(opts.startup_timeout, Duration::from_secs(30));
    }

    #[test]
    fn test_pubsub_defaults() {
        let opts = StreamPubSubOptions::default();
        assert_eq!(opts.pub_sub_type, StreamPubSubType::ExplicitGrainBasedAndImplicit);
    }

    #[test]
    fn test_cache_eviction_defaults() {
        let opts = StreamCacheEvictionOptions::default();
        assert_eq!(opts.eviction_strategy, CacheEvictionStrategy::Chronological);
        assert!(opts.high_watermark > opts.low_watermark);
    }
}
