//! Observer Manager - Subscription management with copy-on-write semantics.
//!
//! The `ObserverManager` provides thread-safe subscription management for
//! observers with automatic expiration-based cleanup.

use parking_lot::RwLock;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, trace, warn};

/// Entry in the observer manager tracking an observer and its last activity.
#[derive(Clone)]
struct ObserverEntry<V> {
    /// The observer value.
    observer: V,
    /// Last time this observer was seen (subscribed or renewed).
    last_seen: Instant,
}

/// Observer manager with subscription management and expiration.
///
/// This manager provides:
/// - Thread-safe subscription/unsubscription
/// - Automatic expiration of stale subscriptions
/// - Copy-on-write semantics for concurrent iteration and modification
/// - Async and sync notification methods
///
/// # Type Parameters
/// * `K` - The key type used to identify observers (must implement `Eq + Hash + Clone`)
/// * `V` - The observer value type (must implement `Clone`)
///
/// # Example
///
/// ```
/// use orleans_observers::ObserverManager;
/// use std::time::Duration;
///
/// // Create a manager with 5-minute expiration
/// let manager: ObserverManager<String, String> = ObserverManager::new(Duration::from_secs(300));
///
/// // Subscribe an observer
/// manager.subscribe("user-1".to_string(), "handler-1".to_string());
///
/// // Check count
/// assert_eq!(manager.count(), 1);
///
/// // Notify all observers
/// manager.notify(|observer| {
///     println!("Notifying: {}", observer);
/// });
/// ```
pub struct ObserverManager<K, V>
where
    K: Eq + Hash + Clone + Debug,
    V: Clone,
{
    /// The observer entries.
    observers: RwLock<HashMap<K, ObserverEntry<V>>>,
    /// Read snapshot for copy-on-write.
    read_snapshot: RwLock<Option<Arc<HashMap<K, ObserverEntry<V>>>>>,
    /// Number of active readers.
    num_readers: AtomicUsize,
    /// Expiration duration for subscriptions.
    expiration: Duration,
}

impl<K, V> ObserverManager<K, V>
where
    K: Eq + Hash + Clone + Debug,
    V: Clone,
{
    /// Creates a new observer manager with the specified expiration duration.
    ///
    /// # Arguments
    /// * `expiration` - How long after last activity before a subscription expires
    pub fn new(expiration: Duration) -> Self {
        Self {
            observers: RwLock::new(HashMap::new()),
            read_snapshot: RwLock::new(None),
            num_readers: AtomicUsize::new(0),
            expiration,
        }
    }

    /// Returns the current number of subscribed observers.
    pub fn count(&self) -> usize {
        self.observers.read().len()
    }

    /// Returns true if there are no subscribed observers.
    pub fn is_empty(&self) -> bool {
        self.observers.read().is_empty()
    }

    /// Returns the expiration duration.
    pub fn expiration(&self) -> Duration {
        self.expiration
    }

    /// Subscribes an observer or renews an existing subscription.
    ///
    /// If an observer with the same key already exists, its entry is updated
    /// and the `last_seen` timestamp is refreshed.
    ///
    /// # Arguments
    /// * `key` - The unique identifier for this observer
    /// * `observer` - The observer value
    pub fn subscribe(&self, key: K, observer: V) {
        let mut observers = self.get_writable_observers();
        let now = Instant::now();

        if let Some(entry) = observers.get_mut(&key) {
            trace!(key = ?key, "Renewing observer subscription");
            entry.last_seen = now;
            entry.observer = observer;
        } else {
            debug!(key = ?key, "Adding new observer subscription");
            observers.insert(
                key,
                ObserverEntry {
                    observer,
                    last_seen: now,
                },
            );
        }
    }

    /// Unsubscribes an observer.
    ///
    /// # Arguments
    /// * `key` - The key of the observer to unsubscribe
    ///
    /// # Returns
    /// `true` if the observer was removed, `false` if not found.
    pub fn unsubscribe(&self, key: &K) -> bool {
        let mut observers = self.get_writable_observers();
        let removed = observers.remove(key).is_some();
        if removed {
            debug!(key = ?key, "Observer unsubscribed");
        }
        removed
    }

    /// Clears all observers.
    pub fn clear(&self) {
        let mut observers = self.get_writable_observers();
        let count = observers.len();
        observers.clear();
        debug!(count, "Cleared all observers");
    }

    /// Clears expired observers.
    ///
    /// Removes all observers whose `last_seen` timestamp is older than
    /// the expiration duration.
    ///
    /// # Returns
    /// The number of expired observers that were removed.
    pub fn clear_expired(&self) -> usize {
        let now = Instant::now();
        let expiry_threshold = now - self.expiration;

        let expired: Vec<K> = {
            let observers = self.observers.read();
            observers
                .iter()
                .filter(|(_, entry)| entry.last_seen < expiry_threshold)
                .map(|(k, _)| k.clone())
                .collect()
        };

        if !expired.is_empty() {
            let mut observers = self.get_writable_observers();
            for key in &expired {
                observers.remove(key);
            }
            debug!(count = expired.len(), "Removed expired observers");
        }

        expired.len()
    }

    /// Checks if an observer with the given key exists.
    pub fn contains(&self, key: &K) -> bool {
        self.observers.read().contains_key(key)
    }

    /// Gets an observer by key.
    pub fn get(&self, key: &K) -> Option<V> {
        self.observers
            .read()
            .get(key)
            .map(|entry| entry.observer.clone())
    }

    /// Notifies all observers synchronously.
    ///
    /// Observers that throw exceptions or are expired are automatically removed.
    ///
    /// # Arguments
    /// * `notification` - A closure to call for each observer
    pub fn notify<F>(&self, notification: F)
    where
        F: Fn(&V),
    {
        self.notify_filtered(notification, |_| true)
    }

    /// Notifies observers synchronously with a filter predicate.
    ///
    /// # Arguments
    /// * `notification` - A closure to call for each matching observer
    /// * `predicate` - A filter to select which observers to notify
    pub fn notify_filtered<F, P>(&self, notification: F, predicate: P)
    where
        F: Fn(&V),
        P: Fn(&V) -> bool,
    {
        let now = Instant::now();
        let expiry_threshold = now - self.expiration;
        let mut defunct: Vec<K> = Vec::new();

        // Use a read snapshot for iteration
        let snapshot = self.create_read_snapshot();

        for (key, entry) in snapshot.iter() {
            // Check expiration
            if entry.last_seen < expiry_threshold {
                defunct.push(key.clone());
                continue;
            }

            // Apply predicate filter
            if !predicate(&entry.observer) {
                continue;
            }

            // Notify - catch panics and mark as defunct
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                notification(&entry.observer);
            }));

            if result.is_err() {
                warn!(key = ?key, "Observer notification panicked");
                defunct.push(key.clone());
            }
        }

        // Release read snapshot
        drop(snapshot);
        self.release_read_snapshot();

        // Remove defunct observers
        self.remove_defunct(&defunct);
    }

    /// Notifies all observers asynchronously.
    ///
    /// Note: The observer value is cloned before being passed to the notification
    /// function to allow the async closure to own the value.
    ///
    /// # Arguments
    /// * `notification` - An async closure to call for each observer (receives owned value)
    pub async fn notify_async<F, Fut>(&self, notification: F)
    where
        F: Fn(V) -> Fut,
        Fut: std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>>,
    {
        self.notify_async_filtered(notification, |_| true).await
    }

    /// Notifies observers asynchronously with a filter predicate.
    ///
    /// Note: The observer value is cloned before being passed to the notification
    /// function to allow the async closure to own the value.
    ///
    /// # Arguments
    /// * `notification` - An async closure to call for each matching observer (receives owned value)
    /// * `predicate` - A filter to select which observers to notify (receives reference)
    pub async fn notify_async_filtered<F, Fut, P>(&self, notification: F, predicate: P)
    where
        F: Fn(V) -> Fut,
        Fut: std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>>,
        P: Fn(&V) -> bool,
    {
        let now = Instant::now();
        let expiry_threshold = now - self.expiration;
        let mut defunct: Vec<K> = Vec::new();

        // Use a read snapshot for iteration
        let snapshot = self.create_read_snapshot();

        for (key, entry) in snapshot.iter() {
            // Check expiration
            if entry.last_seen < expiry_threshold {
                defunct.push(key.clone());
                continue;
            }

            // Apply predicate filter
            if !predicate(&entry.observer) {
                continue;
            }

            // Clone the observer and notify (allows async closure to own the value)
            let observer_clone = entry.observer.clone();
            if let Err(e) = notification(observer_clone).await {
                warn!(key = ?key, error = %e, "Observer notification failed");
                defunct.push(key.clone());
            }
        }

        // Release read snapshot
        drop(snapshot);
        self.release_read_snapshot();

        // Remove defunct observers
        self.remove_defunct(&defunct);
    }

    /// Creates a read snapshot for iteration.
    fn create_read_snapshot(&self) -> Arc<HashMap<K, ObserverEntry<V>>> {
        self.num_readers.fetch_add(1, Ordering::SeqCst);

        let observers = self.observers.read();
        let snapshot = Arc::new(observers.clone());

        let mut read_snapshot = self.read_snapshot.write();
        *read_snapshot = Some(snapshot.clone());

        snapshot
    }

    /// Releases a read snapshot after iteration.
    fn release_read_snapshot(&self) {
        let prev = self.num_readers.fetch_sub(1, Ordering::SeqCst);
        if prev == 1 {
            // Last reader, clear the snapshot
            let mut read_snapshot = self.read_snapshot.write();
            *read_snapshot = None;
        }
    }

    /// Gets a writable observer map, copying if readers exist.
    fn get_writable_observers(&self) -> parking_lot::RwLockWriteGuard<'_, HashMap<K, ObserverEntry<V>>> {
        let mut observers = self.observers.write();

        // If readers exist and we're sharing the map with the snapshot, copy
        // to maintain copy-on-write semantics for safe concurrent iteration
        if self.num_readers.load(Ordering::SeqCst) > 0 {
            let read_snapshot = self.read_snapshot.read();
            if read_snapshot.is_some() {
                // Readers exist, clone to avoid modifying the snapshot
                drop(read_snapshot);
                *observers = observers.clone();
            }
        }

        observers
    }

    /// Removes defunct observers.
    fn remove_defunct(&self, defunct: &[K]) {
        if defunct.is_empty() {
            return;
        }

        let mut observers = self.get_writable_observers();
        for key in defunct {
            observers.remove(key);
        }
        trace!(count = defunct.len(), "Removed defunct observers");
    }

    /// Returns an iterator over observer keys (snapshot).
    pub fn keys(&self) -> Vec<K> {
        self.observers.read().keys().cloned().collect()
    }

    /// Returns an iterator over observers (snapshot).
    pub fn values(&self) -> Vec<V> {
        self.observers
            .read()
            .values()
            .map(|e| e.observer.clone())
            .collect()
    }
}

impl<K, V> Debug for ObserverManager<K, V>
where
    K: Eq + Hash + Clone + Debug,
    V: Clone,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObserverManager")
            .field("count", &self.count())
            .field("expiration", &self.expiration)
            .finish()
    }
}

impl<K, V> Default for ObserverManager<K, V>
where
    K: Eq + Hash + Clone + Debug,
    V: Clone,
{
    fn default() -> Self {
        // Default expiration of 5 minutes
        Self::new(Duration::from_secs(300))
    }
}

/// Type alias for a simple observer manager keyed by the observer itself.
pub type SimpleObserverManager<T> = ObserverManager<T, T>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU32;

    #[test]
    fn test_new() {
        let manager: ObserverManager<String, i32> = ObserverManager::new(Duration::from_secs(60));
        assert!(manager.is_empty());
        assert_eq!(manager.count(), 0);
        assert_eq!(manager.expiration(), Duration::from_secs(60));
    }

    #[test]
    fn test_default() {
        let manager: ObserverManager<String, i32> = ObserverManager::default();
        assert_eq!(manager.expiration(), Duration::from_secs(300));
    }

    #[test]
    fn test_subscribe() {
        let manager: ObserverManager<String, i32> = ObserverManager::new(Duration::from_secs(60));

        manager.subscribe("key-1".to_string(), 100);
        assert_eq!(manager.count(), 1);
        assert!(manager.contains(&"key-1".to_string()));
        assert_eq!(manager.get(&"key-1".to_string()), Some(100));
    }

    #[test]
    fn test_subscribe_updates_existing() {
        let manager: ObserverManager<String, i32> = ObserverManager::new(Duration::from_secs(60));

        manager.subscribe("key-1".to_string(), 100);
        manager.subscribe("key-1".to_string(), 200);

        assert_eq!(manager.count(), 1);
        assert_eq!(manager.get(&"key-1".to_string()), Some(200));
    }

    #[test]
    fn test_unsubscribe() {
        let manager: ObserverManager<String, i32> = ObserverManager::new(Duration::from_secs(60));

        manager.subscribe("key-1".to_string(), 100);
        assert!(manager.unsubscribe(&"key-1".to_string()));
        assert!(!manager.unsubscribe(&"key-1".to_string()));
        assert!(manager.is_empty());
    }

    #[test]
    fn test_clear() {
        let manager: ObserverManager<String, i32> = ObserverManager::new(Duration::from_secs(60));

        manager.subscribe("key-1".to_string(), 100);
        manager.subscribe("key-2".to_string(), 200);
        manager.subscribe("key-3".to_string(), 300);

        assert_eq!(manager.count(), 3);
        manager.clear();
        assert!(manager.is_empty());
    }

    #[test]
    fn test_notify() {
        let manager: ObserverManager<String, i32> = ObserverManager::new(Duration::from_secs(60));

        manager.subscribe("key-1".to_string(), 1);
        manager.subscribe("key-2".to_string(), 2);
        manager.subscribe("key-3".to_string(), 3);

        let sum = Arc::new(AtomicU32::new(0));
        let sum_clone = sum.clone();

        manager.notify(move |value| {
            sum_clone.fetch_add(*value as u32, Ordering::SeqCst);
        });

        assert_eq!(sum.load(Ordering::SeqCst), 6);
    }

    #[test]
    fn test_notify_filtered() {
        let manager: ObserverManager<String, i32> = ObserverManager::new(Duration::from_secs(60));

        manager.subscribe("key-1".to_string(), 1);
        manager.subscribe("key-2".to_string(), 2);
        manager.subscribe("key-3".to_string(), 3);

        let sum = Arc::new(AtomicU32::new(0));
        let sum_clone = sum.clone();

        // Only notify observers with value > 1
        manager.notify_filtered(
            move |value| {
                sum_clone.fetch_add(*value as u32, Ordering::SeqCst);
            },
            |value| *value > 1,
        );

        assert_eq!(sum.load(Ordering::SeqCst), 5); // 2 + 3
    }

    #[tokio::test]
    async fn test_notify_async() {
        let manager: ObserverManager<String, i32> = ObserverManager::new(Duration::from_secs(60));

        manager.subscribe("key-1".to_string(), 10);
        manager.subscribe("key-2".to_string(), 20);

        let sum = Arc::new(AtomicU32::new(0));
        let sum_clone = sum.clone();

        manager
            .notify_async(move |value| {
                let sum = sum_clone.clone();
                async move {
                    sum.fetch_add(value as u32, Ordering::SeqCst);
                    Ok(())
                }
            })
            .await;

        assert_eq!(sum.load(Ordering::SeqCst), 30);
    }

    #[test]
    fn test_clear_expired() {
        let manager: ObserverManager<String, i32> = ObserverManager::new(Duration::from_millis(10));

        manager.subscribe("key-1".to_string(), 100);

        // Wait for expiration
        std::thread::sleep(Duration::from_millis(20));

        let removed = manager.clear_expired();
        assert_eq!(removed, 1);
        assert!(manager.is_empty());
    }

    #[test]
    fn test_clear_expired_keeps_fresh() {
        let manager: ObserverManager<String, i32> =
            ObserverManager::new(Duration::from_millis(100));

        manager.subscribe("key-1".to_string(), 100);
        manager.subscribe("key-2".to_string(), 200);

        // Wait a bit but not enough to expire
        std::thread::sleep(Duration::from_millis(20));

        // Renew key-1
        manager.subscribe("key-1".to_string(), 150);

        // Wait more so key-2 expires but key-1 doesn't
        std::thread::sleep(Duration::from_millis(90));

        let removed = manager.clear_expired();
        assert_eq!(removed, 1);
        assert_eq!(manager.count(), 1);
        assert!(manager.contains(&"key-1".to_string()));
    }

    #[test]
    fn test_keys() {
        let manager: ObserverManager<String, i32> = ObserverManager::new(Duration::from_secs(60));

        manager.subscribe("key-1".to_string(), 100);
        manager.subscribe("key-2".to_string(), 200);

        let mut keys = manager.keys();
        keys.sort();
        assert_eq!(keys, vec!["key-1".to_string(), "key-2".to_string()]);
    }

    #[test]
    fn test_values() {
        let manager: ObserverManager<String, i32> = ObserverManager::new(Duration::from_secs(60));

        manager.subscribe("key-1".to_string(), 100);
        manager.subscribe("key-2".to_string(), 200);

        let mut values = manager.values();
        values.sort();
        assert_eq!(values, vec![100, 200]);
    }

    #[test]
    fn test_debug() {
        let manager: ObserverManager<String, i32> = ObserverManager::new(Duration::from_secs(60));
        manager.subscribe("key-1".to_string(), 100);

        let debug = format!("{:?}", manager);
        assert!(debug.contains("ObserverManager"));
        assert!(debug.contains("count"));
        assert!(debug.contains("1"));
    }

    #[test]
    fn test_concurrent_subscribe_unsubscribe() {
        use std::thread;

        let manager = Arc::new(ObserverManager::<i32, i32>::new(Duration::from_secs(60)));

        let handles: Vec<_> = (0..10)
            .map(|i| {
                let manager = manager.clone();
                thread::spawn(move || {
                    for j in 0..100 {
                        let key = i * 100 + j;
                        manager.subscribe(key, key * 2);
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(manager.count(), 1000);
    }

    #[test]
    fn test_notify_removes_panicking_observer() {
        let manager: ObserverManager<String, i32> = ObserverManager::new(Duration::from_secs(60));

        manager.subscribe("good-1".to_string(), 1);
        manager.subscribe("bad".to_string(), -1);
        manager.subscribe("good-2".to_string(), 2);

        // Notify with a handler that panics on negative values
        manager.notify(|value| {
            if *value < 0 {
                panic!("Negative value!");
            }
        });

        // Bad observer should be removed
        assert_eq!(manager.count(), 2);
        assert!(!manager.contains(&"bad".to_string()));
        assert!(manager.contains(&"good-1".to_string()));
        assert!(manager.contains(&"good-2".to_string()));
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn prop_subscribe_unsubscribe_count(operations in proptest::collection::vec((0..100i32, any::<bool>()), 0..100)) {
            let manager: ObserverManager<i32, i32> = ObserverManager::new(Duration::from_secs(60));
            let mut expected_keys: std::collections::HashSet<i32> = std::collections::HashSet::new();

            for (key, subscribe) in operations {
                if subscribe {
                    manager.subscribe(key, key * 2);
                    expected_keys.insert(key);
                } else {
                    manager.unsubscribe(&key);
                    expected_keys.remove(&key);
                }
            }

            prop_assert_eq!(manager.count(), expected_keys.len());
        }

        #[test]
        fn prop_get_returns_subscribed_value(key in 0..1000i32, value in 0..1000i32) {
            let manager: ObserverManager<i32, i32> = ObserverManager::new(Duration::from_secs(60));
            manager.subscribe(key, value);
            prop_assert_eq!(manager.get(&key), Some(value));
        }

        #[test]
        fn prop_clear_empties_manager(keys in proptest::collection::vec(0..100i32, 0..50)) {
            let manager: ObserverManager<i32, i32> = ObserverManager::new(Duration::from_secs(60));

            for key in keys {
                manager.subscribe(key, key * 2);
            }

            manager.clear();
            prop_assert!(manager.is_empty());
        }
    }
}
