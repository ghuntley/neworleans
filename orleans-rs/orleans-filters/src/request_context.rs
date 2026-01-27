//! Request context for propagating data through async call chains.
//!
//! Request context provides a mechanism for passing contextual information
//! (such as user identity, tracing IDs, etc.) through grain call chains
//! without explicitly passing them as method parameters.
//!
//! The context uses task-local storage to maintain isolation between
//! concurrent requests.

use parking_lot::RwLock;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use crate::error::{FilterError, FilterResult};

/// A boxed context value that can be cloned.
struct ContextValueBox {
    value: Box<dyn Any + Send + Sync>,
    clone_fn: fn(&(dyn Any + Send + Sync)) -> Box<dyn Any + Send + Sync>,
}

impl ContextValueBox {
    fn new<T: Clone + Send + Sync + 'static>(value: T) -> Self {
        Self {
            value: Box::new(value),
            clone_fn: |any| {
                let typed = any.downcast_ref::<T>().expect("Type mismatch in ContextValueBox");
                Box::new(typed.clone())
            },
        }
    }

    fn get<T: 'static>(&self) -> Option<&T> {
        self.value.downcast_ref::<T>()
    }
}

impl Clone for ContextValueBox {
    fn clone(&self) -> Self {
        Self {
            value: (self.clone_fn)(&*self.value),
            clone_fn: self.clone_fn,
        }
    }
}

/// Thread-local request context storage.
///
/// This holds context data that flows through async call chains.
/// Uses copy-on-write semantics for isolation.
#[derive(Clone, Default)]
pub struct ContextProperties {
    /// The context values.
    values: HashMap<String, ContextValueBox>,
}

impl ContextProperties {
    /// Create a new empty context.
    pub fn new() -> Self {
        Self {
            values: HashMap::new(),
        }
    }

    /// Get a value from the context.
    pub fn get<T: Clone + 'static>(&self, key: &str) -> Option<T> {
        self.values.get(key).and_then(|v| v.get::<T>().cloned())
    }

    /// Set a value in the context.
    pub fn set<T: Clone + Send + Sync + 'static>(&mut self, key: impl Into<String>, value: T) {
        self.values.insert(key.into(), ContextValueBox::new(value));
    }

    /// Remove a value from the context.
    pub fn remove(&mut self, key: &str) -> bool {
        self.values.remove(key).is_some()
    }

    /// Clear all values from the context.
    pub fn clear(&mut self) {
        self.values.clear();
    }

    /// Check if a key exists in the context.
    pub fn contains_key(&self, key: &str) -> bool {
        self.values.contains_key(key)
    }

    /// Get all keys in the context.
    pub fn keys(&self) -> impl Iterator<Item = &str> {
        self.values.keys().map(|s| s.as_str())
    }

    /// Get the number of values in the context.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Check if the context is empty.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Merge another context's values into this one.
    pub fn merge(&mut self, other: &ContextProperties) {
        for (key, value) in &other.values {
            self.values.insert(key.clone(), value.clone());
        }
    }
}

tokio::task_local! {
    /// The current request context for this task.
    static CURRENT_CONTEXT: Arc<RwLock<ContextProperties>>;
}

/// Static accessor for the request context.
///
/// This provides a convenient interface for accessing the task-local
/// request context.
pub struct RequestContext;

impl RequestContext {
    /// Get a value from the current request context.
    ///
    /// Returns `None` if the key doesn't exist or if called outside
    /// of a request context scope.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let user_id: Option<String> = RequestContext::get("user_id");
    /// ```
    pub fn get<T: Clone + 'static>(key: &str) -> Option<T> {
        CURRENT_CONTEXT.try_with(|ctx| {
            ctx.read().get(key)
        }).ok().flatten()
    }

    /// Get a value from the current request context, returning an error if not found.
    pub fn get_required<T: Clone + 'static>(key: &str) -> FilterResult<T> {
        RequestContext::get(key).ok_or_else(|| FilterError::ContextKeyNotFound {
            key: key.to_string(),
        })
    }

    /// Set a value in the current request context.
    ///
    /// If called outside of a request context scope, this is a no-op.
    ///
    /// # Example
    ///
    /// ```ignore
    /// RequestContext::set("user_id", "user123".to_string());
    /// ```
    pub fn set<T: Clone + Send + Sync + 'static>(key: impl Into<String>, value: T) {
        let _ = CURRENT_CONTEXT.try_with(|ctx| {
            ctx.write().set(key, value);
        });
    }

    /// Remove a value from the current request context.
    ///
    /// Returns `true` if the key was present and removed.
    pub fn remove(key: &str) -> bool {
        CURRENT_CONTEXT.try_with(|ctx| {
            ctx.write().remove(key)
        }).unwrap_or(false)
    }

    /// Clear all values from the current request context.
    pub fn clear() {
        let _ = CURRENT_CONTEXT.try_with(|ctx| {
            ctx.write().clear();
        });
    }

    /// Check if a key exists in the current request context.
    pub fn contains_key(key: &str) -> bool {
        CURRENT_CONTEXT.try_with(|ctx| {
            ctx.read().contains_key(key)
        }).unwrap_or(false)
    }

    /// Get all keys in the current request context.
    pub fn keys() -> Vec<String> {
        CURRENT_CONTEXT.try_with(|ctx| {
            ctx.read().keys().map(|s| s.to_string()).collect()
        }).unwrap_or_default()
    }

    /// Get a snapshot of the current context for propagation.
    ///
    /// This is used when making outgoing calls to propagate context.
    pub fn snapshot() -> Option<ContextProperties> {
        CURRENT_CONTEXT.try_with(|ctx| {
            ctx.read().clone()
        }).ok()
    }

    /// Run a future with the given context properties.
    ///
    /// This establishes a new request context scope for the duration
    /// of the future execution.
    pub async fn scope<F, R>(properties: ContextProperties, f: F) -> R
    where
        F: std::future::Future<Output = R>,
    {
        let ctx = Arc::new(RwLock::new(properties));
        CURRENT_CONTEXT.scope(ctx, f).await
    }

    /// Run a future with an empty context.
    pub async fn with_empty_context<F, R>(f: F) -> R
    where
        F: std::future::Future<Output = R>,
    {
        Self::scope(ContextProperties::new(), f).await
    }

    /// Run a future inheriting the current context (copy-on-write).
    ///
    /// The child context starts with a copy of the current context
    /// but modifications are isolated from the parent.
    pub async fn with_inherited_context<F, R>(f: F) -> R
    where
        F: std::future::Future<Output = R>,
    {
        let properties = Self::snapshot().unwrap_or_default();
        Self::scope(properties, f).await
    }
}

/// Well-known context keys used by Orleans.
pub mod context_keys {
    /// Key for the call chain reentrancy ID.
    pub const CALL_CHAIN_REENTRANCY: &str = "Orleans.CallChainReentrancy";

    /// Key for the activity/trace ID.
    pub const TRACE_ID: &str = "Orleans.TraceId";

    /// Key for the span ID.
    pub const SPAN_ID: &str = "Orleans.SpanId";

    /// Key for the parent span ID.
    pub const PARENT_SPAN_ID: &str = "Orleans.ParentSpanId";

    /// Key for the correlation ID.
    pub const CORRELATION_ID: &str = "Orleans.CorrelationId";

    /// Key for the user identity.
    pub const USER_ID: &str = "Orleans.UserId";

    /// Key for the tenant ID.
    pub const TENANT_ID: &str = "Orleans.TenantId";
}

/// A value that can be stored in the request context.
/// This is used for type-erased storage in the context properties.
pub trait ContextValue: Send + Sync + 'static {
    /// Get the type name for error messages.
    fn type_name(&self) -> &'static str;
}

impl<T: Send + Sync + 'static> ContextValue for T {
    fn type_name(&self) -> &'static str {
        std::any::type_name::<T>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_context_properties_basic() {
        let mut ctx = ContextProperties::new();
        assert!(ctx.is_empty());

        ctx.set("key1", "value1".to_string());
        assert!(!ctx.is_empty());
        assert_eq!(ctx.len(), 1);
        assert!(ctx.contains_key("key1"));
        assert_eq!(ctx.get::<String>("key1"), Some("value1".to_string()));
    }

    #[test]
    fn test_context_properties_remove() {
        let mut ctx = ContextProperties::new();
        ctx.set("key1", "value1".to_string());
        assert!(ctx.contains_key("key1"));

        assert!(ctx.remove("key1"));
        assert!(!ctx.contains_key("key1"));
        assert!(!ctx.remove("key1"));
    }

    #[test]
    fn test_context_properties_clear() {
        let mut ctx = ContextProperties::new();
        ctx.set("key1", "value1".to_string());
        ctx.set("key2", "value2".to_string());
        assert_eq!(ctx.len(), 2);

        ctx.clear();
        assert!(ctx.is_empty());
    }

    #[test]
    fn test_context_properties_keys() {
        let mut ctx = ContextProperties::new();
        ctx.set("key1", "value1".to_string());
        ctx.set("key2", "value2".to_string());

        let keys: Vec<&str> = ctx.keys().collect();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&"key1"));
        assert!(keys.contains(&"key2"));
    }

    #[test]
    fn test_context_properties_merge() {
        let mut ctx1 = ContextProperties::new();
        ctx1.set("key1", "value1".to_string());

        let mut ctx2 = ContextProperties::new();
        ctx2.set("key2", "value2".to_string());
        ctx2.set("key1", "overwritten".to_string());

        ctx1.merge(&ctx2);
        assert_eq!(ctx1.get::<String>("key1"), Some("overwritten".to_string()));
        assert_eq!(ctx1.get::<String>("key2"), Some("value2".to_string()));
    }

    #[test]
    fn test_context_properties_different_types() {
        let mut ctx = ContextProperties::new();
        ctx.set("string_key", "hello".to_string());
        ctx.set("int_key", 42i32);
        ctx.set("bool_key", true);

        assert_eq!(ctx.get::<String>("string_key"), Some("hello".to_string()));
        assert_eq!(ctx.get::<i32>("int_key"), Some(42));
        assert_eq!(ctx.get::<bool>("bool_key"), Some(true));

        // Type mismatch returns None
        assert_eq!(ctx.get::<i32>("string_key"), None);
    }

    #[test]
    fn test_context_properties_clone() {
        let mut ctx1 = ContextProperties::new();
        ctx1.set("key1", "value1".to_string());

        let ctx2 = ctx1.clone();
        assert_eq!(ctx2.get::<String>("key1"), Some("value1".to_string()));

        // Modifications to clone don't affect original
        let mut ctx3 = ctx1.clone();
        ctx3.set("key1", "modified".to_string());
        assert_eq!(ctx1.get::<String>("key1"), Some("value1".to_string()));
        assert_eq!(ctx3.get::<String>("key1"), Some("modified".to_string()));
    }

    #[tokio::test]
    async fn test_request_context_scope() {
        let mut props = ContextProperties::new();
        props.set("user", "alice".to_string());

        RequestContext::scope(props, async {
            assert_eq!(RequestContext::get::<String>("user"), Some("alice".to_string()));

            // Nested scope with inheritance
            RequestContext::with_inherited_context(async {
                assert_eq!(RequestContext::get::<String>("user"), Some("alice".to_string()));

                // Modify in nested scope
                RequestContext::set("user", "bob".to_string());
                assert_eq!(RequestContext::get::<String>("user"), Some("bob".to_string()));
            }).await;

            // Original scope unchanged
            assert_eq!(RequestContext::get::<String>("user"), Some("alice".to_string()));
        }).await;
    }

    #[tokio::test]
    async fn test_request_context_outside_scope() {
        // Outside of a scope, get returns None
        assert_eq!(RequestContext::get::<String>("key"), None);

        // Set is a no-op
        RequestContext::set("key", "value".to_string());
        assert_eq!(RequestContext::get::<String>("key"), None);
    }

    #[tokio::test]
    async fn test_request_context_snapshot() {
        let mut props = ContextProperties::new();
        props.set("key1", "value1".to_string());

        RequestContext::scope(props, async {
            let snapshot = RequestContext::snapshot();
            assert!(snapshot.is_some());

            let snapshot = snapshot.unwrap();
            assert_eq!(snapshot.get::<String>("key1"), Some("value1".to_string()));
        }).await;
    }

    #[tokio::test]
    async fn test_request_context_keys() {
        let mut props = ContextProperties::new();
        props.set("key1", "value1".to_string());
        props.set("key2", "value2".to_string());

        RequestContext::scope(props, async {
            let keys = RequestContext::keys();
            assert_eq!(keys.len(), 2);
            assert!(keys.contains(&"key1".to_string()));
            assert!(keys.contains(&"key2".to_string()));
        }).await;
    }

    #[tokio::test]
    async fn test_request_context_contains_key() {
        let mut props = ContextProperties::new();
        props.set("key1", "value1".to_string());

        RequestContext::scope(props, async {
            assert!(RequestContext::contains_key("key1"));
            assert!(!RequestContext::contains_key("key2"));
        }).await;
    }

    #[tokio::test]
    async fn test_request_context_remove() {
        let mut props = ContextProperties::new();
        props.set("key1", "value1".to_string());

        RequestContext::scope(props, async {
            assert!(RequestContext::contains_key("key1"));
            assert!(RequestContext::remove("key1"));
            assert!(!RequestContext::contains_key("key1"));
            assert!(!RequestContext::remove("key1"));
        }).await;
    }

    #[tokio::test]
    async fn test_request_context_clear() {
        let mut props = ContextProperties::new();
        props.set("key1", "value1".to_string());
        props.set("key2", "value2".to_string());

        RequestContext::scope(props, async {
            assert_eq!(RequestContext::keys().len(), 2);
            RequestContext::clear();
            assert_eq!(RequestContext::keys().len(), 0);
        }).await;
    }

    #[tokio::test]
    async fn test_request_context_get_required() {
        let mut props = ContextProperties::new();
        props.set("key1", "value1".to_string());

        RequestContext::scope(props, async {
            let result = RequestContext::get_required::<String>("key1");
            assert_eq!(result.unwrap(), "value1".to_string());

            let result = RequestContext::get_required::<String>("missing");
            assert!(result.is_err());
        }).await;
    }

    #[test]
    fn test_context_keys_constants() {
        // Just verify the constants exist and have reasonable values
        assert!(!context_keys::CALL_CHAIN_REENTRANCY.is_empty());
        assert!(!context_keys::TRACE_ID.is_empty());
        assert!(!context_keys::CORRELATION_ID.is_empty());
    }
}
