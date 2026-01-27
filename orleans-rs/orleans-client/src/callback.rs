//! Callback management for request/response correlation.
//!
//! This module provides the infrastructure for tracking pending requests
//! and completing them when responses arrive.

use crate::error::{ClientError, ClientResult};
use dashmap::DashMap;
use orleans_messaging::{CorrelationId, Message};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;
use tracing::{debug, instrument, trace, warn};

/// Data associated with a pending callback.
#[derive(Debug)]
pub struct CallbackData {
    /// The original request message.
    request: Message,

    /// Sender for the response.
    sender: oneshot::Sender<ClientResult<Message>>,

    /// When the request was sent.
    sent_at: Instant,

    /// Timeout for the request.
    timeout: Duration,
}

impl CallbackData {
    /// Create new callback data.
    pub fn new(
        request: Message,
        sender: oneshot::Sender<ClientResult<Message>>,
        timeout: Duration,
    ) -> Self {
        Self {
            request,
            sender,
            sent_at: Instant::now(),
            timeout,
        }
    }

    /// Get the original request.
    pub fn request(&self) -> &Message {
        &self.request
    }

    /// Get when the request was sent.
    pub fn sent_at(&self) -> Instant {
        self.sent_at
    }

    /// Get the timeout.
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Check if the request has timed out.
    pub fn is_timed_out(&self) -> bool {
        self.sent_at.elapsed() > self.timeout
    }

    /// Get the remaining time before timeout.
    pub fn remaining_time(&self) -> Duration {
        self.timeout.saturating_sub(self.sent_at.elapsed())
    }

    /// Complete the callback with a response.
    pub fn complete(self, response: ClientResult<Message>) {
        let _ = self.sender.send(response);
    }
}

/// Manager for pending callbacks.
///
/// Tracks all pending requests and their associated callbacks,
/// allowing responses to be matched with their requests.
pub struct CallbackDataManager {
    /// Map of correlation ID to callback data.
    callbacks: DashMap<CorrelationId, CallbackData>,

    /// Counter for active callbacks.
    active_count: AtomicUsize,

    /// Maximum number of pending callbacks.
    max_pending: usize,
}

impl CallbackDataManager {
    /// Create a new callback data manager.
    pub fn new(max_pending: usize) -> Self {
        Self {
            callbacks: DashMap::new(),
            active_count: AtomicUsize::new(0),
            max_pending,
        }
    }

    /// Get the number of active callbacks.
    pub fn active_count(&self) -> usize {
        self.active_count.load(Ordering::Relaxed)
    }

    /// Check if we can accept more callbacks.
    pub fn can_accept(&self) -> bool {
        self.active_count() < self.max_pending
    }

    /// Add a callback for a pending request.
    ///
    /// Returns a receiver that will complete when the response arrives.
    #[instrument(skip(self, request), fields(correlation_id = %request.id()))]
    pub fn add(
        &self,
        request: Message,
        timeout: Duration,
    ) -> ClientResult<oneshot::Receiver<ClientResult<Message>>> {
        if !self.can_accept() {
            return Err(ClientError::Internal(format!(
                "too many pending requests (max {})",
                self.max_pending
            )));
        }

        let (sender, receiver) = oneshot::channel();
        let correlation_id = request.id().clone();

        trace!(
            correlation_id = %correlation_id,
            timeout_ms = timeout.as_millis(),
            "adding callback"
        );

        let callback_data = CallbackData::new(request, sender, timeout);
        self.callbacks.insert(correlation_id, callback_data);
        self.active_count.fetch_add(1, Ordering::Relaxed);

        Ok(receiver)
    }

    /// Try to complete a callback with a response.
    ///
    /// Returns true if the callback was found and completed.
    #[instrument(skip(self, response), fields(correlation_id = %response.id()))]
    pub fn try_complete(&self, response: Message) -> bool {
        let correlation_id = response.id().clone();

        if let Some((_, callback_data)) = self.callbacks.remove(&correlation_id) {
            let elapsed = callback_data.sent_at.elapsed();

            trace!(
                correlation_id = %correlation_id,
                elapsed_ms = elapsed.as_millis(),
                "completing callback"
            );

            self.active_count.fetch_sub(1, Ordering::Relaxed);
            callback_data.complete(Ok(response));
            true
        } else {
            debug!(
                correlation_id = %correlation_id,
                "callback not found for response"
            );
            false
        }
    }

    /// Remove a callback and return its data.
    pub fn remove(&self, correlation_id: &CorrelationId) -> Option<CallbackData> {
        if let Some((_, callback_data)) = self.callbacks.remove(correlation_id) {
            self.active_count.fetch_sub(1, Ordering::Relaxed);
            Some(callback_data)
        } else {
            None
        }
    }

    /// Fail a callback with an error.
    #[instrument(skip(self))]
    pub fn fail(&self, correlation_id: &CorrelationId, error: ClientError) -> bool {
        if let Some(callback_data) = self.remove(correlation_id) {
            trace!(
                correlation_id = %correlation_id,
                error = %error,
                "failing callback"
            );
            callback_data.complete(Err(error));
            true
        } else {
            false
        }
    }

    /// Expire all timed out callbacks.
    ///
    /// Returns the number of expired callbacks.
    #[instrument(skip(self))]
    pub fn expire_timed_out(&self) -> usize {
        let mut expired = Vec::new();

        // First pass: collect expired callbacks
        for entry in self.callbacks.iter() {
            if entry.value().is_timed_out() {
                expired.push(entry.key().clone());
            }
        }

        let count = expired.len();

        // Second pass: fail expired callbacks
        for correlation_id in expired {
            if let Some(callback_data) = self.remove(&correlation_id) {
                let timeout = callback_data.timeout;
                warn!(
                    correlation_id = %correlation_id,
                    timeout_ms = timeout.as_millis(),
                    "request timed out"
                );
                callback_data.complete(Err(ClientError::RequestTimeout(timeout)));
            }
        }

        if count > 0 {
            debug!(expired_count = count, "expired timed out callbacks");
        }

        count
    }

    /// Fail all pending callbacks with an error.
    ///
    /// Returns the number of failed callbacks.
    #[instrument(skip(self))]
    pub fn fail_all(&self, error: ClientError) -> usize {
        let mut count = 0;

        // Collect all keys first to avoid holding locks
        let keys: Vec<_> = self.callbacks.iter().map(|e| e.key().clone()).collect();

        for correlation_id in keys {
            if let Some(callback_data) = self.remove(&correlation_id) {
                callback_data.complete(Err(ClientError::Internal(error.to_string())));
                count += 1;
            }
        }

        debug!(failed_count = count, "failed all callbacks");
        count
    }

    /// Clear all callbacks without failing them.
    pub fn clear(&self) {
        self.callbacks.clear();
        self.active_count.store(0, Ordering::Relaxed);
    }
}

impl Drop for CallbackDataManager {
    fn drop(&mut self) {
        let remaining = self.active_count();
        if remaining > 0 {
            warn!(
                remaining_callbacks = remaining,
                "callback manager dropped with pending callbacks"
            );
        }
    }
}

/// Helper to start a background task that expires timed out callbacks.
pub fn start_expiration_task(
    manager: Arc<CallbackDataManager>,
    interval: Duration,
    shutdown: tokio_util::sync::CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval_timer = tokio::time::interval(interval);
        loop {
            tokio::select! {
                _ = interval_timer.tick() => {
                    manager.expire_timed_out();
                }
                _ = shutdown.cancelled() => {
                    trace!("callback expiration task shutting down");
                    break;
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use orleans_core::{GrainId, GrainType, IdSpan, SiloAddress};
    use orleans_messaging::GrainInterfaceType;

    fn create_test_message() -> Message {
        let silo = SiloAddress::new("127.0.0.1:11111".parse().unwrap(), 1);
        Message::new_request(
            GrainId::new(GrainType::create("Test"), IdSpan::from_str("key")),
            GrainInterfaceType::create("ITest"),
            1,
            Bytes::new(),
            silo,
        )
    }

    #[test]
    fn test_callback_data_timeout() {
        let (sender, _receiver) = oneshot::channel();
        let callback = CallbackData::new(
            create_test_message(),
            sender,
            Duration::from_millis(100),
        );

        assert!(!callback.is_timed_out());
        assert!(callback.remaining_time() > Duration::ZERO);
    }

    #[test]
    fn test_callback_manager_creation() {
        let manager = CallbackDataManager::new(1000);
        assert_eq!(manager.active_count(), 0);
        assert!(manager.can_accept());
    }

    #[test]
    fn test_callback_manager_add_and_complete() {
        let manager = CallbackDataManager::new(100);
        let request = create_test_message();
        let correlation_id = request.id().clone();

        let receiver = manager.add(request, Duration::from_secs(30)).unwrap();
        assert_eq!(manager.active_count(), 1);

        let response = create_test_message();
        // Create a response with the same correlation ID
        let mut response = Message::new_request(
            response.target_grain().clone(),
            response.interface_type().clone(),
            response.method_id(),
            Bytes::new(),
            response.sending_silo().clone(),
        );
        // Set the correlation ID to match
        response.id = correlation_id;

        assert!(manager.try_complete(response));
        assert_eq!(manager.active_count(), 0);
    }

    #[test]
    fn test_callback_manager_fail() {
        let manager = CallbackDataManager::new(100);
        let request = create_test_message();
        let correlation_id = request.id().clone();

        let _receiver = manager.add(request, Duration::from_secs(30)).unwrap();
        assert_eq!(manager.active_count(), 1);

        assert!(manager.fail(&correlation_id, ClientError::GatewayDisconnected("test".into())));
        assert_eq!(manager.active_count(), 0);
    }

    #[test]
    fn test_callback_manager_max_pending() {
        let manager = CallbackDataManager::new(2);

        let _r1 = manager.add(create_test_message(), Duration::from_secs(30)).unwrap();
        let _r2 = manager.add(create_test_message(), Duration::from_secs(30)).unwrap();

        assert!(!manager.can_accept());

        let result = manager.add(create_test_message(), Duration::from_secs(30));
        assert!(result.is_err());
    }

    #[test]
    fn test_callback_manager_fail_all() {
        let manager = CallbackDataManager::new(100);

        let _r1 = manager.add(create_test_message(), Duration::from_secs(30)).unwrap();
        let _r2 = manager.add(create_test_message(), Duration::from_secs(30)).unwrap();
        let _r3 = manager.add(create_test_message(), Duration::from_secs(30)).unwrap();

        assert_eq!(manager.active_count(), 3);

        let count = manager.fail_all(ClientError::ShuttingDown);
        assert_eq!(count, 3);
        assert_eq!(manager.active_count(), 0);
    }

    #[tokio::test]
    async fn test_callback_response_received() {
        let manager = Arc::new(CallbackDataManager::new(100));
        let request = create_test_message();
        let correlation_id = request.id().clone();

        let receiver = manager.add(request, Duration::from_secs(30)).unwrap();

        // Spawn a task to send the response
        let manager_clone = manager.clone();
        let response = create_test_message();
        let mut response = Message::new_request(
            response.target_grain().clone(),
            response.interface_type().clone(),
            response.method_id(),
            Bytes::new(),
            response.sending_silo().clone(),
        );
        response.id = correlation_id;

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            manager_clone.try_complete(response);
        });

        let result = receiver.await.unwrap();
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_callback_timeout_expiration() {
        let manager = CallbackDataManager::new(100);

        // Add a callback with very short timeout
        let request = create_test_message();
        let _receiver = manager.add(request, Duration::from_millis(1)).unwrap();

        // Wait for timeout
        tokio::time::sleep(Duration::from_millis(10)).await;

        let expired = manager.expire_timed_out();
        assert_eq!(expired, 1);
        assert_eq!(manager.active_count(), 0);
    }

    #[test]
    fn test_callback_remove_not_found() {
        let manager = CallbackDataManager::new(100);
        let fake_id = create_test_message().id().clone();

        assert!(manager.remove(&fake_id).is_none());
        assert!(!manager.fail(&fake_id, ClientError::Internal("test".into())));
    }
}
