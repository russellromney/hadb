//! Webhook notifications for failure events.
//!
//! Generic webhook infrastructure for the hadb ecosystem. Sends HTTP POST
//! notifications when failures occur:
//! - upload_failed: Persistent upload failure after retries exhausted
//! - auth_failure: Authentication/authorization error
//! - corruption_detected: Data corruption detected via checksum
//! - circuit_breaker_open: Circuit breaker tripped due to cascading failures
//!
//! Engine-specific events (e.g., sync_failed, segment_corrupt) are defined
//! in each product crate and sent as string event names.

use crate::config::WebhookConfig;
use anyhow::Result;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Common webhook event types shared across the ecosystem.
///
/// Engines can also send custom event names as strings via `WebhookSender::send()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WebhookEvent {
    /// Upload failed after all retries exhausted.
    UploadFailed,
    /// Authentication or authorization error.
    AuthFailure,
    /// Data corruption detected.
    CorruptionDetected,
    /// Circuit breaker opened due to cascading failures.
    CircuitBreakerOpen,
}

impl WebhookEvent {
    pub fn as_str(&self) -> &'static str {
        match self {
            WebhookEvent::UploadFailed => "upload_failed",
            WebhookEvent::AuthFailure => "auth_failure",
            WebhookEvent::CorruptionDetected => "corruption_detected",
            WebhookEvent::CircuitBreakerOpen => "circuit_breaker_open",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "upload_failed" => Some(WebhookEvent::UploadFailed),
            "auth_failure" => Some(WebhookEvent::AuthFailure),
            "corruption_detected" => Some(WebhookEvent::CorruptionDetected),
            "circuit_breaker_open" => Some(WebhookEvent::CircuitBreakerOpen),
            _ => None,
        }
    }
}

/// Webhook payload sent to configured URLs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookPayload {
    /// Event type (can be a standard WebhookEvent or a custom engine-specific string).
    pub event: String,
    /// Database or resource name.
    pub database: String,
    /// Error message.
    pub error: String,
    /// Number of retry attempts made.
    pub attempts: u32,
    /// ISO 8601 timestamp.
    pub timestamp: String,
    /// Sender version (crate using hadb-io).
    pub version: String,
    /// Optional additional context.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<serde_json::Value>,
}

impl WebhookPayload {
    /// Create a new payload from a standard event.
    pub fn new(event: WebhookEvent, database: &str, error: &str, attempts: u32) -> Self {
        Self {
            event: event.as_str().to_string(),
            database: database.to_string(),
            error: error.to_string(),
            attempts,
            timestamp: Utc::now().to_rfc3339(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            context: None,
        }
    }

    /// Create a payload with a custom event name (for engine-specific events).
    pub fn custom(event: &str, database: &str, error: &str, attempts: u32) -> Self {
        Self {
            event: event.to_string(),
            database: database.to_string(),
            error: error.to_string(),
            attempts,
            timestamp: Utc::now().to_rfc3339(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            context: None,
        }
    }

    pub fn with_context(mut self, context: serde_json::Value) -> Self {
        self.context = Some(context);
        self
    }
}

/// Webhook sender for dispatching failure notifications.
pub struct WebhookSender {
    configs: Vec<WebhookConfig>,
    client: reqwest::Client,
}

impl WebhookSender {
    /// Create a new webhook sender.
    pub fn new(configs: Vec<WebhookConfig>) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .expect("failed to build webhook HTTP client");

        Self { configs, client }
    }

    /// Check if any webhooks are configured.
    pub fn is_empty(&self) -> bool {
        self.configs.is_empty()
    }

    /// Send webhook notification for an event.
    ///
    /// Sends to all configured webhooks that subscribe to this event type.
    /// Failures are logged at error level but don't propagate — webhooks are best-effort
    /// but must not be silently swallowed.
    pub async fn send(&self, payload: WebhookPayload) {
        let event_name = &payload.event;

        for config in &self.configs {
            if !config.events.iter().any(|e| e == event_name) {
                continue;
            }

            if let Err(e) = self.send_to_webhook(config, &payload).await {
                tracing::error!("Failed to send webhook to {}: {}", config.url, e);
            }
        }
    }

    /// Send payload to a single webhook.
    async fn send_to_webhook(
        &self,
        config: &WebhookConfig,
        payload: &WebhookPayload,
    ) -> Result<()> {
        let body = serde_json::to_string(payload)?;

        let mut request = self
            .client
            .post(&config.url)
            .header("Content-Type", "application/json")
            .header(
                "User-Agent",
                format!("hadb-io/{}", env!("CARGO_PKG_VERSION")),
            );

        // Add HMAC signature if secret is configured
        if let Some(ref secret) = config.secret {
            let signature = compute_hmac_signature(secret, &body);
            request = request.header("X-Hadb-Signature", signature);
        }

        let response = request.body(body).send().await?;

        if !response.status().is_success() {
            tracing::error!(
                "Webhook {} returned status {}: {}",
                config.url,
                response.status(),
                response.text().await.unwrap_or_default()
            );
        } else {
            tracing::debug!("Webhook sent successfully to {}", config.url);
        }

        Ok(())
    }

    /// Send upload_failed notification.
    pub async fn notify_upload_failed(&self, database: &str, error: &str, attempts: u32) {
        let payload = WebhookPayload::new(WebhookEvent::UploadFailed, database, error, attempts);
        self.send(payload).await;
    }

    /// Send auth_failure notification.
    pub async fn notify_auth_failure(&self, database: &str, error: &str) {
        let payload = WebhookPayload::new(WebhookEvent::AuthFailure, database, error, 1);
        self.send(payload).await;
    }

    /// Send corruption_detected notification.
    pub async fn notify_corruption(&self, database: &str, error: &str) {
        let payload = WebhookPayload::new(WebhookEvent::CorruptionDetected, database, error, 0);
        self.send(payload).await;
    }

    /// Send circuit_breaker_open notification.
    pub async fn notify_circuit_breaker_open(&self, database: &str, consecutive_failures: u32) {
        let payload = WebhookPayload::new(
            WebhookEvent::CircuitBreakerOpen,
            database,
            &format!(
                "Circuit breaker opened after {} consecutive failures",
                consecutive_failures
            ),
            consecutive_failures,
        );
        self.send(payload).await;
    }
}

/// Compute HMAC-SHA256 signature for webhook payload.
pub fn compute_hmac_signature(secret: &str, body: &str) -> String {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    type HmacSha256 = Hmac<Sha256>;

    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC can take key of any size");
    mac.update(body.as_bytes());
    let result = mac.finalize();

    format!("sha256={}", hex::encode(result.into_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_webhook_event_serialization() {
        assert_eq!(WebhookEvent::UploadFailed.as_str(), "upload_failed");
        assert_eq!(WebhookEvent::AuthFailure.as_str(), "auth_failure");
        assert_eq!(
            WebhookEvent::CorruptionDetected.as_str(),
            "corruption_detected"
        );
        assert_eq!(
            WebhookEvent::CircuitBreakerOpen.as_str(),
            "circuit_breaker_open"
        );
    }

    #[test]
    fn test_webhook_event_from_str() {
        assert_eq!(
            WebhookEvent::parse("upload_failed"),
            Some(WebhookEvent::UploadFailed)
        );
        assert_eq!(
            WebhookEvent::parse("auth_failure"),
            Some(WebhookEvent::AuthFailure)
        );
        assert_eq!(WebhookEvent::parse("invalid"), None);
    }

    #[test]
    fn test_webhook_payload_creation() {
        let payload =
            WebhookPayload::new(WebhookEvent::UploadFailed, "mydb", "Connection timeout", 5);

        assert_eq!(payload.event, "upload_failed");
        assert_eq!(payload.database, "mydb");
        assert_eq!(payload.error, "Connection timeout");
        assert_eq!(payload.attempts, 5);
        assert!(!payload.timestamp.is_empty());
    }

    #[test]
    fn test_webhook_payload_custom_event() {
        let payload = WebhookPayload::custom("sync_failed", "mydb", "Error", 3);
        assert_eq!(payload.event, "sync_failed");
    }

    #[test]
    fn test_webhook_payload_json() {
        let payload = WebhookPayload::new(WebhookEvent::UploadFailed, "mydb", "Error", 3);

        let json = serde_json::to_string(&payload).unwrap();
        assert!(json.contains("\"event\":\"upload_failed\""));
        assert!(json.contains("\"database\":\"mydb\""));
        // context should be absent when None
        assert!(!json.contains("context"));
    }

    #[test]
    fn test_webhook_payload_with_context() {
        let payload = WebhookPayload::new(WebhookEvent::UploadFailed, "mydb", "Error", 3)
            .with_context(serde_json::json!({"key": "s3://bucket/path"}));

        let json = serde_json::to_string(&payload).unwrap();
        assert!(json.contains("\"context\""));
        assert!(json.contains("s3://bucket/path"));
    }

    #[test]
    fn test_hmac_signature() {
        let signature = compute_hmac_signature("secret", "test body");
        assert!(signature.starts_with("sha256="));
        assert_eq!(signature.len(), 71); // "sha256=" (7) + 64 hex chars
    }

    #[test]
    fn test_hmac_signature_deterministic() {
        let sig1 = compute_hmac_signature("secret", "test body");
        let sig2 = compute_hmac_signature("secret", "test body");
        assert_eq!(sig1, sig2);
    }

    #[test]
    fn test_hmac_signature_different_inputs() {
        let sig1 = compute_hmac_signature("secret", "body1");
        let sig2 = compute_hmac_signature("secret", "body2");
        assert_ne!(sig1, sig2);
    }

    #[test]
    fn test_webhook_sender_empty() {
        let sender = WebhookSender::new(vec![]);
        assert!(sender.is_empty());
    }

    #[test]
    fn test_webhook_config_event_filtering() {
        let config = WebhookConfig {
            url: "https://example.com/webhook".to_string(),
            events: vec!["upload_failed".to_string(), "auth_failure".to_string()],
            secret: None,
        };

        assert!(config.events.iter().any(|e| e == "upload_failed"));
        assert!(config.events.iter().any(|e| e == "auth_failure"));
        assert!(!config.events.iter().any(|e| e == "corruption_detected"));
    }
}
