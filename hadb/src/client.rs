//! Generic HA client with leader discovery and HTTP forwarding.
//!
//! Database-agnostic: knows nothing about SQL, Cypher, or any wire format.
//! Discovers a leader from S3 (via LeaseStore), forwards typed JSON requests,
//! retries on failure with automatic leader re-discovery.
//!
//! ```ignore
//! use hadb::{HaClient, InMemoryLeaseStore};
//!
//! let client = HaClient::builder()
//!     .lease_store(lease_store)
//!     .prefix("myapp/")
//!     .db_name("mydb")
//!     .secret("token")
//!     .connect()
//!     .await?;
//!
//! let result: MyResponse = client.forward("/api/endpoint", &my_request).await?;
//! ```

use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{anyhow, Result};
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::lease::DbLease;
use crate::traits::LeaseStore;

const DEFAULT_PREFIX: &str = "ha/";
const DEFAULT_DB_NAME: &str = "db";
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

/// Builder for creating a generic HA client.
pub struct HaClientBuilder {
    lease_store: Option<Arc<dyn LeaseStore>>,
    prefix: String,
    db_name: String,
    timeout: Duration,
    secret: Option<String>,
}

impl HaClientBuilder {
    fn new() -> Self {
        Self {
            lease_store: None,
            prefix: DEFAULT_PREFIX.to_string(),
            db_name: DEFAULT_DB_NAME.to_string(),
            timeout: DEFAULT_TIMEOUT,
            secret: None,
        }
    }

    /// Set the lease store for leader discovery.
    pub fn lease_store(mut self, store: Arc<dyn LeaseStore>) -> Self {
        self.lease_store = Some(store);
        self
    }

    /// S3 key prefix. Default: "ha/".
    pub fn prefix(mut self, prefix: &str) -> Self {
        self.prefix = prefix.to_string();
        self
    }

    /// Database name. Default: "db".
    pub fn db_name(mut self, name: &str) -> Self {
        self.db_name = name.to_string();
        self
    }

    /// HTTP request timeout. Default: 5s.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Shared secret for Bearer token auth.
    pub fn secret(mut self, secret: &str) -> Self {
        self.secret = Some(secret.to_string());
        self
    }

    /// Connect by discovering the leader from the lease store.
    pub async fn connect(self) -> Result<HaClient> {
        let lease_store = self
            .lease_store
            .ok_or_else(|| anyhow!("lease_store is required"))?;

        let http_client = reqwest::Client::builder()
            .timeout(self.timeout)
            .build()?;

        let leader_addr = discover_leader(&lease_store, &self.prefix, &self.db_name).await?;

        Ok(HaClient {
            lease_store,
            prefix: self.prefix,
            db_name: self.db_name,
            http_client,
            leader_address: RwLock::new(leader_addr),
            secret: self.secret,
        })
    }
}

/// Generic HA client: leader discovery + HTTP forwarding with retry.
///
/// Database-agnostic. Use `forward()` to send typed requests to the leader.
/// On connection failure, re-discovers the leader from the lease store and retries once.
pub struct HaClient {
    lease_store: Arc<dyn LeaseStore>,
    prefix: String,
    db_name: String,
    http_client: reqwest::Client,
    leader_address: RwLock<String>,
    secret: Option<String>,
}

impl HaClient {
    /// Start building a client.
    pub fn builder() -> HaClientBuilder {
        HaClientBuilder::new()
    }

    /// Create a disconnected client with no leader.
    ///
    /// `forward()` will return an error. Useful for read-replica-only clients
    /// that never need to contact the leader.
    pub fn disconnected() -> Self {
        Self {
            lease_store: Arc::new(crate::InMemoryLeaseStore::new()),
            prefix: String::new(),
            db_name: String::new(),
            http_client: reqwest::Client::new(),
            leader_address: RwLock::new(String::new()),
            secret: None,
        }
    }

    /// Forward a typed request to the leader with automatic retry.
    ///
    /// On connection failure, re-discovers the leader and retries once.
    /// The request and response types must implement serde Serialize/Deserialize.
    pub async fn forward<Req: Serialize, Resp: DeserializeOwned>(
        &self,
        path: &str,
        body: &Req,
    ) -> Result<Resp> {
        let raw = self.post_with_retry(path, body).await?;
        serde_json::from_value(raw).map_err(|e| anyhow!("Failed to parse response: {}", e))
    }

    /// Get the current leader address (cached — may be stale).
    pub fn leader_address(&self) -> String {
        self.leader_address.read().unwrap().clone()
    }

    /// Returns true if a leader address is known.
    pub fn has_leader(&self) -> bool {
        !self.leader_address.read().unwrap().is_empty()
    }

    /// Force re-discovery of the leader from the lease store.
    pub async fn refresh_leader(&self) -> Result<()> {
        let addr = discover_leader(&self.lease_store, &self.prefix, &self.db_name).await?;
        *self.leader_address.write().unwrap() = addr;
        Ok(())
    }

    // ========================================================================
    // Internal
    // ========================================================================

    /// POST to the leader with automatic retry on connection failure.
    async fn post_with_retry<T: Serialize>(
        &self,
        path: &str,
        body: &T,
    ) -> Result<serde_json::Value> {
        let addr = self.leader_address.read().unwrap().clone();
        if addr.is_empty() {
            return Err(anyhow!(
                "No leader address — cannot forward request. \
                 If this is a disconnected client, use it for local reads only."
            ));
        }
        let url = format!("{}{}", addr, path);

        match self.do_post(&url, body).await {
            Ok(val) => Ok(val),
            Err(first_err) => {
                tracing::warn!(
                    "Request to {} failed ({}), re-discovering leader...",
                    url,
                    first_err
                );
                match discover_leader(&self.lease_store, &self.prefix, &self.db_name).await {
                    Ok(new_addr) => {
                        *self.leader_address.write().unwrap() = new_addr.clone();
                        let retry_url = format!("{}{}", new_addr, path);
                        self.do_post(&retry_url, body).await
                    }
                    Err(discover_err) => Err(anyhow!(
                        "Request failed ({}) and leader re-discovery also failed ({})",
                        first_err,
                        discover_err
                    )),
                }
            }
        }
    }

    async fn do_post<T: Serialize>(
        &self,
        url: &str,
        body: &T,
    ) -> Result<serde_json::Value> {
        let mut req = self.http_client.post(url).json(body);
        if let Some(ref secret) = self.secret {
            req = req.bearer_auth(secret);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| anyhow!("Request to {} failed: {}", url, e))?;

        if !resp.status().is_success() {
            return Err(anyhow!(
                "Leader returned error: {} {}",
                resp.status(),
                resp.text().await.unwrap_or_default()
            ));
        }

        resp.json()
            .await
            .map_err(|e| anyhow!("Failed to parse response: {}", e))
    }
}

/// Discover the leader's address by reading the lease from the store.
async fn discover_leader(
    lease_store: &Arc<dyn LeaseStore>,
    prefix: &str,
    db_name: &str,
) -> Result<String> {
    let lease_key = format!("{}{}/_lease.json", prefix, db_name);
    let lease = DbLease::new(
        lease_store.clone(),
        &lease_key,
        "client", // dummy instance_id — we never claim
        "client",
        5,
    );

    match lease.read().await? {
        Some((data, _etag)) => {
            if data.sleeping {
                Err(anyhow!("Cluster is sleeping — no active leader"))
            } else if data.is_expired() {
                Err(anyhow!("Leader lease is expired — no active leader"))
            } else if data.address.is_empty() {
                Err(anyhow!("Leader lease has no address"))
            } else {
                Ok(data.address)
            }
        }
        None => Err(anyhow!("No lease found — cluster not initialized")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_disconnected_has_no_leader() {
        let client = HaClient::disconnected();
        assert!(!client.has_leader());
        assert!(client.leader_address().is_empty());
    }

    #[tokio::test]
    async fn test_forward_no_leader_errors() {
        let client = HaClient::disconnected();
        let result: Result<serde_json::Value> = client
            .forward("/test", &serde_json::json!({"key": "value"}))
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("No leader address"), "got: {}", err);
    }

    #[tokio::test]
    async fn test_builder_connects_with_lease() {
        let store = Arc::new(crate::InMemoryLeaseStore::new()) as Arc<dyn LeaseStore>;

        // Pre-populate a lease so discovery succeeds.
        let lease_data = crate::LeaseData {
            instance_id: "leader-1".to_string(),
            address: "http://127.0.0.1:9999".to_string(),
            session_id: "sess-1".to_string(),
            claimed_at: chrono::Utc::now().timestamp() as u64,
            ttl_secs: 300,
            sleeping: false,
        };
        let data = serde_json::to_vec(&lease_data).unwrap();
        let key = format!("test-prefix/testdb/_lease.json");
        store.write_if_not_exists(&key, data).await.unwrap();

        let client = HaClient::builder()
            .lease_store(store)
            .prefix("test-prefix/")
            .db_name("testdb")
            .connect()
            .await
            .unwrap();

        assert!(client.has_leader());
        assert_eq!(client.leader_address(), "http://127.0.0.1:9999");
    }
}
