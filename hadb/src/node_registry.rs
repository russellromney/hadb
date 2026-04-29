//! Node registration for read replica discovery.
//!
//! Each follower registers itself in storage at `{prefix}{db_name}/_nodes/{instance_id}.json`.
//! Registration includes the current leader's `session_id` — clients use this to
//! filter stale registrations from previous leadership epochs.
//!
//! Node registration uses unconditional writes (no CAS needed — each node only writes
//! its own file). Discovery lists all registrations and filters by session + TTL.

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Mutex;

/// Node registration data stored in the registry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeRegistration {
    pub instance_id: String,
    pub address: String,
    pub role: String,
    pub leader_session_id: String,
    pub last_seen: u64,
}

impl NodeRegistration {
    /// Check if this registration is valid: belongs to the current leader session
    /// and has been seen recently (within `ttl_secs`).
    pub fn is_valid(&self, current_session_id: &str, ttl_secs: u64) -> bool {
        let now = chrono::Utc::now().timestamp() as u64;
        self.leader_session_id == current_session_id && now < self.last_seen + ttl_secs
    }
}

/// Trait for node registration operations.
#[async_trait]
pub trait NodeRegistry: Send + Sync {
    /// Register a node under `{prefix}{db_name}/_nodes/{instance_id}.json`.
    async fn register(
        &self,
        prefix: &str,
        db_name: &str,
        registration: &NodeRegistration,
    ) -> Result<()>;

    /// Deregister a node by deleting its registration file.
    async fn deregister(&self, prefix: &str, db_name: &str, instance_id: &str) -> Result<()>;

    /// Discover all registered nodes for a database.
    async fn discover_all(&self, prefix: &str, db_name: &str) -> Result<Vec<NodeRegistration>>;
}

/// Build the storage key for a node's registration.
pub fn node_key(prefix: &str, db_name: &str, instance_id: &str) -> String {
    format!("{}{}/_nodes/{}.json", prefix, db_name, instance_id)
}

/// Build the prefix for listing all node registrations.
pub fn nodes_prefix(prefix: &str, db_name: &str) -> String {
    format!("{}{}/_nodes/", prefix, db_name)
}

// ============================================================================
// In-memory implementation (for testing)
// ============================================================================

/// In-memory node registry for testing.
pub struct InMemoryNodeRegistry {
    /// Key: "{prefix}{db_name}/_nodes/{instance_id}.json"
    store: Mutex<HashMap<String, NodeRegistration>>,
}

impl InMemoryNodeRegistry {
    pub fn new() -> Self {
        Self {
            store: Mutex::new(HashMap::new()),
        }
    }
}

impl Default for InMemoryNodeRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl NodeRegistry for InMemoryNodeRegistry {
    async fn register(
        &self,
        prefix: &str,
        db_name: &str,
        registration: &NodeRegistration,
    ) -> Result<()> {
        let key = node_key(prefix, db_name, &registration.instance_id);
        self.store.lock().unwrap().insert(key, registration.clone());
        Ok(())
    }

    async fn deregister(&self, prefix: &str, db_name: &str, instance_id: &str) -> Result<()> {
        let key = node_key(prefix, db_name, instance_id);
        self.store.lock().unwrap().remove(&key);
        Ok(())
    }

    async fn discover_all(&self, prefix: &str, db_name: &str) -> Result<Vec<NodeRegistration>> {
        let list_prefix = nodes_prefix(prefix, db_name);
        let store = self.store.lock().unwrap();
        Ok(store
            .iter()
            .filter(|(k, _)| k.starts_with(&list_prefix))
            .map(|(_, v)| v.clone())
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // NodeRegistration tests
    // ========================================================================

    #[test]
    fn test_node_registration_is_valid() {
        let now = chrono::Utc::now().timestamp() as u64;
        let reg = NodeRegistration {
            instance_id: "inst-1".to_string(),
            address: "addr-1".to_string(),
            role: "Follower".to_string(),
            leader_session_id: "session-abc".to_string(),
            last_seen: now,
        };

        assert!(reg.is_valid("session-abc", 60));
    }

    #[test]
    fn test_node_registration_invalid_session() {
        let now = chrono::Utc::now().timestamp() as u64;
        let reg = NodeRegistration {
            instance_id: "inst-1".to_string(),
            address: "addr-1".to_string(),
            role: "Follower".to_string(),
            leader_session_id: "session-abc".to_string(),
            last_seen: now,
        };

        assert!(!reg.is_valid("session-xyz", 60));
    }

    #[test]
    fn test_node_registration_expired() {
        let now = chrono::Utc::now().timestamp() as u64;
        let reg = NodeRegistration {
            instance_id: "inst-1".to_string(),
            address: "addr-1".to_string(),
            role: "Follower".to_string(),
            leader_session_id: "session-abc".to_string(),
            last_seen: now - 120, // 120 seconds ago
        };

        assert!(!reg.is_valid("session-abc", 60));
    }

    #[test]
    fn test_node_registration_serialization() {
        let reg = NodeRegistration {
            instance_id: "test-instance".to_string(),
            address: "localhost:9000".to_string(),
            role: "Leader".to_string(),
            leader_session_id: "session-123".to_string(),
            last_seen: 1234567890,
        };

        let json = serde_json::to_string(&reg).unwrap();
        let deserialized: NodeRegistration = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.instance_id, "test-instance");
        assert_eq!(deserialized.address, "localhost:9000");
        assert_eq!(deserialized.role, "Leader");
        assert_eq!(deserialized.leader_session_id, "session-123");
        assert_eq!(deserialized.last_seen, 1234567890);
    }

    // ========================================================================
    // Helper function tests
    // ========================================================================

    #[test]
    fn test_node_key() {
        let key = node_key("prefix/", "mydb", "inst-1");
        assert_eq!(key, "prefix/mydb/_nodes/inst-1.json");
    }

    #[test]
    fn test_node_key_empty_prefix() {
        let key = node_key("", "mydb", "inst-1");
        assert_eq!(key, "mydb/_nodes/inst-1.json");
    }

    #[test]
    fn test_nodes_prefix() {
        let prefix = nodes_prefix("prefix/", "mydb");
        assert_eq!(prefix, "prefix/mydb/_nodes/");
    }

    // ========================================================================
    // InMemoryNodeRegistry tests
    // ========================================================================

    #[tokio::test]
    async fn test_in_memory_registry_register() {
        let registry = InMemoryNodeRegistry::new();
        let now = chrono::Utc::now().timestamp() as u64;

        let reg = NodeRegistration {
            instance_id: "inst-1".to_string(),
            address: "addr-1".to_string(),
            role: "Follower".to_string(),
            leader_session_id: "session-1".to_string(),
            last_seen: now,
        };

        registry.register("", "db1", &reg).await.unwrap();

        let nodes = registry.discover_all("", "db1").await.unwrap();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].instance_id, "inst-1");
    }

    #[tokio::test]
    async fn test_in_memory_registry_deregister() {
        let registry = InMemoryNodeRegistry::new();
        let now = chrono::Utc::now().timestamp() as u64;

        let reg = NodeRegistration {
            instance_id: "inst-1".to_string(),
            address: "addr-1".to_string(),
            role: "Follower".to_string(),
            leader_session_id: "session-1".to_string(),
            last_seen: now,
        };

        registry.register("", "db1", &reg).await.unwrap();
        registry.deregister("", "db1", "inst-1").await.unwrap();

        let nodes = registry.discover_all("", "db1").await.unwrap();
        assert_eq!(nodes.len(), 0);
    }

    #[tokio::test]
    async fn test_in_memory_registry_discover_multiple() {
        let registry = InMemoryNodeRegistry::new();
        let now = chrono::Utc::now().timestamp() as u64;

        for i in 1..=3 {
            let reg = NodeRegistration {
                instance_id: format!("inst-{}", i),
                address: format!("addr-{}", i),
                role: "Follower".to_string(),
                leader_session_id: "session-1".to_string(),
                last_seen: now,
            };
            registry.register("", "db1", &reg).await.unwrap();
        }

        let nodes = registry.discover_all("", "db1").await.unwrap();
        assert_eq!(nodes.len(), 3);

        let instance_ids: Vec<_> = nodes.iter().map(|n| n.instance_id.as_str()).collect();
        assert!(instance_ids.contains(&"inst-1"));
        assert!(instance_ids.contains(&"inst-2"));
        assert!(instance_ids.contains(&"inst-3"));
    }

    #[tokio::test]
    async fn test_in_memory_registry_discover_filters_by_db() {
        let registry = InMemoryNodeRegistry::new();
        let now = chrono::Utc::now().timestamp() as u64;

        let reg1 = NodeRegistration {
            instance_id: "inst-1".to_string(),
            address: "addr-1".to_string(),
            role: "Follower".to_string(),
            leader_session_id: "session-1".to_string(),
            last_seen: now,
        };
        registry.register("", "db1", &reg1).await.unwrap();

        let reg2 = NodeRegistration {
            instance_id: "inst-2".to_string(),
            address: "addr-2".to_string(),
            role: "Follower".to_string(),
            leader_session_id: "session-1".to_string(),
            last_seen: now,
        };
        registry.register("", "db2", &reg2).await.unwrap();

        let nodes_db1 = registry.discover_all("", "db1").await.unwrap();
        assert_eq!(nodes_db1.len(), 1);
        assert_eq!(nodes_db1[0].instance_id, "inst-1");

        let nodes_db2 = registry.discover_all("", "db2").await.unwrap();
        assert_eq!(nodes_db2.len(), 1);
        assert_eq!(nodes_db2[0].instance_id, "inst-2");
    }

    #[tokio::test]
    async fn test_in_memory_registry_overwrite_registration() {
        let registry = InMemoryNodeRegistry::new();
        let now = chrono::Utc::now().timestamp() as u64;

        let reg1 = NodeRegistration {
            instance_id: "inst-1".to_string(),
            address: "addr-1".to_string(),
            role: "Follower".to_string(),
            leader_session_id: "session-1".to_string(),
            last_seen: now,
        };
        registry.register("", "db1", &reg1).await.unwrap();

        // Overwrite with new address
        let reg2 = NodeRegistration {
            instance_id: "inst-1".to_string(),
            address: "addr-2".to_string(),
            role: "Leader".to_string(),
            leader_session_id: "session-2".to_string(),
            last_seen: now + 10,
        };
        registry.register("", "db1", &reg2).await.unwrap();

        let nodes = registry.discover_all("", "db1").await.unwrap();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].address, "addr-2");
        assert_eq!(nodes[0].role, "Leader");
        assert_eq!(nodes[0].leader_session_id, "session-2");
    }

    // ========================================================================
    // Integration tests
    // ========================================================================

    #[tokio::test]
    async fn test_node_lifecycle() {
        let registry = InMemoryNodeRegistry::new();
        let now = chrono::Utc::now().timestamp() as u64;

        // Register node
        let reg = NodeRegistration {
            instance_id: "inst-1".to_string(),
            address: "addr-1".to_string(),
            role: "Follower".to_string(),
            leader_session_id: "session-1".to_string(),
            last_seen: now,
        };
        registry.register("prefix/", "mydb", &reg).await.unwrap();

        // Discover
        let nodes = registry.discover_all("prefix/", "mydb").await.unwrap();
        assert_eq!(nodes.len(), 1);
        assert!(nodes[0].is_valid("session-1", 60));

        // Update (heartbeat)
        let reg2 = NodeRegistration {
            last_seen: now + 30,
            ..reg
        };
        registry.register("prefix/", "mydb", &reg2).await.unwrap();

        // Discover again (should still be 1 node)
        let nodes = registry.discover_all("prefix/", "mydb").await.unwrap();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].last_seen, now + 30);

        // Deregister
        registry
            .deregister("prefix/", "mydb", "inst-1")
            .await
            .unwrap();

        // Discover after deregister
        let nodes = registry.discover_all("prefix/", "mydb").await.unwrap();
        assert_eq!(nodes.len(), 0);
    }
}
