//! S3-backed node registry implementation.

use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;

use hadb::{NodeRegistration, NodeRegistry, StorageBackend};

/// S3-backed node registry using `StorageBackend`.
pub struct S3NodeRegistry {
    storage: Arc<dyn StorageBackend>,
}

impl S3NodeRegistry {
    pub fn new(storage: Arc<dyn StorageBackend>) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl NodeRegistry for S3NodeRegistry {
    async fn register(
        &self,
        prefix: &str,
        db_name: &str,
        registration: &NodeRegistration,
    ) -> Result<()> {
        let key = hadb::node_key(prefix, db_name, &registration.instance_id);
        let data = serde_json::to_vec(registration)?;
        self.storage.upload(&key, &data).await
    }

    async fn deregister(&self, prefix: &str, db_name: &str, instance_id: &str) -> Result<()> {
        let key = hadb::node_key(prefix, db_name, instance_id);
        self.storage.delete(&key).await
    }

    async fn discover_all(&self, prefix: &str, db_name: &str) -> Result<Vec<NodeRegistration>> {
        let list_prefix = hadb::nodes_prefix(prefix, db_name);
        // Use max_keys to prevent OOM on large clusters
        let keys = self.storage.list(&list_prefix, Some(1000)).await?;
        let mut registrations = Vec::new();
        for key in &keys {
            if !key.ends_with(".json") {
                continue;
            }
            match self.storage.download(key).await {
                Ok(data) => match serde_json::from_slice::<NodeRegistration>(&data) {
                    Ok(reg) => registrations.push(reg),
                    Err(e) => {
                        tracing::error!("Corrupted node registration {}: {}", key, e);
                    }
                },
                Err(e) => {
                    tracing::error!("Failed to download node registration {}: {}", key, e);
                }
            }
        }
        Ok(registrations)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_s3_node_registry_new() {
        // Just test that the constructor works - actual S3 operations tested in integration tests
        let storage = Arc::new(crate::S3StorageBackend::new(
            aws_sdk_s3::Client::from_conf(
                aws_sdk_s3::Config::builder()
                    .region(aws_sdk_s3::config::Region::new("us-east-1"))
                    .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
                    .build(),
            ),
            "test-bucket".to_string(),
        ));
        let _registry = S3NodeRegistry::new(storage);
    }
}
