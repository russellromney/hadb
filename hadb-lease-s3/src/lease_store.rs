//! S3LeaseStore: LeaseStore implementation using S3 conditional PUTs.

use anyhow::{anyhow, bail, Result};
use async_trait::async_trait;
use hadb_lease::{CasResult, LeaseStore};
use serde::{Deserialize, Serialize};

use crate::error::{is_not_found, is_precondition_failed};

const FORMAT_METADATA: &str = "hadb-lease-format";
const FORMAT_VERSION: &str = "1";
const LEGACY_TOKEN_PREFIX: &str = "s3-legacy:";

/// The revision is in the body so identical payload updates still change the
/// physical ETag. Metadata alone cannot prevent same-content CAS ABA.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    version: u8,
    revision: u64,
    payload: Payload,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Payload {
    Live(Vec<u8>),
    Tombstone,
}

struct Record {
    physical_etag: String,
    /// Zero is private legacy state, never a published fence revision.
    revision: u64,
    payload: Option<Vec<u8>>,
}

impl Record {
    fn token(&self) -> String {
        if self.revision == 0 {
            format!("{LEGACY_TOKEN_PREFIX}{}", self.physical_etag)
        } else {
            self.revision.to_string()
        }
    }

    fn next_revision(&self) -> Result<u64> {
        self.revision
            .checked_add(1)
            .ok_or_else(|| anyhow!("S3 lease revision exhausted"))
    }
}

fn conflict() -> CasResult {
    CasResult {
        success: false,
        etag: None,
    }
}

/// LeaseStore backed by atomic S3 conditional PUTs.
///
/// Public CAS tokens are persisted, per-key numeric revisions, not physical S3
/// ETags. Payload bytes remain opaque. Logical deletion retains a tombstone so
/// release/reclaim cannot reset the revision. Exclude lease objects from bucket
/// lifecycle deletion and never mix this format with old raw-object writers.
///
/// Legacy raw objects remain readable with opaque tokens. Their first matched
/// mutation migrates them conditionally. Migration requires stopping old clients;
/// it cannot recover numeric history lost through an earlier physical deletion.
///
/// `key_for(scope)` returns `"{prefix}{scope}/_lease.json"`. Other operations use
/// the supplied key unchanged. Requires strongly consistent GET and atomic
/// conditional PUT; unsupported conditional requests fail without any fallback.
/// AWS SDK retry configuration remains under the caller's control.
pub struct S3LeaseStore {
    client: aws_sdk_s3::Client,
    bucket: String,
    prefix: String,
}

impl S3LeaseStore {
    pub fn new(client: aws_sdk_s3::Client, bucket: String) -> Self {
        Self {
            client,
            bucket,
            prefix: String::new(),
        }
    }

    /// Set the key prefix used by `key_for`.
    pub fn with_prefix(mut self, prefix: &str) -> Self {
        self.prefix = prefix.to_string();
        self
    }

    async fn read_record(&self, key: &str) -> Result<Option<Record>> {
        let output = match self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(output) => output,
            Err(e) if is_not_found(&e) => return Ok(None),
            Err(e) => return Err(anyhow!("S3 GetObject failed: {e}")),
        };
        let physical_etag = output
            .e_tag()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| anyhow!("S3 GetObject returned no ETag"))?
            .to_string();
        let format = output
            .metadata()
            .and_then(|m| m.get(FORMAT_METADATA))
            .cloned();
        let body = output.body.collect().await?.into_bytes().to_vec();
        match format.as_deref() {
            None => Ok(Some(Record {
                physical_etag,
                revision: 0,
                payload: Some(body),
            })),
            Some(FORMAT_VERSION) => {
                let envelope: Envelope = serde_json::from_slice(&body)?;
                if envelope.version != 1 || envelope.revision == 0 {
                    bail!("Invalid S3 lease envelope version or revision");
                }
                Ok(Some(Record {
                    physical_etag,
                    revision: envelope.revision,
                    payload: match envelope.payload {
                        Payload::Live(bytes) => Some(bytes),
                        Payload::Tombstone => None,
                    },
                }))
            }
            Some(_) => bail!("Unsupported S3 lease format"),
        }
    }

    async fn put_record(
        &self,
        key: &str,
        revision: u64,
        payload: Option<Vec<u8>>,
        expected: Option<&str>,
    ) -> Result<CasResult> {
        let body = serde_json::to_vec(&Envelope {
            version: 1,
            revision,
            payload: match payload {
                Some(bytes) => Payload::Live(bytes),
                None => Payload::Tombstone,
            },
        })?;
        let request = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .metadata(FORMAT_METADATA, FORMAT_VERSION)
            .content_type("application/json")
            .body(body.into());
        let request = match expected {
            Some(etag) => request.if_match(etag),
            None => request.if_none_match("*"),
        };
        match request.send().await {
            Ok(output) => {
                // Missing receipts stay uncertain. Never fabricate success after
                // errors, or use HEAD plus an unconditional PUT to recover.
                if output.e_tag().filter(|s| !s.is_empty()).is_none() {
                    bail!("S3 conditional PUT returned no ETag receipt");
                }
                Ok(CasResult {
                    success: true,
                    etag: Some(revision.to_string()),
                })
            }
            Err(e) if is_precondition_failed(&e) || (expected.is_some() && is_not_found(&e)) => {
                Ok(conflict())
            }
            Err(e) => Err(anyhow!("S3 conditional PUT failed: {e}")),
        }
    }
}

#[async_trait]
impl LeaseStore for S3LeaseStore {
    fn key_for(&self, scope: &str) -> String {
        format!("{}{}/_lease.json", self.prefix, scope)
    }

    async fn read(&self, key: &str) -> Result<Option<(Vec<u8>, String)>> {
        Ok(self.read_record(key).await?.and_then(|record| {
            let version = record.token();
            record.payload.map(|payload| (payload, version))
        }))
    }

    async fn write_if_not_exists(&self, key: &str, data: Vec<u8>) -> Result<CasResult> {
        match self.read_record(key).await? {
            None => self.put_record(key, 1, Some(data), None).await,
            Some(record) if record.payload.is_none() => {
                self.put_record(
                    key,
                    record.next_revision()?,
                    Some(data),
                    Some(&record.physical_etag),
                )
                .await
            }
            Some(_) => Ok(conflict()),
        }
    }

    async fn write_if_match(&self, key: &str, data: Vec<u8>, etag: &str) -> Result<CasResult> {
        match self.read_record(key).await? {
            Some(record) if record.payload.is_some() && record.token() == etag => {
                self.put_record(
                    key,
                    record.next_revision()?,
                    Some(data),
                    Some(&record.physical_etag),
                )
                .await
            }
            _ => Ok(conflict()),
        }
    }

    async fn release_if_match(&self, key: &str, etag: &str) -> Result<bool> {
        match self.read_record(key).await? {
            Some(record) if record.payload.is_some() && record.token() == etag => Ok(self
                .put_record(
                    key,
                    record.next_revision()?,
                    None,
                    Some(&record.physical_etag),
                )
                .await?
                .success),
            _ => Ok(false),
        }
    }

    async fn delete(&self, key: &str) -> Result<()> {
        if let Some(record) = self.read_record(key).await? {
            if record.payload.is_some() {
                let result = self
                    .put_record(
                        key,
                        record.next_revision()?,
                        None,
                        Some(&record.physical_etag),
                    )
                    .await?;
                if !result.success {
                    bail!("S3 lease changed during logical delete");
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_client() -> aws_sdk_s3::Client {
        let conf = aws_sdk_s3::Config::builder()
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .credentials_provider(aws_sdk_s3::config::Credentials::new(
                "test", "test", None, None, "test",
            ))
            .build();
        aws_sdk_s3::Client::from_conf(conf)
    }

    #[test]
    fn key_for_no_prefix() {
        let s = S3LeaseStore::new(dummy_client(), "bkt".into());
        assert_eq!(s.key_for("mydb"), "mydb/_lease.json");
    }

    #[test]
    fn key_for_with_prefix() {
        let s = S3LeaseStore::new(dummy_client(), "bkt".into()).with_prefix("placement/");
        assert_eq!(
            s.key_for("placement-db"),
            "placement/placement-db/_lease.json"
        );
    }
}
