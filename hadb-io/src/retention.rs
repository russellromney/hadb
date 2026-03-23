//! Retention policy for snapshot/segment compaction.
//!
//! Uses Grandfather/Father/Son (GFS) rotation scheme:
//! - Hourly tier: Keep N snapshots from the last 24 hours
//! - Daily tier: Keep N snapshots, one per day for the last week
//! - Weekly tier: Keep N snapshots, one per week for the last 12 weeks
//! - Monthly tier: Keep N snapshots, one per month beyond 12 weeks
//!
//! Engine-agnostic: operates on (key, timestamp, sequence, size) tuples.
//! Works for LTX snapshots, .graphj segments, Kuzu tarballs, or any other format.

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Retention policy configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionPolicy {
    /// Number of hourly snapshots to keep (default: 24).
    pub hourly: usize,
    /// Number of daily snapshots to keep (default: 7).
    pub daily: usize,
    /// Number of weekly snapshots to keep (default: 12).
    pub weekly: usize,
    /// Number of monthly snapshots to keep (default: 12).
    pub monthly: usize,
    /// Minimum total snapshots to keep (safety floor).
    pub minimum: usize,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            hourly: 24,
            daily: 7,
            weekly: 12,
            monthly: 12,
            minimum: 2,
        }
    }
}

impl RetentionPolicy {
    /// Create a new policy with custom values.
    pub fn new(hourly: usize, daily: usize, weekly: usize, monthly: usize) -> Self {
        Self {
            hourly,
            daily,
            weekly,
            monthly,
            minimum: 2,
        }
    }
}

/// Tier classification for snapshots.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tier {
    /// Less than 24 hours old.
    Hourly,
    /// 24 hours to 7 days old.
    Daily,
    /// 7 days to 12 weeks old.
    Weekly,
    /// More than 12 weeks old.
    Monthly,
}

impl Tier {
    /// Classify a snapshot based on its age.
    pub fn classify(now: DateTime<Utc>, created_at: DateTime<Utc>) -> Self {
        let age = now.signed_duration_since(created_at);

        if age < Duration::hours(24) {
            Tier::Hourly
        } else if age < Duration::days(7) {
            Tier::Daily
        } else if age < Duration::weeks(12) {
            Tier::Weekly
        } else {
            Tier::Monthly
        }
    }
}

/// A snapshot/segment entry for retention analysis.
///
/// Generic: `key` is the S3 key or local filename, `sequence` is the ordering
/// value (TXID for walrust, journal sequence for graphstream, etc.).
#[derive(Debug, Clone)]
pub struct SnapshotEntry {
    /// S3 key or local filename.
    pub key: String,
    /// Creation timestamp.
    pub created_at: DateTime<Utc>,
    /// Sequence number for ordering (TXID, journal sequence, etc.).
    pub sequence: u64,
    /// File size in bytes.
    pub size: u64,
}

/// Result of retention analysis.
#[derive(Debug, Clone)]
pub struct RetentionPlan {
    /// Entries to keep.
    pub keep: Vec<SnapshotEntry>,
    /// Entries to delete.
    pub delete: Vec<SnapshotEntry>,
    /// Bytes that will be freed.
    pub bytes_freed: u64,
}

impl RetentionPlan {
    /// Check if there's anything to delete.
    pub fn has_deletions(&self) -> bool {
        !self.delete.is_empty()
    }

    /// Get a summary of the plan.
    pub fn summary(&self) -> String {
        format!(
            "Keep: {} entries, Delete: {} entries, Free: {} bytes ({:.2} MB)",
            self.keep.len(),
            self.delete.len(),
            self.bytes_freed,
            self.bytes_freed as f64 / (1024.0 * 1024.0)
        )
    }
}

/// Generate a bucket key for grouping entries by time period.
fn bucket_key(tier: Tier, created_at: DateTime<Utc>) -> String {
    match tier {
        Tier::Hourly => created_at.format("%Y-%m-%dT%H").to_string(),
        Tier::Daily => created_at.format("%Y-%m-%d").to_string(),
        Tier::Weekly => created_at.format("%Y-W%W").to_string(),
        Tier::Monthly => created_at.format("%Y-%m").to_string(),
    }
}

/// Analyze entries and determine which to keep/delete based on retention policy.
///
/// Algorithm:
/// 1. Always keep the entry with the highest sequence (latest — safety).
/// 2. Classify each entry into tiers based on age.
/// 3. Within each tier, group by time bucket (hour/day/week/month).
/// 4. Keep the latest entry from each bucket, up to the tier limit.
/// 5. Ensure minimum total is maintained.
pub fn analyze_retention(
    entries: &[SnapshotEntry],
    policy: &RetentionPolicy,
    now: DateTime<Utc>,
) -> RetentionPlan {
    if entries.is_empty() {
        return RetentionPlan {
            keep: vec![],
            delete: vec![],
            bytes_freed: 0,
        };
    }

    // Always keep the latest entry
    let latest = entries
        .iter()
        .max_by_key(|s| s.sequence)
        .expect("entries not empty");

    let mut keep_set: HashMap<String, bool> = HashMap::new();
    keep_set.insert(latest.key.clone(), true);

    // Group entries by tier and bucket
    let mut hourly_buckets: HashMap<String, Vec<&SnapshotEntry>> = HashMap::new();
    let mut daily_buckets: HashMap<String, Vec<&SnapshotEntry>> = HashMap::new();
    let mut weekly_buckets: HashMap<String, Vec<&SnapshotEntry>> = HashMap::new();
    let mut monthly_buckets: HashMap<String, Vec<&SnapshotEntry>> = HashMap::new();

    for entry in entries {
        let tier = Tier::classify(now, entry.created_at);
        let key = bucket_key(tier, entry.created_at);

        match tier {
            Tier::Hourly => hourly_buckets.entry(key).or_default().push(entry),
            Tier::Daily => daily_buckets.entry(key).or_default().push(entry),
            Tier::Weekly => weekly_buckets.entry(key).or_default().push(entry),
            Tier::Monthly => monthly_buckets.entry(key).or_default().push(entry),
        }
    }

    fn select_from_buckets(
        buckets: HashMap<String, Vec<&SnapshotEntry>>,
        limit: usize,
        keep_set: &mut HashMap<String, bool>,
    ) {
        let mut best: Vec<&SnapshotEntry> = buckets
            .into_values()
            .filter_map(|mut items| {
                items.sort_by_key(|e| std::cmp::Reverse(e.sequence));
                items.into_iter().next()
            })
            .collect();

        best.sort_by_key(|e| std::cmp::Reverse(e.sequence));

        for entry in best.into_iter().take(limit) {
            keep_set.insert(entry.key.clone(), true);
        }
    }

    select_from_buckets(hourly_buckets, policy.hourly, &mut keep_set);
    select_from_buckets(daily_buckets, policy.daily, &mut keep_set);
    select_from_buckets(weekly_buckets, policy.weekly, &mut keep_set);
    select_from_buckets(monthly_buckets, policy.monthly, &mut keep_set);

    // Ensure minimum is met
    if keep_set.len() < policy.minimum {
        let mut all_sorted: Vec<&SnapshotEntry> = entries.iter().collect();
        all_sorted.sort_by_key(|s| s.sequence);

        for entry in all_sorted {
            if keep_set.len() >= policy.minimum {
                break;
            }
            keep_set.insert(entry.key.clone(), true);
        }
    }

    // Split into keep and delete lists
    let mut keep = Vec::new();
    let mut delete = Vec::new();
    let mut bytes_freed = 0;

    for entry in entries {
        if keep_set.contains_key(&entry.key) {
            keep.push(entry.clone());
        } else {
            bytes_freed += entry.size;
            delete.push(entry.clone());
        }
    }

    keep.sort_by_key(|s| std::cmp::Reverse(s.sequence));
    delete.sort_by_key(|s| std::cmp::Reverse(s.sequence));

    RetentionPlan {
        keep,
        delete,
        bytes_freed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(key: &str, hours_ago: i64, sequence: u64, size: u64) -> SnapshotEntry {
        let now = Utc::now();
        SnapshotEntry {
            key: key.to_string(),
            created_at: now - Duration::hours(hours_ago),
            sequence,
            size,
        }
    }

    fn make_entry_days(key: &str, days_ago: i64, sequence: u64, size: u64) -> SnapshotEntry {
        let now = Utc::now();
        SnapshotEntry {
            key: key.to_string(),
            created_at: now - Duration::days(days_ago),
            sequence,
            size,
        }
    }

    #[test]
    fn test_tier_classification() {
        let now = Utc::now();

        assert_eq!(Tier::classify(now, now - Duration::hours(1)), Tier::Hourly);
        assert_eq!(Tier::classify(now, now - Duration::hours(23)), Tier::Hourly);
        assert_eq!(Tier::classify(now, now - Duration::hours(25)), Tier::Daily);
        assert_eq!(Tier::classify(now, now - Duration::days(6)), Tier::Daily);
        assert_eq!(Tier::classify(now, now - Duration::days(8)), Tier::Weekly);
        assert_eq!(
            Tier::classify(now, now - Duration::weeks(11)),
            Tier::Weekly
        );
        assert_eq!(
            Tier::classify(now, now - Duration::weeks(13)),
            Tier::Monthly
        );
    }

    #[test]
    fn test_empty_entries() {
        let policy = RetentionPolicy::default();
        let plan = analyze_retention(&[], &policy, Utc::now());

        assert!(plan.keep.is_empty());
        assert!(plan.delete.is_empty());
        assert_eq!(plan.bytes_freed, 0);
    }

    #[test]
    fn test_single_entry_always_kept() {
        let now = Utc::now();
        let entries = vec![make_entry("entry1", 1, 100, 1000)];

        let policy = RetentionPolicy::default();
        let plan = analyze_retention(&entries, &policy, now);

        assert_eq!(plan.keep.len(), 1);
        assert!(plan.delete.is_empty());
        assert_eq!(plan.bytes_freed, 0);
    }

    #[test]
    fn test_minimum_entries_enforced() {
        let now = Utc::now();
        let entries = vec![
            make_entry("s1", 1, 1, 1000),
            make_entry("s2", 2, 2, 1000),
            make_entry("s3", 3, 3, 1000),
        ];

        let policy = RetentionPolicy {
            hourly: 1,
            daily: 0,
            weekly: 0,
            monthly: 0,
            minimum: 2,
        };

        let plan = analyze_retention(&entries, &policy, now);
        assert!(plan.keep.len() >= 2);
    }

    #[test]
    fn test_latest_always_kept() {
        let now = Utc::now();
        let entries = vec![
            make_entry("old", 100, 1, 1000),
            make_entry("new", 1, 100, 1000),
        ];

        let policy = RetentionPolicy {
            hourly: 0,
            daily: 0,
            weekly: 0,
            monthly: 0,
            minimum: 1,
        };

        let plan = analyze_retention(&entries, &policy, now);
        assert!(plan.keep.iter().any(|s| s.key == "new"));
    }

    #[test]
    fn test_hourly_bucketing() {
        let now = Utc::now();
        let entries = vec![
            make_entry("h1_a", 0, 10, 1000),
            make_entry("h1_b", 0, 20, 1000), // same hour, higher sequence
            make_entry("h2", 2, 30, 1000),
            make_entry("h3", 3, 40, 1000),
        ];

        let policy = RetentionPolicy {
            hourly: 2,
            daily: 0,
            weekly: 0,
            monthly: 0,
            minimum: 1,
        };

        let plan = analyze_retention(&entries, &policy, now);
        assert!(plan.keep.len() >= 2);
        assert!(plan.keep.iter().any(|s| s.key == "h3"));
    }

    #[test]
    fn test_daily_bucketing() {
        let now = Utc::now();
        let entries = vec![
            make_entry_days("d1_a", 2, 10, 1000),
            make_entry_days("d1_b", 2, 20, 1000),
            make_entry_days("d2", 3, 30, 1000),
            make_entry_days("d3", 4, 40, 1000),
        ];

        let policy = RetentionPolicy {
            hourly: 0,
            daily: 2,
            weekly: 0,
            monthly: 0,
            minimum: 1,
        };

        let plan = analyze_retention(&entries, &policy, now);
        assert!(plan.keep.len() >= 2);
        assert!(plan.keep.iter().any(|s| s.key == "d3"));
    }

    #[test]
    fn test_realistic_scenario() {
        let now = Utc::now();
        let mut entries = Vec::new();

        // Recent hourly
        for i in 0..24 {
            entries.push(make_entry(
                &format!("hourly_{}", i),
                i,
                100 + i as u64,
                10000,
            ));
        }

        // Daily
        for i in 1..8 {
            entries.push(make_entry_days(
                &format!("daily_{}", i),
                i,
                50 + i as u64,
                10000,
            ));
        }

        // Weekly
        for i in 2..14 {
            entries.push(SnapshotEntry {
                key: format!("weekly_{}", i),
                created_at: now - Duration::weeks(i),
                sequence: 20 + i as u64,
                size: 10000,
            });
        }

        // Monthly
        for i in 4..10 {
            entries.push(SnapshotEntry {
                key: format!("monthly_{}", i),
                created_at: now - Duration::weeks(12 + i * 4),
                sequence: i as u64,
                size: 10000,
            });
        }

        let policy = RetentionPolicy::default();
        let plan = analyze_retention(&entries, &policy, now);

        assert!(plan.has_deletions());
        assert!(plan.keep.len() <= 60);
        assert!(plan.keep.len() >= 20);

        let max_seq = entries.iter().map(|s| s.sequence).max().unwrap();
        assert!(plan.keep.iter().any(|s| s.sequence == max_seq));

        let summary = plan.summary();
        assert!(summary.contains("Keep:"));
        assert!(summary.contains("Delete:"));
        assert!(summary.contains("Free:"));
    }

    #[test]
    fn test_bucket_key_format() {
        let dt = DateTime::parse_from_rfc3339("2024-01-15T10:30:00Z")
            .unwrap()
            .with_timezone(&Utc);

        assert_eq!(bucket_key(Tier::Hourly, dt), "2024-01-15T10");
        assert_eq!(bucket_key(Tier::Daily, dt), "2024-01-15");
        assert_eq!(bucket_key(Tier::Monthly, dt), "2024-01");
    }

    #[test]
    fn test_bytes_freed_calculation() {
        let now = Utc::now();
        let entries = vec![
            make_entry("keep", 1, 100, 5000),
            make_entry("delete1", 2, 50, 3000),
            make_entry("delete2", 3, 30, 2000),
        ];

        let policy = RetentionPolicy {
            hourly: 1,
            daily: 0,
            weekly: 0,
            monthly: 0,
            minimum: 1,
        };

        let plan = analyze_retention(&entries, &policy, now);
        let deleted_size: u64 = plan.delete.iter().map(|s| s.size).sum();
        assert_eq!(plan.bytes_freed, deleted_size);
    }

    #[test]
    fn test_all_same_tier_respects_limit() {
        let now = Utc::now();
        // 10 entries all in the same hour
        let entries: Vec<SnapshotEntry> = (0..10)
            .map(|i| SnapshotEntry {
                key: format!("entry_{}", i),
                created_at: now - Duration::minutes(i as i64),
                sequence: i as u64,
                size: 1000,
            })
            .collect();

        let policy = RetentionPolicy {
            hourly: 1,
            daily: 0,
            weekly: 0,
            monthly: 0,
            minimum: 1,
        };

        let plan = analyze_retention(&entries, &policy, now);
        // Should keep only the latest (highest sequence) from the single hourly bucket
        assert!(plan.keep.len() <= 2); // latest + 1 from bucket (they may be the same)
        assert!(plan.delete.len() >= 8);
    }
}
