//! Property-based tests for retry and circuit breaker in hadb-io.
//!
//! Verifies circuit breaker opens after threshold and retry backoff
//! never exceeds configured maximum.

use hadb_io::{CircuitBreaker, CircuitState, RetryConfig, RetryPolicy};
use proptest::prelude::*;
use std::time::Duration;

proptest! {
    #[test]
    fn circuit_breaker_opens_after_threshold(
        threshold in 1u32..20,
        cooldown_ms in 100u64..120_000u64,
        extra_failures in 0u32..5u32,
    ) {
        let cb = CircuitBreaker::new(threshold, cooldown_ms);

        prop_assert_eq!(cb.state(), CircuitState::Closed);
        prop_assert!(cb.should_allow());

        for i in 0..threshold {
            cb.record_failure();
            if i < threshold - 1 {
                prop_assert_eq!(cb.state(), CircuitState::Closed,
                    "circuit must stay closed until threshold reached");
                prop_assert!(cb.should_allow());
            }
        }

        prop_assert_eq!(cb.consecutive_failures(), threshold);
        prop_assert_eq!(cb.state(), CircuitState::Open);
        prop_assert!(!cb.should_allow());

        for _ in 0..extra_failures {
            cb.record_failure();
            prop_assert_eq!(cb.state(), CircuitState::Open,
                "circuit must stay open during cooldown");
            prop_assert!(!cb.should_allow());
        }

        prop_assert_eq!(cb.consecutive_failures(), threshold + extra_failures);
    }

    #[test]
    fn retry_backoff_never_exceeds_max(
        base_delay_ms in 1u64..10_000u64,
        max_delay_ms in 100u64..60_000u64,
        attempt in 0u32..30u32,
    ) {
        let config = RetryConfig {
            base_delay_ms,
            max_delay_ms: max_delay_ms.max(base_delay_ms),
            ..Default::default()
        };
        let policy = RetryPolicy::new(config);

        let effective_max = max_delay_ms.max(base_delay_ms);

        for _ in 0..20 {
            let delay = policy.calculate_delay(attempt);
            prop_assert!(
                delay <= Duration::from_millis(effective_max),
                "delay {:?} must not exceed max {}ms at attempt {}",
                delay, effective_max, attempt
            );
        }
    }
}
