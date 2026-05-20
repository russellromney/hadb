# Adversarial Review — Lease / Fence Consensus Safety

Scope: `hadb-lease` (LeaseStore trait, LeaseData, fence) and `hadb`
(DbLease lease manager, follower lease monitor + leader renewal loop).
Focus: split-brain under clock skew and partition, ambiguous CAS recovery,
fencing-token monotonicity.

The lease model: a leader claims a key via CAS, renews it with
`write_if_match`, and stamps a monotonic fence revision (the lease etag,
parsed to `u64`) onto every protected write through an `AtomicFence`.
Followers read the lease, and once it looks expired for
`required_expired_reads` consecutive polls they CAS-take it over. Storage
backends reject any write whose fence revision is older than the current
lease (strict `>` on the server side).

---

## Finding 1 — [High] `LeaseData::is_expired` has no skew margin (split-brain at the TTL boundary)

- File: `hadb-lease/src/lib.rs:57-60`
- Bug: `is_expired()` was `now >= claimed_at + ttl_secs` on the bare
  wall clock at second granularity. The holder and a would-be claimer
  each evaluate this on their *own* clock. With even a one-second skew
  the claimer can decide the lease is takeable at the exact instant the
  holder still trusts it. There was a single boundary, so two nodes
  could both believe they own the lease across that boundary — classic
  split-brain.
- Fix: introduce an explicit, configurable skew margin
  (`LEASE_SKEW_MARGIN_SECS`, default 2s). Split the single boundary into
  two:
  - the HOLDER self-relinquishes at `claimed_at + ttl_secs - margin`
    (`should_relinquish(now)`), and
  - a CLAIMER may only take over at `claimed_at + ttl_secs + margin`
    (`is_claimable_by_other(now)`).
  The claimer therefore always requires strictly more elapsed time than
  the holder trusts — a `2 * margin` dead zone where nobody acts. Kept
  `is_expired()` (now defined as the plain `>= claimed_at + ttl_secs`
  midpoint, used only by read-side discovery where neither node is
  contending) and migrated all *takeover/self-trust* call-sites to the
  margined methods. Added `now`-injecting variants (`*_at`) so timing is
  unit-testable without sleeping.
- Call-site migration:
  - `DbLease::try_claim` (`hadb/src/lease.rs`): takeover of another
    instance's lease now gated on `is_claimable_by_other`.
  - `recover_ambiguous_renewal_conflict`: uses `should_relinquish`
    (self-trust check — see Finding 3).
  - follower lease monitor (`hadb/src/follower.rs`): takeover decision
    uses `is_claimable_by_other`.
  - client `discover_leader` keeps `is_expired` (pure read, no claim).
- Tests: `is_claimable_by_other_requires_margin_past_ttl`,
  `holder_relinquishes_before_claimer_can_take`,
  `skew_dead_zone_no_overlap` in `hadb-lease/src/lib.rs`.
- Status: FIXED

---

## Finding 2 — [High] Leader only relinquishes reactively; stays unfenced for ~N·interval after partition

- File: `hadb/src/follower.rs` (`run_leader_renewal`, ~388-463)
- Bug: a partitioned leader kept leadership until either a renewal
  returned a CAS conflict or `max_consecutive_renewal_errors` (default
  3) transient errors accumulated. With `renew_interval≈2s` and `max=3`
  the leader could stay leader and keep its fence live for ~6s — well
  past a `ttl_secs=5` lease that a follower can already claim at
  `ttl + margin`. During that window two nodes write.
- Fix: add a PROACTIVE self-fence independent of the error count. Track
  the wall-clock deadline of the last successful renewal
  (`last_successful_renew_deadline = claimed_at + ttl_secs - margin`,
  surfaced from `DbLease` via `renew_deadline()` / `claim_deadline()`).
  On every renewal-loop tick — including ticks where the renew call
  *errors* — if `now >= deadline` the leader clears its fence, removes
  itself from the replicator, emits `RoleEvent::Demoted`, and returns.
  This fires strictly before TTL, so the old leader is fenced before any
  follower's `is_claimable_by_other` window opens.
- The reactive paths (CAS conflict, sustained errors) are retained as
  fast-paths; the proactive deadline is the safety backstop.
- Test: `proactive_self_fence_fires_before_ttl` in
  `hadb/src/follower.rs` — a leader whose renewals all error fences in
  `< ttl_secs`, well before `max_consecutive_renewal_errors` ticks would
  elapse.
- Status: FIXED

---

## Finding 3 — [Med] `recover_ambiguous_renewal_conflict` can falsely re-confirm a lost lease

- File: `hadb/src/lease.rs:159-182`
- Bug: on a renewal CAS conflict the recovery path re-read the lease and
  re-confirmed leadership if the stored lease still named our
  instance+session and `!is_expired()`. It never checked the read-back
  *etag* against what we believe is current. Under a non-atomic CAS
  backend (read-after-write lag, or a backend that mutates then reports
  conflict) the stored bytes can still carry our identity from a stale
  view while the lease has actually moved on — falsely re-confirming a
  lost lease, and then republishing a fence revision the server has
  superseded.
- Fix: require the read-back etag to have advanced *consistently* from
  the etag we last held — it must differ from our prior etag (a real
  conflict means the store moved) and parse as a `u64` that is
  `>= our prior revision` when both are numeric. Any etag that is equal
  to our stale etag, or that regresses, is treated as lost leadership.
  Also switched the expiry guard from `!is_expired()` to
  `!should_relinquish(now)` so self-trust uses the margined holder check
  from Finding 1.
- Test: `recover_rejects_unexpected_etag` in `hadb/src/lease.rs` — a
  read-back whose etag did not advance past our prior revision is
  treated as lost; the existing
  `test_renew_recovers_when_prior_commit_succeeded_but_response_was_lost`
  still passes (etag legitimately advanced).
- Status: FIXED

---

## Finding 4 — [Med] SharedWriter / acquire_lease unfenced-write path

- Bug (as described): a `SharedWriter`/`acquire_lease` path that compares
  expiry with bare wall-clock and writes after winning a CAS without
  stamping/checking a fence revision.
- Investigation: in this repo `SharedWriter` is only a `HaMode` enum
  variant (`hadb/src/types.rs:44`). `validate_mode_combination`
  *rejects* every replicated SharedWriter combination as unsafe and the
  only allowed pairing (`SharedWriter + Local`) does no lease/CAS
  takeover. There is no `acquire_lease` function and no unfenced
  post-CAS write path in hadb. The actual fenced-write path
  (`DbLease` → `AtomicFence` → storage `apply_fence` →
  `Fence-Token` header → server strict-`>` check) already carries the
  won etag/fence into subsequent writes; Findings 1 and 5 harden it.
- Conclusion: the described unsafe path is OUT OF REPO — it would live
  in a consumer (e.g. a libsql/haqlite shared-writer integration). No
  hadb change required. Documented here for the consumer follow-up: any
  shared-writer consumer must (a) use `LeaseData::is_claimable_by_other`
  for takeover, not bare `is_expired`, and (b) carry the won lease etag
  as a fence revision on every write via `AtomicFence`/`FenceSource`.
- Status: NOT APPLICABLE (out of repo) — consumer follow-up documented.

---

## Finding 5 — [Med] Make the fencing-token contract explicit and strictly increasing

- Files: `hadb-lease/src/fence.rs`, `hadb-lease/src/lib.rs`,
  `hadb/src/lease.rs`
- State before: the fence already existed and was monotonic in practice
  — `DbLease::update_fence_token` parses the lease etag to `u64` and
  publishes it via `AtomicFenceWriter`; storage adapters read it via
  `FenceSource::require()` and stamp it on writes; the manifest server
  compares with strict `>` and rejects `<=`. But the monotonic-fence
  contract was implicit (no single place said "the fence is the lease
  revision and it strictly increases across a takeover", and there was
  no test pinning the strict-reject comparison at the lease layer).
- Fix: document the contract explicitly on `FenceSource` /
  `AtomicFenceWriter` (the published revision is the lease etag and is
  strictly increasing across claims/renewals/takeovers; consumers stamp
  it and storage rejects any revision `<=` the current one). Added a
  `LeaseData`-independent helper `fence_accepts(current, incoming)`
  documenting the canonical strict-`>` rule, and a test that a fence
  strictly increases across a simulated takeover and that a stale fence
  is rejected (`<=` rejects, `>` accepts). No existing strict comparison
  was weakened.
- Tests: `fence_strictly_increases_across_takeover`,
  `stale_fence_is_rejected` in `hadb-lease/src/fence.rs`.
- Status: FIXED

---

## Test / build notes

- `cargo build --workspace` and `cargo test --workspace` green.
- Live-network integration tests (S3/NATS) are gated/ignored and not
  exercised here; noted, not run.
