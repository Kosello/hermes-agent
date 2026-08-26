# Hindsight Offline Retention Outbox Implementation Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task.

**Goal:** Ensure Hindsight retains are durably queued when the server is unavailable and automatically replayed after recovery.

**Architecture:** Add a small client-side SQLite-backed delivery buffer in front of the existing Hindsight client. Hindsight's server, API, database, extraction, embedding, reranking, and recall behavior remain unchanged. Hermes stores a complete retain request locally before attempting delivery; a background uploader sends it to Hindsight when reachable, keeps it on disk when unreachable, and retries it after recovery. The existing in-memory queue remains the normal dispatch trigger, while SQLite is the durable source of truth.

**Tech Stack:** Python stdlib `sqlite3`, `hindsight-client>=0.9.2` for async retain idempotency, existing provider writer/replay threads, pytest temporary directories, structured logging.

---

## Delivered Scope

This branch delivers the client-side durability slice only:

- SQLite persistence before dispatch, with profile-scoped storage and `0600` file permissions where supported.
- Atomic claims across multiple provider/outbox instances.
- Retry with bounded exponential backoff; failed rows are retained rather than silently discarded.
- Startup replay of rows left by an earlier process, while fresh rows stay with the existing lazy writer.
- Stable persisted `operation_id` values for asynchronous retain calls when supported by the SDK, preventing normal retry duplicates.
- Graceful shutdown that does not close the outbox underneath a still-running worker.
- Session-switch flushes persist the old-session payload before dispatch and append
  only turns not already queued for the old document.

The crash window is closed for supported asynchronous idempotency (`hindsight-client>=0.9.2` and Hindsight's existing API). Synchronous retains and older SDKs remain at-least-once if the process crashes after the server accepts a request but before local acknowledgement.

## Deferred Scope

The following are intentionally separate follow-ups and are not acceptance criteria for this branch: permanent-failure state/status APIs, administrative outbox inspection/purge commands, persistence of server-side operation completion state, and a production gateway restart. The temporary HTTP integration test remains a follow-up before an upstream PR; production Hindsight must not be taken offline for testing.

## Acceptance Criteria

- A retain queued while Hindsight is unreachable is not reported as uploaded and survives Hermes restart.
- The same outbox row is retried with bounded exponential backoff without silently deleting data.
- Successful retains are removed only after the retain API call succeeds.
- Replays use a persisted operation ID for supported asynchronous clients and a unique deduplication key for local enqueue races.
- Concurrent outbox instances cannot claim the same row.
- Shutdown does not lose rows already persisted in the outbox.
- Hindsight's server, database, memory processing, and recall behavior remain unchanged.
- Focused outbox and provider tests pass in an environment with `hindsight-client==0.9.2`.

## Non-goals

- Do not change the Hindsight server or database schema in the first implementation.
- Do not make recall wait for an offline outbox; recall should degrade gracefully while retention remains durable.
- Do not add a second external memory provider or a cloud message broker.
- Do not delete old session history as part of this change.

---

### Task 1: Define the outbox data model and lifecycle states

**Objective:** Specify the minimal durable record needed to replay a retain exactly.

**Files:**
- Create: `plugins/memory/hindsight/outbox.py`
- Test: `tests/plugins/memory/test_hindsight_outbox.py`

**Steps:**
1. Define an `OutboxEntry` record containing: `id`, `created_at`, `available_at`, `attempts`, `state`, `bank_id`, `document_id`, `update_mode`, `retain_async`, serialized retain item, metadata, and optional `operation_ids`.
2. Define states: `pending`, `in_flight`, `awaiting_operation`, `completed`, `failed_permanent`.
3. Define explicit transitions and reject invalid transitions.
4. Write unit tests for valid/invalid transitions and JSON round-tripping.
5. Run: `python -m pytest tests/plugins/memory/test_hindsight_outbox.py -q -o addopts=`.

### Task 2: Implement the SQLite-backed outbox

**Objective:** Make queued retain payloads survive process and machine restarts.

**Files:**
- Modify: `plugins/memory/hindsight/outbox.py`
- Test: `tests/plugins/memory/test_hindsight_outbox.py`

**Steps:**
1. Store the database under the profile-scoped Hermes home, not the repository or `/tmp`; use `get_hermes_home()`.
2. Create the database/table/indexes with `sqlite3`, WAL mode, busy timeout, and restrictive file permissions where supported.
3. Implement atomic `enqueue`, `claim_due`, `acknowledge`, `reschedule`, `record_operation_ids`, `complete_operation`, and `release_expired_claims` methods.
4. Use a unique deterministic key derived from the session/document/turn payload so a crash between persistence and in-memory enqueue cannot duplicate the request.
5. Test two outbox instances claiming the same row; exactly one must win.
6. Test reopening the database and finding the pending row.
7. Run the focused tests.

### Task 3: Persist before dispatching the in-memory writer job

**Objective:** Close the current data-loss window in `sync_turn()`.

**Files:**
- Modify: `plugins/memory/hindsight/__init__.py` around `sync_turn()` and retain payload construction.
- Test: `tests/plugins/memory/test_hindsight_provider.py`
- Test: `tests/plugins/memory/test_hindsight_outbox.py`

**Steps:**
1. Build and freeze the complete retain request before queueing it, including bank, document, update mode, tags, metadata, and turn content.
2. Insert the frozen request into the outbox first.
3. Only after the insert succeeds, enqueue the writer job and emit the saving indicator.
4. If local persistence fails, do not claim the retain was queued; log a visible error and preserve the normal conversation response path.
5. Add a test proving `sync_turn()` creates an outbox row before the writer runs.
6. Add a test proving duplicate calls with the same turn key create one row.
7. Run the focused tests.

### Task 4: Make the writer drain durable rows instead of deleting failed work

**Objective:** Retry network failures without blocking the chat reply.

**Files:**
- Modify: `plugins/memory/hindsight/__init__.py` writer loop and shutdown logic.
- Test: `tests/plugins/memory/test_hindsight_provider.py`

**Steps:**
1. Make the writer claim due outbox rows and attempt the API call.
2. On connection, timeout, 5xx, or rate-limit errors, reschedule with bounded exponential backoff plus jitter; keep the payload intact.
3. On non-retryable 4xx/auth/schema errors, move the row to `failed_permanent` and expose the reason; do not silently discard it.
4. Ensure `task_done()` applies to the in-memory trigger queue while the durable row remains pending or failed as appropriate.
5. Release stale `in_flight` claims at startup and after a bounded lease timeout.
6. Test that an offline endpoint leaves the row present and increases `attempts`.
7. Test that a later successful attempt removes/finishes the row.

### Task 5: Handle asynchronous Hindsight retain operations safely

**Objective:** Avoid marking an async retain complete merely because Hindsight accepted it.

**Files:**
- Modify: `plugins/memory/hindsight/__init__.py` operation tracking and prefetch wait code.
- Test: `tests/plugins/memory/test_hindsight_provider.py`

**Steps:**
1. Store returned `operation_id` values in the outbox row as soon as `aretain_batch()` accepts the request.
2. Keep the row in `awaiting_operation` until all operation IDs report `completed`.
3. Treat documented `404/not found` eviction as complete only when the API contract guarantees eviction means completion; otherwise keep it retryable and document the behavior.
4. On transient status failures, retain the row and retry status checks later rather than letting an exception kill the prefetch thread.
5. Make `prefetch_waits_for_retain` wait only up to its existing bounded timeout; it must never block the reply indefinitely.
6. Test pending → completed, pending → timeout, transient status error, and process restart while awaiting completion.

### Task 6: Add startup replay and bounded periodic retry

**Objective:** Recover offline writes automatically after Hermes or Hindsight returns.

**Files:**
- Modify: `plugins/memory/hindsight/__init__.py`
- Test: `tests/plugins/memory/test_hindsight_provider.py`

**Steps:**
1. On provider initialization, release stale claims and signal the writer if due rows exist.
2. Have the writer wake on a bounded interval or explicit signal, claim due rows, and retry them serially.
3. Avoid a tight retry loop when the NAS is down; enforce a minimum delay and cap the backoff.
4. Keep retries independent of recall so a dead Hindsight server does not stall normal chat.
5. Test: enqueue offline, destroy provider, create a new provider with a working fake client, and verify replay.
6. Test that an unavailable endpoint does not create unbounded threads or CPU usage.

### Task 7: Expose operational status and user-visible semantics

**Objective:** Make it clear whether memories are saved, queued, retrying, or failed.

**Files:**
- Modify: `plugins/memory/hindsight/__init__.py`
- Modify: `agent/memory_provider.py` if a shared status contract is needed.
- Test: `tests/plugins/memory/test_hindsight_provider.py`

**Steps:**
1. Extend the provider status callback with distinct messages such as `saving`, `queued for retry`, and `saved` without claiming upload success on enqueue.
2. Add a provider diagnostic/status method reporting counts by outbox state, oldest pending age, and next retry time—without exposing memory contents or secrets.
3. Ensure `/status` or `hermes memory status` can report degraded Hindsight availability separately from built-in memory availability.
4. Add tests for status transitions and empty/non-empty outbox reporting.

### Task 8: Add an administrative recovery path

**Objective:** Prevent permanent failures from becoming invisible or unrecoverable.

**Files:**
- Modify: `plugins/memory/hindsight/__init__.py`
- Modify: relevant Hermes memory CLI/status module discovered during implementation.
- Test: `tests/plugins/memory/test_hindsight_provider.py`

**Steps:**
1. Add a safe `retry failed` operation for `failed_permanent` rows after credentials or server configuration are corrected.
2. Add a safe `inspect outbox` operation showing IDs, timestamps, attempts, and error summaries—not full sensitive payloads by default.
3. Do not add deletion by default; if cleanup is required, require an explicit, separately reviewed purge operation.
4. Test retrying a permanent failure and verifying the row returns to `pending`.

### Task 9: Verify against a real local HTTP test server

**Objective:** Exercise network failures and recovery without touching production Hindsight.

**Files:**
- Create: `tests/plugins/memory/test_hindsight_outbox_integration.py`
- Modify: test fixtures as needed.

**Steps:**
1. Start a temporary stdlib HTTP server that implements the minimal retain and operation-status responses.
2. Run a test with the server refusing connections, then start it and verify replay.
3. Force a process-like restart by closing the provider and reopening the same temporary Hermes home/database.
4. Verify one durable retain and one successful async operation, with no duplicate document/turn submissions.
5. Run: `python -m pytest tests/plugins/memory/test_hindsight_outbox*.py tests/plugins/memory/test_hindsight_provider.py -q -o addopts=`.
6. Record any dependency failures separately from functional failures; do not call the feature verified if the integration tests are skipped.

### Task 10: Document configuration, recovery, and migration behavior

**Objective:** Make the new durability guarantees and limits supportable.

**Files:**
- Modify: `plugins/memory/hindsight/__init__.py` module documentation.
- Modify: `website/docs/user-guide/features/memory-providers.md`.
- Modify: `skills/mlops/hermes-memory-providers/SKILL.md` if the repository copy is canonical.

**Steps:**
1. Document outbox location, retention of sensitive payloads, retry/backoff limits, permanent-failure handling, and status meanings.
2. Document that Hindsight recall can remain unavailable while queued retains are safe.
3. Document upgrade behavior: existing in-memory-only failed writes cannot be recovered, but future writes use the outbox automatically.
4. Add a rollback note: disabling the outbox must not delete existing rows.
5. Run documentation link/lint checks used by the repository.

## Final Verification

Run the focused suite first:

```bash
python -m pytest tests/plugins/memory/test_hindsight_outbox*.py tests/plugins/memory/test_hindsight_provider.py -q -o addopts=
```

Then run the relevant full suite:

```bash
python -m pytest tests/plugins/memory/ tests/agent/test_memory_provider.py -q -o addopts=
```

Acceptance is complete only when the offline/restart integration test passes, the outbox row is observed before dispatch, and a recovered Hindsight endpoint receives the retain exactly once. Do not take the production NAS Hindsight service offline for validation; use the temporary local HTTP server and a temporary `HERMES_HOME`.
