# Hindsight Durable Outbox and Upgrade Runbook

**Status:** operational documentation for the Hermes Agent Hindsight integration
**Last reviewed:** 2026-08-26
**Scope:** client-side Hermes integration plus the self-hosted Hindsight deployment

## Purpose

This document records exactly what was changed in Hermes and provides the repeatable
procedure for upgrading the self-hosted Hindsight server and the Python client to the
newest verified release.

The durable outbox is a **client-side delivery buffer**. It does not change Hindsight's
server, database schema, extraction, embedding, reranking, consolidation, or recall
behavior.

## Current deployment and version terminology

These are separate version streams:

- **Hindsight server API:** the service running on the NAS. Its `/version` endpoint
  reports the API version.
- **`hindsight-client`:** the Python SDK imported by Hermes. It is released separately
  from the server and should be tested against the deployed server.
- **Hermes Agent:** the client application containing the Hindsight provider.

State before this upgrade:

- Hindsight server API: `0.9.1`
- Hermes runtime SDK before the final upgrade: `0.8.6`

Current verified state after this upgrade:

- Hindsight server API: `0.9.2`
- Hindsight image: `ghcr.io/vectorize-io/hindsight:0.9.2`
- Image digest: `sha256:84ab276b8f501546deb6ea9c64a57291718b4e16a59dd9e02a02fdd5adfe9028`
- Hermes runtime SDK: `hindsight-client==0.9.2`
- `aretain_batch` operation-ID support: verified
- Live `/health`: healthy, database connected
- Live read-only recall through the `0.9.2` SDK: verified against the production bank

Latest release checks used:

- Hindsight GitHub release: `v0.9.2`
- PyPI SDK release: `hindsight-client==0.9.2`

Do not assume that a server version and client version are interchangeable. Verify both
from their authoritative sources before upgrading:

```bash
gh api repos/vectorize-io/hindsight/releases/latest --jq '.tag_name'
/home/kosello/.hermes/hermes-agent/venv/bin/python -m pip index versions hindsight-client
curl -fsS http://192.168.1.2:8889/version
```

## Exact Hermes changes

The local feature branch contains the following client-side changes:

### `plugins/memory/hindsight/outbox.py`

- Added a SQLite-backed durable outbox for complete retain requests.
- Stores rows before the request is handed to a worker.
- Stores `bank_id`, `document_id`, `update_mode`, `retain_async`, the complete retain
  item, retry count, retry time, last error, and a persisted `operation_id`.
- Uses a unique `dedupe_key` to prevent duplicate local enqueue rows.
- Uses SQLite `BEGIN IMMEDIATE` plus a process lock so separate outbox instances cannot
  claim the same row concurrently.
- Claims carry a unique lease token; acknowledgement and retry state changes require
  the current token, so a stale worker cannot mutate a row reclaimed after a crash.
- Same-document rows are predecessor-ordered: a later row cannot be claimed while an
  earlier row for that document is pending or in flight.
- Uses WAL mode and a busy timeout for concurrent readers/workers.
- Releases stale claims after restart and activates pending rows for replay.
- Uses a `replay_eligible` marker so the replay scheduler cannot steal a freshly
  enqueued row from Hermes' existing lazy writer.
- Uses bounded exponential retry delays and keeps failed rows on disk.
- Applies file mode `0600` where the operating system supports it.

### `plugins/memory/hindsight/__init__.py`

- Persists the retain request before dispatching the in-memory writer job.
- Persists session-switch flushes before dispatch too, snapshots the old session
  state before rotation, and flushes only the unsent suffix for append-mode
  documents.
- Keeps the existing lazy writer behavior for new rows.
- Adds a separate durable replay scheduler for rows left by a previous process or retry.
- The replay scheduler only signals the writer; replay and fresh retains share one ordered
  writer coordinator and therefore cannot dispatch concurrently through two paths.
- Passes the stable `operation_id` to `aretain_batch` when the installed SDK exposes
  that parameter and the retain is asynchronous.
- Wakes the replay scheduler when a new retain is queued.
- Avoids closing the SQLite database or Hindsight client underneath a worker that did
  not stop within the bounded shutdown window.
- Keeps the first server-operation status poll fast: 404 detection uses the SDK
  exception's status/class shape without importing the generated Pydantic model graph
  on the prefetch thread. With `hindsight-client==0.9.2`, that lazy import was measured
  at roughly 4.4 seconds and could exceed the test/short-prefetch join budget.

### `tools/lazy_deps.py`

The Hindsight SDK pin must match the tested upgrade target:

```python
"memory.hindsight": ("hindsight-client==0.9.2",),
```

The provider minimum client version must also be `0.9.2` after the upgrade.

### Tests

The tests cover:

- persistence across reopening the database;
- local deduplication;
- failed delivery and retry;
- atomic claims across two SQLite instances;
- startup replay activation;
- successful acknowledgement;
- stable operation ID delivery;
- private database permissions;
- provider behavior when Hindsight is offline, including session-switch flushes;
- append-mode session-switch flushing without duplicate turns.
- same-document claim ordering and stale-worker fencing.

Run them with an interpreter containing the pinned SDK:

```bash
/tmp/hermes-hindsight-test-venv/bin/python -m pytest \
  tests/plugins/memory/test_hindsight_outbox.py \
  tests/plugins/memory/test_hindsight_provider.py \
  -q -o addopts=
```

## NAS deployment details

The Hindsight Compose project is:

```text
Project:     hindsight
Compose:     /mnt/user/appdata/hindsight/docker-compose.yml
Working dir: /mnt/user/appdata/hindsight
Service:     hindsight
Image:       ghcr.io/vectorize-io/hindsight:0.9.2
Data:        /mnt/user/appdata/hindsight/data -> /home/hindsight/.pg0
Codex:       /mnt/user/appdata/hindsight/codex -> /home/hindsight/codex
Ports:       NAS:8889 -> container:8888
             NAS:9999 -> container:9999
Restart:     unless-stopped
```

The image should be verified by its OCI label and digest, not only by its tag:

```bash
ssh nas 'docker inspect hindsight --format \
  "version={{index .Config.Labels \"org.opencontainers.image.version\"}}\nimage={{.Image}}"'
```

## Safe upgrade procedure

### 1. Check health and capture the rollback identity

Do not start the upgrade if the current service is already unhealthy or the database is
not connected.

```bash
curl -fsS http://192.168.1.2:8889/version
ssh nas 'docker inspect hindsight --format \
  "version={{index .Config.Labels \"org.opencontainers.image.version\"}}\nimage={{.Image}}\nrepo={{index .RepoDigests 0}}"'
ssh nas 'docker compose -f /mnt/user/appdata/hindsight/docker-compose.yml ps'
```

Record the current image digest. The previous known digest was:

```text
ghcr.io/vectorize-io/hindsight@sha256:a0e937366261b8a8f20ebcaf13758c689c381dcbbf01684e4375c2787c8c666d
```

### 2. Make a dated backup before stopping the service

A running PostgreSQL data directory must not be treated as a consistent file backup.
For a consistent appdata snapshot, stop only the Hindsight Compose service, copy the
persistent data and Compose file, and then start it again. The backup must be on a
separate path from the live appdata.

```bash
ssh nas 'set -eu
stamp=$(date +%Y%m%d-%H%M%S)
backup=/mnt/user/kosello/Backups/hindsight/$stamp
mkdir -p "$backup"
cp /mnt/user/appdata/hindsight/docker-compose.yml "$backup/"
docker inspect hindsight > "$backup/container-inspect.json"
docker image inspect ghcr.io/vectorize-io/hindsight:0.9.2 > "$backup/image-inspect.json"
docker compose -f /mnt/user/appdata/hindsight/docker-compose.yml stop hindsight
tar --xattrs --acls -czf "$backup/appdata.tar.gz" -C /mnt/user/appdata hindsight
docker compose -f /mnt/user/appdata/hindsight/docker-compose.yml start hindsight
printf "backup=%s\\n" "$backup"
ls -lh "$backup/appdata.tar.gz"'
```

Verify after the backup that the service is healthy again:

```bash
curl -fsS http://192.168.1.2:8889/version
```

### 3. Pull and recreate only the Hindsight service

Use the Compose file, not a blind `docker run`, so bind mounts, ports, restart policy,
and environment configuration remain unchanged.

```bash
ssh nas 'set -eu
cd /mnt/user/appdata/hindsight
docker compose pull hindsight
docker compose up -d hindsight
docker compose ps
'
```

Do not run `docker system prune`, remove volumes, or remove the persistent data
directory as part of this upgrade.

### 4. Verify the new server

```bash
curl -fsS http://192.168.1.2:8889/version
ssh nas 'docker inspect hindsight --format \
  "version={{index .Config.Labels \"org.opencontainers.image.version\"}}\nimage={{.Image}}\nrepo={{index .RepoDigests 0}}"'
ssh nas 'docker compose -f /mnt/user/appdata/hindsight/docker-compose.yml ps'
```

Expected server API version after the upgrade:

```text
0.9.2
```

Also inspect recent logs for startup/database errors without printing environment
variables:

```bash
ssh nas 'docker logs --since 5m --tail 100 hindsight'
```

### 5. Upgrade the Hermes Python client

The client version is installed into Hermes' runtime virtual environment. The source
pin and provider minimum version must be updated together.

```bash
cd /home/kosello/.hermes/hermes-agent
/home/kosello/.hermes/hermes-agent/venv/bin/python -m pip install \
  --upgrade 'hindsight-client==0.9.2'
```

Then update these source values to `0.9.2`:

```text
tools/lazy_deps.py:       hindsight-client==0.9.2
plugins/memory/hindsight/__init__.py: _MIN_CLIENT_VERSION = "0.9.2"
```

Verify the installed SDK exposes idempotent asynchronous retain:

```bash
/home/kosello/.hermes/hermes-agent/venv/bin/python -c '
import importlib.metadata as metadata
import inspect
from hindsight_client import Hindsight
print("version=" + metadata.version("hindsight-client"))
print("operation_id=" + str("operation_id" in inspect.signature(Hindsight.aretain_batch).parameters))
'
```

### 6. Run tests before restarting Hermes

```bash
/tmp/hermes-hindsight-test-venv/bin/python -m pytest \
  tests/plugins/memory/test_hindsight_outbox.py \
  tests/plugins/memory/test_hindsight_provider.py \
  -q -o addopts=

/tmp/hermes-hindsight-test-venv/bin/python -m pytest \
  tests/plugins/memory/ tests/agent/test_memory_provider.py \
  -q -o addopts=

python -m py_compile \
  plugins/memory/hindsight/outbox.py \
  plugins/memory/hindsight/__init__.py \
  tests/plugins/memory/test_hindsight_outbox.py \
  tools/lazy_deps.py

git diff --check
```

A failure in an unrelated optional provider must be recorded separately. Do not call
the Hindsight integration verified if the Hindsight-focused tests fail.

### 7. Restart Hermes to load the upgraded client

Installing a new SDK does not replace modules already loaded by a running gateway. The
restart must be performed externally because a Hermes process cannot restart its own
parent gateway safely.

```bash
hermes gateway status
hermes gateway restart
hermes gateway status
```

If the command is issued from inside the active Telegram gateway and Hermes refuses the
self-restart, run it from a separate SSH/terminal session instead. Do not force-kill the
parent from the active chat process.

### 8. Verify read-only memory behavior

Confirm the provider is available and perform a read-only recall against the existing
bank. Do not use broad memory-delete endpoints for cleanup: the Hindsight deployment's
memory-delete behavior has previously been verified as unsafe for scoped cleanup.

```bash
hermes memory status
```

A retain/outbox test should use a temporary local HTTP test server or a temporary test
bank that is explicitly isolated and removed through a verified, supported API. Never
take the production Hindsight service offline to simulate an outage.

## Rollback procedure

If the upgraded server is unhealthy:

1. Stop only the Hindsight Compose service.
2. Restore the dated Compose/data backup if data needs restoration.
3. Pin the image to the recorded prior digest or the prior release tag.
4. Start the service and verify `/version`, database connectivity, and read-only recall.
5. Install the matching prior Python SDK in Hermes' runtime venv.
6. Restart Hermes from an external terminal.

Do not delete the outbox database during rollback. Pending rows were created for the
client-side delivery guarantee and must remain available for retry after the service is
healthy.

## Limitations and semantics

- SQLite is the durable source of truth; the in-memory queue is only a dispatch trigger.
- Hindsight is unaware of the local buffer.
- Failed requests remain locally queued and are retried with bounded backoff.
- Supported asynchronous retains use the persisted Hindsight `operation_id`, so a crash
  after server acceptance and before local acknowledgement can be retried idempotently.
- Older SDKs that do not expose `operation_id`, and synchronous retains, remain
  at-least-once across that narrow crash window.
- The outbox contains conversation-derived retain payloads and must be protected and
  backed up according to the user's privacy policy. Its directory is `0700`, the
  database is `0600`, and WAL/SHM sidecars are kept private where supported.
- A profile's outbox is tied to its configured Hindsight endpoint and credentials.
  Do not switch endpoint/account while pending rows exist; drain or archive the old
  outbox before starting the new configuration.
- Existing in-memory-only failures from before the outbox was installed cannot be
  reconstructed by the outbox; they may still exist in Hermes session history.

## Change history

- Added SQLite outbox and provider wiring in the Hermes checkout.
- Reproduced and fixed the concurrent-claim race.
- Preserved lazy writer startup by separating durable replay from normal dispatch.
- Added persisted asynchronous operation IDs and upgraded the minimum SDK requirement.
- Added private file permissions and shutdown safety.
- Updated this runbook after verifying the newest available Hindsight release is
  `v0.9.2`.
