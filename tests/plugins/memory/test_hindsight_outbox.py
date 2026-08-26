import concurrent.futures
import stat
import threading

from plugins.memory.hindsight import HindsightMemoryProvider
from plugins.memory.hindsight.outbox import HindsightOutbox


def entry(**overrides):
    value = {
        "dedupe_key": "session-1:turn-1",
        "bank_id": "hermes",
        "document_id": "doc-1",
        "update_mode": "append",
        "retain_async": True,
        "item": {"content": "hello", "context": "test"},
    }
    value.update(overrides)
    return value


def test_outbox_survives_reopen(tmp_path):
    path = tmp_path / "hindsight-outbox.sqlite3"
    first = HindsightOutbox(path)
    row_id = first.enqueue(**entry())
    first.close()

    second = HindsightOutbox(path)
    rows = second.claim_due(limit=10)

    assert [row.id for row in rows] == [row_id]
    assert rows[0].item["content"] == "hello"
    assert rows[0].operation_id
    second.close()


def test_outbox_file_is_private_on_posix(tmp_path):
    outbox = HindsightOutbox(tmp_path / "hindsight-outbox.sqlite3")

    mode = stat.S_IMODE(outbox.path.stat().st_mode)
    parent_mode = stat.S_IMODE(outbox.path.parent.stat().st_mode)

    assert mode == 0o600
    assert parent_mode == 0o700
    for suffix in ("-wal", "-shm"):
        sidecar = outbox.path.with_name(outbox.path.name + suffix)
        if sidecar.exists():
            assert stat.S_IMODE(sidecar.stat().st_mode) == 0o600
    outbox.close()


def test_duplicate_dedupe_key_is_stored_once(tmp_path):
    outbox = HindsightOutbox(tmp_path / "outbox.sqlite3")

    first = outbox.enqueue(**entry())
    second = outbox.enqueue(**entry())

    assert first == second
    assert len(outbox.claim_due(limit=10)) == 1
    outbox.close()


def test_failed_delivery_remains_due_after_reschedule(tmp_path):
    outbox = HindsightOutbox(tmp_path / "outbox.sqlite3")
    row_id = outbox.enqueue(**entry())
    row = outbox.claim_due(limit=1)[0]

    outbox.reschedule(
        row.id, "connection refused", claim_token=row.claim_token, delay_seconds=0
    )
    retry = outbox.claim_due(limit=1)[0]

    assert retry.id == row_id
    assert retry.attempts == 1
    assert retry.last_error == "connection refused"
    outbox.close()


def test_claim_due_is_atomic_across_outbox_connections(tmp_path):
    path = tmp_path / "outbox.sqlite3"
    first = HindsightOutbox(path)
    second = HindsightOutbox(path)
    first.enqueue(**entry())
    barrier = threading.Barrier(2)

    def claim(outbox):
        barrier.wait()
        return outbox.claim_due(limit=1)

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(claim, (first, second)))

    assert sum(bool(rows) for rows in results) == 1
    first.close()
    second.close()


def test_same_document_rows_are_claimed_in_order(tmp_path):
    outbox = HindsightOutbox(tmp_path / "outbox.sqlite3")
    first_id = outbox.enqueue(**entry(dedupe_key="session-1:turn-1"))
    second_id = outbox.enqueue(**entry(dedupe_key="session-1:turn-2"))

    first = outbox.claim_due(limit=1)[0]
    assert first.id == first_id
    assert outbox.claim_due(limit=1) == []

    outbox.acknowledge(first.id, claim_token=first.claim_token)
    second = outbox.claim_due(limit=1)[0]
    assert second.id == second_id
    outbox.close()


def test_stale_claimant_cannot_mutate_reclaimed_row(tmp_path):
    outbox = HindsightOutbox(tmp_path / "outbox.sqlite3")
    row_id = outbox.enqueue(**entry())
    stale = outbox.claim_due(limit=1)[0]
    assert stale.claim_token

    outbox.release_stale_claims(lease_seconds=0)
    fresh = outbox.claim_due(limit=1)[0]
    assert fresh.claim_token != stale.claim_token

    outbox.acknowledge(row_id, claim_token=stale.claim_token)
    outbox.reschedule(row_id, "stale", claim_token=stale.claim_token, delay_seconds=0)

    outbox.release_stale_claims(lease_seconds=0)
    current = outbox.claim_due(limit=1)[0]
    assert current.attempts == 0
    assert current.last_error is None
    outbox.close()


def test_restart_activates_pending_rows_for_replay(tmp_path):
    path = tmp_path / "outbox.sqlite3"
    first = HindsightOutbox(path)
    row_id = first.enqueue(**entry())
    first.close()

    second = HindsightOutbox(path)
    assert second.claim_due(limit=1, replay_only=True) == []
    assert second.activate_replay_rows() == 1
    rows = second.claim_due(limit=1, replay_only=True)

    assert [row.id for row in rows] == [row_id]
    second.close()


def test_acknowledge_removes_successful_delivery(tmp_path):
    outbox = HindsightOutbox(tmp_path / "outbox.sqlite3")
    row_id = outbox.enqueue(**entry())
    row = outbox.claim_due(limit=1)[0]

    outbox.acknowledge(row_id, claim_token=row.claim_token)

    assert outbox.claim_due(limit=1) == []
    outbox.close()


def test_operation_id_is_sent_when_client_supports_idempotent_retain(tmp_path):
    outbox = HindsightOutbox(tmp_path / "outbox.sqlite3")
    row_id = outbox.enqueue(**entry())
    row = outbox.claim(row_id)
    calls = []

    class Client:
        def aretain_batch(self, *, bank_id, items, document_id, retain_async, operation_id=None):
            calls.append(
                {
                    "bank_id": bank_id,
                    "items": items,
                    "document_id": document_id,
                    "retain_async": retain_async,
                    "operation_id": operation_id,
                }
            )
            return object()

    provider = HindsightMemoryProvider()
    provider._outbox = outbox
    provider._run_hindsight_operation = lambda operation: operation(Client())

    provider._deliver_outbox_row(row)

    assert len(calls) == 1
    assert calls[0]["operation_id"] == row.operation_id
    outbox.close()


def test_provider_keeps_delivery_when_hindsight_is_offline(tmp_path):
    outbox = HindsightOutbox(tmp_path / "outbox.sqlite3")
    provider = HindsightMemoryProvider()
    provider._outbox = outbox
    row_id = outbox.enqueue(**entry(retain_async=False))
    row = outbox.claim(row_id)
    calls = []

    def offline(_operation):
        raise ConnectionError("Hindsight offline")

    provider._run_hindsight_operation = offline
    provider._deliver_outbox_row(row)
    import time
    time.sleep(1.05)
    retry = outbox.claim_due(limit=1)[0]
    assert retry.id == row_id
    assert retry.attempts == 1
    assert "ConnectionError" in retry.last_error

    provider._run_hindsight_operation = lambda operation: calls.append(operation) or object()
    provider._deliver_outbox_row(retry)

    assert outbox.claim_due(limit=1) == []
    assert len(calls) == 1
    outbox.close()
