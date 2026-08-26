"""Durable client-side delivery buffer for Hindsight retain requests."""

from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class OutboxRow:
    id: int
    dedupe_key: str
    bank_id: str
    document_id: str
    update_mode: str | None
    retain_async: bool
    operation_id: str | None
    claim_token: str | None
    item: dict[str, Any]
    attempts: int
    available_at: float
    state: str
    last_error: str | None


class HindsightOutbox:
    """Small SQLite outbox; Hindsight is unaware that it exists."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self.path.parent.chmod(0o700)
        except OSError:
            pass
        self._db = sqlite3.connect(self.path, timeout=30, check_same_thread=False)
        try:
            self.path.chmod(0o600)
        except OSError:
            pass
        self._db.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA busy_timeout=30000")
        self._secure_sidecars()
        self._db.execute(
            """
            CREATE TABLE IF NOT EXISTS hindsight_outbox (
                id INTEGER PRIMARY KEY,
                dedupe_key TEXT NOT NULL UNIQUE,
                bank_id TEXT NOT NULL,
                document_id TEXT NOT NULL,
                update_mode TEXT,
                retain_async INTEGER NOT NULL,
                operation_id TEXT,
                claim_token TEXT,
                item_json TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                available_at REAL NOT NULL,
                replay_eligible INTEGER NOT NULL DEFAULT 1,
                state TEXT NOT NULL DEFAULT 'pending',
                last_error TEXT,
                claimed_at REAL
            )
            """
        )
        columns = {
            row["name"] for row in self._db.execute("PRAGMA table_info(hindsight_outbox)")
        }
        if "operation_id" not in columns:
            self._db.execute("ALTER TABLE hindsight_outbox ADD COLUMN operation_id TEXT")
        if "claim_token" not in columns:
            self._db.execute("ALTER TABLE hindsight_outbox ADD COLUMN claim_token TEXT")
        if "replay_eligible" not in columns:
            self._db.execute(
                "ALTER TABLE hindsight_outbox "
                "ADD COLUMN replay_eligible INTEGER NOT NULL DEFAULT 1"
            )
        self._db.execute(
            "CREATE INDEX IF NOT EXISTS hindsight_outbox_due "
            "ON hindsight_outbox(state, available_at)"
        )
        self._db.commit()

    def _secure_sidecars(self) -> None:
        """Keep SQLite WAL/SHM files private; they contain retain payloads."""
        for suffix in ("-wal", "-shm"):
            sidecar = Path(f"{self.path}{suffix}")
            if sidecar.exists():
                try:
                    sidecar.chmod(0o600)
                except OSError:
                    pass

    def enqueue(
        self,
        *,
        dedupe_key: str,
        bank_id: str,
        document_id: str,
        update_mode: str | None,
        retain_async: bool,
        item: dict[str, Any],
        operation_id: str | None = None,
    ) -> int:
        operation_id = operation_id or str(uuid.uuid4())
        with self._lock, self._db:
            cur = self._db.execute(
                """
                INSERT INTO hindsight_outbox
                  (dedupe_key, bank_id, document_id, update_mode, retain_async,
                   operation_id, item_json, available_at, replay_eligible)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)
                ON CONFLICT(dedupe_key) DO NOTHING
                """,
                (
                    dedupe_key,
                    bank_id,
                    document_id,
                    update_mode,
                    int(retain_async),
                    operation_id,
                    json.dumps(item, ensure_ascii=False, sort_keys=True),
                    time.time(),
                ),
            )
            if cur.rowcount:
                return int(cur.lastrowid)
            row = self._db.execute(
                "SELECT id FROM hindsight_outbox WHERE dedupe_key = ?", (dedupe_key,)
            ).fetchone()
            if row is None:
                raise RuntimeError("Hindsight outbox insert was not committed")
            return int(row[0])

    def claim_due(self, *, limit: int = 1, replay_only: bool = False) -> list[OutboxRow]:
        now = time.time()
        replay_clause = " AND current.replay_eligible = 1" if replay_only else ""
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                rows = self._db.execute(
                    f"""
                    SELECT current.* FROM hindsight_outbox AS current
                    WHERE current.state = 'pending' AND current.available_at <= ?{replay_clause}
                      AND NOT EXISTS (
                        SELECT 1 FROM hindsight_outbox AS previous
                        WHERE previous.document_id = current.document_id
                          AND previous.id < current.id
                          AND previous.state IN ('pending', 'in_flight')
                      )
                    ORDER BY current.id LIMIT ?
                    """,
                    (now, limit),
                ).fetchall()
                ids = [row["id"] for row in rows]
                claim_tokens = {row_id: str(uuid.uuid4()) for row_id in ids}
                if ids:
                    self._db.executemany(
                        """
                        UPDATE hindsight_outbox
                        SET state='in_flight', claimed_at=?, claim_token=?, replay_eligible=0
                        WHERE id=?
                        """,
                        [(now, claim_tokens[row_id], row_id) for row_id in ids],
                    )
                self._db.commit()
            except Exception:
                self._db.rollback()
                raise
        return [
            self._row({**dict(row), "claim_token": claim_tokens[row["id"]]})
            for row in rows
        ]

    def claim(self, row_id: int) -> OutboxRow | None:
        now = time.time()
        claim_token = str(uuid.uuid4())
        with self._lock, self._db:
            cur = self._db.execute(
                """
                UPDATE hindsight_outbox
                SET state='in_flight', claimed_at=?, claim_token=?, replay_eligible=0
                WHERE id=? AND state='pending' AND available_at <= ?
                  AND NOT EXISTS (
                    SELECT 1 FROM hindsight_outbox AS previous
                    WHERE previous.document_id = hindsight_outbox.document_id
                      AND previous.id < hindsight_outbox.id
                      AND previous.state IN ('pending', 'in_flight')
                  )
                """,
                (now, claim_token, row_id, now),
            )
            if cur.rowcount != 1:
                return None
            row = self._db.execute(
                "SELECT * FROM hindsight_outbox WHERE id = ?", (row_id,)
            ).fetchone()
        return self._row(row) if row else None

    def ensure_operation_id(self, row_id: int, operation_id: str | None = None) -> str:
        operation_id = operation_id or str(uuid.uuid4())
        with self._lock, self._db:
            self._db.execute(
                """
                UPDATE hindsight_outbox
                SET operation_id = COALESCE(operation_id, ?)
                WHERE id = ?
                """,
                (operation_id, row_id),
            )
            row = self._db.execute(
                "SELECT operation_id FROM hindsight_outbox WHERE id = ?", (row_id,)
            ).fetchone()
        if row is None or not row["operation_id"]:
            raise KeyError(f"Hindsight outbox row not found: {row_id}")
        return str(row["operation_id"])

    def acknowledge(self, row_id: int, *, claim_token: str | None) -> bool:
        if not claim_token:
            return False
        with self._lock, self._db:
            cur = self._db.execute(
                """
                DELETE FROM hindsight_outbox
                WHERE id=? AND state='in_flight' AND claim_token=?
                """,
                (row_id, claim_token),
            )
        return cur.rowcount == 1

    def reschedule(
        self,
        row_id: int,
        error: str,
        *,
        claim_token: str | None,
        delay_seconds: float,
    ) -> bool:
        if not claim_token:
            return False
        with self._lock, self._db:
            cur = self._db.execute(
                """
                UPDATE hindsight_outbox
                SET state='pending', attempts=attempts+1, available_at=?,
                    last_error=?, claimed_at=NULL, claim_token=NULL, replay_eligible=1
                WHERE id=? AND state='in_flight' AND claim_token=?
                """,
                (
                    time.time() + max(0.0, delay_seconds),
                    str(error)[:2000],
                    row_id,
                    claim_token,
                ),
            )
        return cur.rowcount == 1

    def release_stale_claims(self, *, lease_seconds: float = 300) -> int:
        with self._lock, self._db:
            cur = self._db.execute(
                """
                UPDATE hindsight_outbox
                SET state='pending', claimed_at=NULL, claim_token=NULL, available_at=?
                WHERE state='in_flight' AND claimed_at < ?
                """,
                (time.time(), time.time() - lease_seconds),
            )
        return cur.rowcount

    def activate_replay_rows(self) -> int:
        """Make rows from a previous process eligible for startup replay."""
        with self._lock, self._db:
            cur = self._db.execute(
                """
                UPDATE hindsight_outbox
                SET replay_eligible=1
                WHERE state='pending'
                """
            )
        return cur.rowcount

    def has_due(self, *, replay_only: bool = False) -> bool:
        """Return whether a pending row is ready for the delivery coordinator."""
        replay_clause = " AND replay_eligible = 1" if replay_only else ""
        with self._lock:
            row = self._db.execute(
                f"""
                SELECT 1 FROM hindsight_outbox
                WHERE state='pending' AND available_at <= ?{replay_clause}
                LIMIT 1
                """,
                (time.time(),),
            ).fetchone()
        return row is not None

    def _row(self, row: sqlite3.Row) -> OutboxRow:
        return OutboxRow(
            id=int(row["id"]),
            dedupe_key=row["dedupe_key"],
            bank_id=row["bank_id"],
            document_id=row["document_id"],
            update_mode=row["update_mode"],
            retain_async=bool(row["retain_async"]),
            operation_id=row["operation_id"],
            claim_token=row["claim_token"],
            item=json.loads(row["item_json"]),
            attempts=int(row["attempts"]),
            available_at=float(row["available_at"]),
            state=row["state"],
            last_error=row["last_error"],
        )

    def close(self) -> None:
        with self._lock:
            self._db.close()
