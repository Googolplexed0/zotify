"""Crash-safe lifecycle journal for downloaded media files.

The journal is intentionally advisory: it never makes an archive entry valid.
Only a non-empty, probed, tagged stage atomically published by the downloader
may be marked complete.
"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from threading import RLock
from time import time
from typing import Iterator


class DownloadJournal:
    """Persist the last download lifecycle state for each Spotify URI."""

    _instances: dict[str, DownloadJournal] = {}
    _instance_lock = RLock()

    def __new__(cls, root: str | Path):
        root_path = Path(root).expanduser().resolve()
        key = str(root_path)
        with cls._instance_lock:
            instance = cls._instances.get(key)
            if instance is None:
                instance = super().__new__(cls)
                instance._root_path = root_path
                cls._instances[key] = instance
            return instance

    def __init__(self, root: str | Path):
        with self._instance_lock:
            if getattr(self, "_initialized", False):
                return
            self.path = self._root_path / ".zotify-downloads.sqlite3"
            self.path.parent.mkdir(parents=True, exist_ok=True)
            db = sqlite3.connect(self.path, timeout=30)
            try:
                db.execute("PRAGMA journal_mode=WAL")
                db.execute("PRAGMA synchronous=FULL")
                db.execute("""CREATE TABLE IF NOT EXISTS downloads (
                    uri TEXT PRIMARY KEY,
                    state TEXT NOT NULL,
                    stage_path TEXT,
                    final_path TEXT,
                    error TEXT,
                    tag_attempts INTEGER NOT NULL DEFAULT 0,
                    updated_at REAL NOT NULL
                )""")
                columns = {row[1] for row in db.execute("PRAGMA table_info(downloads)")}
                if "tag_attempts" not in columns:
                    db.execute("ALTER TABLE downloads ADD COLUMN tag_attempts INTEGER NOT NULL DEFAULT 0")
                db.commit()
            finally:
                db.close()
            self._initialized = True

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        db = sqlite3.connect(self.path, timeout=30)
        db.execute("PRAGMA synchronous=FULL")
        try:
            yield db
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def get(self, uri: str) -> dict | None:
        with self._connection() as db:
            row = db.execute(
                "SELECT state, stage_path, final_path, error, updated_at, tag_attempts "
                "FROM downloads WHERE uri = ?", (uri,)
            ).fetchone()
        if row is None:
            return None
        return dict(zip(("state", "stage_path", "final_path", "error", "updated_at", "tag_attempts"), row))

    def increment_tag_attempts(self, uri: str) -> int:
        with self._connection() as db:
            db.execute("UPDATE downloads SET tag_attempts = tag_attempts + 1, updated_at = ? WHERE uri = ?",
                       (time(), uri))
            row = db.execute("SELECT tag_attempts FROM downloads WHERE uri = ?", (uri,)).fetchone()
        return int(row[0]) if row else 0

    def reset_tag_attempts(self, uri: str) -> None:
        with self._connection() as db:
            db.execute("UPDATE downloads SET tag_attempts = 0, updated_at = ? WHERE uri = ?",
                       (time(), uri))

    def set_state(self, uri: str, state: str, stage_path: Path | None = None,
                  final_path: Path | None = None, error: str | None = None) -> None:
        with self._connection() as db:
            db.execute("""INSERT INTO downloads
                (uri, state, stage_path, final_path, error, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(uri) DO UPDATE SET
                    state=excluded.state, stage_path=excluded.stage_path,
                    final_path=excluded.final_path, error=excluded.error,
                    updated_at=excluded.updated_at""",
                (uri, state, str(stage_path) if stage_path else None,
                 str(final_path) if final_path else None, error, time()))
