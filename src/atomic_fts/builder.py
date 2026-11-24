"""Atomic shadow-build SQLite + FTS5 indexer."""

from __future__ import annotations

import gzip
import json
import os
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple, Union

from atomic_fts.schema import (
    INSERT_DOCUMENT_SQL,
    INSERT_FTS_SQL,
    SCHEMA_VERSION,
    DocumentInput,
    coerce_document_row,
    create_schema,
)

SIDECAR_SUFFIXES: Tuple[str, ...] = ("-wal", "-shm", "-journal")
ALLOWED_FINAL_JOURNAL_MODES = frozenset({"DELETE", "WAL"})


class BuildError(RuntimeError):
    """Raised when an atomic index build fails."""


class IntegrityCheckError(BuildError):
    """Raised when SQLite or FTS5 integrity verification fails."""


@dataclass(frozen=True)
class BuildConfig:
    """Tuning and durability parameters for atomic shadow builds."""

    chunk_size: int = 20_000
    cache_size_kib: int = 128_000
    final_journal_mode: str = "DELETE"
    run_analyze: bool = True
    run_fts_integrity_check: bool = True

    def validated_journal_mode(self) -> str:
        mode = self.final_journal_mode.strip().upper()
        if mode not in ALLOWED_FINAL_JOURNAL_MODES:
            raise ValueError(
                f"final_journal_mode must be one of {sorted(ALLOWED_FINAL_JOURNAL_MODES)}, got {self.final_journal_mode!r}"
            )
        return mode

    def validate(self) -> None:
        if self.chunk_size < 1:
            raise ValueError(f"chunk_size must be >= 1, got {self.chunk_size}")
        if self.cache_size_kib < 1:
            raise ValueError(f"cache_size_kib must be >= 1, got {self.cache_size_kib}")
        self.validated_journal_mode()


@dataclass(frozen=True)
class BuildStats:
    """Summary metrics emitted after a successful atomic index swap."""

    target_path: Path
    documents_indexed: int
    fts_rows_indexed: int
    elapsed_seconds: float
    docs_per_second: float
    db_size_bytes: int
    db_size_mb: float
    journal_mode: str
    integrity_status: str


def _remove_file_if_exists(path: Path) -> None:
    if path.exists():
        path.unlink()


def _remove_sidecars(base_path: Path) -> None:
    for suffix in SIDECAR_SUFFIXES:
        _remove_file_if_exists(Path(f"{base_path}{suffix}"))


def _cleanup_shadow_artifacts(tmp_path: Path) -> None:
    _remove_sidecars(tmp_path)
    _remove_file_if_exists(tmp_path)


def _close_quietly(conn: Optional[sqlite3.Connection]) -> None:
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass


def _populate_fts_in_chunks(conn: sqlite3.Connection, chunk_size: int) -> int:
    """Populate `documents_fts` in keyset-paginated chunks preserving `rowid == documents.id`."""
    read_cur = conn.cursor()
    write_cur = conn.cursor()
    last_id = 0
    total_fts = 0

    try:
        while True:
            rows = read_cur.execute(
                """
                SELECT id, service_key, title, body, tags, doc_type, severity
                FROM documents
                WHERE id > ?
                ORDER BY id ASC
                LIMIT ?
                """,
                (last_id, chunk_size),
            ).fetchall()
            if not rows:
                break
            last_id = int(rows[-1][0])
            write_cur.executemany(INSERT_FTS_SQL, rows)
            total_fts += len(rows)
    finally:
        read_cur.close()
        write_cur.close()

    return total_fts


def _verify_integrity(conn: sqlite3.Connection, verify_fts: bool) -> str:
    cur = conn.cursor()
    try:
        rows = cur.execute("PRAGMA integrity_check;").fetchall()
        if rows != [("ok",)]:
            raise IntegrityCheckError(f"SQLite PRAGMA integrity_check failed: {rows!r}")
        if verify_fts:
            cur.execute("INSERT INTO documents_fts(documents_fts) VALUES('integrity-check');")
            conn.commit()
    finally:
        cur.close()
    return "ok"


def iter_jsonl_documents(paths: Sequence[Union[str, Path]]) -> Iterator[Dict[str, Any]]:
    """Stream incident/post-mortem/runbook JSON objects from .jsonl or .jsonl.gz files."""
    for raw_path in paths:
        path = Path(raw_path)
        if not path.exists():
            raise FileNotFoundError(f"Input file not found: {path}")
        opener = gzip.open if path.suffix.lower() == ".gz" else open
        with opener(path, "rt", encoding="utf-8") as handle:
            for line_no, raw_line in enumerate(handle, start=1):
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise ValueError(f"Invalid JSON in {path}:{line_no}: {exc}") from exc
                if not isinstance(record, dict):
                    raise ValueError(f"Expected JSON object in {path}:{line_no}, got {type(record).__name__}")
                yield record


def build_atomic_index(
    documents: Iterable[DocumentInput],
    target_path: Union[str, Path],
    config: Optional[BuildConfig] = None,
    on_stage: Optional[Callable[[str, int], None]] = None,
) -> BuildStats:
    """Build a SQLite + FTS5 archive in `<target>.tmp` and swap atomically into place.

    Any exception raised during schema creation, bulk ingestion, FTS population,
    integrity verification, or final swap cleans up `.tmp` artifacts and leaves any
    pre-existing live database at `target_path` completely untouched.
    """
    cfg = config or BuildConfig()
    cfg.validate()
    final_mode = cfg.validated_journal_mode()

    target = Path(target_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = Path(f"{target}.tmp")

    # Remove stale temporary artifacts from any prior aborted attempt before opening.
    _cleanup_shadow_artifacts(tmp_path)

    started = time.perf_counter()
    conn: Optional[sqlite3.Connection] = None
    doc_count = 0
    fts_count = 0
    observed_mode = final_mode
    integrity_status = "unknown"

    try:
        conn = sqlite3.connect(str(tmp_path))
        # Disposable shadow build: disable rollback journal & fsync during bulk load.
        conn.execute("PRAGMA journal_mode = OFF;")
        conn.execute("PRAGMA synchronous = OFF;")
        conn.execute("PRAGMA temp_store = MEMORY;")
        conn.execute(f"PRAGMA cache_size = -{int(cfg.cache_size_kib)};")

        create_schema(conn)
        if on_stage is not None:
            on_stage("schema_created", 0)

        write_cur = conn.cursor()
        batch: List[Tuple[Any, ...]] = []
        try:
            conn.execute("BEGIN")
            for item in documents:
                batch.append(coerce_document_row(item))
                if len(batch) >= cfg.chunk_size:
                    write_cur.executemany(INSERT_DOCUMENT_SQL, batch)
                    doc_count += len(batch)
                    batch.clear()
                    if on_stage is not None:
                        on_stage("documents_chunk", doc_count)

            if batch:
                write_cur.executemany(INSERT_DOCUMENT_SQL, batch)
                doc_count += len(batch)
                batch.clear()
                if on_stage is not None:
                    on_stage("documents_chunk", doc_count)
        finally:
            write_cur.close()

        fts_count = _populate_fts_in_chunks(conn, cfg.chunk_size)
        if on_stage is not None:
            on_stage("fts_populated", fts_count)

        built_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        meta_rows = [
            ("schema_version", SCHEMA_VERSION),
            ("doc_count", str(doc_count)),
            ("fts_count", str(fts_count)),
            ("built_at_utc", built_at),
            ("journal_mode", final_mode),
        ]
        conn.executemany("INSERT INTO engine_meta (key, value) VALUES (?, ?)", meta_rows)
        conn.commit()

        if cfg.run_analyze:
            conn.execute("ANALYZE;")
        conn.execute("PRAGMA optimize;")
        if conn.in_transaction:
            conn.commit()

        # Transition cleanly out of journal_mode=OFF before integrity validation & swap.
        mode_row = conn.execute(f"PRAGMA journal_mode = {final_mode};").fetchone()
        observed_mode = str(mode_row[0]).upper() if mode_row else final_mode
        conn.execute("PRAGMA synchronous = NORMAL;")
        if final_mode == "WAL":
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")

        integrity_status = _verify_integrity(conn, verify_fts=cfg.run_fts_integrity_check)
        if final_mode == "WAL":
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")

        if on_stage is not None:
            on_stage("integrity_verified", doc_count)

        # Windows SQLite requirement: close every connection handle before file cleanup or swap.
        conn.close()
        conn = None

        _remove_sidecars(tmp_path)
        _remove_sidecars(target)
        os.replace(str(tmp_path), str(target))
    except Exception:
        _close_quietly(conn)
        conn = None
        _cleanup_shadow_artifacts(tmp_path)
        raise

    elapsed = max(time.perf_counter() - started, 1e-9)
    size_bytes = target.stat().st_size
    return BuildStats(
        target_path=target,
        documents_indexed=doc_count,
        fts_rows_indexed=fts_count,
        elapsed_seconds=elapsed,
        docs_per_second=(doc_count / elapsed) if doc_count else 0.0,
        db_size_bytes=size_bytes,
        db_size_mb=size_bytes / (1024.0 * 1024.0),
        journal_mode=observed_mode,
        integrity_status=integrity_status,
    )
