"""Tests for atomic shadow-build lifecycle, crash safety, sidecar cleanup, and Unicode FTS5."""

from __future__ import annotations

import hashlib
import os
import sqlite3
from pathlib import Path
from typing import Iterator, List, Tuple

import pytest

import atomic_fts.builder as builder_mod
from atomic_fts.builder import BuildConfig, IntegrityCheckError, build_atomic_index
from atomic_fts.query import AtomicSearchEngine
from atomic_fts.schema import IncidentDocument


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _sample_doc(idx: int, *, title: str = "", body: str = "", service: str = "payments-gateway") -> IncidentDocument:
    return IncidentDocument(
        doc_uid=f"INC-2026-{idx:05d}",
        doc_type="incident" if idx % 2 == 0 else "postmortem",
        service_key=service,
        severity="SEV1" if idx % 3 == 0 else "SEV2",
        timestamp_ms=1_767_225_600_000 + idx * 1000,
        author="sre-oncall",
        title=title or f"Incident {idx} on {service}",
        body=body or f"Diagnostic payload for record {idx} with HikariPool-{100 + idx}",
        tags="postgres timeout",
        metadata={"seq": idx},
    )


def test_atomic_build_across_chunks_and_rowid_sync(tmp_path: Path) -> None:
    db_path = tmp_path / "archive.db"
    docs = [_sample_doc(i) for i in range(1, 24)]

    stats = build_atomic_index(
        docs,
        db_path,
        config=BuildConfig(chunk_size=7, final_journal_mode="DELETE"),
    )

    assert stats.documents_indexed == 23
    assert stats.fts_rows_indexed == 23
    assert stats.integrity_status == "ok"
    assert stats.journal_mode == "DELETE"
    assert db_path.exists()
    assert not Path(f"{db_path}.tmp").exists()
    for suffix in ("-wal", "-shm", "-journal"):
        assert not Path(f"{db_path}{suffix}").exists()
        assert not Path(f"{db_path}.tmp{suffix}").exists()

    conn = sqlite3.connect(str(db_path))
    try:
        mismatches = conn.execute(
            """
            SELECT COUNT(*)
            FROM documents d
            LEFT JOIN documents_fts f ON f.rowid = d.id
            WHERE f.rowid IS NULL
               OR f.service_key != d.service_key
               OR f.title != d.title
               OR f.body != d.body
            """
        ).fetchone()[0]
        assert mismatches == 0

        indexes = {
            row[0]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='index'").fetchall()
        }
        assert "idx_documents_service_ts" in indexes
        assert "idx_documents_severity_ts" in indexes
    finally:
        conn.close()


def test_simulated_crash_mid_ingestion_preserves_live_db(tmp_path: Path) -> None:
    db_path = tmp_path / "live_incidents.db"
    v1_docs = [_sample_doc(i, title=f"V1 Live Incident {i}") for i in range(1, 11)]
    build_atomic_index(v1_docs, db_path, config=BuildConfig(chunk_size=5))

    live_digest_before = _sha256(db_path)

    def crashing_stream() -> Iterator[IncidentDocument]:
        # Exceed chunk_size=4 so multiple chunks flush to <target>.db.tmp before crashing.
        for i in range(100, 110):
            yield _sample_doc(i, title=f"V2 Partial Incident {i}")
        raise RuntimeError("simulated power loss mid-rebuild")

    with pytest.raises(RuntimeError, match="simulated power loss mid-rebuild"):
        build_atomic_index(crashing_stream(), db_path, config=BuildConfig(chunk_size=4))

    assert _sha256(db_path) == live_digest_before
    assert not Path(f"{db_path}.tmp").exists()
    for suffix in ("-wal", "-shm", "-journal"):
        assert not Path(f"{db_path}.tmp{suffix}").exists()

    with AtomicSearchEngine(db_path) as engine:
        stats = engine.get_stats()
        assert stats.document_count == 10
        resp = engine.search("V1 Live")
        assert resp.total_returned == 10
        assert engine.search("V2 Partial").total_returned == 0


def test_simulated_crash_during_fts_stage_preserves_live_db(tmp_path: Path) -> None:
    db_path = tmp_path / "live_incidents.db"
    build_atomic_index([_sample_doc(1, title="Baseline Incident")], db_path)
    live_digest_before = _sha256(db_path)

    def fail_on_fts(stage: str, count: int) -> None:
        if stage == "fts_populated":
            raise OSError("simulated disk error before atomic swap")

    with pytest.raises(OSError, match="simulated disk error before atomic swap"):
        build_atomic_index(
            [_sample_doc(2, title="Aborted Candidate")],
            db_path,
            on_stage=fail_on_fts,
        )

    assert _sha256(db_path) == live_digest_before
    assert not Path(f"{db_path}.tmp").exists()

    with AtomicSearchEngine(db_path) as engine:
        assert engine.get_document("INC-2026-00001") is not None
        assert engine.get_document("INC-2026-00002") is None


def test_atomic_rebuild_replaces_live_db_and_supports_wal_mode(tmp_path: Path) -> None:
    db_path = tmp_path / "incidents.db"
    build_atomic_index([_sample_doc(1, title="Old Outage")], db_path)

    stats = build_atomic_index(
        [_sample_doc(2, title="New Runbook Verification")],
        db_path,
        config=BuildConfig(final_journal_mode="WAL"),
    )
    assert stats.journal_mode == "WAL"
    assert stats.integrity_status == "ok"
    assert not Path(f"{db_path}.tmp").exists()

    with AtomicSearchEngine(db_path) as engine:
        assert engine.get_document("INC-2026-00001") is None
        doc = engine.get_document("INC-2026-00002")
        assert doc is not None
        assert doc.title == "New Runbook Verification"


def test_unicode_diacritic_insensitive_fts5_search(tmp_path: Path) -> None:
    db_path = tmp_path / "unicode_incidents.db"
    docs = [
        IncidentDocument(
            doc_uid="INC-2026-PT-01",
            doc_type="postmortem",
            service_key="auth-edge",
            severity="SEV1",
            timestamp_ms=1_767_225_600_000,
            title="Exaustão crítica no serviço de autenticação",
            body=(
                "Ação emergencial aplicada após falha de reconciliação bancária e "
                "saturação no pool de conexões TLS."
            ),
            tags="autenticacao exaustao conexoes",
        ),
        IncidentDocument(
            doc_uid="RB-2026-EN-02",
            doc_type="runbook",
            service_key="cdn-router",
            severity="INFO",
            timestamp_ms=1_767_225_700_000,
            title="BGP egress drain procedure",
            body="Drain POP edge traffic gracefully before router maintenance.",
            tags="network bgp",
        ),
    ]
    build_atomic_index(docs, db_path)

    with AtomicSearchEngine(db_path) as engine:
        for query in (
            "exaustao critica",
            "EXAUSTÃO CRÍTICA",
            "reconciliacao bancaria",
            "conexoes",
            "acao emergencial",
            "autenticacao",
        ):
            resp = engine.search(query)
            assert resp.total_returned == 1, f"Expected match for {query!r}"
            assert resp.hits[0].doc_uid == "INC-2026-PT-01"
            assert "[" in resp.hits[0].snippet


def test_empty_corpus_and_invalid_config(tmp_path: Path) -> None:
    db_path = tmp_path / "empty.db"
    stats = build_atomic_index([], db_path)
    assert stats.documents_indexed == 0
    assert stats.fts_rows_indexed == 0
    assert stats.integrity_status == "ok"

    with pytest.raises(ValueError):
        build_atomic_index([], db_path, config=BuildConfig(chunk_size=0))
    with pytest.raises(ValueError):
        build_atomic_index([], db_path, config=BuildConfig(final_journal_mode="MEMORY"))
    with pytest.raises(IntegrityCheckError):
        raise IntegrityCheckError("synthetic check")


def test_shadow_temp_files_use_unique_pid_uuid_and_cleanup_completely(tmp_path: Path) -> None:
    db_path = tmp_path / "isolated.db"
    observed_files = []

    def inspect_mid_build(stage: str, _count: int) -> None:
        if stage == "indexes_created":
            observed_files.extend(p.name for p in tmp_path.glob("*.tmp"))

    stats = build_atomic_index(
        [_sample_doc(1, title="Isolated Build One"), _sample_doc(2, title="Isolated Build Two")],
        db_path,
        on_stage=inspect_mid_build,
    )
    assert stats.integrity_status == "ok"
    assert len(observed_files) == 1
    assert observed_files[0].startswith("isolated.db.")
    assert observed_files[0].endswith(".tmp")
    assert observed_files[0] != "isolated.db.tmp"
    assert list(tmp_path.glob("*.tmp*")) == []


def test_pre_swap_fsync_runs_on_shadow_db_before_os_replace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_path = tmp_path / "durable.db"
    events: List[Tuple[str, str]] = []
    real_fsync_file = builder_mod._fsync_file
    real_fsync_parent = builder_mod._fsync_parent_dir
    real_replace = os.replace

    def tracked_fsync_file(path: Path) -> None:
        events.append(("fsync_file", str(path)))
        real_fsync_file(path)

    def tracked_replace(src: str, dst: str) -> None:
        events.append(("replace", f"{src}->{dst}"))
        real_replace(src, dst)

    def tracked_fsync_parent(path: Path) -> None:
        events.append(("fsync_parent", str(path)))
        real_fsync_parent(path)

    monkeypatch.setattr(builder_mod, "_fsync_file", tracked_fsync_file)
    monkeypatch.setattr(os, "replace", tracked_replace)
    monkeypatch.setattr(builder_mod, "_fsync_parent_dir", tracked_fsync_parent)

    build_atomic_index([_sample_doc(1, title="Durability Check")], db_path)

    assert [name for name, _ in events] == ["fsync_file", "replace", "fsync_parent"]
    assert events[0][1].endswith(".tmp")
    assert events[1][1].endswith(f"->{db_path}")
    assert events[2][1] == str(db_path)


