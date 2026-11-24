"""Tests for read-only query engine: query_only enforcement, BM25, snippets, filters, and regex."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from atomic_fts.builder import BuildConfig, build_atomic_index
from atomic_fts.query import AtomicSearchEngine, InvalidQueryError, build_safe_fts_expression
from atomic_fts.schema import IncidentDocument


@pytest.fixture()
def seeded_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "archive.db"
    base_ts = 1_767_225_600_000
    docs = [
        IncidentDocument(
            doc_uid="INC-001",
            doc_type="incident",
            service_key="payments-gateway",
            severity="SEV1",
            timestamp_ms=base_ts + 10_000,
            author="oncall-alpha",
            title="Timeout cascade on HikariPool-42",
            body=(
                "HikariPool-42 exhausted all database connections. "
                "Timeout persisted for 14 minutes with ERR-504-901 responses."
            ),
            tags="postgres hikari timeout",
            metadata={"region": "sa-east-1", "mttr": 14},
        ),
        IncidentDocument(
            doc_uid="INC-002",
            doc_type="incident",
            service_key="payments-gateway",
            severity="SEV2",
            timestamp_ms=base_ts + 20_000,
            author="oncall-beta",
            title="Elevated GC pause during settlement batch",
            body="Minor timeout observed on HikariPool-07 during nightly compaction; recovered automatically.",
            tags="jvm gc",
            metadata={"region": "us-east-1", "mttr": 3},
        ),
        IncidentDocument(
            doc_uid="PM-003",
            doc_type="postmortem",
            service_key="checkout-api",
            severity="SEV1",
            timestamp_ms=base_ts + 30_000,
            author="staff-sre",
            title="Post-mortem: Replica deadlock and connection timeout",
            body="Deadlock on shard-4 triggered lock wait timeout and ERR-502-404 on checkout.",
            tags="deadlock postgres",
            metadata={"region": "us-east-1", "mttr": 27},
        ),
        IncidentDocument(
            doc_uid="RB-004",
            doc_type="runbook",
            service_key="auth-edge",
            severity="INFO",
            timestamp_ms=base_ts + 40_000,
            author="security-ops",
            title="Runbook: Emergency TLS certificate rollback",
            body="Use when ACME rotation reports livelock shard-4 or ERR-502-118 handshake failure.",
            tags="tls runbook",
            metadata={"region": "eu-west-1"},
        ),
    ]
    build_atomic_index(docs, db_path)
    return db_path


def test_pragma_query_only_blocks_all_mutations(seeded_db: Path) -> None:
    with AtomicSearchEngine(seeded_db) as engine:
        flag = engine.connection.execute("PRAGMA query_only;").fetchone()[0]
        assert flag == 1

        mutations = (
            "INSERT INTO documents (doc_uid, doc_type, service_key, severity, timestamp_ms, occurred_at) "
            "VALUES ('MUT-1', 'incident', 'x', 'SEV1', 1, '2026-01-01T00:00:00Z')",
            "UPDATE documents SET severity = 'INFO' WHERE doc_uid = 'INC-001'",
            "DELETE FROM documents WHERE doc_uid = 'INC-001'",
            "CREATE TABLE rogue (id INTEGER)",
            "DROP TABLE documents",
        )
        for sql in mutations:
            with pytest.raises(sqlite3.OperationalError):
                engine.connection.execute(sql)


def test_bm25_ranking_and_snippet_extraction(seeded_db: Path) -> None:
    with AtomicSearchEngine(seeded_db) as engine:
        resp = engine.search("timeout")
        assert resp.strategy == "fts5_bm25"
        assert resp.elapsed_ms >= 0.0
        assert resp.total_returned == 3

        # INC-001 mentions 'timeout' in title, body, and tags -> highest relevance (lowest negative BM25).
        assert resp.hits[0].doc_uid == "INC-001"
        assert resp.hits[0].bm25_score <= resp.hits[1].bm25_score
        assert "[Timeout]" in resp.hits[0].snippet or "[timeout]" in resp.hits[0].snippet.lower()


def test_composite_index_query_plans_and_filters(seeded_db: Path) -> None:
    base_ts = 1_767_225_600_000
    with AtomicSearchEngine(seeded_db) as engine:
        # Verify SQLite planner actually uses our composite indexes.
        eqp_service = engine.connection.execute(
            "EXPLAIN QUERY PLAN SELECT id FROM documents WHERE service_key = ? AND timestamp_ms BETWEEN ? AND ?",
            ("payments-gateway", base_ts, base_ts + 50_000),
        ).fetchall()
        assert any("idx_documents_service_ts" in str(row[-1]) for row in eqp_service)

        eqp_severity = engine.connection.execute(
            "EXPLAIN QUERY PLAN SELECT id FROM documents WHERE severity = ? AND timestamp_ms >= ?",
            ("SEV1", base_ts),
        ).fetchall()
        assert any("idx_documents_severity_ts" in str(row[-1]) for row in eqp_severity)

        resp_svc = engine.search("timeout", service_key="payments-gateway", severity="SEV1")
        assert [h.doc_uid for h in resp_svc.hits] == ["INC-001"]

        resp_window = engine.search(
            "",
            start_ts_ms=base_ts + 20_000,
            end_ts_ms=base_ts + 30_000,
            sort_by="recent",
        )
        assert resp_window.strategy == "metadata_scan"
        assert [h.doc_uid for h in resp_window.hits] == ["PM-003", "INC-002"]


def test_fts5_prefilter_and_regex_refinement(seeded_db: Path) -> None:
    with AtomicSearchEngine(seeded_db) as engine:
        # 1. Explicit FTS query + regex refinement
        resp1 = engine.search("HikariPool", regex_pattern=r"HikariPool-42\b")
        assert resp1.strategy == "fts5_prefilter_regex"
        assert [h.doc_uid for h in resp1.hits] == ["INC-001"]

        # 2. Regex-only with sound mandatory FTS5 pre-filters ('ERR' and '502')
        resp2 = engine.search("", regex_pattern=r"ERR-502-\d{3}", limit=10)
        assert resp2.strategy == "fts5_prefilter_regex"
        assert resp2.fts_expression == '"ERR"* "502"*'
        assert {h.doc_uid for h in resp2.hits} == {"PM-003", "RB-004"}

        # 2b. Regex with character class narrows pre-filter to mandatory 'ERR' token
        resp2b = engine.search("", regex_pattern=r"ERR-50[24]-\d{3}", limit=10)
        assert resp2b.strategy == "fts5_prefilter_regex"
        assert resp2b.fts_expression == '"ERR"*'
        assert {h.doc_uid for h in resp2b.hits} == {"INC-001", "PM-003", "RB-004"}

        # 3. Pagination with regex refinement
        resp2_page2 = engine.search("", regex_pattern=r"ERR-502-\d{3}", limit=1, offset=1)
        assert resp2_page2.total_returned == 1
        assert len(resp2.hits) == 2
        assert resp2_page2.hits[0].doc_uid == resp2.hits[1].doc_uid

        # 4. Alternation regex falls back safely without false negatives
        resp3 = engine.search("", regex_pattern=r"(deadlock|livelock) on|livelock shard-4")
        assert resp3.strategy == "metadata_regex"
        assert {h.doc_uid for h in resp3.hits} == {"PM-003", "RB-004"}


def test_safe_fts_expression_and_invalid_queries(seeded_db: Path) -> None:
    assert build_safe_fts_expression("ERR-504 AND auth*") == '"ERR-504" "AND" "auth"*'
    assert build_safe_fts_expression("/// ---") == ""

    with AtomicSearchEngine(seeded_db) as engine:
        assert engine.search("///").strategy == "empty_tokens"
        with pytest.raises(InvalidQueryError):
            engine.search("timeout", regex_pattern="[unclosed(")
        with pytest.raises(InvalidQueryError):
            engine.search("timeout", sort_by="random")
        with pytest.raises(InvalidQueryError):
            engine.search("timeout", start_ts_ms=200, end_ts_ms=100)


def test_get_stats_and_get_document(seeded_db: Path) -> None:
    with AtomicSearchEngine(seeded_db) as engine:
        st = engine.get_stats()
        assert st.document_count == 4
        assert st.fts_count == 4
        assert st.query_only is True
        assert st.by_severity == {"INFO": 1, "SEV1": 2, "SEV2": 1}
        assert st.by_doc_type == {"incident": 2, "postmortem": 1, "runbook": 1}

        doc = engine.get_document("PM-003")
        assert doc is not None
        assert doc.metadata["mttr"] == 27
        assert engine.get_document("MISSING-999") is None


def test_raw_fts_syntax_error_and_filtered_regex_offset_pagination(tmp_path: Path) -> None:
    db_path = tmp_path / "paginated.db"
    base_ts = 1_767_225_600_000
    docs = [
        IncidentDocument(
            doc_uid=f"INC-PAG-{idx:02d}",
            doc_type="incident",
            service_key="orders-api" if idx <= 3 else "billing-api",
            severity="SEV1" if idx <= 3 else "SEV2",
            timestamp_ms=base_ts + idx * 1000,
            title=f"Worker saturation alert {idx}",
            body=f"Thread pool limit reached WARN-{100 + idx} on worker node",
            tags="pool saturation",
        )
        for idx in range(1, 5)
    ]
    build_atomic_index(docs, db_path, config=BuildConfig(final_journal_mode="WAL"))

    with AtomicSearchEngine(db_path) as engine:
        with pytest.raises(InvalidQueryError):
            engine.search('AND OR "unclosed', raw_fts=True)

        page = engine.search(
            "saturation",
            service_key="orders-api",
            severity="SEV1",
            regex_pattern=r"WARN-10[123]",
            limit=1,
            offset=1,
        )
        assert page.total_returned == 1
        assert page.hits[0].service_key == "orders-api"
        assert page.hits[0].severity == "SEV1"
        assert page.hits[0].doc_uid in {"INC-PAG-01", "INC-PAG-02", "INC-PAG-03"}

