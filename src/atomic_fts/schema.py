"""Schema definitions and record normalization for the incident & runbook archive."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Mapping, Sequence, Tuple, Union

SCHEMA_VERSION = "1"
VALID_DOC_TYPES = ("incident", "postmortem", "runbook")
VALID_SEVERITIES = ("SEV1", "SEV2", "SEV3", "SEV4", "INFO")

DOCUMENTS_TABLE_SQL = """
CREATE TABLE documents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    doc_uid TEXT NOT NULL UNIQUE,
    doc_type TEXT NOT NULL,
    service_key TEXT NOT NULL,
    severity TEXT NOT NULL,
    timestamp_ms INTEGER NOT NULL,
    occurred_at TEXT NOT NULL,
    author TEXT NOT NULL DEFAULT '',
    title TEXT NOT NULL DEFAULT '',
    body TEXT NOT NULL DEFAULT '',
    tags TEXT NOT NULL DEFAULT '',
    metadata_json TEXT NOT NULL DEFAULT '{}'
);
"""

ENGINE_META_TABLE_SQL = """
CREATE TABLE engine_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""

INDEXES_SQL: Tuple[str, ...] = (
    "CREATE INDEX idx_documents_service_ts ON documents(service_key, timestamp_ms);",
    "CREATE INDEX idx_documents_severity_ts ON documents(severity, timestamp_ms);",
    "CREATE INDEX idx_documents_ts ON documents(timestamp_ms);",
    "CREATE INDEX idx_documents_type_ts ON documents(doc_type, timestamp_ms);",
)

FTS5_TABLE_SQL = """
CREATE VIRTUAL TABLE documents_fts USING fts5(
    service_key,
    title,
    body,
    tags,
    doc_type UNINDEXED,
    severity UNINDEXED,
    tokenize='unicode61 remove_diacritics 2'
);
"""

INSERT_DOCUMENT_SQL = (
    "INSERT INTO documents ("
    "doc_uid, doc_type, service_key, severity, timestamp_ms, occurred_at, "
    "author, title, body, tags, metadata_json"
    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
)

INSERT_FTS_SQL = (
    "INSERT INTO documents_fts ("
    "rowid, service_key, title, body, tags, doc_type, severity"
    ") VALUES (?, ?, ?, ?, ?, ?, ?)"
)


@dataclass(frozen=True)
class IncidentDocument:
    """Canonical record representing an incident log, post-mortem, or runbook."""

    doc_uid: str
    doc_type: str
    service_key: str
    severity: str
    timestamp_ms: int
    title: str
    body: str
    occurred_at: str = ""
    author: str = ""
    tags: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)


DocumentInput = Union[IncidentDocument, Mapping[str, Any]]


def ts_ms_to_iso_utc(timestamp_ms: int) -> str:
    """Convert UTC epoch milliseconds into an ISO-8601 UTC timestamp string."""
    try:
        dt = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except (OSError, OverflowError, ValueError):
        return ""


def _normalize_tags(raw_tags: Any) -> str:
    if raw_tags is None:
        return ""
    if isinstance(raw_tags, str):
        return raw_tags.strip()
    if isinstance(raw_tags, Sequence):
        return " ".join(str(item).strip() for item in raw_tags if str(item).strip())
    return str(raw_tags).strip()


def _normalize_metadata_json(raw_meta: Any) -> str:
    if raw_meta is None:
        return "{}"
    if isinstance(raw_meta, str):
        stripped = raw_meta.strip()
        return stripped or "{}"
    if isinstance(raw_meta, Mapping):
        return json.dumps(dict(raw_meta), ensure_ascii=False, separators=(",", ":"))
    return json.dumps(raw_meta, ensure_ascii=False, separators=(",", ":"))


def coerce_document_row(
    item: DocumentInput,
) -> Tuple[str, str, str, str, int, str, str, str, str, str, str]:
    """Validate and convert an IncidentDocument or mapping into a SQL row tuple."""
    if isinstance(item, IncidentDocument):
        doc_uid = item.doc_uid
        doc_type = item.doc_type
        service_key = item.service_key
        severity = item.severity
        timestamp_ms = int(item.timestamp_ms)
        occurred_at = item.occurred_at
        author = item.author
        title = item.title
        body = item.body
        tags = item.tags
        raw_meta: Any = item.metadata
    elif isinstance(item, Mapping):
        doc_uid = str(item.get("doc_uid") or "").strip()
        doc_type = str(item.get("doc_type") or "incident").strip()
        service_key = str(item.get("service_key") or "").strip()
        severity = str(item.get("severity") or "INFO").strip()
        ts_raw = item.get("timestamp_ms")
        if ts_raw is None:
            raise ValueError(f"Document '{doc_uid or '<unknown>'}' is missing timestamp_ms")
        timestamp_ms = int(ts_raw)
        occurred_at = str(item.get("occurred_at") or "").strip()
        author = str(item.get("author") or "").strip()
        title = str(item.get("title") or "").strip()
        body = str(item.get("body") or "").strip()
        tags = _normalize_tags(item.get("tags"))
        raw_meta = item.get("metadata_json") if "metadata_json" in item else item.get("metadata")
    else:
        raise TypeError(f"Unsupported document record type: {type(item).__name__}")

    doc_uid = str(doc_uid).strip()
    doc_type = str(doc_type).strip().lower()
    service_key = str(service_key).strip()
    severity = str(severity).strip().upper()

    if not doc_uid:
        raise ValueError("Document doc_uid must be non-empty")
    if not doc_type:
        raise ValueError(f"Document '{doc_uid}' doc_type must be non-empty")
    if not service_key:
        raise ValueError(f"Document '{doc_uid}' service_key must be non-empty")
    if not severity:
        raise ValueError(f"Document '{doc_uid}' severity must be non-empty")

    occurred_at_clean = str(occurred_at).strip() or ts_ms_to_iso_utc(timestamp_ms)
    metadata_json = _normalize_metadata_json(raw_meta)

    return (
        doc_uid,
        doc_type,
        service_key,
        severity,
        timestamp_ms,
        occurred_at_clean,
        str(author).strip(),
        str(title).strip(),
        str(body).strip(),
        _normalize_tags(tags),
        metadata_json,
    )


def create_indexes(conn: sqlite3.Connection) -> None:
    """Create secondary composite B-tree indexes (deferred until after bulk shadow load)."""
    cur = conn.cursor()
    try:
        for index_sql in INDEXES_SQL:
            cur.execute(index_sql)
    finally:
        cur.close()


def create_schema(conn: sqlite3.Connection, include_indexes: bool = True) -> None:
    """Create core tables, FTS5 virtual table, and optionally secondary composite indexes."""
    cur = conn.cursor()
    try:
        cur.execute(ENGINE_META_TABLE_SQL)
        cur.execute(DOCUMENTS_TABLE_SQL)
        if include_indexes:
            for index_sql in INDEXES_SQL:
                cur.execute(index_sql)
        cur.execute(FTS5_TABLE_SQL)
    finally:
        cur.close()
