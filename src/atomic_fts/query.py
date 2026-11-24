"""Deterministic read-only FTS5 query engine with BM25, snippets, filters, and regex refinement."""

from __future__ import annotations

import json
import re
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Pattern, Sequence, Tuple, Union

DEFAULT_BM25_WEIGHTS: Tuple[float, ...] = (1.5, 2.5, 1.0, 1.5, 0.0, 0.0)
ALLOWED_SORT_MODES = frozenset({"bm25", "recent"})


class InvalidQueryError(ValueError):
    """Raised when a search query, sort option, or regex pattern is invalid."""


@dataclass(frozen=True)
class SearchHit:
    """Single incident, post-mortem, or runbook match returned by the query engine."""

    doc_id: int
    doc_uid: str
    doc_type: str
    service_key: str
    severity: str
    timestamp_ms: int
    occurred_at: str
    author: str
    title: str
    body: str
    tags: str
    snippet: str
    bm25_score: float
    metadata: Dict[str, Any]


@dataclass(frozen=True)
class SearchResponse:
    """Search result payload including deterministic hits and execution telemetry."""

    query: str
    fts_expression: str
    regex_pattern: Optional[str]
    hits: List[SearchHit]
    total_returned: int
    elapsed_ms: float
    strategy: str


@dataclass(frozen=True)
class ArchiveStats:
    """Read-only diagnostics and cardinality summary for an archive database."""

    db_path: Path
    db_size_bytes: int
    db_size_mb: float
    journal_mode: str
    query_only: bool
    document_count: int
    fts_count: int
    min_timestamp_ms: Optional[int]
    max_timestamp_ms: Optional[int]
    by_service: Dict[str, int]
    by_severity: Dict[str, int]
    by_doc_type: Dict[str, int]
    meta: Dict[str, str]


def build_safe_fts_expression(query: str, raw_fts: bool = False) -> str:
    """Convert a user search string into a safe FTS5 MATCH expression.

    When `raw_fts` is False (default), whitespace-separated terms are sanitized and
    individually double-quoted so punctuation (e.g., `ERR-504`, `v2.1`) or uppercase
    keywords (`AND`, `OR`, `NOT`) never trigger FTS5 syntax errors. A trailing `*`
    on a non-empty alphanumeric stem enables FTS5 prefix matching (`"auth"*`).
    """
    text = (query or "").strip()
    if not text:
        return ""
    if raw_fts:
        return text

    parts: List[str] = []
    for raw_tok in text.split():
        is_prefix = raw_tok.endswith("*") and len(raw_tok) > 1
        core = raw_tok[:-1] if is_prefix else raw_tok
        if not any(ch.isalnum() for ch in core):
            continue
        escaped = core.replace('"', '""')
        parts.append(f'"{escaped}"*' if is_prefix else f'"{escaped}"')

    return " ".join(parts)


def _extract_mandatory_fts_prefix_tokens(pattern: str) -> List[str]:
    """Extract sound FTS5 prefix pre-filter tokens from a regex pattern.

    Returns tokens ONLY when they are guaranteed to start on a word boundary in every
    possible regex match (no top-level alternation `|`, outside groups/char classes,
    not preceded by arbitrary prefix operators, and not followed by optional `?`/`*`/`{0`).
    This prevents false negatives when accelerating regex search via FTS5.
    """
    if not pattern:
        return []

    tokens: List[str] = []
    n = len(pattern)
    i = 0
    paren_depth = 0
    in_char_class = False

    # Abort pre-filter extraction if an unescaped top-level alternation exists.
    idx = 0
    depth = 0
    in_cc = False
    while idx < n:
        ch = pattern[idx]
        if ch == "\\":
            idx += 2
            continue
        if in_cc:
            if ch == "]":
                in_cc = False
            idx += 1
            continue
        if ch == "[":
            in_cc = True
        elif ch == "(":
            depth += 1
        elif ch == ")" and depth > 0:
            depth -= 1
        elif ch == "|" and depth == 0:
            return []
        idx += 1

    while i < n:
        ch = pattern[i]
        if ch == "\\":
            i += 2
            continue
        if in_char_class:
            if ch == "]":
                in_char_class = False
            i += 1
            continue
        if ch == "[":
            in_char_class = True
            i += 1
            continue
        if ch == "(":
            paren_depth += 1
            i += 1
            continue
        if ch == ")" and paren_depth > 0:
            paren_depth -= 1
            i += 1
            continue

        if paren_depth == 0 and ch.isalnum():
            # Verify token starts on a guaranteed boundary (start, delimiter, or \b).
            prev_ok = False
            if i == 0:
                prev_ok = True
            elif pattern[i - 1] in (" ", "\t", "-", "_", ":", "/", ".", "^"):
                prev_ok = True
            elif i >= 2 and pattern[i - 2 : i] in (r"\b", r"\A"):
                prev_ok = True

            j = i
            while j < n and pattern[j].isalnum():
                j += 1

            # Ensure the run did not stop right before a quantifier on its last char,
            # or inside an open group / optional suffix.
            next_ch = pattern[j] if j < n else ""
            is_optional = next_ch in ("?", "*") or (
                j + 1 < n and pattern[j : j + 2] == "{0"
            )
            token = pattern[i:j]
            if prev_ok and not is_optional and len(token) >= 3:
                tokens.append(token)
            i = j
            continue

        i += 1

    # Deduplicate while preserving order
    return list(dict.fromkeys(tokens))


def _compile_regex(pattern: str, flags: int) -> Pattern[str]:
    try:
        return re.compile(pattern, flags)
    except re.error as exc:
        raise InvalidQueryError(f"Invalid regular expression {pattern!r}: {exc}") from exc


def _build_regex_snippet(text: str, rx: Pattern[str], radius: int = 80) -> str:
    clean = re.sub(r"\s+", " ", (text or "").strip())
    if not clean:
        return ""
    match = rx.search(clean)
    if not match:
        return clean[: radius * 2] + ("..." if len(clean) > radius * 2 else "")
    start = max(0, match.start() - radius)
    end = min(len(clean), match.end() + radius)
    prefix = "..." if start > 0 else ""
    suffix = "..." if end < len(clean) else ""
    return f"{prefix}{clean[start:match.start()]}[{clean[match.start():match.end()]}]{clean[match.end():end]}{suffix}"


def _build_preview_snippet(text: str, max_len: int = 180) -> str:
    clean = re.sub(r"\s+", " ", (text or "").strip())
    if len(clean) <= max_len:
        return clean
    return clean[: max_len - 3] + "..."


def _decode_metadata(raw_json: str) -> Dict[str, Any]:
    if not raw_json:
        return {}
    try:
        parsed = json.loads(raw_json)
        return parsed if isinstance(parsed, dict) else {"value": parsed}
    except json.JSONDecodeError:
        return {}


def _open_read_only_connection(db_path: Path) -> sqlite3.Connection:
    uri = f"{db_path.resolve().as_uri()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON;")
    return conn


class AtomicSearchEngine:
    """Read-only SQLite FTS5 query engine enforcing `PRAGMA query_only = ON`."""

    def __init__(
        self,
        db_path: Union[str, Path],
        bm25_weights: Sequence[float] = DEFAULT_BM25_WEIGHTS,
    ) -> None:
        self.db_path = Path(db_path)
        if not self.db_path.is_file():
            raise FileNotFoundError(f"Archive database not found: {self.db_path}")
        if len(bm25_weights) != 6:
            raise ValueError(f"bm25_weights must have 6 floats (one per FTS5 column), got {len(bm25_weights)}")
        self._bm25_weights = tuple(float(w) for w in bm25_weights)
        self._conn: Optional[sqlite3.Connection] = _open_read_only_connection(self.db_path)

    @property
    def connection(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError("AtomicSearchEngine connection is closed")
        return self._conn

    def close(self) -> None:
        """Explicitly close the underlying SQLite connection (required on Windows before swap/unlink)."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> AtomicSearchEngine:
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()

    def _build_metadata_filters(
        self,
        service_key: Optional[str],
        severity: Optional[str],
        doc_type: Optional[str],
        start_ts_ms: Optional[int],
        end_ts_ms: Optional[int],
        table_alias: str = "d",
    ) -> Tuple[List[str], List[Any]]:
        clauses: List[str] = []
        params: List[Any] = []
        if service_key:
            clauses.append(f"{table_alias}.service_key = ?")
            params.append(service_key.strip())
        if severity:
            clauses.append(f"{table_alias}.severity = ?")
            params.append(severity.strip().upper())
        if doc_type:
            clauses.append(f"{table_alias}.doc_type = ?")
            params.append(doc_type.strip().lower())
        if start_ts_ms is not None:
            clauses.append(f"{table_alias}.timestamp_ms >= ?")
            params.append(int(start_ts_ms))
        if end_ts_ms is not None:
            clauses.append(f"{table_alias}.timestamp_ms <= ?")
            params.append(int(end_ts_ms))
        return clauses, params

    @staticmethod
    def _fetch_snippets_for_rowids(
        cur: sqlite3.Cursor, fts_expr: str, row_ids: Sequence[int]
    ) -> Dict[int, str]:
        if not row_ids:
            return {}
        placeholders = ", ".join("?" for _ in row_ids)
        snip_rows = cur.execute(
            f"SELECT rowid, snippet(documents_fts, -1, '[', ']', '...', 24) AS snip "
            f"FROM documents_fts "
            f"WHERE documents_fts MATCH ? AND rowid IN ({placeholders})",
            [fts_expr] + list(row_ids),
        ).fetchall()
        return {int(sr["rowid"]): str(sr["snip"] or "") for sr in snip_rows}

    @staticmethod
    def _row_to_hit(row: sqlite3.Row, snippet_text: str, bm25_score: float) -> SearchHit:
        return SearchHit(
            doc_id=int(row["id"]),
            doc_uid=str(row["doc_uid"]),
            doc_type=str(row["doc_type"]),
            service_key=str(row["service_key"]),
            severity=str(row["severity"]),
            timestamp_ms=int(row["timestamp_ms"]),
            occurred_at=str(row["occurred_at"]),
            author=str(row["author"]),
            title=str(row["title"]),
            body=str(row["body"]),
            tags=str(row["tags"]),
            snippet=snippet_text,
            bm25_score=float(bm25_score),
            metadata=_decode_metadata(str(row["metadata_json"])),
        )

    def search(
        self,
        query: str = "",
        *,
        service_key: Optional[str] = None,
        severity: Optional[str] = None,
        doc_type: Optional[str] = None,
        start_ts_ms: Optional[int] = None,
        end_ts_ms: Optional[int] = None,
        regex_pattern: Optional[str] = None,
        regex_flags: int = re.IGNORECASE,
        raw_fts: bool = False,
        sort_by: str = "bm25",
        limit: int = 20,
        offset: int = 0,
    ) -> SearchResponse:
        """Execute a deterministic read-only search with BM25, snippets, filters, and optional regex."""
        started = time.perf_counter()
        if limit < 1:
            raise InvalidQueryError(f"limit must be >= 1, got {limit}")
        if offset < 0:
            raise InvalidQueryError(f"offset must be >= 0, got {offset}")
        sort_mode = sort_by.strip().lower()
        if sort_mode not in ALLOWED_SORT_MODES:
            raise InvalidQueryError(
                f"sort_by must be one of {sorted(ALLOWED_SORT_MODES)}, got {sort_by!r}"
            )
        if start_ts_ms is not None and end_ts_ms is not None and start_ts_ms > end_ts_ms:
            raise InvalidQueryError(
                f"start_ts_ms ({start_ts_ms}) must be <= end_ts_ms ({end_ts_ms})"
            )

        compiled_rx = _compile_regex(regex_pattern, regex_flags) if regex_pattern else None
        fts_expr = build_safe_fts_expression(query, raw_fts=raw_fts)

        if query.strip() and not fts_expr:
            # Query contained only punctuation/separators with no indexable tokens.
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            return SearchResponse(
                query=query,
                fts_expression="",
                regex_pattern=regex_pattern,
                hits=[],
                total_returned=0,
                elapsed_ms=elapsed_ms,
                strategy="empty_tokens",
            )

        if not fts_expr and compiled_rx is not None:
            prefilter_tokens = _extract_mandatory_fts_prefix_tokens(regex_pattern or "")
            if prefilter_tokens:
                fts_expr = " ".join(f'"{tok.replace(chr(34), chr(34) * 2)}"*' for tok in prefilter_tokens)

        where_clauses, filter_params = self._build_metadata_filters(
            service_key=service_key,
            severity=severity,
            doc_type=doc_type,
            start_ts_ms=start_ts_ms,
            end_ts_ms=end_ts_ms,
        )

        hits: List[SearchHit] = []
        conn = self.connection
        cur = conn.cursor()

        try:
            if fts_expr:
                weights_sql = ", ".join(f"{w:.6f}" for w in self._bm25_weights)
                use_fts_only_phase1 = (
                    compiled_rx is None
                    and sort_mode == "bm25"
                    and start_ts_ms is None
                    and end_ts_ms is None
                )

                if use_fts_only_phase1:
                    strategy = "fts5_bm25"
                    fts_where = ["documents_fts MATCH ?"]
                    fts_params: List[Any] = [fts_expr]
                    if service_key:
                        fts_where.append("documents_fts.service_key = ?")
                        fts_params.append(service_key.strip())
                    if severity:
                        fts_where.append("documents_fts.severity = ?")
                        fts_params.append(severity.strip().upper())
                    if doc_type:
                        fts_where.append("documents_fts.doc_type = ?")
                        fts_params.append(doc_type.strip().lower())

                    phase1_sql = (
                        f"SELECT rowid, bm25(documents_fts, {weights_sql}) AS bm25_score "
                        f"FROM documents_fts "
                        f"WHERE {' AND '.join(fts_where)} "
                        f"ORDER BY bm25_score ASC, rowid DESC "
                        f"LIMIT ? OFFSET ?"
                    )
                    try:
                        scored_ids = cur.execute(phase1_sql, fts_params + [limit, offset]).fetchall()
                    except sqlite3.OperationalError as exc:
                        raise InvalidQueryError(f"FTS5 query error for {fts_expr!r}: {exc}") from exc

                    if scored_ids:
                        row_ids = [int(r["rowid"]) for r in scored_ids]
                        placeholders = ", ".join("?" for _ in row_ids)
                        doc_rows = cur.execute(
                            f"SELECT id, doc_uid, doc_type, service_key, severity, "
                            f"timestamp_ms, occurred_at, author, title, body, tags, metadata_json "
                            f"FROM documents WHERE id IN ({placeholders})",
                            row_ids,
                        ).fetchall()
                        docs_by_id = {int(r["id"]): r for r in doc_rows}
                        snippets_by_id = self._fetch_snippets_for_rowids(cur, fts_expr, row_ids)

                        for sr in scored_ids:
                            doc_id = int(sr["rowid"])
                            doc_row = docs_by_id.get(doc_id)
                            if doc_row is not None:
                                hits.append(
                                    self._row_to_hit(
                                        doc_row,
                                        snippet_text=snippets_by_id.get(doc_id, ""),
                                        bm25_score=float(sr["bm25_score"]),
                                    )
                                )
                else:
                    where_sql = " AND ".join(["documents_fts MATCH ?"] + where_clauses)
                    order_sql = (
                        "ORDER BY bm25_score ASC, d.timestamp_ms DESC, d.id ASC"
                        if sort_mode == "bm25"
                        else "ORDER BY d.timestamp_ms DESC, d.id DESC"
                    )
                    base_select_sql = (
                        f"SELECT d.id, d.doc_uid, d.doc_type, d.service_key, d.severity, "
                        f"d.timestamp_ms, d.occurred_at, d.author, d.title, d.body, d.tags, d.metadata_json, "
                        f"bm25(documents_fts, {weights_sql}) AS bm25_score "
                        f"FROM documents_fts "
                        f"JOIN documents d ON d.id = documents_fts.rowid "
                        f"WHERE {where_sql} "
                        f"{order_sql}"
                    )
                    params: List[Any] = [fts_expr] + filter_params

                    if compiled_rx is None:
                        strategy = "fts5_bm25"
                        sql = f"{base_select_sql} LIMIT ? OFFSET ?"
                        try:
                            top_rows = cur.execute(sql, params + [limit, offset]).fetchall()
                        except sqlite3.OperationalError as exc:
                            raise InvalidQueryError(f"FTS5 query error for {fts_expr!r}: {exc}") from exc

                        row_ids = [int(r["id"]) for r in top_rows]
                        snippets_by_id = self._fetch_snippets_for_rowids(cur, fts_expr, row_ids)

                        for row in top_rows:
                            doc_id = int(row["id"])
                            hits.append(
                                self._row_to_hit(
                                    row,
                                    snippet_text=snippets_by_id.get(doc_id, ""),
                                    bm25_score=float(row["bm25_score"]),
                                )
                            )
                    else:
                        strategy = "fts5_prefilter_regex"
                        try:
                            cur.execute(base_select_sql, params)
                        except sqlite3.OperationalError as exc:
                            raise InvalidQueryError(f"FTS5 query error for {fts_expr!r}: {exc}") from exc

                        matched = 0
                        while len(hits) < limit:
                            batch = cur.fetchmany(256)
                            if not batch:
                                break
                            for row in batch:
                                haystack = f"{row['title']}\n{row['body']}\n{row['tags']}\n{row['service_key']}"
                                if compiled_rx.search(haystack):
                                    if matched >= offset:
                                        snip = _build_regex_snippet(haystack, compiled_rx)
                                        hits.append(
                                            self._row_to_hit(
                                                row,
                                                snippet_text=snip,
                                                bm25_score=float(row["bm25_score"]),
                                            )
                                        )
                                        if len(hits) >= limit:
                                            break
                                    matched += 1
            else:
                where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
                sql = (
                    f"SELECT d.id, d.doc_uid, d.doc_type, d.service_key, d.severity, "
                    f"d.timestamp_ms, d.occurred_at, d.author, d.title, d.body, d.tags, d.metadata_json "
                    f"FROM documents d "
                    f"{where_sql} "
                    f"ORDER BY d.timestamp_ms DESC, d.id DESC"
                )
                if compiled_rx is None:
                    strategy = "metadata_scan"
                    sql += " LIMIT ? OFFSET ?"
                    rows = cur.execute(sql, filter_params + [limit, offset]).fetchall()
                    for row in rows:
                        preview = _build_preview_snippet(str(row["body"] or row["title"]))
                        hits.append(self._row_to_hit(row, snippet_text=preview, bm25_score=0.0))
                else:
                    strategy = "metadata_regex"
                    cur.execute(sql, filter_params)
                    matched = 0
                    while len(hits) < limit:
                        batch = cur.fetchmany(256)
                        if not batch:
                            break
                        for row in batch:
                            haystack = f"{row['title']}\n{row['body']}\n{row['tags']}\n{row['service_key']}"
                            if compiled_rx.search(haystack):
                                if matched >= offset:
                                    snip = _build_regex_snippet(
                                        str(row["body"] or row["title"]), compiled_rx
                                    )
                                    hits.append(
                                        self._row_to_hit(row, snippet_text=snip, bm25_score=0.0)
                                    )
                                    if len(hits) >= limit:
                                        break
                                matched += 1
        finally:
            cur.close()

        elapsed_ms = (time.perf_counter() - started) * 1000.0
        return SearchResponse(
            query=query,
            fts_expression=fts_expr,
            regex_pattern=regex_pattern,
            hits=hits,
            total_returned=len(hits),
            elapsed_ms=elapsed_ms,
            strategy=strategy,
        )

    def get_document(self, doc_uid: str) -> Optional[SearchHit]:
        """Fetch a single document by its canonical `doc_uid`."""
        cur = self.connection.cursor()
        try:
            row = cur.execute(
                """
                SELECT id, doc_uid, doc_type, service_key, severity,
                       timestamp_ms, occurred_at, author, title, body, tags, metadata_json
                FROM documents
                WHERE doc_uid = ?
                """,
                (doc_uid.strip(),),
            ).fetchone()
        finally:
            cur.close()
        if row is None:
            return None
        return self._row_to_hit(
            row,
            snippet_text=_build_preview_snippet(str(row["body"] or row["title"])),
            bm25_score=0.0,
        )

    def get_stats(self) -> ArchiveStats:
        """Return archive counts, cardinality breakdowns, and SQLite PRAGMA telemetry."""
        conn = self.connection
        cur = conn.cursor()
        try:
            journal_mode = str(cur.execute("PRAGMA journal_mode;").fetchone()[0]).upper()
            query_only = bool(cur.execute("PRAGMA query_only;").fetchone()[0])
            doc_count = int(cur.execute("SELECT COUNT(*) FROM documents").fetchone()[0])
            fts_count = int(cur.execute("SELECT COUNT(*) FROM documents_fts").fetchone()[0])
            min_ts, max_ts = cur.execute(
                "SELECT MIN(timestamp_ms), MAX(timestamp_ms) FROM documents"
            ).fetchone()

            by_service = {
                str(r[0]): int(r[1])
                for r in cur.execute(
                    "SELECT service_key, COUNT(*) FROM documents GROUP BY service_key ORDER BY COUNT(*) DESC, service_key ASC"
                ).fetchall()
            }
            by_severity = {
                str(r[0]): int(r[1])
                for r in cur.execute(
                    "SELECT severity, COUNT(*) FROM documents GROUP BY severity ORDER BY severity ASC"
                ).fetchall()
            }
            by_doc_type = {
                str(r[0]): int(r[1])
                for r in cur.execute(
                    "SELECT doc_type, COUNT(*) FROM documents GROUP BY doc_type ORDER BY doc_type ASC"
                ).fetchall()
            }
            meta = {
                str(r[0]): str(r[1])
                for r in cur.execute("SELECT key, value FROM engine_meta").fetchall()
            }
        finally:
            cur.close()

        size_bytes = self.db_path.stat().st_size
        return ArchiveStats(
            db_path=self.db_path,
            db_size_bytes=size_bytes,
            db_size_mb=size_bytes / (1024.0 * 1024.0),
            journal_mode=journal_mode,
            query_only=query_only,
            document_count=doc_count,
            fts_count=fts_count,
            min_timestamp_ms=int(min_ts) if min_ts is not None else None,
            max_timestamp_ms=int(max_ts) if max_ts is not None else None,
            by_service=by_service,
            by_severity=by_severity,
            by_doc_type=by_doc_type,
            meta=meta,
        )
