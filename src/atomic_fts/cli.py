"""Command-line interface for atomic shadow index builds and read-only incident search."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Sequence

from atomic_fts.builder import BuildConfig, build_atomic_index, iter_jsonl_documents
from atomic_fts.query import AtomicSearchEngine, InvalidQueryError


def _ensure_utf8_stdio() -> None:
    for stream in (sys.stdout, sys.stderr):
        reconf = getattr(stream, "reconfigure", None)
        if callable(reconf):
            try:
                reconf(encoding="utf-8", errors="replace")
            except Exception:
                pass


def parse_timestamp_arg(raw: Optional[str], *, end_of_day_if_date_only: bool = False) -> Optional[int]:
    """Parse an ISO-8601 UTC date/datetime string (Python 3.9+ safe) into epoch milliseconds."""
    if raw is None:
        return None
    text = raw.strip()
    if not text:
        return None

    if len(text) == 10 and text[4] == "-" and text[7] == "-":
        dt = datetime.fromisoformat(text).replace(tzinfo=timezone.utc)
        ms = int(dt.timestamp() * 1000)
        if end_of_day_if_date_only:
            ms += 86_400_000 - 1
        return ms

    normalized = f"{text[:-1]}+00:00" if text.endswith(("Z", "z")) else text
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return int(dt.timestamp() * 1000)


def _cmd_build(args: argparse.Namespace) -> int:
    config = BuildConfig(
        chunk_size=args.chunk_size,
        cache_size_kib=args.cache_size_kib,
        final_journal_mode=args.journal_mode,
    )
    docs = iter_jsonl_documents(args.input)
    stats = build_atomic_index(docs, args.db, config=config)

    if args.json:
        payload = {
            "target_path": str(stats.target_path),
            "documents_indexed": stats.documents_indexed,
            "fts_rows_indexed": stats.fts_rows_indexed,
            "elapsed_seconds": round(stats.elapsed_seconds, 6),
            "docs_per_second": round(stats.docs_per_second, 2),
            "db_size_bytes": stats.db_size_bytes,
            "db_size_mb": round(stats.db_size_mb, 4),
            "journal_mode": stats.journal_mode,
            "integrity_status": stats.integrity_status,
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(
            f"[OK] Built {stats.target_path} | docs={stats.documents_indexed:,} "
            f"| fts={stats.fts_rows_indexed:,} | size={stats.db_size_mb:.2f} MB "
            f"| elapsed={stats.elapsed_seconds:.3f}s ({stats.docs_per_second:,.0f} docs/s) "
            f"| mode={stats.journal_mode} | integrity={stats.integrity_status}"
        )
    return 0


def _cmd_search(args: argparse.Namespace) -> int:
    start_ms = args.start_ms if args.start_ms is not None else parse_timestamp_arg(args.start, end_of_day_if_date_only=False)
    end_ms = args.end_ms if args.end_ms is not None else parse_timestamp_arg(args.end, end_of_day_if_date_only=True)

    with AtomicSearchEngine(args.db) as engine:
        resp = engine.search(
            args.query,
            service_key=args.service,
            severity=args.severity,
            doc_type=args.doc_type,
            start_ts_ms=start_ms,
            end_ts_ms=end_ms,
            regex_pattern=args.regex,
            raw_fts=args.raw_fts,
            sort_by=args.sort,
            limit=args.limit,
            offset=args.offset,
        )

    if args.json:
        payload: Dict[str, Any] = {
            "query": resp.query,
            "fts_expression": resp.fts_expression,
            "regex_pattern": resp.regex_pattern,
            "strategy": resp.strategy,
            "total_returned": resp.total_returned,
            "elapsed_ms": round(resp.elapsed_ms, 4),
            "hits": [
                {
                    "doc_id": h.doc_id,
                    "doc_uid": h.doc_uid,
                    "doc_type": h.doc_type,
                    "service_key": h.service_key,
                    "severity": h.severity,
                    "timestamp_ms": h.timestamp_ms,
                    "occurred_at": h.occurred_at,
                    "author": h.author,
                    "title": h.title,
                    "snippet": h.snippet,
                    "tags": h.tags,
                    "bm25_score": round(h.bm25_score, 6),
                    "metadata": h.metadata,
                }
                for h in resp.hits
            ],
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0

    print(
        f"=== SEARCH ({resp.strategy}) | hits={resp.total_returned} "
        f"| elapsed_ms={resp.elapsed_ms:.3f} | fts={resp.fts_expression or '-'} ==="
    )
    if not resp.hits:
        print("No matching incident logs, post-mortems, or runbooks found.")
        return 0

    for idx, h in enumerate(resp.hits, start=1):
        print(
            f"[{idx}] {h.doc_uid} [{h.severity}] [{h.doc_type}] "
            f"service={h.service_key} at={h.occurred_at} bm25={h.bm25_score:.4f}"
        )
        print(f"    title: {h.title}")
        print(f"    match: {h.snippet}")
    return 0


def _cmd_show(args: argparse.Namespace) -> int:
    with AtomicSearchEngine(args.db) as engine:
        doc = engine.get_document(args.doc_uid)

    if doc is None:
        print(f"Document not found: {args.doc_uid}", file=sys.stderr)
        return 1

    if args.json:
        payload = {
            "doc_id": doc.doc_id,
            "doc_uid": doc.doc_uid,
            "doc_type": doc.doc_type,
            "service_key": doc.service_key,
            "severity": doc.severity,
            "timestamp_ms": doc.timestamp_ms,
            "occurred_at": doc.occurred_at,
            "author": doc.author,
            "title": doc.title,
            "body": doc.body,
            "tags": doc.tags,
            "metadata": doc.metadata,
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"[{doc.doc_uid}] [{doc.severity}] [{doc.doc_type}] {doc.service_key} ({doc.occurred_at})")
        print(f"Title: {doc.title}")
        if doc.author:
            print(f"Author: {doc.author}")
        if doc.tags:
            print(f"Tags: {doc.tags}")
        print("-" * 60)
        print(doc.body)
    return 0


def _cmd_stats(args: argparse.Namespace) -> int:
    with AtomicSearchEngine(args.db) as engine:
        st = engine.get_stats()

    if args.json:
        payload = {
            "db_path": str(st.db_path),
            "db_size_bytes": st.db_size_bytes,
            "db_size_mb": round(st.db_size_mb, 4),
            "journal_mode": st.journal_mode,
            "query_only": st.query_only,
            "document_count": st.document_count,
            "fts_count": st.fts_count,
            "min_timestamp_ms": st.min_timestamp_ms,
            "max_timestamp_ms": st.max_timestamp_ms,
            "by_service": st.by_service,
            "by_severity": st.by_severity,
            "by_doc_type": st.by_doc_type,
            "meta": st.meta,
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(
            f"Archive: {st.db_path} ({st.db_size_mb:.2f} MB) | "
            f"docs={st.document_count:,} | fts={st.fts_count:,} | "
            f"journal_mode={st.journal_mode} | query_only={st.query_only}"
        )
        print(f"By severity: {st.by_severity}")
        print(f"By type: {st.by_doc_type}")
        print(f"Services ({len(st.by_service)}): {st.by_service}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="atomic-fts",
        description="Atomic shadow-build SQLite FTS5 archive engine for production incidents, post-mortems, and runbooks.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_build = sub.add_parser("build", help="Atomically build or rebuild an archive .db from JSONL/JSONL.GZ inputs")
    p_build.add_argument("--input", nargs="+", required=True, type=Path, help="Input .jsonl or .jsonl.gz paths")
    p_build.add_argument("--db", required=True, type=Path, help="Target SQLite database path")
    p_build.add_argument("--chunk-size", type=int, default=20_000, help="Batch size for executemany chunks")
    p_build.add_argument("--cache-size-kib", type=int, default=128_000, help="SQLite page cache budget in KiB")
    p_build.add_argument(
        "--journal-mode",
        choices=("DELETE", "WAL"),
        default="DELETE",
        help="Final safe journal mode after bulk build",
    )
    p_build.add_argument("--json", action="store_true", help="Emit JSON output")

    p_search = sub.add_parser("search", help="Read-only BM25/FTS5 search with optional metadata filters and regex")
    p_search.add_argument("query", nargs="?", default="", help="FTS5 search terms (optional if filtering or using --regex)")
    p_search.add_argument("--db", required=True, type=Path, help="Target SQLite database path")
    p_search.add_argument("--service", default=None, help="Filter by service_key")
    p_search.add_argument("--severity", default=None, help="Filter by severity (e.g. SEV1, SEV2)")
    p_search.add_argument("--type", dest="doc_type", default=None, help="Filter by doc_type (incident, postmortem, runbook)")
    p_search.add_argument("--start-ms", type=int, default=None, help="Inclusive start timestamp in epoch ms")
    p_search.add_argument("--end-ms", type=int, default=None, help="Inclusive end timestamp in epoch ms")
    p_search.add_argument("--start", default=None, help="Inclusive start UTC date/ISO-8601 timestamp")
    p_search.add_argument("--end", default=None, help="Inclusive end UTC date/ISO-8601 timestamp")
    p_search.add_argument("--regex", default=None, help="Optional regex refinement pattern")
    p_search.add_argument("--raw-fts", action="store_true", help="Pass query directly as raw FTS5 syntax")
    p_search.add_argument("--sort", choices=("bm25", "recent"), default="bm25", help="Result ordering")
    p_search.add_argument("--limit", type=int, default=20, help="Maximum results to return")
    p_search.add_argument("--offset", type=int, default=0, help="Result pagination offset")
    p_search.add_argument("--json", action="store_true", help="Emit JSON output")

    p_show = sub.add_parser("show", help="Display a single document by doc_uid")
    p_show.add_argument("doc_uid", help="Unique document identifier (e.g. INC-2026-0001)")
    p_show.add_argument("--db", required=True, type=Path, help="Target SQLite database path")
    p_show.add_argument("--json", action="store_true", help="Emit JSON output")

    p_stats = sub.add_parser("stats", help="Display read-only archive cardinality and PRAGMA diagnostics")
    p_stats.add_argument("--db", required=True, type=Path, help="Target SQLite database path")
    p_stats.add_argument("--json", action="store_true", help="Emit JSON output")

    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    _ensure_utf8_stdio()
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)

    try:
        if args.command == "build":
            return _cmd_build(args)
        if args.command == "search":
            return _cmd_search(args)
        if args.command == "show":
            return _cmd_show(args)
        if args.command == "stats":
            return _cmd_stats(args)
        parser.print_help()
        return 2
    except (FileNotFoundError, InvalidQueryError, ValueError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
