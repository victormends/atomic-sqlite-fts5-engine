"""Atomic shadow-build SQLite FTS5 indexing and deterministic read-only search engine."""

from __future__ import annotations

from atomic_fts.builder import BuildConfig, BuildError, BuildStats, IntegrityCheckError, build_atomic_index, iter_jsonl_documents
from atomic_fts.query import ArchiveStats, AtomicSearchEngine, InvalidQueryError, SearchHit, SearchResponse, build_safe_fts_expression
from atomic_fts.schema import IncidentDocument, SCHEMA_VERSION, create_schema

__version__ = "0.1.0"

__all__ = [
    "ArchiveStats",
    "AtomicSearchEngine",
    "BuildConfig",
    "BuildError",
    "BuildStats",
    "IncidentDocument",
    "IntegrityCheckError",
    "InvalidQueryError",
    "SCHEMA_VERSION",
    "SearchHit",
    "SearchResponse",
    "__version__",
    "build_atomic_index",
    "build_safe_fts_expression",
    "create_schema",
    "iter_jsonl_documents",
]
