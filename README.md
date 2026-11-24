# atomic-sqlite-fts5-engine

Zero-dependency Python 3.9+ stdlib (`sqlite3`) atomic shadow-build indexer and deterministic read-only search engine for **Production Incident Logs, Post-Mortems & Runbook Archives**.

---

## Why Atomic Shadow Builds?

Rebuilding a full-text search index directly inside a live SQLite database introduces three operational hazards:

1. **Write-Amplification & Lock Contention:** Maintaining a rollback journal or WAL across tens of thousands of `INSERT`s plus FTS5 inverted-index merges forces repeated disk flushes and causes `SQLITE_BUSY` contention for concurrent readers.
2. **Torn Rebuilds on Crash:** An unhandled exception, OOM kill, or disk error mid-rebuild leaves partial `documents` rows or out-of-sync `documents_fts` shadow tables in production.
3. **Sidecar Drift Across Swaps:** Replacing a database file while stale `-wal` or `-shm` sidecars remain next to the target path can corrupt page visibility or replay orphaned frames.

`atomic-sqlite-fts5-engine` eliminates all three failure modes by decoupling **build** from **serving**:

```text
  JSONL / Iterator
         │
         ▼  (1) Disposable Shadow Build on <target>.db.tmp
  PRAGMA journal_mode=OFF; synchronous=OFF; temp_store=MEMORY; cache_size=-128000;
         │
         ▼  (2) Chunked executemany() + keyset FTS5 sync (rowid == documents.id)
         │
         ▼  (3) ANALYZE + PRAGMA optimize;
         │
         ▼  (4) Durable mode transition (DELETE or WAL + wal_checkpoint(TRUNCATE))
         │
         ▼  (5) PRAGMA integrity_check; + FTS5 'integrity-check'
         │
         ▼  (6) Explicit cursor & Connection.close() + sidecar cleanup
         │
         ▼  (7) os.replace(<target>.db.tmp, <target>.db)
  Live Production Archive (atomic swap)
```

* **Crash Safety:** Any exception raised during steps `(1)–(7)` explicitly closes open handles, removes `<target>.db.tmp` (and any `-wal`/`-shm`/`-journal` sidecars), and leaves the existing live `<target>.db` byte-for-byte untouched.
* **Windows File-Lock Hygiene:** Unlike POSIX `rename(2)`, where unlinked files stay attached to open file descriptors, Windows enforces mandatory sharing locks on active SQLite handles. This engine explicitly closes every `sqlite3.Connection` and cursor before unlinking sidecars or invoking `os.replace()`. Note that Python's `with sqlite3.connect(...)` context manager only commits/rolls back transactions and does **not** close the connection; all connections here are closed explicitly via `close()` / `try...finally`.

---

## Production Reader Isolation

Production query processes use `AtomicSearchEngine`, which isolates readers from build workloads through:

1. **Zero Write Contention During Builds:** Rebuilds target `<target>.db.tmp` exclusively, so live readers never block on writer locks or observe intermediate schema/FTS5 states.
2. **Dual Read-Only Enforcement:** Connections open via URI (`file:///...?mode=ro`) **and** immediately apply `PRAGMA query_only = ON;`. Any accidental `INSERT`, `UPDATE`, `DELETE`, `CREATE`, or `DROP` raises `sqlite3.OperationalError` at both VFS and SQLite VM layers.
3. **Self-Contained Snapshots by Default (`DELETE` Mode):** Finalizing the shadow artifact in `PRAGMA journal_mode = DELETE` ensures zero `-wal` or `-shm` sidecars are required or generated when read-only workers open the archive—safe even on read-only mounted filesystems or immutable container layers. (`WAL` mode with `wal_checkpoint(TRUNCATE)` is also supported when post-swap concurrency requires it.)

---

## Trade-offs vs. Distributed Full-Text Engines

| Dimension | `atomic-sqlite-fts5-engine` | Distributed Engines (Elasticsearch / OpenSearch / Typesense) |
| :--- | :--- | :--- |
| **Operational Footprint** | Zero external runtime deps, zero daemon/JVM, single `.db` artifact | Multi-node cluster, JVM/native memory tuning, shard/replica ops |
| **Cold / Local Read Latency** | Sub-millisecond to low-millisecond in-process C library reads | Network hop + coordinator scatter-gather overhead |
| **Hybrid Structured + Text** | Single query plan joins `documents_fts` BM25 with composite B-tree indexes `(service_key, timestamp_ms)` & `(severity, timestamp_ms)` | Requires denormalized document duplication or costly cross-index joins |
| **Artifact Distribution** | Immutable snapshot push (`S3` / `rsync` / `os.replace`) with deterministic results | Mutable cluster state requiring index aliases and snapshot/restore pipelines |
| **Write Pattern Fit** | **Batch / periodic shadow rebuilds** (archive logs, post-mortems, runbooks) | **High-QPS continuous real-time mutations** across multiple concurrent writers |
| **Scale Ceiling** | Single-node storage/RAM envelope (tens of thousands to millions of docs) | Multi-terabyte horizontal sharding across clusters |

**Rule of thumb:** Choose `atomic-sqlite-fts5-engine` when your incident/runbook corpus fits on a single node, updates via batch/CI rebuilds, and benefits from deterministic local speed and zero infrastructure overhead. Choose a distributed cluster when ingestion requires continuous multi-writer sub-second visibility or multi-terabyte horizontal sharding.

---

## Schema & Query Architecture

* **`documents`**: Canonical table storing `doc_uid` (`UNIQUE`), `doc_type` (`incident`, `postmortem`, `runbook`), `service_key`, `severity`, `timestamp_ms`, `occurred_at`, `author`, `title`, `body`, `tags`, and `metadata_json`.
* **Composite Indexes**:
  * `idx_documents_service_ts` on `(service_key, timestamp_ms)`
  * `idx_documents_severity_ts` on `(severity, timestamp_ms)`
  * `idx_documents_ts` on `(timestamp_ms)`
  * `idx_documents_type_ts` on `(doc_type, timestamp_ms)`
* **`documents_fts`**: FTS5 virtual table (`tokenize='unicode61 remove_diacritics 2'`) synchronized 1:1 via `rowid == documents.id`, enabling case- and accent-insensitive matching across Unicode text (e.g., `"exaustao critica"` matches `"Exaustão crítica"`).
* **Two-Phase BM25 & Deferred `snippet()`**: Top-K scoring executes without invoking `snippet()` across every matching row; `snippet()` is evaluated only for the returned `LIMIT` rowids (`<= 20` by default).
* **FTS5 Pre-Filter + Optional Regex Refinement**:
  * Explicit FTS queries paired with `--regex` narrow candidates via FTS5 first, then stream candidate batches through compiled Python `re.Pattern` filters.
  * Regex-only queries automatically extract sound mandatory word-boundary prefix tokens (avoiding false negatives on alternations `|` or optional quantifiers `?`/`*`/`{0}`) to accelerate regex scans via FTS5 when mathematically safe.

---

## Installation

Requires Python 3.9+ (pure stdlib runtime; `pytest` only for running tests).

```powershell
python -m pip install -e ".[dev]"
```

---

## CLI Usage

```powershell
# 1. Atomically build (or rebuild) an archive from JSONL / JSONL.GZ files
atomic-fts build --input incidents.jsonl runbooks.jsonl.gz --db ops_archive.db

# 2. BM25 search with service, severity, and UTC date filters
atomic-fts search "HikariPool timeout" --db ops_archive.db --service payments-gateway --severity SEV1 --start 2026-01-01 --end 2026-03-31

# 3. FTS5 pre-filter + regex refinement (JSON output with elapsed_ms telemetry)
atomic-fts search "HikariPool" --regex "HikariPool-\d+ exhausted" --db ops_archive.db --json

# 4. Inspect a single record by doc_uid or check archive cardinality
atomic-fts show INC-2026-000142 --db ops_archive.db
atomic-fts stats --db ops_archive.db
```

---

## Benchmark

Run the synthetic incident/post-mortem/runbook benchmark (defaults to 25,000 documents and 50 mixed BM25, filtered, diacritic-folded, and regex-refined queries):

```powershell
python scripts/benchmark_build.py --docs 25000 --queries 50
```

Observed run on Python 3.9.13 (Windows, SQLite 3.37.2, `chunk_size=20,000`, `DELETE` mode):

```text
Documents indexed : 25,000 (FTS5 rows: 25,000)
Journal / Check   : DELETE / integrity=ok
Build time        : 2.5493 s
Build throughput  : 9,806.44 docs/sec
Database size     : 31.297 MB (32,817,152 bytes)
Queries executed  : 50 (total hits: 1,000)
Query latency (ms): min=0.935 | P50=35.785 | P95=68.399 | max=105.926 | mean=35.403
```

---

## Running Tests

```powershell
pytest -v
```

Test coverage includes:
* `tests/test_atomic_builder.py`: chunk boundary ingestion, `rowid == documents.id` parity, simulated crashes mid-ingestion and post-FTS preserving the live `.db` SHA-256 unchanged, `WAL` & `DELETE` swaps, sidecar cleanup, and Unicode diacritic-insensitive search.
* `tests/test_query_engine.py`: `PRAGMA query_only = ON` mutation rejection (`INSERT`/`UPDATE`/`DELETE`/`CREATE`/`DROP`), BM25 ranking & `snippet()`, `EXPLAIN QUERY PLAN` verification on composite indexes, and FTS5 pre-filter + regex refinement (including alternation soundness).
* `tests/test_cli_and_benchmark.py`: end-to-end CLI (`build`, `search`, `show`, `stats`), Python 3.9 ISO-8601 `"Z"` timestamp parsing, and synthetic benchmark execution.

---

## License

[MIT](LICENSE)
