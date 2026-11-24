# atomic-sqlite-fts5-engine

A zero-dependency Python 3.11+ stdlib (`sqlite3`) shadow-build indexer (CI-verified on Ubuntu 3.11 and 3.12) and deterministic read-only search engine for operational incident logs, post-mortems, and runbook archives.

---

## Why This Exists

Rebuilding an FTS5 search index directly inside a live SQLite database introduces three operational hazards:

1. **Write-Amplification & Lock Contention:** Maintaining a rollback journal or WAL across tens of thousands of inserts plus FTS5 inverted-index merges triggers repeated fsync flushes and `SQLITE_BUSY` contention for active readers.
2. **Torn Rebuilds on Crash:** An unhandled exception, OOM kill, or disk full mid-rebuild leaves partial `documents` rows or corrupted FTS5 shadow tables in production.
3. **Sidecar Drift & Windows File Locks:** Swapping a `.db` file next to stale `-wal`/`-shm` sidecars corrupts visibility, and on Windows any unclosed SQLite handle breaks atomic file replacement (`os.replace`).

`atomic-sqlite-fts5-engine` separates **offline shadow building** (`<target>.<pid>.<uuid>.tmp` with `PRAGMA journal_mode=OFF` and deferred secondary indexes) from **strict read-only serving** (`?mode=ro` + `PRAGMA query_only=ON`).

## Origin & Sanitization

The atomic shadow-build lifecycle, explicit connection cleanup for Windows, and deterministic two-phase FTS5 + regex query engine were extracted and rewritten from a private high-volume local archive indexing pipeline. All private message tables, personal lifelog schemas, and user data were removed and replaced with a synthetic incident, post-mortem, and runbook schema.

## What It Intentionally Does Not Do

This engine is intentionally optimized for immutable or periodic batch snapshots (CI builds, daily log archives, local CLI runbook indexes).

It does **not**:

- handle continuous multi-writer single-row streaming mutations in place
- shard indexes across multiple hosts or replace Elasticsearch/OpenSearch for multi-terabyte telemetry lakes

---

## Shadow Build & Swap Lifecycle

```text
  JSONL / Iterator
         |
         v  (1) Disposable Shadow Build on <target>.<pid>.<uuid>.tmp
  PRAGMA journal_mode=OFF; synchronous=OFF; temp_store=MEMORY; cache_size=-128000;
         |
         v  (2) Chunked executemany() + keyset FTS5 sync + deferred B-tree indexes
         |
         v  (3) ANALYZE + PRAGMA optimize;
         |
         v  (4) Durable mode transition (DELETE or WAL + wal_checkpoint(TRUNCATE))
         |
         v  (5) PRAGMA integrity_check; + FTS5 'integrity-check'
         |
         v  (6) Explicit cursor & Connection.close() + sidecar cleanup + _fsync_file(.tmp)
         |
         v  (7) os.replace(<target>.<pid>.<uuid>.tmp, <target>.db) + _fsync_parent_dir()
  Live Archive (atomic swap)
```

- **Process-Crash Isolation vs. Power-Loss Durability:** Each build writes to a unique PID + UUID shadow path (`<target>.<pid>.<uuid>.tmp`) with secondary B-tree index creation deferred until after bulk ingestion and FTS5 sync (avoiding B-tree page splits during bulk load). Any Python exception or process kill during steps `(1)-(6)` cleans up shadow artifacts and leaves any existing `<target>.db` byte-for-byte untouched (process-crash isolation). For power-loss durability on the final commit boundary, `_fsync_file()` opens the closed `.tmp` database in `"rb+"` mode and calls `os.fsync()` before `os.replace()`, followed by `_fsync_parent_dir()` on POSIX to persist the updated directory entry.
- **Windows File-Lock Hygiene:** Python's `with sqlite3.connect(...)` context manager only commits/rolls back transactions and does not close the connection. Every build path explicitly calls `close()` in `try...finally` before unlinking sidecars or invoking `os.replace()`.

---

## Reader Isolation & Self-Contained Snapshots

`AtomicSearchEngine` opens databases via URI (`file:///...?mode=ro`) and immediately applies `PRAGMA query_only = ON;`. Any accidental `INSERT`, `UPDATE`, `DELETE`, `CREATE`, or `DROP` raises `sqlite3.OperationalError` at both VFS and VM layers.

`BuildConfig.final_journal_mode` defaults to `"DELETE"` so the swapped artifact needs zero `-wal` or `-shm` sidecars and opens cleanly via `?mode=ro` even on read-only mounted filesystems or immutable container layers (`"WAL"` with `wal_checkpoint(TRUNCATE)` is also supported when post-swap concurrency requires it).

---

## Trade-offs vs. Distributed Full-Text Engines

| Dimension | `atomic-sqlite-fts5-engine` | Distributed Engines (Elasticsearch / OpenSearch) |
| :--- | :--- | :--- |
| **Operational Footprint** | Zero external deps, zero daemon/JVM, single `.db` artifact | Multi-node cluster, JVM memory tuning, shard/replica ops |
| **Local Read Latency** | Low-tens-of-milliseconds in-process C library reads (measured 1.7 ms min / 17.3 ms P50 on 5k docs with BM25 + snippet extraction; zero network hop) | Network hop + coordinator scatter-gather overhead |
| **Hybrid Structured + Text** | Single SQLite query joining `documents_fts` BM25 with composite B-tree indexes | Requires denormalized duplication or costly cross-index joins |
| **Write Pattern Fit** | **Batch / periodic shadow rebuilds** (archives, post-mortems, runbooks) | **Continuous high-QPS real-time writes** across concurrent writers |
| **Scale Ceiling** | Single-node storage/RAM envelope | Multi-terabyte horizontal sharding across clusters |

---

## Schema & Query Architecture

- **`documents`**: Canonical table storing `doc_uid` (`UNIQUE`), `doc_type` (`incident`, `postmortem`, `runbook`), `service_key`, `severity`, `timestamp_ms`, `occurred_at`, `author`, `title`, `body`, `tags`, and `metadata_json`.
- **Composite Indexes**: `idx_documents_service_ts (service_key, timestamp_ms)`, `idx_documents_severity_ts (severity, timestamp_ms)`, `idx_documents_ts (timestamp_ms)`, `idx_documents_type_ts (doc_type, timestamp_ms)`.
- **`documents_fts`**: FTS5 virtual table (`tokenize='unicode61 remove_diacritics 2'`) synced 1:1 via `rowid == documents.id`, supporting accent-insensitive Unicode lookup (`"exaustao critica"` matches `"Exaustão crítica"`).
- **Two-Phase BM25 & Deferred `snippet()`**: Top-K ranking executes first; `snippet()` evaluates only on the returned `LIMIT` rowids.
- **FTS5 Pre-Filter + Regex Refinement**: Combines FTS5 candidate narrowing with compiled `re.Pattern` filtering and safely falls back to metadata scan when alternations `|` or optional quantifiers make token extraction unsound.

---

## Installation & CLI Usage

Requires **Python 3.11+** (CI-verified on Ubuntu 3.11 and 3.12, `pytest` for tests).

```bash
python -m pip install -e ".[dev]"

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

## Benchmark & Tests

```bash
python scripts/benchmark_build.py --docs 5000 --queries 50
pytest -v
```

## Measured Benchmark Results

Output from `python scripts/benchmark_build.py --docs 5000 --queries 50` (with pre-swap `_fsync_file` enabled):

| Metric | Measured Value |
| :--- | :--- |
| Documents & FTS5 Rows Indexed | `5,000` (`5,000` FTS5 rows) |
| Final Journal / Integrity Check | `DELETE` / `integrity=ok` |
| Shadow Build + Fsync + Swap Time | `0.7049 s` (`7,092.72 docs/sec`) |
| Final Archive Size | `5.863 MB` (`6,148,096 bytes`) |
| Queries Executed | `50` (`1,000` total hits) |
| Query Latency (`min` / `P50` / `P95` / `max` / `mean`) | `1.726 ms` / `17.337 ms` / `27.365 ms` / `31.072 ms` / `16.690 ms` |

## License

MIT - see [LICENSE](LICENSE).
