"""Synthetic benchmark for atomic shadow-build throughput and read-only FTS5 latency."""

from __future__ import annotations

import argparse
import json
import math
import random
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Sequence

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from atomic_fts.builder import BuildConfig, build_atomic_index  # noqa: E402
from atomic_fts.query import AtomicSearchEngine  # noqa: E402
from atomic_fts.schema import IncidentDocument  # noqa: E402

SERVICES: Sequence[str] = (
    "payments-gateway",
    "auth-edge",
    "checkout-api",
    "ledger-worker",
    "kafka-ingest",
    "search-proxy",
    "billing-scheduler",
    "cdn-router",
)

SEVERITIES: Sequence[str] = ("SEV1", "SEV2", "SEV3", "SEV4", "INFO")
DOC_TYPES: Sequence[str] = ("incident", "postmortem", "runbook")

SCENARIOS: Sequence[Dict[str, str]] = (
    {
        "title": "HikariPool connection exhaustion during traffic spike",
        "body": (
            "HikariPool-{pool_id} exhausted all 120 active PostgreSQL connections after canary rollout. "
            "Upstream callers observed ERR-504-{code} upstream timeout and elevated p99 latency. "
            "Mitigation rolled back deployment and drained stuck idle-in-transaction sessions."
        ),
        "tags": "postgresql hikari connection-pool timeout",
    },
    {
        "title": "PostgreSQL primary-replica WAL lag and deadlock contention",
        "body": (
            "Replica read lag exceeded 18s on shard-{shard} while batch reconciliation held row locks. "
            "Deadlock detector aborted 43 concurrent transactions across settlement workers. "
            "Runbook step: pause backfill consumer and promote read-only circuit breaker."
        ),
        "tags": "postgresql deadlock replication-lag wal",
    },
    {
        "title": "Kubernetes pod OOMKilled under burst payload deserialization",
        "body": (
            "Container exited with OOMKilled (exit code 137) after memory limit 2Gi was breached by "
            "unbounded protobuf payload buffering. HPA scaled replicas from 6 to 18 within 90s."
        ),
        "tags": "kubernetes oomkilled memory grpc",
    },
    {
        "title": "Redis cluster eviction storm and cache stampede",
        "body": (
            "Volatile-lru evictions spiked to 45k ops/sec causing origin database CPU saturation. "
            "Applied jittered TTL and request coalescing singleflight wrapper to stabilize hit ratio."
        ),
        "tags": "redis cache-stampede eviction cpu",
    },
    {
        "title": "TLS certificate rotation failure at ingress proxy",
        "body": (
            "Automated ACME renewal failed DNS-01 propagation check, triggering ERR-502-{code} handshake "
            "errors on edge POP gru-01. Emergency runbook restored backup wildcard secret."
        ),
        "tags": "tls ingress certificate outage",
    },
    {
        "title": "Exaustão crítica no pool de autenticação e latência elevada",
        "body": (
            "Detectada exaustão crítica na validação de tokens OAuth2 durante pico de reconciliação bancária. "
            "Latência de autenticação atingiu 4200ms com falhas intermitentes de conexão no cluster secundário."
        ),
        "tags": "autenticacao exaustao latencia reconciliacao",
    },
    {
        "title": "Kafka consumer group rebalance storm and offset lag",
        "body": (
            "Long GC pause triggered max.poll.interval.ms expiration, cascading into repeated partition "
            "rebalances across 32 partitions. Increased session timeout and capped batch poll records."
        ),
        "tags": "kafka rebalance consumer-lag gc",
    },
    {
        "title": "BGP route flap on regional transit provider",
        "body": (
            "Packet loss rose to 14% between us-east and sa-east links due to upstream BGP route oscillation. "
            "Traffic engineering shifted egress weights to secondary transit peer."
        ),
        "tags": "network bgp packet-loss latency",
    },
)


def generate_synthetic_documents(count: int, seed: int = 42) -> Iterator[IncidentDocument]:
    """Stream `count` deterministic synthetic incident, post-mortem, and runbook records."""
    rng = random.Random(seed)
    base_ts_ms = 1_767_225_600_000  # 2026-01-01T00:00:00Z

    for idx in range(count):
        scenario = SCENARIOS[idx % len(SCENARIOS)]
        service = SERVICES[(idx + rng.randrange(len(SERVICES))) % len(SERVICES)]
        severity = SEVERITIES[rng.randrange(len(SEVERITIES))]
        doc_type = DOC_TYPES[idx % len(DOC_TYPES)]
        ts_ms = base_ts_ms + idx * 60_000

        pool_id = 10 + (idx % 90)
        shard = 1 + (idx % 16)
        code = 100 + (idx % 900)
        body = scenario["body"].format(pool_id=pool_id, shard=shard, code=code)
        prefix = {"incident": "INC", "postmortem": "PM", "runbook": "RB"}[doc_type]

        yield IncidentDocument(
            doc_uid=f"{prefix}-2026-{idx + 1:06d}",
            doc_type=doc_type,
            service_key=service,
            severity=severity,
            timestamp_ms=ts_ms,
            author=f"sre-oncall-{(idx % 12) + 1:02d}",
            title=f"[{service}] {scenario['title']}",
            body=body,
            tags=f"{scenario['tags']} {service}",
            metadata={
                "region": ("us-east-1", "sa-east-1", "eu-west-1")[idx % 3],
                "mttr_minutes": 5 + (idx % 95),
                "shard": shard,
            },
        )


def percentile(sorted_values: Sequence[float], pct: float) -> float:
    """Compute percentile (0..100) using linear interpolation on a non-empty sorted list."""
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return float(sorted_values[0])
    rank = (pct / 100.0) * (len(sorted_values) - 1)
    lo = math.floor(rank)
    hi = math.ceil(rank)
    if lo == hi:
        return float(sorted_values[lo])
    frac = rank - lo
    return float(sorted_values[lo] * (1.0 - frac) + sorted_values[hi] * frac)


def _summarize_latencies(values: Sequence[float]) -> Dict[str, Any]:
    if not values:
        return {
            "count": 0,
            "min_ms": 0.0,
            "p50_ms": 0.0,
            "p95_ms": 0.0,
            "max_ms": 0.0,
            "mean_ms": 0.0,
        }
    s = sorted(values)
    return {
        "count": len(s),
        "min_ms": round(s[0], 3),
        "p50_ms": round(percentile(s, 50.0), 3),
        "p95_ms": round(percentile(s, 95.0), 3),
        "max_ms": round(s[-1], 3),
        "mean_ms": round(sum(s) / len(s), 3),
    }


def build_query_workload(base_ts_ms: int, total_docs: int) -> List[Dict[str, Any]]:
    mid_ts = base_ts_ms + (total_docs // 2) * 60_000
    end_ts = base_ts_ms + max(total_docs - 1, 0) * 60_000
    return [
        {"query": "HikariPool timeout", "limit": 20},
        {"query": "deadlock replica lag", "service_key": "payments-gateway", "limit": 20},
        {"query": "OOMKilled", "severity": "SEV1", "limit": 20},
        {"query": "exaustao critica autenticacao", "limit": 20},
        {"query": "reconciliacao bancaria", "doc_type": "incident", "limit": 20},
        {"query": "redis eviction", "start_ts_ms": mid_ts, "end_ts_ms": end_ts, "limit": 20},
        {"query": "kafka rebalance", "severity": "SEV2", "limit": 20},
        {"query": "HikariPool", "regex_pattern": r"HikariPool-\d{2} exhausted", "limit": 20},
        {"query": "", "regex_pattern": r"ERR-(502|504)-\d{3}", "limit": 20},
        {"query": "", "service_key": "auth-edge", "severity": "SEV1", "start_ts_ms": mid_ts, "limit": 20},
    ]


def run_benchmark(
    *,
    docs: int = 25_000,
    queries: int = 50,
    chunk_size: int = 20_000,
    journal_mode: str = "DELETE",
    seed: int = 42,
    db_path: Optional[Path] = None,
) -> Dict[str, Any]:
    """Run build and query latency benchmark and return structured telemetry."""
    if docs < 1:
        raise ValueError(f"docs must be >= 1, got {docs}")
    if queries < 1:
        raise ValueError(f"queries must be >= 1, got {queries}")

    tmp_ctx: Optional[tempfile.TemporaryDirectory] = None
    if db_path is None:
        tmp_ctx = tempfile.TemporaryDirectory(prefix="atomic_fts_bench_")
        target_db = Path(tmp_ctx.name) / "incidents_benchmark.db"
    else:
        target_db = Path(db_path)

    try:
        config = BuildConfig(chunk_size=chunk_size, final_journal_mode=journal_mode)
        build_stats = build_atomic_index(
            generate_synthetic_documents(docs, seed=seed),
            target_db,
            config=config,
        )

        workload = build_query_workload(1_767_225_600_000, docs)
        latencies_ms: List[float] = []
        fts5_bm25_latencies_ms: List[float] = []
        regex_latencies_ms: List[float] = []
        total_hits = 0

        engine = AtomicSearchEngine(target_db)
        try:
            # Include true cold-to-warm mix across all N queries directly.
            for idx in range(queries):
                spec = workload[idx % len(workload)]
                resp = engine.search(**spec)
                latencies_ms.append(resp.elapsed_ms)
                if bool(spec.get("regex_pattern")):
                    regex_latencies_ms.append(resp.elapsed_ms)
                elif bool(spec.get("query")):
                    fts5_bm25_latencies_ms.append(resp.elapsed_ms)
                total_hits += resp.total_returned
        finally:
            # Explicitly close SQLite connection before TemporaryDirectory cleanup on Windows.
            engine.close()

        sorted_lat = sorted(latencies_ms)
        fts5_summary = _summarize_latencies(fts5_bm25_latencies_ms)
        regex_summary = _summarize_latencies(regex_latencies_ms)
        return {
            "docs": build_stats.documents_indexed,
            "fts_rows": build_stats.fts_rows_indexed,
            "chunk_size": chunk_size,
            "journal_mode": build_stats.journal_mode,
            "integrity_status": build_stats.integrity_status,
            "build_seconds": round(build_stats.elapsed_seconds, 4),
            "throughput_docs_per_sec": round(build_stats.docs_per_second, 2),
            "db_size_bytes": build_stats.db_size_bytes,
            "db_size_mb": round(build_stats.db_size_mb, 3),
            "queries_executed": queries,
            "total_hits_returned": total_hits,
            "latency_min_ms": round(sorted_lat[0], 3),
            "latency_p50_ms": round(percentile(sorted_lat, 50.0), 3),
            "latency_p95_ms": round(percentile(sorted_lat, 95.0), 3),
            "latency_max_ms": round(sorted_lat[-1], 3),
            "latency_mean_ms": round(sum(sorted_lat) / len(sorted_lat), 3),
            "fts5_bm25_breakdown": fts5_summary,
            "regex_breakdown": regex_summary,
        }
    finally:
        if tmp_ctx is not None:
            tmp_ctx.cleanup()


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Benchmark atomic SQLite FTS5 shadow build and query latency.")
    parser.add_argument("--docs", type=int, default=25_000, help="Number of synthetic incident/post-mortem records")
    parser.add_argument("--queries", type=int, default=50, help="Number of read-only queries to benchmark")
    parser.add_argument("--chunk-size", type=int, default=20_000, help="Chunk size for executemany batches")
    parser.add_argument("--journal-mode", choices=("DELETE", "WAL"), default="DELETE", help="Final safe journal mode")
    parser.add_argument("--seed", type=int, default=42, help="PRNG seed for deterministic synthetic generation")
    parser.add_argument("--db", type=Path, default=None, help="Optional output DB path (defaults to temp dir)")
    parser.add_argument("--json", action="store_true", help="Emit JSON telemetry")

    args = parser.parse_args(list(argv) if argv is not None else None)
    report = run_benchmark(
        docs=args.docs,
        queries=args.queries,
        chunk_size=args.chunk_size,
        journal_mode=args.journal_mode,
        seed=args.seed,
        db_path=args.db,
    )

    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print("============================================================")
        print("ATOMIC SQLITE FTS5 ENGINE - BUILD & QUERY BENCHMARK")
        print("============================================================")
        print(f"Documents indexed : {report['docs']:,} (FTS5 rows: {report['fts_rows']:,})")
        print(f"Journal / Check   : {report['journal_mode']} / integrity={report['integrity_status']}")
        print(f"Build time        : {report['build_seconds']:.4f} s")
        print(f"Build throughput  : {report['throughput_docs_per_sec']:,.2f} docs/sec")
        print(f"Database size     : {report['db_size_mb']:.3f} MB ({report['db_size_bytes']:,} bytes)")
        print(f"Queries executed  : {report['queries_executed']} (total hits: {report['total_hits_returned']:,})")
        print(
            f"Query latency (ms): min={report['latency_min_ms']:.3f} | "
            f"P50={report['latency_p50_ms']:.3f} | "
            f"P95={report['latency_p95_ms']:.3f} | "
            f"max={report['latency_max_ms']:.3f} | "
            f"mean={report['latency_mean_ms']:.3f}"
        )
        fts = report["fts5_bm25_breakdown"]
        rx = report["regex_breakdown"]
        print(
            f"Pure FTS5 BM25    : count={fts['count']} | "
            f"P50={fts['p50_ms']:.3f} ms | P95={fts['p95_ms']:.3f} ms | mean={fts['mean_ms']:.3f} ms"
        )
        print(
            f"Regex / Fallback  : count={rx['count']} | "
            f"P50={rx['p50_ms']:.3f} ms | P95={rx['p95_ms']:.3f} ms | mean={rx['mean_ms']:.3f} ms"
        )
        print("============================================================")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
