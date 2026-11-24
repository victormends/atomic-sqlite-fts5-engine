"""Tests for the CLI commands and synthetic benchmark runner."""

from __future__ import annotations

import gzip
import json
import subprocess
import sys
from pathlib import Path

from atomic_fts.cli import main as cli_main, parse_timestamp_arg

REPO_ROOT = Path(__file__).resolve().parent.parent
BENCHMARK_SCRIPT = REPO_ROOT / "scripts" / "benchmark_build.py"


def test_parse_timestamp_arg_python39_iso_utc() -> None:
    ms_z = parse_timestamp_arg("2026-03-14T12:00:00Z")
    ms_off = parse_timestamp_arg("2026-03-14T12:00:00+00:00")
    assert ms_z == ms_off

    day_start = parse_timestamp_arg("2026-03-14", end_of_day_if_date_only=False)
    day_end = parse_timestamp_arg("2026-03-14", end_of_day_if_date_only=True)
    assert day_start is not None and day_end is not None
    assert day_end - day_start == 86_400_000 - 1


def test_cli_build_search_show_and_stats(tmp_path: Path, capsys) -> None:
    jsonl_path = tmp_path / "incidents.jsonl"
    gz_path = tmp_path / "runbooks.jsonl.gz"
    db_path = tmp_path / "ops_archive.db"

    records_a = [
        {
            "doc_uid": "INC-2026-101",
            "doc_type": "incident",
            "service_key": "ledger-worker",
            "severity": "SEV1",
            "timestamp_ms": 1_773_489_600_000,  # 2026-03-14T12:00:00Z
            "title": "PostgreSQL replication lag spike",
            "body": "Replication lag hit 24s with ERR-504-777 on ledger reconciliation.",
            "tags": ["postgres", "lag"],
        }
    ]
    records_b = [
        {
            "doc_uid": "RB-2026-202",
            "doc_type": "runbook",
            "service_key": "ledger-worker",
            "severity": "INFO",
            "timestamp_ms": 1_773_576_000_000,  # 2026-03-15T12:00:00Z
            "title": "Runbook: PostgreSQL replica catch-up",
            "body": "Throttle batch backfill workers when lag exceeds 10s.",
            "tags": "postgres runbook",
        }
    ]

    jsonl_path.write_text("\n".join(json.dumps(r) for r in records_a) + "\n", encoding="utf-8")
    with gzip.open(gz_path, "wt", encoding="utf-8") as gz:
        gz.write("\n".join(json.dumps(r) for r in records_b) + "\n")

    assert (
        cli_main(
            [
                "build",
                "--input",
                str(jsonl_path),
                str(gz_path),
                "--db",
                str(db_path),
                "--chunk-size",
                "1",
                "--json",
            ]
        )
        == 0
    )
    build_out = json.loads(capsys.readouterr().out)
    assert build_out["documents_indexed"] == 2
    assert build_out["integrity_status"] == "ok"

    assert (
        cli_main(
            [
                "search",
                "replication",
                "--db",
                str(db_path),
                "--service",
                "ledger-worker",
                "--start",
                "2026-03-14",
                "--end",
                "2026-03-14",
                "--regex",
                r"ERR-504-\d{3}",
                "--json",
            ]
        )
        == 0
    )
    search_out = json.loads(capsys.readouterr().out)
    assert search_out["total_returned"] == 1
    assert search_out["hits"][0]["doc_uid"] == "INC-2026-101"
    assert search_out["elapsed_ms"] >= 0.0

    assert cli_main(["show", "RB-2026-202", "--db", str(db_path), "--json"]) == 0
    show_out = json.loads(capsys.readouterr().out)
    assert show_out["doc_type"] == "runbook"

    assert cli_main(["show", "NON-EXISTENT", "--db", str(db_path)]) == 1
    capsys.readouterr()

    assert cli_main(["stats", "--db", str(db_path), "--json"]) == 0
    stats_out = json.loads(capsys.readouterr().out)
    assert stats_out["document_count"] == 2
    assert stats_out["query_only"] is True


def test_benchmark_script_execution_and_metrics(tmp_path: Path) -> None:
    bench_db = tmp_path / "bench.db"
    proc = subprocess.run(
        [
            sys.executable,
            str(BENCHMARK_SCRIPT),
            "--docs",
            "300",
            "--queries",
            "25",
            "--db",
            str(bench_db),
            "--json",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    payload = json.loads(proc.stdout)
    assert payload["docs"] == 300
    assert payload["fts_rows"] == 300
    assert payload["queries_executed"] == 25
    assert payload["integrity_status"] == "ok"
    assert payload["throughput_docs_per_sec"] > 0
    assert payload["db_size_mb"] > 0
    assert payload["latency_p50_ms"] <= payload["latency_p95_ms"]
    assert payload["total_hits_returned"] > 0
