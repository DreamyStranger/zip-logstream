#!/usr/bin/env python3
"""
scripts.benchmark_streaming
===========================

Performance benchmark for the zipstreamer package.

Overview
--------
This script benchmarks end-to-end streaming performance of
``zipstreamer.LineStreamer`` against synthetic ZIP archives generated on
demand.

It is intended for:
- local performance checks during development
- comparing configuration choices such as chunk size
- validating behavior across different input shapes
- generating repeatable benchmark results for documentation or release notes

What it measures
----------------
For each benchmark case, the script measures:

- wall-clock duration
- number of lines yielded
- total decoded text bytes yielded
- throughput in MiB/s
- lines per second
- optional peak Python memory tracked via ``tracemalloc``

Benchmark philosophy
--------------------
This script is *not* part of the automated test suite. It is deliberately
placed under ``scripts/`` because benchmark results are machine-dependent
and should be run on demand.

The benchmark focuses on the public high-level API:

    ZIP archive
      -> member resolution
      -> ZIP member open
      -> BufferedLineReader
      -> decoded line iteration

This makes it suitable as a practical end-to-end performance check.

Examples
--------
Run all default benchmark cases:

    python scripts/benchmark_streaming.py

Run only one case with multiple repeats and memory tracking:

    python scripts/benchmark_streaming.py --case many-short-lines --repeat 5 --track-memory

Use a custom working directory for generated inputs:

    python scripts/benchmark_streaming.py --workspace .benchmarks

Write results to JSON:

    python scripts/benchmark_streaming.py --json-out benchmark_results.json
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
import time
import tracemalloc
import zipfile
from dataclasses import asdict, dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Callable, Iterable

from ziplogstream import LineStreamer, LineStreamerConfig

MiB = 1024 * 1024


@dataclass(frozen=True, slots=True)
class BenchmarkCase:
    """
    Definition of a single benchmark scenario.

    Attributes:
        name:
            Stable identifier for the benchmark case.

        description:
            Human-readable explanation of the scenario.

        member_name:
            ZIP member path to generate and later stream.

        content_factory:
            Callable that returns the member text payload.
    """

    name: str
    description: str
    member_name: str
    content_factory: Callable[[], str]


@dataclass(frozen=True, slots=True)
class RunMetrics:
    """
    Metrics collected from one benchmark run.
    """

    case_name: str
    run_index: int
    duration_seconds: float
    line_count: int
    decoded_bytes: int
    throughput_mib_per_sec: float
    lines_per_sec: float
    peak_memory_bytes: int | None


@dataclass(frozen=True, slots=True)
class CaseSummary:
    """
    Aggregate metrics across repeated runs for a benchmark case.
    """

    case_name: str
    description: str
    repeats: int
    line_count: int
    decoded_bytes: int
    mean_duration_seconds: float
    median_duration_seconds: float
    min_duration_seconds: float
    max_duration_seconds: float
    mean_throughput_mib_per_sec: float
    median_throughput_mib_per_sec: float
    mean_lines_per_sec: float
    median_lines_per_sec: float
    peak_memory_bytes_max: int | None


def build_many_short_lines() -> str:
    """
    Build a payload dominated by many small newline-delimited lines.

    This approximates common log files with short entries.
    """
    line = "INFO request completed status=200 latency_ms=12 path=/healthcheck\n"
    return line * 400_000


def build_medium_lines() -> str:
    """
    Build a payload with medium-width structured log lines.

    This case is useful for seeing how performance behaves when per-line
    decode and yield overhead is amortized across larger lines.
    """
    lines: list[str] = []
    for i in range(120_000):
        lines.append(
            f"2026-03-11T12:00:{i % 60:02d}Z "
            f"service=api level=INFO req_id={i:08d} "
            f"user_id={i % 10000:05d} method=GET path=/v1/resource/{i % 250} "
            f"status=200 latency_ms={(i % 47) + 3} region=us-east-1\n"
        )
    return "".join(lines)


def build_crlf_lines() -> str:
    """
    Build a CRLF-terminated payload to exercise Windows-style line endings.
    """
    line = "INFO windows-style log line with CRLF ending\r\n"
    return line * 300_000


def build_single_huge_line() -> str:
    """
    Build one extremely large unterminated logical line.

    This stresses the oversized-buffer protection path.
    """
    return "X" * (24 * MiB)


def build_large_final_partial_line() -> str:
    """
    Build a payload with many normal lines and a final non-terminated tail.
    """
    head = ("INFO normal line before final partial tail\n" * 150_000)
    tail = "TAIL" * (2 * MiB // 4)
    return head + tail


def build_empty_lines_dense() -> str:
    """
    Build a payload with many empty lines interspersed with content.
    """
    chunk = "alpha\n\nbeta\n\n\ncharlie\n"
    return chunk * 180_000


DEFAULT_CASES: tuple[BenchmarkCase, ...] = (
    BenchmarkCase(
        name="many-short-lines",
        description="High line-count case with short LF-terminated log lines.",
        member_name="logs/app.log",
        content_factory=build_many_short_lines,
    ),
    BenchmarkCase(
        name="medium-lines",
        description="Structured medium-width log lines with realistic field payloads.",
        member_name="logs/app.log",
        content_factory=build_medium_lines,
    ),
    BenchmarkCase(
        name="crlf-lines",
        description="Windows-style CRLF line endings through the full pipeline.",
        member_name="logs/app.log",
        content_factory=build_crlf_lines,
    ),
    BenchmarkCase(
        name="single-huge-line",
        description="One oversized unterminated line to exercise forced flush behavior.",
        member_name="logs/app.log",
        content_factory=build_single_huge_line,
    ),
    BenchmarkCase(
        name="large-final-partial-line",
        description="Many normal lines followed by a large final partial line.",
        member_name="logs/app.log",
        content_factory=build_large_final_partial_line,
    ),
    BenchmarkCase(
        name="dense-empty-lines",
        description="Frequent empty lines mixed with normal lines.",
        member_name="logs/app.log",
        content_factory=build_empty_lines_dense,
    ),
)


def mib_from_bytes(num_bytes: int) -> float:
    """
    Convert bytes to mebibytes.
    """
    return num_bytes / MiB


def create_zip_payload(zip_path: Path, member_name: str, text: str, encoding: str) -> None:
    """
    Create a ZIP archive containing one text member.

    Args:
        zip_path:
            Output ZIP path.

        member_name:
            Name of the member inside the archive.

        text:
            Text content to encode and write.

        encoding:
            Encoding used for the member bytes.
    """
    zip_path.parent.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(member_name, text.encode(encoding))


def run_single_benchmark(
    *,
    zip_path: Path,
    target: str,
    config: LineStreamerConfig,
    track_memory: bool,
    run_index: int,
    case_name: str,
) -> RunMetrics:
    """
    Run one benchmark iteration and collect metrics.
    """
    if track_memory:
        tracemalloc.start()

    started = time.perf_counter()
    line_count = 0
    decoded_bytes = 0

    streamer = LineStreamer(zip_path, target, config=config)

    for line in streamer.stream():
        line_count += 1
        decoded_bytes += len(line.encode(config.encoding, errors=config.errors))

    duration = time.perf_counter() - started

    peak_memory_bytes: int | None
    if track_memory:
        _, peak_memory_bytes = tracemalloc.get_traced_memory()
        tracemalloc.stop()
    else:
        peak_memory_bytes = None

    throughput_mib_per_sec = (
        mib_from_bytes(decoded_bytes) / duration if duration > 0 else math.inf
    )
    lines_per_sec = line_count / duration if duration > 0 else math.inf

    return RunMetrics(
        case_name=case_name,
        run_index=run_index,
        duration_seconds=duration,
        line_count=line_count,
        decoded_bytes=decoded_bytes,
        throughput_mib_per_sec=throughput_mib_per_sec,
        lines_per_sec=lines_per_sec,
        peak_memory_bytes=peak_memory_bytes,
    )


def summarize_case(case: BenchmarkCase, runs: list[RunMetrics]) -> CaseSummary:
    """
    Aggregate repeated run metrics for one benchmark case.
    """
    durations = [r.duration_seconds for r in runs]
    throughputs = [r.throughput_mib_per_sec for r in runs]
    line_rates = [r.lines_per_sec for r in runs]
    peak_values = [r.peak_memory_bytes for r in runs if r.peak_memory_bytes is not None]

    first = runs[0]
    return CaseSummary(
        case_name=case.name,
        description=case.description,
        repeats=len(runs),
        line_count=first.line_count,
        decoded_bytes=first.decoded_bytes,
        mean_duration_seconds=statistics.mean(durations),
        median_duration_seconds=statistics.median(durations),
        min_duration_seconds=min(durations),
        max_duration_seconds=max(durations),
        mean_throughput_mib_per_sec=statistics.mean(throughputs),
        median_throughput_mib_per_sec=statistics.median(throughputs),
        mean_lines_per_sec=statistics.mean(line_rates),
        median_lines_per_sec=statistics.median(line_rates),
        peak_memory_bytes_max=max(peak_values) if peak_values else None,
    )


def format_bytes(num_bytes: int | None) -> str:
    """
    Format a byte count for human-readable output.
    """
    if num_bytes is None:
        return "-"
    if num_bytes < 1024:
        return f"{num_bytes} B"
    if num_bytes < MiB:
        return f"{num_bytes / 1024:.1f} KiB"
    return f"{num_bytes / MiB:.2f} MiB"


def print_case_header(case: BenchmarkCase) -> None:
    """
    Print a readable heading for one benchmark case.
    """
    print()
    print(f"[{case.name}]")
    print(case.description)


def print_run_metrics(metrics: RunMetrics) -> None:
    """
    Print one run's metrics in a compact human-readable form.
    """
    print(
        f"  run {metrics.run_index + 1:>2}: "
        f"{metrics.duration_seconds:8.4f} s | "
        f"{mib_from_bytes(metrics.decoded_bytes):8.2f} MiB | "
        f"{metrics.line_count:>10,d} lines | "
        f"{metrics.throughput_mib_per_sec:8.2f} MiB/s | "
        f"{metrics.lines_per_sec:10,.0f} lines/s | "
        f"peak mem {format_bytes(metrics.peak_memory_bytes)}"
    )


def print_summary_table(summaries: Iterable[CaseSummary]) -> None:
    """
    Print a final summary table.
    """
    rows = list(summaries)
    if not rows:
        return

    print()
    print("=" * 122)
    print(
        f"{'case':<24} "
        f"{'MiB':>10} "
        f"{'lines':>12} "
        f"{'mean s':>10} "
        f"{'median s':>10} "
        f"{'mean MiB/s':>12} "
        f"{'median MiB/s':>14} "
        f"{'mean lines/s':>14} "
        f"{'peak mem':>12}"
    )
    print("-" * 122)

    for row in rows:
        print(
            f"{row.case_name:<24} "
            f"{mib_from_bytes(row.decoded_bytes):>10.2f} "
            f"{row.line_count:>12,d} "
            f"{row.mean_duration_seconds:>10.4f} "
            f"{row.median_duration_seconds:>10.4f} "
            f"{row.mean_throughput_mib_per_sec:>12.2f} "
            f"{row.median_throughput_mib_per_sec:>14.2f} "
            f"{row.mean_lines_per_sec:>14,.0f} "
            f"{format_bytes(row.peak_memory_bytes_max):>12}"
        )

    print("=" * 122)


def resolve_cases(selected_names: list[str] | None) -> list[BenchmarkCase]:
    """
    Resolve case names to benchmark definitions.
    """
    if not selected_names:
        return list(DEFAULT_CASES)

    case_map = {case.name: case for case in DEFAULT_CASES}
    missing = [name for name in selected_names if name not in case_map]
    if missing:
        available = ", ".join(sorted(case_map))
        raise SystemExit(
            f"Unknown benchmark case(s): {', '.join(missing)}. "
            f"Available cases: {available}"
        )

    return [case_map[name] for name in selected_names]


def build_arg_parser() -> argparse.ArgumentParser:
    """
    Construct the command-line interface.
    """
    parser = argparse.ArgumentParser(
        description="Benchmark end-to-end ZIP member streaming with zipstreamer."
    )
    parser.add_argument(
        "--case",
        action="append",
        dest="cases",
        help="Benchmark case name to run. May be repeated. Defaults to all cases.",
    )
    parser.add_argument(
        "--repeat",
        type=int,
        default=3,
        help="Number of timed runs per case. Default: 3",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=1 << 20,
        help="LineStreamerConfig.chunk_size in bytes. Default: 1048576",
    )
    parser.add_argument(
        "--max-line-bytes",
        type=int,
        default=32 * (1 << 20),
        help="LineStreamerConfig.max_line_bytes in bytes. Default: 33554432",
    )
    parser.add_argument(
        "--encoding",
        default="utf-8",
        help="Text encoding for generated payloads and streaming. Default: utf-8",
    )
    parser.add_argument(
        "--errors",
        default="replace",
        help="Decode error handler. Default: replace",
    )
    parser.add_argument(
        "--workspace",
        type=Path,
        help=(
            "Optional directory for generated benchmark ZIP files. "
            "If omitted, a temporary directory is used."
        ),
    )
    parser.add_argument(
        "--track-memory",
        action="store_true",
        help="Track peak Python memory with tracemalloc.",
    )
    parser.add_argument(
        "--json-out",
        type=Path,
        help="Optional path to write detailed benchmark results as JSON.",
    )
    return parser


def main() -> int:
    """
    Entry point for the benchmark script.
    """
    parser = build_arg_parser()
    args = parser.parse_args()

    if args.repeat <= 0:
        raise SystemExit("--repeat must be greater than 0")

    config = LineStreamerConfig(
        chunk_size=args.chunk_size,
        encoding=args.encoding,
        errors=args.errors,
        max_line_bytes=args.max_line_bytes,
    )
    cases = resolve_cases(args.cases)

    json_payload: dict[str, object] = {
        "config": {
            "chunk_size": config.chunk_size,
            "encoding": config.encoding,
            "errors": config.errors,
            "max_line_bytes": config.max_line_bytes,
        },
        "cases": [],
    }

    summaries: list[CaseSummary] = []

    def execute(workspace: Path) -> None:
        for case in cases:
            print_case_header(case)

            zip_path = workspace / f"{case.name}.zip"
            text = case.content_factory()
            create_zip_payload(
                zip_path=zip_path,
                member_name=case.member_name,
                text=text,
                encoding=config.encoding,
            )

            runs: list[RunMetrics] = []
            for run_index in range(args.repeat):
                metrics = run_single_benchmark(
                    zip_path=zip_path,
                    target=Path(case.member_name).name,
                    config=config,
                    track_memory=args.track_memory,
                    run_index=run_index,
                    case_name=case.name,
                )
                runs.append(metrics)
                print_run_metrics(metrics)

            summary = summarize_case(case, runs)
            summaries.append(summary)

            json_payload["cases"].append(
                {
                    "case": {
                        "name": case.name,
                        "description": case.description,
                        "member_name": case.member_name,
                    },
                    "runs": [asdict(run) for run in runs],
                    "summary": asdict(summary),
                }
            )

    if args.workspace is not None:
        workspace = args.workspace
        workspace.mkdir(parents=True, exist_ok=True)
        execute(workspace)
    else:
        with TemporaryDirectory(prefix="zipstreamer-bench-") as tmpdir:
            execute(Path(tmpdir))

    print_summary_table(summaries)

    if args.json_out is not None:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(json.dumps(json_payload, indent=2), encoding="utf-8")
        print()
        print(f"Wrote JSON results to: {args.json_out}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())