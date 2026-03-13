#!/usr/bin/env python3
"""
scripts.stress_large_zip
========================

Manual stress test for streaming a very large ZIP member with zipstreamer.

Overview
--------
This script generates a large ZIP archive containing one text member and
then streams it through ``zipstreamer.LineStreamer`` while reporting:

- total lines streamed
- total decoded bytes
- elapsed time
- throughput in MiB/s
- optional peak Python memory via tracemalloc

Purpose
-------
This is a manual stress tool, not an automated test. It is useful for
checking that:

- streaming remains stable on very large inputs
- memory usage stays bounded on realistic workloads
- throughput is acceptable on the target machine
"""

from __future__ import annotations

import argparse
import time
import tracemalloc
import zipfile
from pathlib import Path

from ziplogstream import LineStreamer, LineStreamerConfig

MiB = 1024 * 1024
GiB = 1024 * MiB


def format_bytes(num_bytes: int) -> str:
    """
    Format a byte count for display.
    """
    if num_bytes < 1024:
        return f"{num_bytes} B"
    if num_bytes < MiB:
        return f"{num_bytes / 1024:.2f} KiB"
    if num_bytes < GiB:
        return f"{num_bytes / MiB:.2f} MiB"
    return f"{num_bytes / GiB:.2f} GiB"


def generate_large_zip(
    zip_path: Path,
    member_name: str,
    target_size_bytes: int,
    *,
    line_template: str,
    encoding: str = "utf-8",
) -> None:
    """
    Generate a ZIP archive containing one large text member.

    The member is written incrementally so the generation process itself
    does not build the full 1 GiB text in memory at once.
    """
    zip_path.parent.mkdir(parents=True, exist_ok=True)

    encoded_line = line_template.encode(encoding)
    line_size = len(encoded_line)

    if line_size == 0:
        raise ValueError("line_template must not encode to empty bytes")

    written = 0

    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        with zf.open(member_name, "w") as member:
            while written < target_size_bytes:
                remaining = target_size_bytes - written
                if remaining >= line_size:
                    member.write(encoded_line)
                    written += line_size
                else:
                    member.write(encoded_line[:remaining])
                    written += remaining


def stream_large_zip(
    zip_path: Path,
    target: str,
    *,
    config: LineStreamerConfig,
    track_memory: bool,
) -> None:
    """
    Stream the ZIP member and print performance and memory statistics.
    """
    if track_memory:
        tracemalloc.start()

    started = time.perf_counter()
    total_lines = 0
    total_decoded_bytes = 0

    streamer = LineStreamer(zip_path, target, config=config)

    for line in streamer.stream():
        total_lines += 1
        total_decoded_bytes += len(line.encode(config.encoding, errors=config.errors))

    elapsed = time.perf_counter() - started

    peak_memory = None
    if track_memory:
        _, peak_memory = tracemalloc.get_traced_memory()
        tracemalloc.stop()

    throughput_mib_per_sec = (total_decoded_bytes / MiB) / elapsed if elapsed > 0 else 0.0

    print()
    print("Streaming complete")
    print("------------------")
    print(f"ZIP path:         {zip_path}")
    print(f"Decoded bytes:    {format_bytes(total_decoded_bytes)}")
    print(f"Lines streamed:   {total_lines:,}")
    print(f"Elapsed time:     {elapsed:.2f} s")
    print(f"Throughput:       {throughput_mib_per_sec:.2f} MiB/s")
    if peak_memory is not None:
        print(f"Peak Python mem:  {format_bytes(peak_memory)}")


def build_arg_parser() -> argparse.ArgumentParser:
    """
    Build command-line arguments.
    """
    parser = argparse.ArgumentParser(
        description="Generate and stream a very large ZIP member."
    )
    parser.add_argument(
        "--zip-path",
        type=Path,
        default=Path(".benchmarks/large_logs_1g.zip"),
        help="Output ZIP file path.",
    )
    parser.add_argument(
        "--member-name",
        default="logs/app.log",
        help="ZIP member name to generate and stream.",
    )
    parser.add_argument(
        "--size-gib",
        type=float,
        default=1.0,
        help="Approximate decoded member size in GiB. Default: 1.0",
    )
    parser.add_argument(
        "--line-template",
        default="INFO request completed status=200 latency_ms=12 path=/healthcheck\n",
        help="Line template used to build the large member.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=1 << 20,
        help="LineStreamer chunk size in bytes. Default: 1 MiB",
    )
    parser.add_argument(
        "--max-line-bytes",
        type=int,
        default=32 * (1 << 20),
        help="Maximum buffered line bytes. Default: 32 MiB",
    )
    parser.add_argument(
        "--encoding",
        default="utf-8",
        help="Text encoding. Default: utf-8",
    )
    parser.add_argument(
        "--errors",
        default="replace",
        help="Decode error handler. Default: replace",
    )
    parser.add_argument(
        "--track-memory",
        action="store_true",
        help="Track peak Python memory with tracemalloc.",
    )
    parser.add_argument(
        "--skip-generate",
        action="store_true",
        help="Do not generate the ZIP; only stream the existing file.",
    )
    return parser


def main() -> int:
    """
    Program entry point.
    """
    parser = build_arg_parser()
    args = parser.parse_args()

    target_size_bytes = int(args.size_gib * GiB)

    config = LineStreamerConfig(
        chunk_size=args.chunk_size,
        encoding=args.encoding,
        errors=args.errors,
        max_line_bytes=args.max_line_bytes,
    )

    if not args.skip_generate:
        print("Generating large ZIP archive...")
        print("-------------------------------")
        print(f"Target ZIP path:   {args.zip_path}")
        print(f"Member name:       {args.member_name}")
        print(f"Target size:       {args.size_gib:.2f} GiB decoded")
        generate_large_zip(
            zip_path=args.zip_path,
            member_name=args.member_name,
            target_size_bytes=target_size_bytes,
            line_template=args.line_template,
            encoding=args.encoding,
        )
        print("Generation complete.")

    print()
    print("Streaming large ZIP archive...")
    print("------------------------------")
    stream_large_zip(
        zip_path=args.zip_path,
        target=Path(args.member_name).name,
        config=config,
        track_memory=args.track_memory,
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())