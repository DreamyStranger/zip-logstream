# zip-logstream

[![CI](https://github.com/DreamyStranger/zip-logstream/actions/workflows/ci.yml/badge.svg)](https://github.com/DreamyStranger/zip-logstream/actions/workflows/ci.yml)
[![Release](https://github.com/DreamyStranger/zip-logstream/actions/workflows/release.yml/badge.svg)](https://github.com/DreamyStranger/zip-logstream/actions/workflows/release.yml)
[![Python Versions](https://img.shields.io/pypi/pyversions/zip-logstream.svg)](https://pypi.org/project/zip-logstream/)
[![License](https://img.shields.io/github/license/DreamyStranger/zip-logstream.svg)](LICENSE)

**Streaming line reader for large log files stored inside ZIP archives.**

`zip-logstream` provides a fast, memory-bounded way to iterate over text lines
from a file stored inside a ZIP archive **without extracting the archive to
disk and without loading the entire file into memory**.

It is designed for large log processing pipelines where input files may be
hundreds of megabytes or gigabytes in size.

---

## Features

- **True streaming**
  - Reads ZIP members incrementally
  - Never loads the full file into memory
  - Never extracts files to disk

- **Bounded memory usage**
  - Buffer growth is limited by configuration
  - Oversized lines are force-flushed safely

- **Deterministic member resolution**
  - Supports basename or suffix matching
  - Detects ambiguous matches

- **CRLF normalization**
  - Handles Windows and Unix line endings
  - Removes trailing `\r` for CRLF lines

- **High performance**
  - Optimized hot-path streaming loop
  - Efficient chunked reading and decoding

- **Fully typed**
  - Includes `py.typed` for static type checking

---

## Installation

Install from PyPI:

```bash
pip install zip-logstream
```

Or install directly from source:

```bash
pip install .
```

Or install in editable mode during development:

```bash
pip install -e .
```

Or install with development dependencies:

```bash
pip install -e .[dev]
```

Python **3.10+** is required.

---

## Quick Start

```python
from zip_logstream import LineStreamer

streamer = LineStreamer("logs.zip", "app.log")

for line in streamer.stream():
    print(line)
```

This will:

- open `logs.zip`
- resolve the member named `app.log`
- stream decoded lines one by one

No extraction or full file loading occurs.

---

## Member Resolution

The library must select **exactly one file** inside the ZIP archive.

The default resolver behaves as follows:

1. If the target contains **no path separator**, it prefers exact basename matches.
2. If no basename match exists, it falls back to **suffix matching**.
3. If multiple matches exist, a `ZipMemberAmbiguityError` is raised.
4. If no match exists, a `ZipMemberNotFoundError` is raised.

Archive filenames do not need to end in `.zip`. Any existing file path is
accepted, and the library validates the payload when it opens the archive.

Example archive:

```text
logs.zip
├── service-a/app.log
└── service-b/app.log
```

Calling:

```python
LineStreamer("logs.zip", "app.log")
```

would raise an ambiguity error because two files match.

You can instead target a specific suffix:

```python
LineStreamer("logs.zip", "service-a/app.log")
```

---

## Configuration

Streaming behavior can be configured using `LineStreamerConfig`.

```python
from zip_logstream import LineStreamer, LineStreamerConfig

config = LineStreamerConfig(
    chunk_size=1 << 20,         # 1 MiB read chunks
    encoding="utf-8",
    errors="replace",
    max_line_bytes=32 * (1 << 20),  # 32 MiB max line buffer
)

streamer = LineStreamer(
    "logs.zip",
    "app.log",
    config=config,
)

for line in streamer.stream():
    process(line)
```

### Configuration options

| Option | Description |
|---|---|
| `chunk_size` | Number of bytes read per chunk from the decompressed member stream |
| `encoding` | Text encoding used when decoding bytes |
| `errors` | Error handler passed to `bytes.decode()` |
| `max_line_bytes` | Maximum buffered bytes allowed for an unterminated line before forced flush |

### Configuration notes

- `chunk_size` must be a positive integer
- `max_line_bytes` must be a positive integer
- `max_line_bytes` must be greater than or equal to `chunk_size`
- `encoding` must be a valid codec name
- `errors` must be a valid codec error handler name

If a line exceeds `max_line_bytes`, the current buffer is **force-flushed** to
avoid unbounded memory growth.

---

## Custom Member Resolver

You may provide a custom function to determine which ZIP member should be read.

```python
import zipfile
from zip_logstream import LineStreamer

def pick_latest_log(zf: zipfile.ZipFile, target: str) -> str:
    candidates = [name for name in zf.namelist() if name.endswith(".log")]
    if not candidates:
        raise FileNotFoundError("No .log files found")
    return sorted(candidates)[-1]

streamer = LineStreamer(
    "logs.zip",
    "ignored.log",
    resolver=pick_latest_log,
)

for line in streamer.stream():
    print(line)
```

The resolver contract is:

```python
(zipfile.ZipFile, target_string) -> member_name
```

and it must return **one exact member name**.

---

## Public API

The main public entry points are:

```python
from zip_logstream import (
    LineStreamer,
    LineStreamerConfig,
    default_zip_member_resolver,
    ZipLogStreamError,
    ConfigurationError,
    ZipValidationError,
    ZipMemberNotFoundError,
    ZipMemberAmbiguityError,
)
```

---

## Error Handling

`zip-logstream` exposes a small, explicit exception hierarchy.

```python
from zip_logstream import (
    ConfigurationError,
    ZipMemberAmbiguityError,
    ZipMemberNotFoundError,
    ZipValidationError,
)

try:
    streamer = LineStreamer("logs.zip", "app.log")
    for line in streamer.stream():
        handle(line)
except ZipValidationError as exc:
    print(f"Invalid archive path: {exc}")
except ZipMemberNotFoundError as exc:
    print(f"Target not found: {exc}")
except ZipMemberAmbiguityError as exc:
    print(f"Ambiguous target: {exc}")
except ConfigurationError as exc:
    print(f"Invalid config: {exc}")
```

---

## Line Semantics

`LineStreamer.stream()` yields decoded `str` values with the following behavior:

- trailing newline characters are removed
- CRLF lines have the trailing `\r` removed
- a final partial line is still yielded even if the file does not end in `\n`
- empty lines are preserved
- oversized unterminated lines are force-flushed as decoded chunks

This makes the iterator suitable for large log-style inputs while preserving
bounded memory usage.

---

## Performance

`zip-logstream` is optimized for throughput while preserving a simple API.

Benchmark results (median across 3 runs, 1 MiB chunk size, local machine):

```text
case                     MiB      lines   median MiB/s   median lines/s
------------------------------------------------------------------------
many-short-lines       24.80    400,000          96.32        1,553,886
medium-lines           16.87    120,000         162.79        1,157,946
crlf-lines             12.59    300,000          56.37        1,343,303
single-huge-line       24.00          1         273.35               11
dense-empty-lines       2.75  1,080,000           4.85        1,944,745
```

`single-huge-line` shows raw I/O throughput with no per-line overhead.
`dense-empty-lines` throughput looks low in MiB/s because the payload is
almost entirely newlines — lines/s is the meaningful metric there.

Example 1 GiB memory-tracked stress run (compressed DEFLATE, disk I/O):

```text
Decoded bytes:    1008.48 MiB
Lines streamed:   16,268,816
Elapsed time:     61.14 s
Throughput:       16.49 MiB/s
Peak Python mem:  5.20 MiB
```

The throughput here is DEFLATE-decompression and disk I/O bound, not streamer
bound. The key result is **5.20 MiB peak Python memory** while processing over
1 GiB of decoded content — memory usage stays flat regardless of input size.

---

## Bounded Memory Behavior

For normal line-based workloads, memory usage remains effectively independent
of total file size.

Memory usage is driven primarily by:

- read `chunk_size`
- current buffered partial line
- temporary decode allocations

This makes `zip-logstream` suitable for very large ZIP-contained logs where full
extraction or full in-memory loading would be impractical.

---

## Development

Create a virtual environment and install the project in editable mode:

macOS / Linux:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
```

Windows PowerShell:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -e .[dev]
```

Run tests:

```bash
pytest
```

Run coverage:

```bash
pytest --cov=zip_logstream --cov-report=term-missing
```

Run benchmarks:

```bash
python scripts/benchmark_streaming.py
```

Save the benchmark summary table to a file:

```bash
python scripts/benchmark_streaming.py --table-out results/bench.txt
```

Run a large-file stress test:

```bash
python scripts/stress_large_zip.py --size-gib 1.0 --track-memory --report-out results/stress.txt
```

---

## Project Structure

```text
zip-logstream/
├── .github/
│   └── workflows/
│       ├── ci.yml
│       └── release.yml
├── src/
│   └── zip_logstream/
│       ├── __init__.py
│       ├── py.typed
│       ├── version.py
│       ├── errors.py
│       ├── config.py
│       ├── protocols.py
│       ├── archive/
│       │   ├── __init__.py
│       │   ├── member_resolution.py
│       │   └── validators.py
│       └── streaming/
│           ├── __init__.py
│           ├── buffered_line_reader.py
│           └── line_streamer.py
├── tests/
│   ├── conftest.py
│   ├── unit/
│   └── integration/
├── scripts/
│   ├── benchmark_streaming.py
│   └── stress_large_zip.py
├── README.md
├── LICENSE
├── CHANGELOG.md
├── MANIFEST.in
└── pyproject.toml
```

---

## Why `src/` Layout?

This project uses the `src/` layout so the package must be installed before it
is imported. This helps prevent accidental local-import issues during
development and makes packaging behavior more reliable.

---

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
