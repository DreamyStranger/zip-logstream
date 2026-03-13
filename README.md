# ziplogstream

![CI](https://github.com/DreamyStranger/ziplogstream/actions/workflows/ci.yml/badge.svg)
![Python Versions](https://img.shields.io/pypi/pyversions/ziplogstream.svg)](https://pypi.org/project/ziplogstream/)
![License](https://img.shields.io/badge/license-MIT-green)
![Tests](https://img.shields.io/badge/tests-pytest-blue)

**Streaming line reader for large log files stored inside ZIP archives.**

`ziplogstream` provides a fast, memory-bounded way to iterate over text lines
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

Install directly from source:

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

Python **3.11+** is required.

---

## Quick Start

```python
from ziplogstream import LineStreamer

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
from ziplogstream import LineStreamer, LineStreamerConfig

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
from ziplogstream import LineStreamer

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
from ziplogstream import (
    LineStreamer,
    LineStreamerConfig,
    default_zip_member_resolver,
    ZipLogStreamerError,
    ConfigurationError,
    ZipValidationError,
    ZipMemberNotFoundError,
    ZipMemberAmbiguityError,
)
```

---

## Error Handling

`ziplogstream` exposes a small, explicit exception hierarchy.

```python
from ziplogstream import (
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

`ziplogstream` is optimized for throughput while preserving a simple API.

Example benchmark results from the included benchmark scripts on a local machine:

```text
many-short-lines         ~78 MiB/s
medium-lines            ~145 MiB/s
single-huge-line        ~224 MiB/s
```

Example 1 GiB stress run:

```text
Decoded bytes:    1008.48 MiB
Lines streamed:   16,268,816
Elapsed time:     13.51 s
Throughput:       74.62 MiB/s
```

Example 1 GiB memory-tracked stress run:

```text
Decoded bytes:    1008.48 MiB
Lines streamed:   16,268,816
Elapsed time:     57.37 s
Throughput:       17.58 MiB/s
Peak Python mem:  7.20 MiB
```

The memory-tracked run is slower because `tracemalloc` instruments Python
allocations. It is intended for memory validation, not throughput comparison.

---

## Bounded Memory Behavior

For normal line-based workloads, memory usage remains effectively independent
of total file size.

Memory usage is driven primarily by:

- read `chunk_size`
- current buffered partial line
- temporary decode allocations

This makes `ziplogstream` suitable for very large ZIP-contained logs where full
extraction or full in-memory loading would be impractical.

---

## Development

Create a virtual environment and install the project in editable mode:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
```

Run tests:

```bash
pytest
```

Run coverage:

```bash
pytest --cov=ziplogstream --cov-report=term-missing
```

Run benchmarks:

```bash
python scripts/benchmark_streaming.py
```

Run a large-file stress test:

```bash
python scripts/stress_large_zip.py --size-gib 1.0
```

Track peak Python memory during the stress test:

```bash
python scripts/stress_large_zip.py --size-gib 1.0 --track-memory
```

---

## Project Structure

```text
ziplogstream/
├── .github/
│   └── workflows/
│       └── ci.yml
├── src/
│   └── ziplogstream/
│       ├── __init__.py
│       ├── py.typed
│       ├── version.py
│       ├── errors.py
│       ├── logging.py
│       ├── config/
│       ├── archive/
│       ├── streaming/
│       └── types/
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