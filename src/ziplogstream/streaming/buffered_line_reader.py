"""
ziplogstream.streaming.buffered_line_reader
==========================================

Buffered line reader for chunked binary streams.

Overview
--------
This module provides the low-level streaming engine that converts a
binary input stream into decoded text lines with bounded memory usage.

It is intentionally independent from ZIP-specific archive handling. Any
binary stream object with a `.read()` method can be used as the source.

Public contract
---------------
`BufferedLineReader.iter_lines()` yields decoded `str` values one at a
time without trailing newline characters.

Behavior guarantees:
- Lines are split on the newline byte (`b"\\n"`).
- If a line ends with CRLF (`b"\\r\\n"`), the trailing `b"\\r"` is removed.
- If the input ends with a final partial line lacking `b"\\n"`, that line
  is still emitted.
- If an unterminated line grows beyond `max_line_bytes`, the current
  buffer is force-flushed as a decoded chunk and cleared. This prevents
  unbounded memory growth.

Design goals
------------
- Bounded memory usage for very large files
- Streaming-only operation
- No ZIP-specific logic
- Clear and testable behavior for edge cases

Notes
-----
Oversized buffer flushing is intentionally conservative: when a buffer is
force-flushed due to `max_line_bytes`, no trailing carriage return is
removed because the chunk may not represent the end of a logical line.
"""

from __future__ import annotations

import io
from typing import Iterator

from ziplogstream.config import LineStreamerConfig
from ziplogstream.logging import get_logger
from ziplogstream.streaming.decoding import decode_text, strip_trailing_cr

logger = get_logger(__name__)


class BufferedLineReader:
    """
    Stream decoded text lines from a binary input stream.

    Args:
        raw:
            Binary stream to read from. The stream must support `.read()`.

        config:
            Streaming configuration controlling chunk size, decoding, and
            maximum buffered bytes for an unterminated line.

    Notes
    -----
    The reader wraps the provided binary stream in `io.BufferedReader`
    using `config.chunk_size` as the buffer size for efficient block reads.
    """

    def __init__(self, raw: io.BufferedIOBase, config: LineStreamerConfig) -> None:
        self.raw = raw
        self.config = config

    def iter_lines(self) -> Iterator[str]:
        """
        Iterate decoded text lines from the underlying binary stream.

        Overview
        --------
        This method is the hot-path streaming loop for ``BufferedLineReader``.
        It reads decompressed binary data in fixed-size chunks, identifies line
        boundaries by searching for the newline byte (``b"\\n"``), normalizes
        CRLF endings, and yields decoded text one line at a time.

        Yield semantics
        ---------------
        - Each yielded value is a decoded ``str``.
        - Trailing newline characters are never included.
        - If a line ends with CRLF (``\\r\\n``), the trailing carriage return
        is also removed before decoding.
        - If the stream ends with a final partial line that has no trailing
        newline, that line is still emitted.

        Oversized line behavior
        -----------------------
        If no newline is encountered and the internal buffer grows beyond
        ``config.max_line_bytes``, the current buffer is force-flushed as a
        decoded chunk and then cleared. This preserves bounded-memory behavior
        for malformed inputs or extremely long logical lines.

        Yields:
            One decoded text line at a time.
        """
        cfg = self.config

        # Bind hot-path configuration values locally to reduce repeated
        # attribute lookups inside the tight streaming loop.
        chunk_size = cfg.chunk_size
        encoding = cfg.encoding
        errors = cfg.errors
        max_line_bytes = cfg.max_line_bytes

        # The byte buffer stores unread or partially processed data between
        # chunk reads. It grows until lines are emitted or an oversized flush
        # is triggered.
        buffer = bytearray()

        # Bind frequently used helpers locally. This is a small CPython
        # optimization that can modestly improve throughput in heavily
        # iterative code paths.
        decode = decode_text
        strip_cr = strip_trailing_cr

        reader = io.BufferedReader(self.raw, buffer_size=chunk_size)
        read = reader.read

        while True:
            chunk = read(chunk_size)
            if not chunk:
                break

            buffer.extend(chunk)
            start = 0

            while True:
                newline_pos = buffer.find(b"\n", start)
                if newline_pos == -1:
                    break

                line_bytes = buffer[start:newline_pos]
                normalized_line_bytes = strip_cr(line_bytes)

                yield decode(
                    normalized_line_bytes,
                    encoding=encoding,
                    errors=errors,
                )
                start = newline_pos + 1

            if start:
                del buffer[:start]

            if len(buffer) > max_line_bytes:
                logger.warning(
                    "Oversized line buffer exceeded %d bytes; flushing chunk.",
                    max_line_bytes,
                )
                yield decode(
                    buffer,
                    encoding=encoding,
                    errors=errors,
                )
                buffer.clear()

        if buffer:
            final_bytes = strip_cr(buffer)
            yield decode(
                final_bytes,
                encoding=encoding,
                errors=errors,
            )