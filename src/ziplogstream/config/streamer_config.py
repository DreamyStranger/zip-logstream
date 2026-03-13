"""
ziplogstream.config.line_streamer
================================

Configuration models for ZIP member line streaming.

Overview
--------
This module defines the immutable configuration object used by the
line streaming pipeline. The configuration is intentionally small and
focused only on concerns directly related to reading and decoding text
lines from a binary member stream.

Design goals
------------
- Immutable configuration for predictable behavior
- Early validation of invalid values
- Clear defaults suitable for large log streaming
- No archive-opening or parsing logic in this module

Notes
-----
`LineStreamerConfig` is designed to be passed into higher-level runtime
components such as `ziplogstream.streaming.line_streamer.LineStreamer`
and `ziplogstream.streaming.buffered_line_reader.BufferedLineReader`.

The configuration controls:
- chunked read size
- text decoding behavior
- maximum buffered bytes allowed for a single unterminated line

This module does not perform any I/O.
"""

from __future__ import annotations

import codecs
from dataclasses import dataclass

from ziplogstream.errors import ConfigurationError


@dataclass(frozen=True, slots=True)
class LineStreamerConfig:
    """
    Immutable configuration for line streaming from a ZIP member.

    Attributes:
        chunk_size:
            Number of bytes to read per block from the decompressed member
            stream. Larger values may improve throughput at the cost of
            slightly higher transient memory usage.

        encoding:
            Text encoding used to decode streamed bytes into Python `str`
            values. Defaults to UTF-8.

        errors:
            Decoding error policy passed to the decoder. Common values
            include `"strict"`, `"replace"`, and `"ignore"`.

        max_line_bytes:
            Maximum number of buffered bytes allowed for an unterminated
            line before the current buffer is force-flushed as a chunk.
            This prevents unbounded memory growth when the input contains
            extremely long lines or malformed data with missing newlines.

    Defaults:
        - `chunk_size=1 << 20` (1 MiB)
        - `encoding="utf-8"`
        - `errors="replace"`
        - `max_line_bytes=32 * (1 << 20)` (32 MiB)

    Raises:
        ConfigurationError:
            If any configuration value is invalid.
    """

    chunk_size: int = 1 << 20
    encoding: str = "utf-8"
    errors: str = "replace"
    max_line_bytes: int = 32 * (1 << 20)

    def __post_init__(self) -> None:
        """
        Validate configuration values eagerly.

        Validation rules:
        - `chunk_size` must be a positive integer
        - `max_line_bytes` must be a positive integer
        - `max_line_bytes` must be >= `chunk_size`
        - `encoding` must be a non-empty valid codec name
        - `errors` must be a non-empty valid codec error handler name

        Raises:
            ConfigurationError:
                If validation fails.
        """
        if not isinstance(self.chunk_size, int):
            raise ConfigurationError("chunk_size must be an integer")
        if self.chunk_size <= 0:
            raise ConfigurationError("chunk_size must be greater than 0")

        if not isinstance(self.max_line_bytes, int):
            raise ConfigurationError("max_line_bytes must be an integer")
        if self.max_line_bytes <= 0:
            raise ConfigurationError("max_line_bytes must be greater than 0")

        if self.max_line_bytes < self.chunk_size:
            raise ConfigurationError(
                "max_line_bytes must be greater than or equal to chunk_size"
            )

        if not isinstance(self.encoding, str) or not self.encoding.strip():
            raise ConfigurationError("encoding must be a non-empty string")

        if not isinstance(self.errors, str) or not self.errors.strip():
            raise ConfigurationError("errors must be a non-empty string")

        try:
            codecs.lookup(self.encoding)
        except LookupError as exc:
            raise ConfigurationError(f"Unknown encoding: {self.encoding!r}") from exc

        try:
            codecs.lookup_error(self.errors)
        except LookupError as exc:
            raise ConfigurationError(
                f"Unknown decoding error handler: {self.errors!r}"
            ) from exc