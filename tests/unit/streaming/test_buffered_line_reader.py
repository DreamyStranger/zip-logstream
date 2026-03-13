"""
tests.unit.streaming.test_buffered_line_reader
====================================

Unit tests for :mod:`zipstreamer.streaming.buffered_line_reader`.

Overview
--------
This module verifies the core streaming behavior of
:class:`zipstreamer.streaming.buffered_line_reader.BufferedLineReader`.

The buffered line reader is the most important low-level runtime component
in the package. It is responsible for converting a binary stream into
decoded text lines while preserving bounded-memory behavior. These tests
therefore focus on the precise semantics of chunked reading, newline
handling, CRLF normalization, final partial-line emission, and oversized
buffer flushing.

Behavior under test
-------------------
The buffered line reader is expected to:

- split lines on the newline byte ``b"\\n"``
- remove a trailing carriage return for CRLF-terminated lines
- emit the final partial line even if the input does not end with newline
- produce correct output across chunk boundaries
- flush oversized unterminated buffers as decoded chunks
- avoid stripping ``\\r`` during forced oversized flushes

Test philosophy
---------------
These are focused unit tests. They exercise the binary-to-line streaming
engine using in-memory byte streams and do not involve ZIP archive I/O.
"""

from __future__ import annotations

from ziplogstream.config import LineStreamerConfig
from ziplogstream.streaming import BufferedLineReader


def test_iter_lines_yields_basic_newline_delimited_lines(make_bytes_stream) -> None:
    """
    Ensure a simple newline-delimited byte stream is emitted as individual
    decoded lines without trailing newline characters.
    """
    raw = make_bytes_stream(b"alpha\nbeta\ngamma\n")
    reader = BufferedLineReader(raw, LineStreamerConfig(chunk_size=4))

    assert list(reader.iter_lines()) == ["alpha", "beta", "gamma"]


def test_iter_lines_normalizes_crlf_terminated_lines(make_bytes_stream) -> None:
    """
    Ensure CRLF-terminated lines have the trailing carriage return removed
    after splitting on the newline byte.
    """
    raw = make_bytes_stream(b"alpha\r\nbeta\r\ngamma\r\n")
    reader = BufferedLineReader(raw, LineStreamerConfig(chunk_size=5))

    assert list(reader.iter_lines()) == ["alpha", "beta", "gamma"]


def test_iter_lines_emits_final_partial_line_without_trailing_newline(
    make_bytes_stream,
) -> None:
    """
    Ensure the final line is still emitted when the stream ends without a
    terminating newline byte.
    """
    raw = make_bytes_stream(b"alpha\nbeta\ngamma")
    reader = BufferedLineReader(raw, LineStreamerConfig(chunk_size=4))

    assert list(reader.iter_lines()) == ["alpha", "beta", "gamma"]


def test_iter_lines_handles_line_boundaries_across_small_chunk_reads(
    make_bytes_stream,
) -> None:
    """
    Ensure line reconstruction remains correct when chunk boundaries split
    lines and newline markers across multiple reads.
    """
    raw = make_bytes_stream(b"abcdef\n123456\nxyz\n")
    reader = BufferedLineReader(raw, LineStreamerConfig(chunk_size=2))

    assert list(reader.iter_lines()) == ["abcdef", "123456", "xyz"]


def test_iter_lines_preserves_empty_lines(make_bytes_stream) -> None:
    """
    Ensure consecutive newline bytes produce empty-string lines in the
    output, preserving logical line structure.
    """
    raw = make_bytes_stream(b"alpha\n\nbeta\n")
    reader = BufferedLineReader(raw, LineStreamerConfig(chunk_size=3))

    assert list(reader.iter_lines()) == ["alpha", "", "beta"]


def test_iter_lines_flushes_oversized_unterminated_buffer_as_chunk(
    make_bytes_stream,
) -> None:
    """
    Ensure an unterminated buffer that grows beyond ``max_line_bytes`` is
    force-flushed as a decoded chunk.

    This protects the bounded-memory contract for malformed input or
    extremely large logical lines that contain no newline byte.
    """
    raw = make_bytes_stream(b"abcdefghij")
    reader = BufferedLineReader(
        raw,
        LineStreamerConfig(
            chunk_size=4,
            max_line_bytes=5,
        ),
    )

    assert list(reader.iter_lines()) == ["abcdefgh", "ij"]


def test_iter_lines_does_not_strip_carriage_return_on_forced_flush(
    make_bytes_stream,
) -> None:
    """
    Ensure forced oversized flushes do not strip a trailing carriage
    return.

    A force-flushed chunk may represent only part of a logical line, so
    removing ``\\r`` in that path would risk corrupting the streamed data.
    """
    raw = make_bytes_stream(b"abcde\r")
    reader = BufferedLineReader(
        raw,
        LineStreamerConfig(
            chunk_size=3,
            max_line_bytes=5,
        ),
    )

    assert list(reader.iter_lines()) == ["abcde\r"]


def test_iter_lines_strips_trailing_carriage_return_on_final_partial_line(
    make_bytes_stream,
) -> None:
    """
    Ensure the final partial line has a trailing carriage return removed
    when the stream ends without a newline byte.

    This preserves CRLF normalization even for files whose last line ends
    in ``\\r`` but not ``\\n``.
    """
    raw = make_bytes_stream(b"alpha\r")
    reader = BufferedLineReader(raw, LineStreamerConfig(chunk_size=8))

    assert list(reader.iter_lines()) == ["alpha"]


def test_iter_lines_returns_empty_result_for_empty_stream(make_bytes_stream) -> None:
    """
    Ensure an empty binary stream yields no lines.
    """
    raw = make_bytes_stream(b"")
    reader = BufferedLineReader(raw, LineStreamerConfig(chunk_size=4))

    assert list(reader.iter_lines()) == []