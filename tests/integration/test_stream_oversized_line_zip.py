"""
tests.integration.test_stream_oversized_line_zip
================================================

Integration tests for oversized-line handling in streamed ZIP members.

Overview
--------
This module verifies the package's bounded-memory behavior when streaming
archive members that contain extremely long logical lines without newline
delimiters.

This is one of the most important end-to-end guarantees in the library.
The public streaming contract promises that internal buffering remains
bounded and that oversized unterminated content is force-flushed as a
decoded chunk once the configured byte threshold is exceeded.

Behavior under test
-------------------
The integration cases ensure that the package:

- force-flushes oversized unterminated buffers through the public API
- continues streaming remaining content after a forced flush
- does not require newline delimiters to make forward progress
- preserves final partial content after one or more forced flushes
- does not strip a trailing carriage return during forced oversized flush

Test philosophy
---------------
These are true integration tests. They exercise the full runtime path
using real ZIP archives and the public ``LineStreamer`` API rather than
testing only the lower-level buffered reader in isolation.
"""

from __future__ import annotations

from ziplogstream import LineStreamer, LineStreamerConfig


def test_stream_oversized_unterminated_line_is_force_flushed_end_to_end(
    make_text_zip,
) -> None:
    """
    Ensure an oversized logical line without any newline delimiter is
    force-flushed as decoded chunks through the full public pipeline.

    With ``chunk_size=4`` and ``max_line_bytes=5``, the internal buffer
    grows from 4 bytes after the first read to 8 bytes after the second
    read. At that point it exceeds the configured maximum and is emitted
    as a chunk, after which streaming continues with the remaining bytes.
    """
    zip_path = make_text_zip(
        "oversized_single_line.zip",
        {
            "logs/app.log": "abcdefghij",
        },
    )

    streamer = LineStreamer(
        zip_path,
        "app.log",
        config=LineStreamerConfig(
            chunk_size=4,
            max_line_bytes=5,
        ),
    )

    assert list(streamer.stream()) == ["abcdefgh", "ij"]


def test_stream_oversized_content_can_flush_multiple_times_end_to_end(
    make_text_zip,
) -> None:
    """
    Ensure very long unterminated content can trigger multiple forced
    flushes during a single stream.

    This protects the forward-progress behavior of the public API for
    exceptionally large newline-free inputs.
    """
    zip_path = make_text_zip(
        "oversized_multiple_flushes.zip",
        {
            "logs/app.log": "abcdefghijklmnopqr",
        },
    )

    streamer = LineStreamer(
        zip_path,
        "app.log",
        config=LineStreamerConfig(
            chunk_size=4,
            max_line_bytes=5,
        ),
    )

    assert list(streamer.stream()) == ["abcdefgh", "ijklmnop", "qr"]


def test_stream_oversized_buffer_preserves_remaining_final_partial_content(
    make_text_zip,
) -> None:
    """
    Ensure any residual content remaining after a forced flush is still
    emitted at end-of-stream.

    This verifies that the final partial chunk is not lost after the
    oversized-buffer protection path is exercised.
    """
    zip_path = make_text_zip(
        "oversized_with_tail.zip",
        {
            "logs/app.log": "abcdefghijk",
        },
    )

    streamer = LineStreamer(
        zip_path,
        "app.log",
        config=LineStreamerConfig(
            chunk_size=4,
            max_line_bytes=5,
        ),
    )

    assert list(streamer.stream()) == ["abcdefgh", "ijk"]


def test_stream_oversized_forced_flush_does_not_strip_trailing_carriage_return(
    make_text_zip,
) -> None:
    """
    Ensure a forced oversized flush does not strip a trailing carriage
    return from the emitted chunk.

    During force-flush, the buffered data may represent only part of a
    logical line, so CR removal would be unsafe and could corrupt the
    streamed content.
    """
    zip_path = make_text_zip(
        "oversized_trailing_cr.zip",
        {
            "logs/app.log": "abcde\r",
        },
    )

    streamer = LineStreamer(
        zip_path,
        "app.log",
        config=LineStreamerConfig(
            chunk_size=3,
            max_line_bytes=5,
        ),
    )

    assert list(streamer.stream()) == ["abcde\r"]