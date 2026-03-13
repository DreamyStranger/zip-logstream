"""
tests.integration.test_stream_crlf_zip
======================================

Integration tests for CRLF line-ending handling in streamed ZIP members.

Overview
--------
This module verifies that the full streaming pipeline correctly handles
files that use Windows-style CRLF line endings (``\\r\\n``).

Although CRLF normalization is implemented in the lower-level streaming
components, these tests ensure that the behavior remains correct when
executed through the public ``LineStreamer`` API against real ZIP
archives.

Behavior under test
-------------------
The integration cases ensure that the package:

- strips the trailing ``\\r`` when lines end with ``\\r\\n``
- emits decoded lines without newline characters
- preserves correct behavior across chunk boundaries
- handles a final CRLF-terminated line correctly

Test philosophy
---------------
These tests validate the behavior of CRLF normalization across the
complete runtime pipeline rather than testing the helper functions
directly.
"""

from __future__ import annotations

from ziplogstream import LineStreamer, LineStreamerConfig


def test_stream_crlf_lines_are_normalized_end_to_end(make_text_zip) -> None:
    """
    Ensure CRLF-terminated lines are emitted without the trailing
    carriage return.

    This verifies the most common Windows-style line-ending case.
    """
    zip_path = make_text_zip(
        "crlf_lines.zip",
        {
            "logs/app.log": "alpha\r\nbeta\r\ngamma\r\n",
        },
    )

    streamer = LineStreamer(zip_path, "app.log")

    assert list(streamer.stream()) == ["alpha", "beta", "gamma"]


def test_stream_crlf_lines_across_small_chunks(make_text_zip) -> None:
    """
    Ensure CRLF normalization still works when chunk boundaries split the
    line-ending sequence across multiple reads.

    This protects a subtle edge case where ``\\r`` and ``\\n`` might appear
    in separate chunk reads.
    """
    zip_path = make_text_zip(
        "crlf_chunk_split.zip",
        {
            "logs/app.log": "alpha\r\nbeta\r\ngamma\r\n",
        },
    )

    streamer = LineStreamer(
        zip_path,
        "app.log",
        config=LineStreamerConfig(chunk_size=2),
    )

    assert list(streamer.stream()) == ["alpha", "beta", "gamma"]


def test_stream_crlf_final_line_without_trailing_newline(make_text_zip) -> None:
    """
    Ensure a final CRLF-style line that ends only with ``\\r`` (without
    ``\\n``) is normalized correctly.

    Some real-world log files end with ``\\r`` when truncated or partially
    written.
    """
    zip_path = make_text_zip(
        "crlf_partial_final_line.zip",
        {
            "logs/app.log": "alpha\r\nbeta\r\ngamma\r",
        },
    )

    streamer = LineStreamer(zip_path, "app.log")

    assert list(streamer.stream()) == ["alpha", "beta", "gamma"]