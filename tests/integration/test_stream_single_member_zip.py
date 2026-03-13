"""
tests.integration.test_stream_single_member_zip
===============================================

Integration tests for basic end-to-end ZIP member streaming.

Overview
--------
This module verifies the primary happy-path behavior of the package:
streaming decoded text lines from a single file stored inside a ZIP
archive.

Unlike the unit tests, these checks exercise the full runtime pipeline:

- archive path normalization and validation
- ZIP archive opening
- target member resolution
- buffered binary reads
- byte-to-text decoding
- line emission through the public ``LineStreamer`` API

Behavior under test
-------------------
The integration cases in this module ensure that the library can:

- stream lines from a ZIP containing exactly one matching member
- resolve a nested member by basename using the default resolver
- preserve line ordering across the full end-to-end pipeline
- emit the final partial line when the file does not end with a newline

Test philosophy
---------------
These are true integration tests. They use real temporary ZIP archives
and validate the full public streaming flow rather than isolated helper
functions.
"""

from __future__ import annotations

from ziplogstream import LineStreamer


def test_stream_single_member_archive_end_to_end(make_text_zip) -> None:
    """
    Ensure the public streamer can read and emit lines from a ZIP archive
    containing a single matching target member.

    This is the most fundamental end-to-end success case for the package.
    """
    zip_path = make_text_zip(
        "single_member.zip",
        {
            "logs/app.log": "alpha\nbeta\ngamma\n",
        },
    )

    streamer = LineStreamer(zip_path, "app.log")

    assert list(streamer.stream()) == ["alpha", "beta", "gamma"]


def test_stream_nested_member_resolves_by_basename_end_to_end(make_text_zip) -> None:
    """
    Ensure the default resolver can locate a nested archive member by
    basename and stream it successfully.

    This protects the documented basename-preference behavior through the
    full public API rather than only at the resolver unit-test level.
    """
    zip_path = make_text_zip(
        "nested_member.zip",
        {
            "deeply/nested/logs/app.log": "line-1\nline-2\n",
        },
    )

    streamer = LineStreamer(zip_path, "app.log")

    assert list(streamer.stream()) == ["line-1", "line-2"]


def test_stream_single_member_preserves_line_order_end_to_end(make_text_zip) -> None:
    """
    Ensure lines are yielded in the same order they appear in the archive
    member.

    This confirms that the end-to-end pipeline behaves as a true streaming
    iterator and does not reorder content.
    """
    zip_path = make_text_zip(
        "ordered_lines.zip",
        {
            "logs/app.log": "first\nsecond\nthird\nfourth\n",
        },
    )

    streamer = LineStreamer(zip_path, "app.log")

    assert list(streamer.stream()) == ["first", "second", "third", "fourth"]


def test_stream_single_member_emits_final_partial_line_end_to_end(
    make_text_zip,
) -> None:
    """
    Ensure the final line is emitted even when the target member does not
    end with a trailing newline.

    This verifies the public streaming contract for a common real-world
    edge case in log-like files.
    """
    zip_path = make_text_zip(
        "final_partial_line.zip",
        {
            "logs/app.log": "alpha\nbeta\ngamma",
        },
    )

    streamer = LineStreamer(zip_path, "app.log")

    assert list(streamer.stream()) == ["alpha", "beta", "gamma"]