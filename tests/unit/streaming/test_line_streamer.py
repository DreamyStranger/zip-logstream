"""
tests.unit.streaming.test_line_streamer
=============================

Unit tests for :mod:`zipstreamer.streaming.line_streamer`.

Overview
--------
This module verifies the behavior of the high-level
:class:`zipstreamer.streaming.line_streamer.LineStreamer` API.

Unlike lower-level tests for the streaming engine or resolver logic,
these tests focus on the orchestration responsibilities of the
``LineStreamer`` class:

- validating archive paths
- resolving the correct archive member
- streaming decoded lines from that member
- honoring custom configuration and resolver behavior

Behavior under test
-------------------
The ``LineStreamer`` class is expected to:

- stream lines from a resolved ZIP archive member
- accept both ``str`` and ``Path`` archive inputs
- respect configuration options such as chunk size
- allow caller-provided resolver functions
- raise explicit exceptions when resolution fails

Test philosophy
---------------
These tests operate on small temporary ZIP archives created during test
execution. They validate the high-level orchestration logic without
duplicating the detailed behavior already covered by lower-level unit
tests.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ziplogstream import LineStreamer, LineStreamerConfig
from ziplogstream.errors import ZipMemberNotFoundError


def test_streamer_streams_lines_from_zip_member(make_text_zip) -> None:
    """
    Ensure the streamer yields decoded lines from the resolved archive
    member.
    """
    zip_path = make_text_zip(
        "basic.zip",
        {
            "logs/app.log": "alpha\nbeta\ngamma\n",
        },
    )

    streamer = LineStreamer(zip_path, "app.log")

    assert list(streamer.stream()) == ["alpha", "beta", "gamma"]


def test_streamer_accepts_string_archive_path(make_text_zip) -> None:
    """
    Ensure a string path can be provided instead of a ``Path`` instance.
    """
    zip_path = make_text_zip(
        "string_path.zip",
        {
            "logs/app.log": "hello\nworld\n",
        },
    )

    streamer = LineStreamer(str(zip_path), "app.log")

    assert list(streamer.stream()) == ["hello", "world"]


def test_streamer_respects_custom_configuration(make_text_zip) -> None:
    """
    Ensure caller-provided configuration values are honored by the
    streaming pipeline.
    """
    zip_path = make_text_zip(
        "custom_config.zip",
        {
            "logs/app.log": "abcdef\n123456\nxyz\n",
        },
    )

    cfg = LineStreamerConfig(chunk_size=2)

    streamer = LineStreamer(zip_path, "app.log", config=cfg)

    assert list(streamer.stream()) == ["abcdef", "123456", "xyz"]


def test_streamer_uses_custom_resolver(make_text_zip) -> None:
    """
    Ensure a caller-provided resolver is used instead of the default
    resolution strategy.
    """
    zip_path = make_text_zip(
        "custom_resolver.zip",
        {
            "logs/service.log": "alpha\nbeta\n",
        },
    )

    def resolver(zf, target: str) -> str:
        return "logs/service.log"

    streamer = LineStreamer(zip_path, "ignored.log", resolver=resolver)

    assert list(streamer.stream()) == ["alpha", "beta"]


def test_streamer_raises_when_member_not_found(make_text_zip) -> None:
    """
    Ensure resolution failure propagates as a package-specific exception.
    """
    zip_path = make_text_zip(
        "missing_member.zip",
        {
            "logs/app.log": "alpha\n",
        },
    )

    streamer = LineStreamer(zip_path, "missing.log")

    with pytest.raises(ZipMemberNotFoundError):
        list(streamer.stream())


def test_streamer_supports_path_objects(make_text_zip) -> None:
    """
    Ensure ``Path`` inputs behave identically to string inputs.
    """
    zip_path = make_text_zip(
        "path_object.zip",
        {
            "logs/app.log": "alpha\nbeta\n",
        },
    )

    streamer = LineStreamer(Path(zip_path), "app.log")

    assert list(streamer.stream()) == ["alpha", "beta"]
