"""
tests.integration.test_custom_resolver_zip
==========================================

Integration tests for custom ZIP member resolution.

Overview
--------
This module verifies that caller-provided resolver functions integrate
correctly with the public ``LineStreamer`` API when streaming real ZIP
archives.

The custom resolver hook is the primary extension point of the library.
These tests ensure that custom selection logic is honored end to end and
that resolver-level failures propagate through the public streaming
surface unchanged.

Behavior under test
-------------------
The integration cases ensure that the package:

- uses the caller-provided resolver instead of the default resolver
- streams the member selected by custom resolution logic
- allows custom resolvers to ignore the original target if desired
- propagates custom resolver failures through the public API

Test philosophy
---------------
These are true integration tests. They exercise the full runtime path
using real ZIP archives and the public ``LineStreamer`` API, while
swapping out only the member resolution policy.
"""

from __future__ import annotations

import zipfile

import pytest

from ziplogstream import LineStreamer
from ziplogstream.errors import ZipMemberNotFoundError


def test_stream_uses_custom_resolver_selected_member_end_to_end(
    make_text_zip,
) -> None:
    """
    Ensure a custom resolver can select a different archive member than
    the default resolver would normally choose.

    This verifies the main extension-point contract through the full
    public streaming pipeline.
    """
    zip_path = make_text_zip(
        "custom_resolver_selected_member.zip",
        {
            "logs/app.log": "default\nmember\n",
            "logs/service.log": "custom\nmember\n",
        },
    )

    def resolver(zf: zipfile.ZipFile, target: str) -> str:
        return "logs/service.log"

    streamer = LineStreamer(
        zip_path,
        "app.log",
        resolver=resolver,
    )

    assert list(streamer.stream()) == ["custom", "member"]


def test_stream_custom_resolver_may_ignore_original_target_end_to_end(
    make_text_zip,
) -> None:
    """
    Ensure a custom resolver may choose a member independently of the
    caller-provided target selector.

    This protects the documented flexibility of the resolver extension
    point.
    """
    zip_path = make_text_zip(
        "custom_resolver_ignores_target.zip",
        {
            "archive/first.log": "one\n",
            "archive/second.log": "two\nthree\n",
        },
    )

    def resolver(zf: zipfile.ZipFile, target: str) -> str:
        return "archive/second.log"

    streamer = LineStreamer(
        zip_path,
        "not_used_by_resolver.log",
        resolver=resolver,
    )

    assert list(streamer.stream()) == ["two", "three"]


def test_stream_custom_resolver_can_choose_member_by_archive_metadata(
    make_text_zip,
) -> None:
    """
    Ensure a custom resolver can inspect archive contents and choose a
    member programmatically.

    This demonstrates a realistic extension pattern where the resolver
    derives its result from the available member names.
    """
    zip_path = make_text_zip(
        "custom_resolver_metadata.zip",
        {
            "logs/2026-03-10-app.log": "older\n",
            "logs/2026-03-11-app.log": "newer\nentry\n",
        },
    )

    def resolver(zf: zipfile.ZipFile, target: str) -> str:
        candidates = [name for name in zf.namelist() if name.endswith("-app.log")]
        return sorted(candidates)[-1]

    streamer = LineStreamer(
        zip_path,
        "ignored.log",
        resolver=resolver,
    )

    assert list(streamer.stream()) == ["newer", "entry"]


def test_stream_propagates_custom_resolver_failure_end_to_end(
    make_text_zip,
) -> None:
    """
    Ensure exceptions raised by a custom resolver propagate through the
    public API unchanged.

    This confirms that resolver policy remains caller-controlled and that
    the streaming layer does not mask resolution failures.
    """
    zip_path = make_text_zip(
        "custom_resolver_failure.zip",
        {
            "logs/app.log": "alpha\n",
        },
    )

    def resolver(zf: zipfile.ZipFile, target: str) -> str:
        raise ZipMemberNotFoundError(f"Custom resolver could not resolve: {target}")

    streamer = LineStreamer(
        zip_path,
        "missing.log",
        resolver=resolver,
    )

    with pytest.raises(
        ZipMemberNotFoundError,
        match="Custom resolver could not resolve: missing.log",
    ):
        list(streamer.stream())