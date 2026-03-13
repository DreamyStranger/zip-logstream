"""
tests.unit.archive.test_openers
===============================

Unit tests for :mod:`zipstreamer.archive.openers`.

Overview
--------
This module verifies the small archive orchestration helper that combines
ZIP path normalization, archive validation, and member resolution.

Although the helper is intentionally lightweight, it is still part of the
package's archive layer and deserves direct tests so that its behavior
remains explicit and stable.

Behavior under test
-------------------
The opener helper is expected to:

- normalize supported archive path inputs
- validate archive paths before opening
- resolve the exact target member name using the provided resolver
- return the normalized path and resolved member name
- propagate validation and resolution failures unchanged

Test philosophy
---------------
These are focused unit tests. They exercise archive-level orchestration
without involving the higher-level streaming API.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ziplogstream import ZipMemberNotFoundError, ZipValidationError
from ziplogstream.archive import default_zip_member_resolver, resolve_zip_member_name


def test_resolve_zip_member_name_returns_normalized_path_and_member_name(
    make_text_zip,
) -> None:
    """
    Ensure the helper returns both the normalized archive path and the
    resolved member name.
    """
    zip_path = make_text_zip(
        "resolve.zip",
        {
            "logs/app.log": "alpha\nbeta\n",
        },
    )

    normalized_path, member_name = resolve_zip_member_name(
        zip_path,
        "app.log",
        resolver=default_zip_member_resolver,
    )

    assert normalized_path == Path(zip_path)
    assert member_name == "logs/app.log"


def test_resolve_zip_member_name_accepts_string_path(make_text_zip) -> None:
    """
    Ensure string archive paths are accepted and normalized correctly.
    """
    zip_path = make_text_zip(
        "resolve_string.zip",
        {
            "logs/app.log": "alpha\n",
        },
    )

    normalized_path, member_name = resolve_zip_member_name(
        str(zip_path),
        "app.log",
        resolver=default_zip_member_resolver,
    )

    assert normalized_path == Path(zip_path)
    assert member_name == "logs/app.log"


def test_resolve_zip_member_name_uses_custom_resolver(make_text_zip) -> None:
    """
    Ensure the caller-provided resolver is used for member selection.
    """
    zip_path = make_text_zip(
        "custom_resolver.zip",
        {
            "logs/app.log": "alpha\n",
            "logs/other.log": "beta\n",
        },
    )

    def resolver(zf, target: str) -> str:
        return "logs/other.log"

    normalized_path, member_name = resolve_zip_member_name(
        zip_path,
        "ignored.log",
        resolver=resolver,
    )

    assert normalized_path == Path(zip_path)
    assert member_name == "logs/other.log"


def test_resolve_zip_member_name_raises_for_missing_archive(tmp_path: Path) -> None:
    """
    Ensure archive validation failures propagate unchanged.
    """
    missing = tmp_path / "missing.zip"

    with pytest.raises(FileNotFoundError, match="ZIP not found"):
        resolve_zip_member_name(
            missing,
            "app.log",
            resolver=default_zip_member_resolver,
        )


def test_resolve_zip_member_name_raises_for_invalid_archive_suffix(
    tmp_path: Path,
) -> None:
    """
    Ensure non-ZIP archive paths are rejected before resolution begins.
    """
    path = tmp_path / "not_a_zip.txt"
    path.write_text("hello", encoding="utf-8")

    with pytest.raises(ZipValidationError, match="Expected a '.zip' archive"):
        resolve_zip_member_name(
            path,
            "app.log",
            resolver=default_zip_member_resolver,
        )


def test_resolve_zip_member_name_raises_when_member_cannot_be_resolved(
    make_text_zip,
) -> None:
    """
    Ensure member resolution failures propagate unchanged.
    """
    zip_path = make_text_zip(
        "missing_member.zip",
        {
            "logs/app.log": "alpha\n",
        },
    )

    with pytest.raises(ZipMemberNotFoundError):
        resolve_zip_member_name(
            zip_path,
            "missing.log",
            resolver=default_zip_member_resolver,
        )