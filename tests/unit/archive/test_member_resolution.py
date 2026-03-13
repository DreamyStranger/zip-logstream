"""
tests.unit.archive.test_member_resolution
=================================

Unit tests for :mod:`zipstreamer.archive.member_resolution`.

Overview
--------
This module verifies the behavior of the package's default ZIP member
resolver, :func:`zipstreamer.archive.member_resolution.default_zip_member_resolver`.

The resolver is a core part of the library contract because it determines
which archive member is streamed when the caller provides a target string.
These tests ensure that the default resolution strategy remains stable,
deterministic, and explicit in failure cases.

Behavior under test
-------------------
The default resolver is expected to:

- prefer exact basename matches when the target does not contain ``/``
- fall back to suffix matching when no basename match exists
- raise a package-specific not-found error when nothing matches
- raise a package-specific ambiguity error when multiple matches exist
- reject empty target selectors

Test philosophy
---------------
These are focused unit tests. They exercise member selection logic against
small temporary ZIP archives and do not test the higher-level streaming
pipeline.
"""

from __future__ import annotations

import zipfile

import pytest

from ziplogstream import ZipMemberAmbiguityError, ZipMemberNotFoundError
from ziplogstream.archive import default_zip_member_resolver


def test_resolver_prefers_exact_basename_match_when_target_is_plain_filename(
    make_zip,
) -> None:
    """
    Ensure basename matching is preferred when the target contains no path
    separator.

    If the caller provides a plain filename such as ``"app.log"``, the
    resolver should first search for members whose basename is exactly that
    value, rather than immediately applying broader suffix matching.
    """
    zip_path = make_zip(
        "basename_match.zip",
        {
            "logs/app.log": b"a\n",
            "logs/app.log.1": b"b\n",
        },
    )

    with zipfile.ZipFile(zip_path, "r") as zf:
        resolved = default_zip_member_resolver(zf, "app.log")

    assert resolved == "logs/app.log"


def test_resolver_falls_back_to_suffix_match_when_no_basename_match_exists(
    make_zip,
) -> None:
    """
    Ensure suffix matching is used when basename matching does not produce
    a result.

    This protects the documented fallback behavior that allows callers to
    specify nested suffixes such as ``"service/app.log"``.
    """
    zip_path = make_zip(
        "suffix_match.zip",
        {
            "nested/path/service/app.log": b"a\n",
        },
    )

    with zipfile.ZipFile(zip_path, "r") as zf:
        resolved = default_zip_member_resolver(zf, "service/app.log")

    assert resolved == "nested/path/service/app.log"


def test_resolver_raises_not_found_when_no_member_matches_target(make_zip) -> None:
    """
    Ensure a missing target raises ``ZipMemberNotFoundError``.

    The error should be explicit rather than silently returning an arbitrary
    member or falling back to unrelated archive contents.
    """
    zip_path = make_zip(
        "not_found.zip",
        {
            "logs/app.log": b"a\n",
        },
    )

    with zipfile.ZipFile(zip_path, "r") as zf:
        with pytest.raises(ZipMemberNotFoundError, match="missing.log"):
            default_zip_member_resolver(zf, "missing.log")


def test_resolver_raises_ambiguity_when_multiple_basename_matches_exist(
    make_zip,
) -> None:
    """
    Ensure duplicate basename matches are treated as an error.

    When multiple archive members share the same basename, the resolver
    must fail explicitly rather than guessing which file the caller meant.
    """
    zip_path = make_zip(
        "ambiguous_basename.zip",
        {
            "a/app.log": b"a\n",
            "b/app.log": b"b\n",
        },
    )

    with zipfile.ZipFile(zip_path, "r") as zf:
        with pytest.raises(ZipMemberAmbiguityError, match="Ambiguous target"):
            default_zip_member_resolver(zf, "app.log")


def test_resolver_raises_ambiguity_when_multiple_suffix_matches_exist(
    make_zip,
) -> None:
    """
    Ensure duplicate suffix matches are treated as an error.

    This verifies that the fallback suffix-based resolution path remains
    deterministic by failing on ambiguity instead of choosing one match
    implicitly.
    """
    zip_path = make_zip(
        "ambiguous_suffix.zip",
        {
            "x/service/app.log": b"a\n",
            "y/service/app.log": b"b\n",
        },
    )

    with zipfile.ZipFile(zip_path, "r") as zf:
        with pytest.raises(ZipMemberAmbiguityError, match="Ambiguous target"):
            default_zip_member_resolver(zf, "service/app.log")


def test_resolver_rejects_empty_target_selector(make_zip) -> None:
    """
    Ensure an empty target selector is rejected.

    An empty string is not a meaningful archive member selector and should
    fail immediately with a package-specific error.
    """
    zip_path = make_zip(
        "empty_target.zip",
        {
            "logs/app.log": b"a\n",
        },
    )

    with zipfile.ZipFile(zip_path, "r") as zf:
        with pytest.raises(
            ZipMemberNotFoundError,
            match="Target member selector must be a non-empty string",
        ):
            default_zip_member_resolver(zf, "")