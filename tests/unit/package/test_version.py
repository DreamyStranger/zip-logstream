"""
tests.unit.package.test_version
===============================

Unit tests for :mod:`zipstreamer.version`.
"""

from __future__ import annotations

from ziplogstream.version import __version__


def test_version_is_non_empty_string() -> None:
    """
    Ensure package version metadata is exposed as a non-empty string.
    """
    assert isinstance(__version__, str)
    assert __version__