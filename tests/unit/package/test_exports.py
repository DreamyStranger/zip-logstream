"""
tests.unit.package.test_exports
===============================

Unit tests for the public package export surface.
"""

from __future__ import annotations

import ziplogstream


def test_top_level_exports_are_available() -> None:
    """
    Ensure the documented top-level public API is importable from the
    package root.
    """
    assert hasattr(ziplogstream, "__version__")
    assert hasattr(ziplogstream, "LineStreamer")
    assert hasattr(ziplogstream, "LineStreamerConfig")
    assert hasattr(ziplogstream, "default_zip_member_resolver")
    assert hasattr(ziplogstream, "ZipLogStreamError")
    assert hasattr(ziplogstream, "ConfigurationError")
    assert hasattr(ziplogstream, "ZipValidationError")
    assert hasattr(ziplogstream, "ZipMemberNotFoundError")
    assert hasattr(ziplogstream, "ZipMemberAmbiguityError")