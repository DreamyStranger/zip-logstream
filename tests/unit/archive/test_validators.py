"""
tests.unit.archive.test_validators
==========================

Unit tests for :mod:`zipstreamer.archive.validators`.

Overview
--------
This module verifies the behavior of the archive path validation helpers
used by the high-level streaming API.

The validator layer is intentionally small, but it is important because it
forms the first line of defense against invalid user input. These tests
ensure that path normalization and validation remain explicit, predictable,
and package-specific.

Behavior under test
-------------------
The validator helpers are expected to:

- normalize supported path inputs into ``pathlib.Path`` objects
- reject unsupported input types
- reject empty string paths
- raise ``FileNotFoundError`` for missing archive paths
- raise ``ZipValidationError`` for non-file paths
- raise ``ZipValidationError`` for non-``.zip`` suffixes
- accept existing ZIP files

Test philosophy
---------------
These are focused unit tests. They validate only path normalization and
path-level archive checks. They do not test ZIP member resolution or the
streaming pipeline.
"""

from __future__ import annotations

import zipfile
from pathlib import Path

import pytest

from ziplogstream import ZipValidationError
from ziplogstream.archive import normalize_zip_path, validate_zip_path


def test_normalize_zip_path_accepts_path_instance(tmp_path: Path) -> None:
    """
    Ensure an existing ``Path`` input is returned unchanged.

    This protects the expected behavior for callers that already work with
    ``pathlib.Path`` values and do not need additional conversion.
    """
    path = tmp_path / "file.zip"

    assert normalize_zip_path(path) == path


def test_normalize_zip_path_accepts_non_empty_string(tmp_path: Path) -> None:
    """
    Ensure a non-empty string path is converted into a ``Path`` object.

    This is the most common convenience path for callers constructing
    streamers from string-based configuration or CLI inputs.
    """
    path = tmp_path / "file.zip"

    assert normalize_zip_path(str(path)) == path


def test_normalize_zip_path_rejects_empty_string() -> None:
    """
    Ensure an empty string is rejected as an invalid archive path.

    Path validation should fail early and clearly rather than attempting to
    interpret an empty value later in the pipeline.
    """
    with pytest.raises(ZipValidationError, match="ZIP path must be a non-empty string"):
        normalize_zip_path("")


def test_normalize_zip_path_rejects_unsupported_input_type() -> None:
    """
    Ensure unsupported path input types raise ``ZipValidationError``.

    The public API accepts only ``str`` and ``Path`` values, so other types
    should be rejected immediately.
    """
    with pytest.raises(
        ZipValidationError,
        match="ZIP path must be a pathlib.Path or string",
    ):
        normalize_zip_path(123)  # type: ignore[arg-type]


def test_validate_zip_path_raises_for_missing_archive(tmp_path: Path) -> None:
    """
    Ensure a missing archive path raises ``FileNotFoundError``.

    Missing files are distinct from structurally invalid paths, so the test
    preserves that separation in the public error contract.
    """
    missing = tmp_path / "missing.zip"

    with pytest.raises(FileNotFoundError, match="ZIP not found"):
        validate_zip_path(missing)


def test_validate_zip_path_raises_for_directory_input(tmp_path: Path) -> None:
    """
    Ensure directory paths are rejected.

    The validator should require an actual file path, not merely an
    existing filesystem entry.
    """
    with pytest.raises(ZipValidationError, match="ZIP path is not a file"):
        validate_zip_path(tmp_path)


def test_validate_zip_path_raises_for_non_zip_suffix(tmp_path: Path) -> None:
    """
    Ensure files without a ``.zip`` suffix are rejected.

    This preserves the package's explicit ZIP-only contract at the archive
    validation layer.
    """
    path = tmp_path / "file.txt"
    path.write_text("hello", encoding="utf-8")

    with pytest.raises(ZipValidationError, match="Expected a '.zip' archive"):
        validate_zip_path(path)


def test_validate_zip_path_accepts_existing_zip_file(tmp_path: Path) -> None:
    """
    Ensure an existing ZIP file passes validation.

    This verifies the happy-path case expected before the archive is opened
    for member resolution and streaming.
    """
    path = tmp_path / "valid.zip"

    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("app.log", b"hello\n")

    validate_zip_path(path)